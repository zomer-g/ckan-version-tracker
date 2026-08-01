"""The semantic layer — a declared, validated model that free-text questions compile against.

WHY THIS EXISTS (and why it is not raw text-to-SQL):

The /data console spans ~440 tables and ~4,000 columns. Two published results
decide the design:

  * Schema linking collapses at this scale. On the large Spider 2.0 subsets,
    schema-linking recall measured 0% without a retrieval step — the raw catalog
    does not fit a prompt, and a model handed a truncated slice writes confident
    SQL against tables it cannot see.
  * A declared semantic layer beats raw SQL generation by a wide margin on the
    same questions (published A/Bs land around 90%→98% and 84%→100%), and —
    the part that actually matters here — it fails DIFFERENTLY. Raw text-to-SQL
    fails as a plausible wrong number. A semantic layer fails as "I can't answer
    that", because a field the model never declared simply does not validate.

On a transparency site a confidently wrong number is the one unacceptable
outcome, so this module never lets a language model emit SQL. The model emits a
small JSON query; ``validate_query`` checks every entity/dimension/measure/field
against the declared model and raises on anything unknown; ``compile_sql`` is
plain deterministic string building over already-validated identifiers. The SQL
that comes out still goes through append_store.validate_readonly_sql +
run_readonly_sql (READ ONLY tx, statement_timeout, row cap, least-privilege
role), so the LLM is inside three independent guards, not one.

WHERE THE MODEL COMES FROM: it is DERIVED, not hand-written. The profiler
(app/services/table_profiler.py) already computed, per table, each column's
detected kind, fill rate, distinct estimate, entity guess and — critically —
its ``top_values``. Those top values are the value-linking payload: they let a
Hebrew question mentioning "תל אביב-יפו" or "מאושר" match a real cell rather
than a guessed one, which is exactly the failure class that costs non-English
text-to-SQL its measured ~6 points. Hand-authoring 40 entities would be a week
of work that goes stale on the next poll; deriving them from artifacts we
already maintain does not. ``_OVERLAY`` is where curation goes on top.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.services import append_store, data_catalog

logger = logging.getLogger(__name__)


class SemanticError(ValueError):
    """A query that does not validate against the declared model.

    Deliberately distinct from a SQL error: this is the "מחוץ לתחום" path, and
    the caller turns it into an honest refusal rather than a result set."""


# ── curation overlay ─────────────────────────────────────────────────────────
# The derived model is the floor, not the ceiling. Anything here overrides it
# per table: a better Hebrew title, extra synonyms people actually type, or
# columns to hide from the model (internal ids, hashes) so the LLM never picks
# them. Keep it small and additive — the point of deriving is not having to
# maintain 440 entries by hand.
_OVERLAY: dict[str, dict[str, Any]] = {
    # "append_xxx": {"title": "...", "synonyms": ["..."], "hide": ["col"]},
}

# Columns no question ever means, and that would only give the model a way to
# produce a meaningless grouping. Matched case-insensitively against the name.
_HIDDEN_COL_RE = re.compile(
    r"^(row_hash|first_seen|_id|_full_text|geom|geometry_wkt|.*_hash)$", re.IGNORECASE
)

# A dimension with (almost) as many distinct values as rows is an identifier,
# not a category — grouping by it yields one bar per row, which is never what
# "לפי X" meant. Still allowed as a FILTER field, just not offered as a grouping.
_GROUPABLE_MAX_DISTINCT_RATIO = 0.6
_GROUPABLE_MAX_DISTINCT = 5000

# How many sample values per dimension go into the prompt. Enough for the model
# to match a Hebrew surface form against a real cell; small enough that ~8
# entities still fit comfortably in a cached prefix.
_SAMPLES_IN_PROMPT = 8


# ── Hebrew-aware normalization (shared by retrieval and template matching) ────
# Hebrew questions glue single-letter clitics onto content words ("בתל אביב",
# "לפי מחוז", "שנת"), so naive token equality misses the match that a human
# reads instantly. This is the cheap half of the fix; the expensive half —
# resolving a locality surface form to its canonical code — is already done by
# over_settlement_aliases and happens in SQL, not here.
_HEB_PREFIXES = "בלמושכה"
_TOKEN_RE = re.compile(r"[0-9A-Za-z֐-׿]+")


def norm_token(t: str) -> str:
    """Lowercase and strip Hebrew niqqud / geresh / quotes. No clitic handling —
    see ``tokens`` for why that cannot be decided one token at a time."""
    return re.sub(r"[֑-ׇ׳״'\"]", "", (t or "").lower())


def tokens(s: str) -> list[str]:
    """Tokenize for matching, emitting BOTH readings of a possible clitic.

    "בתל" should match the stored value "תל אביב-יפו", so the ב has to come off.
    "בית" must not become "ית". The two are the same length and the same shape,
    so no rule on a single token can separate them — a length threshold picks one
    to get wrong. Emitting both forms lets the surrounding context decide: the
    correct reading finds a match in the model, the incorrect one matches
    nothing and costs a set element. Matching is set-overlap throughout, so the
    extra token is inert wherever it is wrong."""
    out: list[str] = []
    for raw in _TOKEN_RE.findall(s or ""):
        n = norm_token(raw)
        if len(n) < 2:
            continue
        out.append(n)
        if len(n) >= 3 and n[0] in _HEB_PREFIXES:
            out.append(n[1:])
    return out


# ── model construction ───────────────────────────────────────────────────────

def _kind_of(col_type: str, profile_kind: str | None) -> str:
    """Collapse CKAN-ish and Postgres type names into the three kinds the model
    speaks. The profiler's detected kind wins when present — it saw the values,
    which is how a text column holding "2019" is known to be a date."""
    if profile_kind in ("numeric", "date"):
        return "number" if profile_kind == "numeric" else "date"
    s = (col_type or "").lower()
    if "timestamp" in s or s in ("date", "datetime"):
        return "date"
    if any(s == n or s.startswith(n) for n in
           ("int", "bigint", "smallint", "numeric", "decimal", "real", "double", "float", "number")):
        return "number"
    return "text"


def _groupable(kind: str, profile_col: dict, ratio: float, distinct: int) -> bool:
    """May this dimension be used as a GROUP BY key?

    The two thresholds are CEILINGS, both of which must hold — not alternatives.
    That distinction is load-bearing: a column with 4,995 distinct values in a
    5,000-row table is an identifier, and letting the absolute-count ceiling
    rescue it (because 4,995 < 5,000) offers "לפי מזהה" as a grouping that
    returns one row per record. Conversely a 2%-distinct column in a 4M-row
    table passes the ratio test but still means 80,000 groups, which is not a
    readable answer either. So: near-unique is out, and enormous is out.

    Numbers and dates are always groupable — they are bucketed, not enumerated.
    An unprofiled column is allowed, deliberately: profiling is a background
    job, and refusing here would make every freshly-tracked dataset unusable
    until it catches up. The cost of being wrong is a wide result, not a wrong
    number, and the row cap bounds it.
    """
    if kind != "text":
        return True
    if not profile_col:
        return True
    if ratio and ratio > _GROUPABLE_MAX_DISTINCT_RATIO:
        return False
    if distinct and distinct > _GROUPABLE_MAX_DISTINCT:
        return False
    return True


def _entity_from(rec: dict, profile: dict | None) -> dict | None:
    """One catalog row + its profile → one declared entity, or None if the table
    has nothing a question could ask about."""
    table = rec["table"]
    over = _OVERLAY.get(table, {})
    hide = {h.lower() for h in over.get("hide", [])}
    pcols: dict[str, dict] = ((profile or {}).get("sql_profile") or {}).get("columns") or {}
    enrich_cols: dict[str, dict] = ((profile or {}).get("llm_enrichment") or {}).get("columns") or {}

    dimensions: list[dict] = []
    measures: list[dict] = [{"key": "count", "title": "מספר שורות"}]
    for c in rec.get("columns") or []:
        name = c["name"]
        if name.lower() in hide or _HIDDEN_COL_RE.match(name):
            continue
        p = pcols.get(name) or {}
        kind = _kind_of(c.get("type") or "", p.get("detected_kind"))
        ratio = float(p.get("distinct_ratio") or 0.0)
        distinct = int(p.get("distinct_est") or 0)
        entity_type = (enrich_cols.get(name, {}) or {}).get("semantic_type") \
            or (p.get("entity_guess") or {}).get("guess")
        dimensions.append({
            "key": name,
            "kind": kind,
            # A description from the LLM enrichment pass is worth more to the
            # model than the bare column name; fall back to the name itself.
            "title": (enrich_cols.get(name, {}) or {}).get("description_he") or name,
            "entity_type": entity_type,
            "samples": [t["value"] for t in (p.get("top_values") or [])][:_SAMPLES_IN_PROMPT],
            "min": p.get("min"),
            "max": p.get("max"),
            "groupable": _groupable(kind, p, ratio, distinct),
        })
        if kind == "number":
            for op, label in (("sum", "סכום"), ("avg", "ממוצע"), ("min", "מינימום"), ("max", "מקסימום")):
                measures.append({"key": f"{op}:{name}", "title": f"{label} {name}"})

    if not dimensions:
        return None

    # A locality/authority dimension is what makes cross-dataset enrichment
    # possible without an FK — over_settlement_code() heals the dirty surface
    # form to the canonical CBS code. This is the only join path the model
    # declares, and it is the one that actually exists in this corpus.
    geo_dims = [d["key"] for d in dimensions
                if d.get("entity_type") in ("locality", "municipality")]
    if not geo_dims and (rec.get("field_flags") or {}).get("has_locality"):
        geo_dims = [d["key"] for d in dimensions if d["kind"] == "text"][:1]

    kws = [k["token"] for k in (((profile or {}).get("sql_profile") or {}).get("keywords") or [])][:20]
    return {
        "key": table,
        "schema": rec.get("schema") or "public",
        "title": over.get("title") or rec.get("title") or table,
        "summary": (profile or {}).get("summary_he") or rec.get("description") or "",
        "rows": rec.get("est_rows"),
        "synonyms": list(over.get("synonyms", [])) + (rec.get("tags") or []) + kws,
        "dimensions": dimensions,
        "measures": measures,
        "geo_dims": geo_dims,
        "source_url": rec.get("source_url") or "",
        "page_url": rec.get("page_url") or "",
    }


# The enrichment fields a query may pull from the settlement index. Whitelisted
# on purpose: "enrich" is the one place the compiler emits a JOIN, so the set of
# columns it can reach is fixed here rather than taken from the model's input.
ENRICH_FIELDS: dict[str, str] = {
    "district": "מחוז",
    "subdistrict": "נפה",
    "municipal_status": "מעמד_מוניציפלי",
    "local_authority": "רשות_מקומית",
    "population": "אוכלוסייה",
    "name": "יישוב_רשמי",
}

_MODEL_TTL = 300.0
_model_cache: list[dict] | None = None
_model_cache_at = 0.0
_model_lock = asyncio.Lock()


def invalidate_model_cache() -> None:
    global _model_cache, _model_cache_at
    _model_cache, _model_cache_at = None, 0.0


async def _all_profiles() -> dict[tuple[str, str], dict]:
    """Every stored profile in one round-trip, keyed (schema, table).

    Per-table ``get_profile`` calls would be ~440 queries on a cold model
    build; the profile table is small enough to read whole."""
    try:
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT schema_name, table_name, summary_he, sql_profile, llm_enrichment "
                "FROM public.over_table_profiles"
            )
    except Exception:  # noqa: BLE001 — not profiled yet ⇒ a thinner but valid model
        logger.info("semantic_model: profiles unavailable; building from the catalog alone",
                    exc_info=True)
        return {}
    import json
    out: dict[tuple[str, str], dict] = {}
    for r in rows:
        def _j(v):
            return json.loads(v) if isinstance(v, str) else (v or {})
        out[(r["schema_name"], r["table_name"])] = {
            "summary_he": r["summary_he"],
            "sql_profile": _j(r["sql_profile"]),
            "llm_enrichment": _j(r["llm_enrichment"]),
        }
    return out


async def build_model(db: AsyncSession, *, use_cache: bool = True) -> list[dict]:
    """The declared model: one entity per queryable table that has dimensions.

    Cached process-locally on the same TTL as the catalog it is derived from.
    Callers must treat the result as READ-ONLY — it is the cached list itself."""
    global _model_cache, _model_cache_at
    if use_cache:
        cached, age = _model_cache, time.monotonic() - _model_cache_at
        if cached is not None and age < _MODEL_TTL:
            return cached
        async with _model_lock:
            cached, age = _model_cache, time.monotonic() - _model_cache_at
            if cached is not None and age < _MODEL_TTL:
                return cached
            built = await _build_uncached(db)
            _model_cache, _model_cache_at = built, time.monotonic()
            return built
    return await _build_uncached(db)


async def _build_uncached(db: AsyncSession) -> list[dict]:
    catalog = await data_catalog.build_catalog(db)
    profiles = await _all_profiles()
    out: list[dict] = []
    for rec in catalog:
        # OVER's own index tables are the JOIN target, not a question subject.
        if rec["table"].startswith("over_"):
            continue
        ent = _entity_from(rec, profiles.get((rec.get("schema") or "public", rec["table"])))
        if ent:
            out.append(ent)
    return out


# ── retrieval ────────────────────────────────────────────────────────────────

def score_entity(ent: dict, q_tokens: list[str]) -> float:
    """Lexical relevance of one entity to a tokenized question.

    Deliberately not embeddings. A vector index is another service to run and
    keep fresh, and the failure it fixes (paraphrase) is not the failure this
    corpus has — the miss here is lexical/morphological, which normalized token
    overlap addresses directly. Weighting reflects how specific a match is:
    hitting a stored VALUE ("מאושר") is the strongest signal that a table is the
    right one, hitting a column name is next, the title after that."""
    if not q_tokens:
        return 0.0
    qs = set(q_tokens)
    score = 0.0
    title_t = set(tokens(ent["title"]))
    score += 3.0 * len(qs & title_t)
    score += 1.5 * len(qs & set(tokens(ent.get("summary") or "")))
    score += 1.0 * len(qs & {t for s in ent.get("synonyms", []) for t in tokens(s)})
    for d in ent["dimensions"]:
        dt = set(tokens(d["key"])) | set(tokens(d.get("title") or ""))
        score += 2.0 * len(qs & dt)
        for v in d.get("samples", []):
            if qs & set(tokens(v)):
                score += 2.5
                break
    # Tie-break toward tables people can actually get answers out of. log-ish,
    # so a 4M-row registry does not outrank a well-matched 300-row table.
    rows = ent.get("rows") or 0
    if rows and score:
        score += min(1.0, (len(str(int(rows))) - 2) * 0.15)
    return score


def retrieve(model: list[dict], question: str, k: int = 6) -> list[dict]:
    """Top-k entities for a question. This is the step whose absence measured
    0% schema-linking recall at this scale — never send the whole model."""
    q = tokens(question)
    scored = [(score_entity(e, q), e) for e in model]
    scored = [(s, e) for s, e in scored if s > 0]
    scored.sort(key=lambda p: -p[0])
    return [e for _, e in scored[:k]]


def model_prompt(entities: list[dict]) -> str:
    """The retrieved slice, rendered for the prompt.

    Compact on purpose: this text is the bulk of the input tokens on every call,
    and Hebrew costs roughly 2–3× English per word."""
    lines: list[str] = []
    for e in entities:
        head = f'### {e["key"]} — {e["title"]}'
        if e.get("rows"):
            head += f' (~{int(e["rows"]):,} שורות)'
        lines.append(head)
        if e.get("summary"):
            lines.append(f'   {e["summary"][:300]}')
        for d in e["dimensions"]:
            bits = [f'   - {d["key"]} ({d["kind"]}'
                    + (", ניתן לקיבוץ" if d.get("groupable") else ", לסינון בלבד") + ")"]
            if d.get("title") and d["title"] != d["key"]:
                bits.append(f' — {d["title"]}')
            if d.get("samples"):
                bits.append(" | ערכים: " + ", ".join(str(s) for s in d["samples"][:_SAMPLES_IN_PROMPT]))
            elif d["kind"] in ("number", "date") and d.get("min") is not None:
                bits.append(f' | טווח: {d["min"]}..{d["max"]}')
            if d.get("entity_type") in ("locality", "municipality"):
                bits.append("  [יישוב/רשות — ניתן להעשרה]")
            lines.append("".join(bits))
        lines.append("   מדדים: " + ", ".join(m["key"] for m in e["measures"]))
    return "\n".join(lines)


# ── validation ───────────────────────────────────────────────────────────────

OPS = {"=", "!=", ">", "<", ">=", "<=", "contains", "in", "between", "is_null", "not_null"}
_NUMERIC_OPS = {">", "<", ">=", "<=", "between"}
MAX_LIMIT = 1000


def _find(model: list[dict], key: str) -> dict | None:
    return next((e for e in model if e["key"] == key), None)


def validate_query(model: list[dict], q: dict) -> tuple[dict, dict]:
    """Check a model-emitted query against the declared model.

    Returns ``(entity, clean_query)``. Raises SemanticError — never returns a
    partially-valid query — because "מחוץ לתחום" is a correct answer and a
    silently-dropped filter is not: dropping one turns "כמה בתל אביב" into the
    national total and the user has no way to see it happened."""
    if not isinstance(q, dict):
        raise SemanticError("תשובת המודל אינה בפורמט הצפוי")
    ent = _find(model, str(q.get("entity") or ""))
    if ent is None:
        raise SemanticError(f'הטבלה {q.get("entity")!r} אינה חלק מהמודל')

    dims = {d["key"]: d for d in ent["dimensions"]}
    measure_keys = {m["key"] for m in ent["measures"]}

    measures = [str(m) for m in (q.get("measures") or ["count"])]
    for m in measures:
        if m not in measure_keys:
            raise SemanticError(f'המדד {m!r} אינו קיים בטבלה {ent["key"]}')
    if not measures:
        measures = ["count"]

    group = [str(d) for d in (q.get("dimensions") or [])]
    for d in group:
        if d not in dims:
            raise SemanticError(f'העמודה {d!r} אינה קיימת בטבלה {ent["key"]}')
        if not dims[d].get("groupable"):
            raise SemanticError(
                f'העמודה {d!r} היא מזהה ייחודי ולא קטגוריה — קיבוץ לפיה יחזיר שורה לכל רשומה')

    filters: list[dict] = []
    for f in (q.get("filters") or []):
        if not isinstance(f, dict):
            raise SemanticError("פילטר בפורמט שגוי")
        field = str(f.get("field") or "")
        if field not in dims:
            raise SemanticError(f'העמודה {field!r} אינה קיימת בטבלה {ent["key"]}')
        op = str(f.get("op") or "=")
        if op not in OPS:
            raise SemanticError(f"האופרטור {op!r} אינו נתמך")
        value = f.get("value")
        if op in ("is_null", "not_null"):
            value = None
        elif op == "in":
            if not isinstance(value, list) or not value:
                raise SemanticError("אופרטור in דורש רשימת ערכים")
            value = [_scalar(v) for v in value][:200]
        elif op == "between":
            if not isinstance(value, list) or len(value) != 2:
                raise SemanticError("אופרטור between דורש בדיוק שני ערכים")
            value = [_scalar(v) for v in value]
        else:
            value = _scalar(value)
        # Comparing a text column with < / > silently compares strings, which
        # gives a wrong-looking-but-plausible answer ('9' > '10'). Refuse.
        if op in _NUMERIC_OPS and dims[field]["kind"] == "text":
            raise SemanticError(
                f'לא ניתן להשוות טווח על העמודה הטקסטואלית {field!r}')
        filters.append({"field": field, "op": op, "value": value})

    enrich = [str(e) for e in (q.get("enrich") or [])]
    for e in enrich:
        if e not in ENRICH_FIELDS:
            raise SemanticError(f"שדה ההעשרה {e!r} אינו נתמך")
    if enrich and not ent.get("geo_dims"):
        raise SemanticError("אין בטבלה זו עמודת יישוב/רשות שניתן להעשיר לפיה")

    # Sort order is COERCED, not validated. The line this module draws is
    # whether a bad value could change the answer: an unknown field, filter or
    # measure would, so those raise. A sort order only changes the order rows
    # are displayed in — refusing the whole query over it converts a correct
    # answer into "אין לי תשובה", which is the worse outcome. Observed live:
    # a model returned order.by "count" and a perfectly good "כמה X לפי Y" was
    # rejected outright.
    order = q.get("order") or {}
    raw_by = str(order.get("by") or "").strip().lower()
    if raw_by not in ("measure", "dimension", ""):
        logger.info("semantic_model: coercing unknown order.by %r to the default", raw_by)
        raw_by = ""
    order_by = raw_by or ("measure" if group else "")
    order_dir = "asc" if str(order.get("dir") or "desc").lower() == "asc" else "desc"

    try:
        limit = int(q.get("limit") or 50)
    except (TypeError, ValueError):
        limit = 50
    limit = max(1, min(limit, MAX_LIMIT))

    return ent, {
        "entity": ent["key"], "measures": measures, "dimensions": group,
        "filters": filters, "enrich": enrich,
        "order": {"by": order_by, "dir": order_dir}, "limit": limit,
    }


def _scalar(v: Any) -> str | int | float:
    if isinstance(v, bool):
        raise SemanticError("ערך בוליאני אינו נתמך בפילטר")
    if isinstance(v, (int, float)):
        return v
    s = str(v if v is not None else "")
    if len(s) > 200:
        raise SemanticError("ערך הפילטר ארוך מדי")
    return s


# ── compilation ──────────────────────────────────────────────────────────────

def _qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(v: Any) -> str:
    """A SQL literal for an already-validated value.

    Values are the ONLY user-influenced text that reaches the statement, so they
    are the one place injection could live. Numbers are emitted bare; everything
    else is single-quoted with quotes doubled, and a NUL or backslash — neither
    of which can appear in a legitimate Hebrew filter value — is refused rather
    than escaped, so this stays correct regardless of standard_conforming_strings."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return repr(v)
    s = str(v)
    if "\x00" in s or "\\" in s:
        raise SemanticError("ערך הפילטר מכיל תווים לא חוקיים")
    return "'" + s.replace("'", "''") + "'"


def _numeric(ref: str) -> str:
    """Coerce a possibly-text column to numeric for aggregation/range tests.

    Half this corpus arrives as CSV, so a column that is semantically a number
    is often stored as text (and may carry ₪, commas or a stray space). The
    profiler's ``detected_kind`` already told us it is numeric; without the cast
    a sum errors out and a ``>`` compares strings. NULLIF keeps a non-numeric
    stray value from failing the whole query."""
    return f"NULLIF(regexp_replace({ref}::text, '[^0-9.\\-]', '', 'g'), '')::numeric"


def _where(f: dict, dim: dict) -> str:
    # Every column is qualified with the base-table alias `t` (see compile_sql).
    raw = f't.{_qi(f["field"])}'
    op, v = f["op"], f["value"]
    num = _numeric(raw) if dim["kind"] == "number" else raw
    if op == "is_null":
        return f"{raw} IS NULL"
    if op == "not_null":
        return f"{raw} IS NOT NULL"
    if op == "contains":
        return f"{raw}::text ILIKE {_lit('%' + str(v) + '%')}"
    if op == "in":
        return f"btrim({raw}::text) IN ({', '.join(_lit(str(x)) for x in v)})"
    if op == "between":
        return f"{num} BETWEEN {_lit(v[0])} AND {_lit(v[1])}"
    if op in ("=", "!=") and dim["kind"] == "text":
        # Trim on both sides: leading/trailing whitespace in scraped values is
        # the single most common reason an obviously-correct filter returns 0.
        return f"btrim({raw}::text) {op} btrim({_lit(v)})"
    return f"{num} {op} {_lit(v)}"


def _measure_sql(key: str) -> tuple[str, str]:
    if key == "count":
        return "count(*)", "מספר שורות"
    op, col = key.split(":", 1)
    label = {"sum": "סכום", "avg": "ממוצע", "min": "מינימום", "max": "מקסימום"}[op]
    return f"{op}({_numeric(f't.{_qi(col)}')})", f"{label} {col}"


def compile_sql(entity: dict, q: dict) -> str:
    """Validated query → SQL. Pure string building over checked identifiers.

    Every name here came out of ``validate_query``, i.e. out of the declared
    model, i.e. out of information_schema — none of it is model-authored text.
    The result is still handed to append_store.validate_readonly_sql before it
    runs; this function being correct is not the only thing standing between a
    bad query and the database."""
    dims = {d["key"]: d for d in entity["dimensions"]}
    schema = entity.get("schema") or "public"
    ref = _qi(entity["key"]) if schema == "public" else f'{schema}.{_qi(entity["key"])}'

    # The base table is ALWAYS aliased `t`, join or no join, so every column
    # reference below has exactly one form. Building qualified and unqualified
    # variants and patching one into the other is how a compiler grows a bug
    # that only shows up on the enrichment path.
    def col(name: str) -> str:
        return f"t.{_qi(name)}"

    select_parts: list[str] = []
    group_parts: list[str] = []
    for d in q["dimensions"]:
        if dims[d]["kind"] == "date":
            # Bucket by month so a time grouping is a readable series rather
            # than one row per timestamp.
            expr = f"date_trunc('month', {col(d)})::date"
            select_parts.append(f"{expr} AS {_qi(d)}")
            group_parts.append(expr)
        else:
            select_parts.append(f"{col(d)} AS {_qi(d)}")
            group_parts.append(col(d))

    # Enrichment joins the canonical settlement index on the healed code — the
    # same mechanism JoinBuilder uses, so both surfaces agree on what "מחוז"
    # means for a dirty locality string.
    geo = (entity.get("geo_dims") or [None])[0]
    for e in q["enrich"]:
        select_parts.append(f"_s.{_qi(e)} AS {_qi(ENRICH_FIELDS[e])}")
        group_parts.append(f"_s.{_qi(e)}")

    n_keys = len(select_parts)
    measure_labels: list[str] = []
    for m in q["measures"]:
        expr, label = _measure_sql(m)
        # Two measures can produce the same label only if the model asked for
        # the same one twice; validated input makes that harmless, but keep the
        # alias unique so the chart panel can address columns by name.
        while label in measure_labels:
            label += " "
        measure_labels.append(label)
        select_parts.append(f"{expr} AS {_qi(label)}")

    lines = ["-- נוצר משאלה בשפה חופשית מעל המודל הסמנטי של over.org.il",
             f"-- מקור: {entity['title']}",
             "SELECT " + ", ".join(select_parts),
             f"FROM {ref} t"]
    if q["enrich"]:
        lines.append(
            f"LEFT JOIN over_settlements _s "
            f"ON _s.code = COALESCE(over_settlement_code({col(geo)}), "
            f"over_authority_code({col(geo)}))")

    where = [_where(f, dims[f["field"]]) for f in q["filters"]]
    if where:
        lines.append("WHERE " + "\n  AND ".join(where))
    if group_parts:
        lines.append("GROUP BY " + ", ".join(str(i + 1) for i in range(n_keys)))
        if q["order"]["by"] == "measure" and measure_labels:
            lines.append(f"ORDER BY {n_keys + 1} {q['order']['dir'].upper()}")
        elif q["order"]["by"] == "dimension":
            lines.append(f"ORDER BY 1 {q['order']['dir'].upper()}")
    elif q["order"]["by"] == "dimension" and n_keys:
        lines.append(f"ORDER BY 1 {q['order']['dir'].upper()}")
    lines.append(f"LIMIT {q['limit']}")
    return "\n".join(lines)


def explain_query(entity: dict, q: dict) -> str:
    """One Hebrew sentence describing what was actually run.

    Rendered next to the number. The model does not write this — it is derived
    from the validated query, so it cannot describe a filter that was not
    applied, which is exactly the failure a model-written explanation invites."""
    dims = {d["key"]: d for d in entity["dimensions"]}
    parts = [", ".join(_measure_sql(m)[1] for m in q["measures"]), f'מתוך {entity["title"]}']
    if q["dimensions"]:
        parts.append("בפילוח לפי " + ", ".join(dims[d].get("title") or d for d in q["dimensions"]))
    for f in q["filters"]:
        label = dims[f["field"]].get("title") or f["field"]
        opname = {"=": "=", "!=": "≠", "contains": "מכיל", "in": "מתוך",
                  "between": "בין", "is_null": "ריק", "not_null": "אינו ריק"}.get(f["op"], f["op"])
        val = "" if f["value"] is None else (
            " ו-".join(map(str, f["value"])) if isinstance(f["value"], list) else str(f["value"]))
        parts.append(f"כאשר {label} {opname} {val}".strip())
    if q["enrich"]:
        parts.append("בהעשרה מאינדקס היישובים (" + ", ".join(ENRICH_FIELDS[e] for e in q["enrich"]) + ")")
    return " · ".join(parts)
