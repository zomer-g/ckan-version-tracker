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

# Question scaffolding: interrogatives, prepositions, connectives, and the
# words that name a BREAKDOWN rather than a subject. None of these say what the
# question is about, and counting them as subject evidence is how "כמה מעיינות
# לפי מחוז" retrieved "משטרת ישראל — מקרי רצח לפי מחוז": the crime dataset's
# title contains "לפי" and "מחוז", so it matched the question's grammar instead
# of its topic.
#
# The grouping cues (מחוז, יישוב, שנה…) stay eligible as ATTRIBUTE evidence —
# matching a column called מחוז is a real signal about which dataset can answer
# — they are only barred from deciding what the subject is.
_STOPWORDS = {
    "כמה", "מה", "מהו", "מהי", "מי", "איפה", "איזה", "אילו", "האם", "יש", "ישנם",
    "כל", "של", "את", "עם", "לפי", "בפילוח", "פילוח", "לכל", "הכי", "ביותר",
    "רשימה", "רשימת", "הצג", "תן", "תני", "תראה", "מספר", "כמות", "סך", "סהכ",
    "הכול", "הכל", "בשנת", "בשנה", "בין", "או", "וגם", "גם", "היו", "היה",
    "הם", "הן", "זה", "זו", "אלה", "נא", "בבקשה", "טבלה", "טבלת", "נתונים",
    "מידע", "מאגר", "מאגרי", "רשומות", "שורות", "ממוצע", "סכום", "בכל",
    "how", "many", "what", "which", "show", "list", "the", "of", "by", "in",
    "for", "and", "count", "total", "average",
}
_GROUPING_CUES = {
    "מחוז", "מחוזות", "נפה", "יישוב", "ישוב", "יישובים", "עיר", "ערים",
    "רשות", "רשויות", "שנה", "שנים", "חודש", "אזור", "אזורים", "סוג", "סוגים",
    "קטגוריה", "מין", "גיל", "אוכלוסייה",
}


def content_tokens(question: str) -> list[str]:
    """The question's tokens with scaffolding and breakdown words removed.

    Filtering happens BEFORE clitic expansion, and the expanded form is checked
    too. Both matter: filtering the expanded list alone let "לפי" be dropped
    while its stripped form "פי" survived — and "פי" then matched "פיענוח" in a
    crime dataset's title, which is how "כמה מעיינות לפי מחוז" retrieved
    "משטרת ישראל — מקרי רצח לפי מחוז" ahead of the springs dataset. The same
    leak turned "מחוז" into "חוז".

    Falls back to the full token list when stripping leaves nothing, so a
    question made entirely of cue words still retrieves something."""
    out: list[str] = []
    for raw in _TOKEN_RE.findall(question or ""):
        n = norm_token(raw)
        if len(n) < 2 or n in _STOPWORDS or n in _GROUPING_CUES:
            continue
        out.append(n)
        if len(n) >= 3 and n[0] in _HEB_PREFIXES:
            stripped = n[1:]
            if stripped not in _STOPWORDS and stripped not in _GROUPING_CUES:
                out.append(stripped)
    return out or tokens(question)


def score_entity(ent: dict, q_tokens: list[str],
                 subject_tokens: list[str] | None = None) -> float:
    """Lexical relevance of one entity to a tokenized question.

    Two kinds of evidence, and the distinction between them is the whole point:

      SUBJECT evidence — the question's topic appears in the entity's title,
        summary or synonyms. "עסקים" matching a dataset called "רישיונות עסק".
      ATTRIBUTE evidence — a question word appears in a column name or a stored
        value. "יישוב" matching a `setl_name` column.

    Attribute evidence alone is NOT evidence that this is the right dataset, and
    treating it as such is how the first production version answered "כמה עסקים
    יש בכל יישוב" from a GovMap street layer for one municipality: the layer had
    a locality column, matched "יישוב" twice, and outscored every actual business
    dataset. So an entity with no subject evidence scores zero, and attribute
    evidence is capped at the subject score — it can sharpen a ranking between
    plausible datasets, never decide which subject the question is about.

    Deliberately not embeddings: a vector index is another service to keep
    fresh, and the failure this corpus has is lexical/morphological, which
    normalized token overlap addresses directly."""
    if not q_tokens:
        return 0.0
    qs = set(q_tokens)
    # Subject evidence is judged on CONTENT words only — see _STOPWORDS.
    cs = set(subject_tokens if subject_tokens is not None else q_tokens)

    subject = (
        3.0 * len(cs & set(tokens(ent["title"])))
        + 1.5 * len(cs & set(tokens(ent.get("summary") or "")))
        + 1.0 * len(cs & {t for s in ent.get("synonyms", []) for t in tokens(s)})
    )
    if subject <= 0:
        return 0.0

    attribute = 0.0
    for d in ent["dimensions"]:
        dt = set(tokens(d["key"])) | set(tokens(d.get("title") or ""))
        attribute += 2.0 * len(qs & dt)
        for v in d.get("samples", []):
            if qs & set(tokens(v)):
                attribute += 2.5
                break

    # How much of the question's topic this entity actually accounts for. One
    # word out of four matching is much weaker evidence than one out of one, and
    # the raw score cannot tell them apart — which is how "מה שער הדולר היום"
    # retrieved "שער הירדן" (a river crossing) on the strength of a single
    # homonym. Scaled, not gated: a hard coverage floor also refused
    # "כמה תיקי פשיעה" for matching only "פשיעה", which is a correct answer.
    matched = len({t for t in cs if t in set(tokens(ent["title"]))
                   | set(tokens(ent.get("summary") or ""))})
    coverage = matched / len(cs) if cs else 0.0
    score = (subject + min(attribute, subject)) * (0.5 + 0.5 * coverage)
    # Tie-break toward tables people can actually get answers out of. log-ish,
    # so a 4M-row registry does not outrank a well-matched 300-row table.
    rows = ent.get("rows") or 0
    if rows:
        score += min(1.0, (len(str(int(rows))) - 2) * 0.15)
    return score * _tier_weight(ent)


# The `idx` schema is 904 of the catalog's 1,124 tables — mirrored GovMap layers
# and collection indexes with auto-derived titles ("אבני ק\"מ", "אגרומוזאיק",
# "אנדרטאות מ.א. רמת הנגב"), no descriptions and no curation. They are a
# lower-quality tier for free-text retrieval, but NOT worthless: the licensed-
# doctors and licensed-pharmacists registers live there too.
#
# So they are penalised, not excluded. Measured against the live catalog on a
# 14-question gold set, top-1 correct:
#     no penalty        11/14   (matched "נתוני פליטות לאוויר" for a weather question)
#     penalty x0.75     12/14   ← chosen: mildest penalty that reaches the best score
#     excluded entirely 10/14   (lost both health registers)
# The failures that remain at x0.75 are refusals, not wrong answers.
_IDX_TIER_PENALTY = 0.75


def _tier_weight(ent: dict) -> float:
    if ent.get("schema") != "idx":
        return 1.0
    return 1.0 if _OVERLAY.get(ent["key"], {}).get("nl") else _IDX_TIER_PENALTY


# An entity has to clear this before a paid model is asked about it. Below it,
# nothing in the catalog is plausibly about the question and the honest answer
# is "אין לי את הנתון" — handing six weak candidates to a model does not produce
# a refusal, it produces a confident answer from the least-bad one.
#
# Calibrated against the live catalog: ONE content word matching a title scores
# 3.0 plus a row-size tie-break of up to 1.0, so a correct single-word match
# ("כמה טיסות" → "מאגר טיסות") lands at ~3.5. A threshold of 4.0 rejected those,
# which trades wrong answers for false refusals rather than fixing anything.
MIN_RETRIEVAL_SCORE = 3.0


def retrieve(model: list[dict], question: str, k: int = 6) -> list[dict]:
    """Top-k entities for a question. This is the step whose absence measured
    0% schema-linking recall at this scale — never send the whole model.

    Returns [] when nothing clears MIN_RETRIEVAL_SCORE. That empty list is a
    feature: the caller turns it into a refusal without spending a call."""
    q, c = tokens(question), content_tokens(question)
    scored = [(score_entity(e, q, c), e) for e in model]
    scored = [(s, e) for s, e in scored if s >= MIN_RETRIEVAL_SCORE]
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
            lines.append(f'   {e["summary"][:200]}')

        def render(d: dict) -> str:
            bits = [f'   - {d["key"]} ({d["kind"]})']
            if d.get("title") and d["title"] != d["key"]:
                bits.append(f' — {d["title"][:60]}')
            if d.get("samples"):
                bits.append(" | ערכים: " + ", ".join(str(s) for s in d["samples"][:_SAMPLES_IN_PROMPT]))
            elif d["kind"] in ("number", "date") and d.get("min") is not None:
                bits.append(f' | טווח: {d["min"]}..{d["max"]}')
            if d.get("entity_type") in ("locality", "municipality"):
                bits.append("  [יישוב/רשות]")
            return "".join(bits)

        # Groupable and filter-only columns go in SEPARATE, LABELLED lists
        # rather than sharing one list with a parenthetical marker. Observed in
        # production: a cheap model read past "(text, לסינון בלבד)" and grouped
        # by a unique name column anyway, and the query was rejected. A heading
        # the wrong columns are not under is a much stronger signal than a
        # qualifier inside a line the model is skimming.
        groupable = [d for d in e["dimensions"] if d.get("groupable")]
        filter_only = [d for d in e["dimensions"] if not d.get("groupable")]
        if groupable:
            lines.append("   עמודות לקיבוץ (dimensions):")
            lines.extend(render(d) for d in groupable)
        if filter_only:
            lines.append("   עמודות לסינון בלבד — אסור לקבץ לפיהן:")
            lines.extend(render(d) for d in filter_only)
        lines.append("   מדדים: " + ", ".join(m["key"] for m in e["measures"]))
        if e.get("geo_dims"):
            # Spelled out per entity, next to the column it applies to. The
            # capability existed and went unused: asked for a breakdown by
            # מחוז on a table that has no such column, the model grouped by a
            # name column instead of enriching from the settlement index.
            lines.append(
                f'   העשרה: לטבלה יש עמודת יישוב ({e["geo_dims"][0]}), ולכן אפשר '
                f'לבקש ב-enrich את {", ".join(ENRICH_FIELDS)} גם אם אין להם עמודה משלהם.')
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

    # ── cross-dataset join ───────────────────────────────────────────────
    # Shape: {"entity": "<key>", "measures": [...]}. The model names the OTHER
    # DATASET and what to measure in it — never a join path, because there is
    # only one: the canonical CBS settlement code. That is the design every
    # production semantic layer converges on (Cube, MetricFlow, Malloy all
    # derive joins from the fields requested and refuse to let a query express
    # a path), and it is what keeps the decision inside a cheap model's reach.
    #
    # THE FAN TRAP is why this form is restricted. Joining two datasets row-to-
    # row on a shared locality multiplies them: 100 businesses and 5 springs in
    # one town produce 500 rows, and count(*) returns 500 — a plausible,
    # completely wrong number, which is the exact failure this whole layer
    # exists to prevent. So each side is aggregated to the settlement FIRST and
    # the join happens between two already-aggregated results. That makes the
    # grouping key fixed (the settlement) and per-side `dimensions` meaningless,
    # so they are not accepted.
    join = None
    jspec = q.get("join")
    if jspec:
        if not isinstance(jspec, dict):
            raise SemanticError("join בפורמט שגוי")
        jent = _find(model, str(jspec.get("entity") or ""))
        if jent is None:
            raise SemanticError(f'הטבלה {jspec.get("entity")!r} אינה חלק מהמודל')
        if jent["key"] == ent["key"]:
            raise SemanticError("לא ניתן להצליב טבלה עם עצמה")
        if not ent.get("geo_dims") or not jent.get("geo_dims"):
            raise SemanticError(
                "הצלבה בין מאגרים אפשרית רק כששניהם מכילים עמודת יישוב או רשות")
        jmeasure_keys = {m["key"] for m in jent["measures"]}
        jmeasures = [str(m) for m in (jspec.get("measures") or ["count"])] or ["count"]
        for m in jmeasures:
            if m not in jmeasure_keys:
                raise SemanticError(f'המדד {m!r} אינו קיים בטבלה {jent["key"]}')
        if jspec.get("dimensions") or jspec.get("filters"):
            raise SemanticError(
                "בהצלבה בין מאגרים הפילוח הוא לפי יישוב בלבד — אין פילוח או סינון נפרד "
                "לצד השני")
        if group:
            raise SemanticError(
                "בהצלבה בין מאגרים לא ניתן לקבץ לפי עמודה — הקיבוץ הוא לפי היישוב הקנוני")
        join = {"entity": jent["key"], "measures": jmeasures}

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
    # Enrichment fields are grouping keys too (compile_sql puts them in the
    # GROUP BY), so "כמה מעיינות לפי מחוז" — enrich=["district"], dimensions=[]
    # — is a grouped query and should come back sorted by the measure like any
    # other. Keying the default off `dimensions` alone left it unordered.
    order_by = raw_by or ("measure" if (group or enrich or join) else "")
    order_dir = "asc" if str(order.get("dir") or "desc").lower() == "asc" else "desc"

    try:
        limit = int(q.get("limit") or 50)
    except (TypeError, ValueError):
        limit = 50
    limit = max(1, min(limit, MAX_LIMIT))

    return ent, {
        "entity": ent["key"], "measures": measures, "dimensions": group,
        "filters": filters, "enrich": enrich, "join": join,
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


def _code_expr(alias: str, geo_col: str) -> str:
    """The canonical settlement/authority code for a dirty free-text place value.

    The ONLY join key this layer will use. Joining two datasets on their raw
    Hebrew locality strings would silently under-match — "תל אביב יפו" against
    "תל אביב-יפו" — and no validator catches a join that quietly dropped half
    the rows. over_settlement_code() resolves through the ~30.8k-inflection
    alias index, so both sides land on the same integer or on NULL."""
    ref = f"{alias}.{_qi(geo_col)}"
    return f"COALESCE(over_settlement_code({ref}), over_authority_code({ref}))"


def compile_join_sql(left: dict, right: dict, q: dict) -> str:
    """Two datasets compared side by side, per locality.

    Each side is aggregated to the canonical code BEFORE the join. That is not a
    stylistic choice — a row-level join on a shared locality is a fan trap: 100
    businesses and 5 springs in one town make 500 rows, and count(*) reports
    500. Pre-aggregating makes each side exactly one row per settlement, so the
    join is 1:1 and every measure keeps its meaning.

    FULL OUTER JOIN, not INNER: a settlement present in one dataset and absent
    from the other is usually the most interesting row in the answer, and an
    inner join would delete it without saying so."""
    lq, rq = _qi(left["key"]), _qi(right["key"])
    lref = lq if (left.get("schema") or "public") == "public" else f'{left["schema"]}.{lq}'
    rref = rq if (right.get("schema") or "public") == "public" else f'{right["schema"]}.{rq}'
    lgeo, rgeo = left["geo_dims"][0], right["geo_dims"][0]
    ldims = {d["key"]: d for d in left["dimensions"]}

    def side(measures: list[str], title: str) -> list[tuple[str, str]]:
        """(expression, alias) per measure. Kept as pairs rather than rendered
        strings so the outer SELECT can reference the aliases directly — parsing
        them back out of "expr AS alias" breaks the moment a label contains the
        separator, and these labels are Hebrew free text."""
        out, used = [], set()
        for m in measures:
            expr, label = _measure_sql(m)
            alias = f"{label} — {title[:24]}"
            while alias in used:
                alias += " "
            used.add(alias)
            out.append((expr, alias))
        return out

    lm = side(q["measures"], left["title"])
    rm = side(q["join"]["measures"], right["title"])
    where = [_where(f, ldims[f["field"]]) for f in q["filters"]]

    lines = [
        "-- הצלבה בין שני מאגרים לפי סמל היישוב הקנוני",
        f'-- {left["title"]}  X  {right["title"]}',
        "-- כל צד מסוכם לפי יישוב לפני ההצלבה, כדי שספירה לא תוכפל",
        "WITH a AS (",
        f"  SELECT {_code_expr('t', lgeo)} AS _code, "
        + ", ".join(f"{e} AS {_qi(a)}" for e, a in lm),
        f"  FROM {lref} t",
    ]
    if where:
        lines.append("  WHERE " + "\n    AND ".join(where))
    lines += [
        "  GROUP BY 1",
        "), b AS (",
        f"  SELECT {_code_expr('t', rgeo)} AS _code, "
        + ", ".join(f"{e} AS {_qi(a)}" for e, a in rm),
        f"  FROM {rref} t",
        "  GROUP BY 1",
        ")",
        "SELECT COALESCE(s.name, '(לא זוהה יישוב)') AS " + _qi("יישוב") + ",",
        "       " + ", ".join([f"a.{_qi(a)}" for _, a in lm] + [f"b.{_qi(a)}" for _, a in rm]),
        "FROM a FULL JOIN b ON a._code = b._code",
        "LEFT JOIN over_settlements s ON s.code = COALESCE(a._code, b._code)",
        f'ORDER BY a.{_qi(lm[0][1])} {q["order"]["dir"].upper()} NULLS LAST',
        f'LIMIT {q["limit"]}',
    ]
    return "\n".join(lines)


def compile_sql(entity: dict, q: dict, model: list[dict] | None = None) -> str:
    """Validated query → SQL. Pure string building over checked identifiers.

    Every name here came out of ``validate_query``, i.e. out of the declared
    model, i.e. out of information_schema — none of it is model-authored text.
    The result is still handed to append_store.validate_readonly_sql before it
    runs; this function being correct is not the only thing standing between a
    bad query and the database."""
    if q.get("join"):
        if not model:
            raise SemanticError("הצלבה דורשת את המודל המלא")
        right = _find(model, q["join"]["entity"])
        if right is None:
            raise SemanticError("טבלת ההצלבה לא נמצאה")
        return compile_join_sql(entity, right, q)

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


def explain_query(entity: dict, q: dict, model: list[dict] | None = None) -> str:
    """One Hebrew sentence describing what was actually run.

    Rendered next to the number. The model does not write this — it is derived
    from the validated query, so it cannot describe a filter that was not
    applied, which is exactly the failure a model-written explanation invites."""
    dims = {d["key"]: d for d in entity["dimensions"]}
    parts = [", ".join(_measure_sql(m)[1] for m in q["measures"]), f'מתוך {entity["title"]}']
    if q.get("join"):
        other = _find(model or [], q["join"]["entity"])
        parts.append(
            f'בהצלבה עם {other["title"] if other else q["join"]["entity"]} '
            f'({", ".join(_measure_sql(m)[1] for m in q["join"]["measures"])}) '
            "— לפי סמל היישוב הקנוני, כל צד מסוכם בנפרד")
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
