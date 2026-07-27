"""Table profiler — per-table SQL-first profiling + LLM enrichment.

Scans queryable NEON tables (public ``append_*``, ``odata``, ``knesset``,
``idx``) and builds, per table, a statistical profile computed DETERMINISTICALLY
in SQL first — then, optionally, an LLM enrichment layer on top.

Why SQL-first: the append tables store every value as ``text`` (see
append_store's module docstring — archival robustness over typing). So a
column's *declared* Postgres type tells us nothing about whether it holds
numbers or dates; we must SNIFF that from the data. The profiler samples each
column, classifies it (numeric / date+format / text), then computes exact
min/max/avg over the full table with regex-guarded casts. Typed schemas
(knesset/idx/odata) get their ranges directly. This is the core of the user's
ask: "detect min/max of number & date fields, detect the date format to enable
date parsing".

The profile also extracts: null/fill rate, approximate distinct cardinality
(from pg_stats, no scan), the most frequent values per text column (candidate
recurring entities + keyword source), a Hebrew-aware keyword list, and a
heuristic entity classification per column (locality / corporation / person /
id / amount / date …) using the existing CBS locality gazetteer.

Storage: profiles live IN the append DB — table ``public.over_table_profiles``
— so they are queryable from the SAME /data SQL console as the data they
describe (the user's explicit requirement: "everything together"). The
``over_`` prefix is reserved for OVER-generated companion artifacts: this table
today, and per-column ``over_*`` normalized columns in phase 2. The read/write
pool creates + upserts the table; the least-privilege readonly role auto-gains
SELECT on it (ALTER DEFAULT PRIVILEGES in scripts/create_append_readonly_role).

LLM enrichment reuses the DeepSeek→Anthropic provider selection and the global
daily budget guard already used by ocal_enrich / cbs_ask.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any

from app.config import settings
from app.services import append_store
from app.services.append_store import _qi  # SQL identifier quoting (Hebrew-safe)

logger = logging.getLogger(__name__)

# LLM provider constants — same selection as ocal_enrich / cbs_ask.
_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
_DEEPSEEK_MODEL = "deepseek-chat"
_ANTHROPIC_MODEL = "claude-opus-4-8"

# ── Tunables ────────────────────────────────────────────────────────────────
SAMPLE_SIZE = 1000          # rows sampled per table to classify columns
TOP_VALUES = 15             # most-frequent values kept per text column
MAX_TEXT_COLS_TOPVALUES = 40  # cap top-value queries so a 200-col table is bounded
KEYWORDS_TOP = 25
DATE_MIN_MATCH = 0.80       # fraction of sample that must parse for a date verdict
NUMERIC_MIN_MATCH = 0.90    # fraction of sample that must be numeric
LARGE_TABLE_ROWS = 1_000_000  # above this, top-value scans use TABLESAMPLE
_HIDDEN = {"row_hash"}
_BULK = {"geometry_wkt", "geometry", "geom", "wkt"}

PROFILE_TABLE = "over_table_profiles"

# Schemas the profiler is allowed to read (matches the /data console + readonly role).
PROFILABLE_SCHEMAS = ("public", "odata", "knesset", "idx")


# ── Candidate date formats ────────────────────────────────────────────────────
# Ordered by preference. Israeli data is day-first, so dd/mm variants precede
# mm/dd; ISO forms come first because they are unambiguous. Each entry is a
# (python strptime format, Postgres to_timestamp format) pair — the PG side lets
# us compute exact chronological min/max over the whole table in one scan.
_DATE_FORMATS: list[tuple[str, str | None]] = [
    ("%Y-%m-%dT%H:%M:%S", 'YYYY-MM-DD"T"HH24:MI:SS'),
    ("%Y-%m-%d %H:%M:%S", "YYYY-MM-DD HH24:MI:SS"),
    ("%Y-%m-%d", "YYYY-MM-DD"),
    ("%Y/%m/%d", "YYYY/MM/DD"),
    ("%d/%m/%Y %H:%M:%S", "DD/MM/YYYY HH24:MI:SS"),
    ("%d/%m/%Y %H:%M", "DD/MM/YYYY HH24:MI"),
    ("%d/%m/%Y", "DD/MM/YYYY"),
    ("%d-%m-%Y", "DD-MM-YYYY"),
    ("%d.%m.%Y", "DD.MM.YYYY"),
    ("%m/%d/%Y", "MM/DD/YYYY"),
    ("%Y%m%d", "YYYYMMDD"),
    ("%d/%m/%y", "DD/MM/YY"),
]

_NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")
# Hebrew + Latin word characters; splits on everything else. Keeps quotes/geresh
# inside a token (מנכ"ל, ע"י) by treating them as word-internal.
_TOKEN_RE = re.compile(r"[A-Za-z֐-׿][A-Za-z֐-׿'\"׳״]*")
# Hebrew stopwords — enough to keep keyword lists meaningful, not exhaustive.
_STOPWORDS = {
    "של", "על", "עם", "או", "גם", "כי", "אבל", "אשר", "זה", "זו", "הוא", "היא",
    "הם", "הן", "אני", "אתה", "לא", "כן", "יש", "אין", "מה", "מי", "את", "אל",
    "כל", "בין", "לפי", "עד", "מן", "אם", "כמו", "רק", "פי", "וכן", "וכו",
    "the", "a", "an", "of", "and", "or", "to", "in", "on", "for", "is", "are",
}


# ── Pure helpers (unit-tested offline; no DB) ─────────────────────────────────
def _clean(v: Any) -> str | None:
    """Normalize a raw cell to a trimmed string, treating '' as missing."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def looks_numeric(samples: list[str]) -> float:
    """Fraction of samples that parse as a plain number (thousands-sep stripped)."""
    if not samples:
        return 0.0
    ok = 0
    for s in samples:
        t = re.sub(r"[,\s]", "", s)
        if _NUMERIC_RE.match(t):
            ok += 1
    return ok / len(samples)


def detect_date_format(samples: list[str]) -> dict | None:
    """Best matching date format for a list of text samples, or None.

    Returns ``{python, postgres, match_rate, ambiguous}``. Tries each candidate
    format in preference order and picks the first whose parse rate clears
    DATE_MIN_MATCH; ``ambiguous`` flags day/month formats that a mm/dd reading
    would also satisfy (the caller / LLM can confirm)."""
    if not samples:
        return None
    best: dict | None = None
    scores: list[tuple[str, float]] = []
    for pyfmt, pgfmt in _DATE_FORMATS:
        ok = 0
        for s in samples:
            try:
                datetime.strptime(s, pyfmt)
                ok += 1
            except (ValueError, TypeError):
                pass
        rate = ok / len(samples)
        scores.append((pyfmt, rate))
        if rate >= DATE_MIN_MATCH and best is None:
            best = {"python": pyfmt, "postgres": pgfmt, "match_rate": round(rate, 4)}
    if best is None:
        return None
    # Ambiguity: a day-first format where a month-first sibling scores as well.
    df = best["python"]
    ambiguous = False
    if df in ("%d/%m/%Y", "%d/%m/%y"):
        mm = dict(scores).get("%m/%d/%Y", 0.0)
        ambiguous = mm >= best["match_rate"] - 1e-9
    best["ambiguous"] = ambiguous
    return best


def tokenize_he(text: str) -> list[str]:
    """Hebrew/Latin word tokens from free text, lowercased for Latin."""
    return [t.lower() if t.isascii() else t for t in _TOKEN_RE.findall(text or "")]


def extract_keywords(samples: list[str], top: int = KEYWORDS_TOP) -> list[dict]:
    """Top-N content tokens across a sample of text values (stopwords removed)."""
    counts: dict[str, int] = {}
    for s in samples:
        for tok in tokenize_he(s):
            if len(tok) < 2 or tok in _STOPWORDS:
                continue
            counts[tok] = counts.get(tok, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [{"token": t, "count": c} for t, c in ranked[:top]]


_CORP_RE = re.compile(r'בע["״׳\']?מ|\bבעמ\b|\bחברה\b|\bLtd\.?\b|\bInc\.?\b|\bLLC\b', re.I)
_ID_NAME_RE = re.compile(r"מזה|מספר|קוד|ת\.?ז|id\b|code\b|_id\b|symbol|סמל", re.I)
_AMOUNT_NAME_RE = re.compile(r"סכום|עלות|תקציב|מחיר|שכר|תשלום|amount|cost|budget|price|sum", re.I)
_DATE_NAME_RE = re.compile(r"תארי|מועד|יום|date|time|שנה|חודש", re.I)
_LOCALITY_NAME_RE = re.compile(r"יישוב|ישוב|עיר|רשות|locality|city|town|settlement", re.I)


def classify_entity_heuristic(
    col: str,
    *,
    is_numeric: bool,
    date_fmt: dict | None,
    top_values: list[str],
    distinct_ratio: float,
    locality_names: set[str] | None,
) -> dict:
    """A conservative, pre-LLM guess of a column's SEMANTIC type.

    Combines value-shape (numeric? date? cardinality) with the column name and,
    for localities, a lookup against the CBS gazetteer. Returns
    ``{guess, confidence, evidence}``. The LLM step confirms/overrides this."""
    name = col or ""
    ev: list[str] = []

    if date_fmt:
        return {"guess": "date", "confidence": 0.9,
                "evidence": [f"format={date_fmt['python']} rate={date_fmt['match_rate']}"]}

    # Locality: many top values are known place names.
    if locality_names and top_values:
        hits = sum(1 for v in top_values if _norm_place(v) in locality_names)
        rate = hits / len(top_values)
        if rate >= 0.5:
            return {"guess": "locality", "confidence": round(0.5 + rate / 2, 3),
                    "evidence": [f"{hits}/{len(top_values)} top values are gazetteer localities"]}
        if rate > 0:
            ev.append(f"{hits} top values match localities")

    if any(_CORP_RE.search(v) for v in top_values):
        return {"guess": "corporation", "confidence": 0.7,
                "evidence": ["corporate suffix (בע\"מ/Ltd) in values"]}

    if is_numeric:
        # id vs amount vs plain measure, disambiguated by name + cardinality.
        if _ID_NAME_RE.search(name) or distinct_ratio >= 0.95:
            return {"guess": "id", "confidence": 0.6,
                    "evidence": ev + [f"numeric, distinct_ratio={round(distinct_ratio,3)}"]}
        if _AMOUNT_NAME_RE.search(name):
            return {"guess": "amount", "confidence": 0.6, "evidence": ev + ["numeric + amount-like name"]}
        return {"guess": "number", "confidence": 0.5, "evidence": ev + ["numeric values"]}

    if _LOCALITY_NAME_RE.search(name):
        return {"guess": "locality", "confidence": 0.4, "evidence": ev + ["locality-like name"]}
    if _DATE_NAME_RE.search(name):
        return {"guess": "date", "confidence": 0.35, "evidence": ev + ["date-like name (unparsed)"]}

    return {"guess": "text", "confidence": 0.3, "evidence": ev or ["free text"]}


def _norm_place(s: str) -> str:
    """Loose normalization for gazetteer matching (drop punctuation/spaces)."""
    return re.sub(r"[\s\-־'\"״׳.]+", "", (s or "").strip())


def is_encodable(s) -> bool:
    """True if ``s`` is a str that survives a UTF-8 round-trip.

    Some upstream CSVs were decoded with damage (CP862 / bad sniffing), leaving
    LONE SURROGATES in column names (e.g. 'תא\\udc90ריך'). asyncpg cannot encode
    such a name back to UTF-8, so any query that references the column raises and
    the whole table is lost. We detect these and skip just the bad columns."""
    if not isinstance(s, str):
        return False
    try:
        s.encode("utf-8")
        return True
    except UnicodeEncodeError:
        return False


def _sanitize_json(s: str) -> str:
    """Replace any non-UTF-8-encodable code points (lone surrogates leaking in
    from corrupted row DATA) with U+FFFD, so storing the profile never fails on
    a value asyncpg cannot encode."""
    return s.encode("utf-8", "replace").decode("utf-8")


def table_signature(row_count: int | None, columns: list[dict]) -> str:
    """Stable fingerprint of a table's shape — lets a re-profile skip unchanged
    tables. Sensitive to row count and the ordered (name,type) column list."""
    import hashlib
    payload = json.dumps(
        {"rows": int(row_count or 0),
         "cols": [(c.get("name"), c.get("type")) for c in columns]},
        ensure_ascii=False, sort_keys=True,
    )
    return hashlib.sha256(_sanitize_json(payload).encode("utf-8")).hexdigest()[:16]


# ── Storage (append DB, public.over_table_profiles) ───────────────────────────
async def ensure_profile_table() -> None:
    """Create the profile table if absent. Idempotent; safe on every run."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(PROFILE_TABLE)} (
                schema_name       text        NOT NULL,
                table_name        text        NOT NULL,
                title             text,
                kind              text,
                row_count         bigint,
                column_count      integer,
                signature         text,
                sql_profile       jsonb,
                llm_enrichment    jsonb,
                summary_he        text,
                date_parse_specs  jsonb,
                status            text        NOT NULL DEFAULT 'sql_done',
                profiled_at       timestamptz DEFAULT now(),
                enriched_at       timestamptz,
                PRIMARY KEY (schema_name, table_name)
            )
            """
        )


async def upsert_profile(rec: dict) -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO public.{_qi(PROFILE_TABLE)}
                (schema_name, table_name, title, kind, row_count, column_count,
                 signature, sql_profile, date_parse_specs, status, profiled_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10, now())
            ON CONFLICT (schema_name, table_name) DO UPDATE SET
                title=EXCLUDED.title, kind=EXCLUDED.kind,
                row_count=EXCLUDED.row_count, column_count=EXCLUDED.column_count,
                signature=EXCLUDED.signature, sql_profile=EXCLUDED.sql_profile,
                date_parse_specs=EXCLUDED.date_parse_specs,
                status=EXCLUDED.status, profiled_at=now()
            """,
            rec["schema_name"], rec["table_name"], rec.get("title"), rec.get("kind"),
            rec.get("row_count"), rec.get("column_count"), rec.get("signature"),
            _sanitize_json(json.dumps(rec.get("sql_profile"), ensure_ascii=False)),
            _sanitize_json(json.dumps(rec.get("date_parse_specs"), ensure_ascii=False)),
            rec.get("status", "sql_done"),
        )


async def save_enrichment(schema: str, table: str, enrichment: dict, summary_he: str | None) -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            UPDATE public.{_qi(PROFILE_TABLE)}
               SET llm_enrichment=$3::jsonb, summary_he=$4,
                   status='enriched', enriched_at=now()
             WHERE schema_name=$1 AND table_name=$2
            """,
            schema, table, _sanitize_json(json.dumps(enrichment, ensure_ascii=False)),
            (_sanitize_json(summary_he) if summary_he else summary_he),
        )


async def get_profile(schema: str, table: str) -> dict | None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT * FROM public.{_qi(PROFILE_TABLE)} WHERE schema_name=$1 AND table_name=$2",
            schema, table,
        )
    return _row_to_dict(row) if row else None


async def coverage() -> dict:
    """Counts by status — for the admin coverage card."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT status, count(*) c FROM public.{_qi(PROFILE_TABLE)} GROUP BY status")
        except Exception:  # table not created yet
            return {"total": 0, "by_status": {}}
    by = {r["status"]: r["c"] for r in rows}
    return {"total": sum(by.values()), "by_status": by}


def _row_to_dict(row) -> dict:
    d = dict(row)
    for k in ("sql_profile", "llm_enrichment", "date_parse_specs"):
        v = d.get(k)
        if isinstance(v, str):
            try:
                d[k] = json.loads(v)
            except (ValueError, TypeError):
                pass
    for k in ("profiled_at", "enriched_at"):
        if isinstance(d.get(k), (datetime, date)):
            d[k] = d[k].isoformat()
    return d


# ── SQL profiler ──────────────────────────────────────────────────────────────
async def _sample_column_values(schema: str, table: str, cols: list[str],
                                 est_rows: int | None) -> dict[str, list[str]]:
    """{col: [non-null sample values as text]} — one sampling query for the table.

    Uses TABLESAMPLE on large tables so classification never scans a huge table."""
    ro = await append_store.get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    sel = ", ".join(f"{_qi(c)}::text AS {_qi(c)}" for c in cols)
    if est_rows and est_rows > LARGE_TABLE_ROWS:
        frac = min(100.0, max(0.1, SAMPLE_SIZE * 100.0 / est_rows))
        src = f"{ref} TABLESAMPLE SYSTEM ({frac:.4f})"
    else:
        src = ref
    out: dict[str, list[str]] = {c: [] for c in cols}
    async with ro.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute("SET LOCAL statement_timeout = 20000")
            try:
                recs = await conn.fetch(f"SELECT {sel} FROM {src} LIMIT {SAMPLE_SIZE}")
            except Exception as exc:  # noqa: BLE001 — TABLESAMPLE unsupported on views etc.
                logger.info("sample fallback for %s.%s: %s", schema, table, exc)
                recs = await conn.fetch(f"SELECT {sel} FROM {ref} LIMIT {SAMPLE_SIZE}")
    for r in recs:
        for c in cols:
            v = _clean(r[c])
            if v is not None:
                out[c].append(v)
    return out


def _classify_columns(samples: dict[str, list[str]],
                      col_types: dict[str, str] | None = None) -> dict[str, dict]:
    """Per column: {kind: numeric|date|text|empty, native, date_fmt, numeric_rate}.

    Trusts the DECLARED Postgres type first: a real ``timestamp``/``date`` or
    numeric column (typed knesset/idx/odata schemas, and the append tables'
    ``first_seen`` timestamptz) is classified natively — exact MIN/MAX with no
    format guessing. Only genuinely text-typed columns (all append data columns,
    which store everything as text) are SNIFFED from the sample."""
    col_types = col_types or {}
    result: dict[str, dict] = {}
    for col, vals in samples.items():
        declared = col_types.get(col)
        if declared == "timestamp":
            result[col] = {"kind": "date", "native": True}
            continue
        if declared in ("int", "numeric"):
            result[col] = {"kind": "numeric", "native": True}
            continue
        if not vals:
            result[col] = {"kind": "empty"}
            continue
        num_rate = looks_numeric(vals)
        if num_rate >= NUMERIC_MIN_MATCH:
            result[col] = {"kind": "numeric", "numeric_rate": round(num_rate, 4)}
            continue
        dfmt = detect_date_format(vals)
        if dfmt:
            result[col] = {"kind": "date", "date_fmt": dfmt}
            continue
        result[col] = {"kind": "text"}
    return result


def _numeric_expr(col: str, native: bool = False) -> str:
    q = _qi(col)
    if native:  # already a numeric-typed column — no cast/guard needed
        return (f"MIN({q}) AS min_{{i}}, MAX({q}) AS max_{{i}}, AVG({q}) AS avg_{{i}}")
    nz = f"NULLIF(regexp_replace({q}::text, '[,\\s]', '', 'g'), '')"
    guard = f"{nz} ~ '^-?[0-9]+(\\.[0-9]+)?$'"
    return (
        f"MIN({nz}::numeric) FILTER (WHERE {guard}) AS min_{{i}}, "
        f"MAX({nz}::numeric) FILTER (WHERE {guard}) AS max_{{i}}, "
        f"AVG({nz}::numeric) FILTER (WHERE {guard}) AS avg_{{i}}"
    )


def _date_expr(col: str, pgfmt: str | None, native: bool = False, safe: bool = True) -> str:
    q = _qi(col)
    if native:  # already a timestamp/date-typed column — MIN/MAX directly
        return f"MIN({q}) AS min_{{i}}, MAX({q}) AS max_{{i}}"
    if not pgfmt or not safe:
        # ``safe=False`` is the fallback pass: some text values matched the format
        # in the sample but are invalid dates over the full table (bare year,
        # 31/02, …) and to_timestamp throws "field value out of range", crashing
        # the whole aggregate. Skip the range for text dates on that retry.
        return f"NULL::timestamp AS min_{{i}}, NULL::timestamp AS max_{{i}}"
    # to_timestamp is lenient; guard with a length check to avoid garbage rows.
    src = f"NULLIF({q}::text, '')"
    conv = f"CASE WHEN {src} IS NOT NULL THEN to_timestamp({src}, '{pgfmt}') END"
    return f"MIN({conv}) AS min_{{i}}, MAX({conv}) AS max_{{i}}"


# Above this many rows, the regex-guarded numeric/date casts over the WHOLE table
# blow the aggregate's statement_timeout (vehicles 4.1M, tikufim ~900k). Compute
# the aggregate over a TABLESAMPLE instead — ranges become approximate (flagged).
_AGG_SAMPLE_ROWS = 2_000_000
_AGG_TIMEOUT_MS = 240_000  # 4 min — enough for ~1M-row full scans


async def _aggregate(schema: str, table: str, cols: list[str],
                     classes: dict[str, dict], *, est_rows: int | None = None,
                     date_ranges: bool = True) -> dict:
    """One scan: total count, per-column non-null count, and min/max/avg for the
    classified numeric & date columns. For very large tables the aggregate runs
    over a TABLESAMPLE (``approx=True``) so it stays within the timeout.
    ``date_ranges=False`` is the retry pass that skips text-date min/max after an
    invalid-date crash."""
    ro = await append_store.get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    approx = bool(est_rows and est_rows > _AGG_SAMPLE_ROWS)
    src = ref
    if approx:
        frac = min(100.0, max(0.05, 300_000.0 * 100.0 / est_rows))
        src = f"{ref} TABLESAMPLE SYSTEM ({frac:.4f})"
    parts = ["count(*) AS total"]
    meta: list[tuple[int, str, str]] = []  # (idx, col, kind)
    idx = 0
    for col in cols:
        q = _qi(col)
        parts.append(
            f"count(*) FILTER (WHERE {q} IS NOT NULL AND {q}::text <> '') AS nn_{idx}")
        cls = classes.get(col, {})
        kind = cls.get("kind")
        native = bool(cls.get("native"))
        if kind == "numeric":
            parts.append(_numeric_expr(col, native).format(i=idx))
        elif kind == "date":
            pgfmt = (cls.get("date_fmt") or {}).get("postgres")
            parts.append(_date_expr(col, pgfmt, native, safe=date_ranges).format(i=idx))
        meta.append((idx, col, kind))
        idx += 1
    sql = f"SELECT {', '.join(parts)} FROM {src}"
    async with ro.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {_AGG_TIMEOUT_MS}")
            row = await conn.fetchrow(sql)
    agg = {"total": int(row["total"] or 0), "approx": approx, "columns": {}}
    for i, col, kind in meta:
        entry = {"non_null": int(row[f"nn_{i}"] or 0)}
        if kind == "numeric":
            entry["min"] = _num(row.get(f"min_{i}"))
            entry["max"] = _num(row.get(f"max_{i}"))
            entry["avg"] = _num(row.get(f"avg_{i}"))
        elif kind == "date":
            entry["min"] = _iso(row.get(f"min_{i}"))
            entry["max"] = _iso(row.get(f"max_{i}"))
        agg["columns"][col] = entry
    return agg


def _num(v):
    if v is None:
        return None
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return None


def _iso(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return None


async def _top_values(schema: str, table: str, col: str, est_rows: int | None) -> list[dict]:
    ro = await append_store.get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    q = _qi(col)
    src = ref
    if est_rows and est_rows > LARGE_TABLE_ROWS:
        frac = min(100.0, max(0.5, 200000.0 * 100.0 / est_rows))
        src = f"{ref} TABLESAMPLE SYSTEM ({frac:.4f})"
    sql = (f"SELECT {q}::text AS v, count(*) AS c FROM {src} "
           f"WHERE {q} IS NOT NULL AND {q}::text <> '' GROUP BY 1 ORDER BY c DESC LIMIT {TOP_VALUES}")
    try:
        async with ro.acquire() as conn:
            async with conn.transaction(readonly=True):
                await conn.execute("SET LOCAL statement_timeout = 30000")
                recs = await conn.fetch(sql)
    except Exception as exc:  # noqa: BLE001
        logger.info("top_values skipped for %s.%s.%s: %s", schema, table, col, exc)
        return []
    return [{"value": r["v"], "count": int(r["c"])} for r in recs]


async def _pg_stats_distinct(schema: str, table: str) -> dict[str, float]:
    """{col: n_distinct as reported by pg_stats}. Negative = ratio of rows."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT attname, n_distinct FROM pg_stats WHERE schemaname=$1 AND tablename=$2",
            schema, table)
    return {r["attname"]: float(r["n_distinct"]) for r in rows}


async def profile_table(schema: str, table: str, *, columns: list[dict] | None = None,
                        est_rows: int | None = None,
                        locality_names: set[str] | None = None,
                        title: str | None = None, kind: str | None = None) -> dict:
    """Compute the full deterministic SQL profile for one table and persist it."""
    if schema not in PROFILABLE_SCHEMAS:
        raise ValueError(f"schema not profilable: {schema!r}")
    if columns is None:
        by_table = await append_store.schema_table_columns(schema)
        columns = by_table.get(table, [])
    # Columns whose names carry lone surrogates (corrupted upstream CSV decoding)
    # cannot be referenced in a query without asyncpg failing to encode them —
    # skip just those so the rest of the table still profiles.
    bad_name_cols = [c["name"] for c in columns if not is_encodable(c.get("name"))]
    col_names = [c["name"] for c in columns
                 if is_encodable(c.get("name")) and c["name"] not in _HIDDEN
                 and c.get("type") != "geometry" and str(c["name"]).lower() not in _BULK]
    geom_cols = [c["name"] for c in columns
                 if c.get("type") == "geometry" and is_encodable(c.get("name"))]
    if not col_names:
        rec = {"schema_name": schema, "table_name": table, "title": title, "kind": kind,
               "row_count": 0, "column_count": len(columns), "signature": table_signature(0, columns),
               "sql_profile": {"columns": {}, "geometry_columns": geom_cols, "note": "no scalar columns"},
               "date_parse_specs": {}, "status": "sql_done"}
        await upsert_profile(rec)
        return rec

    col_types = {c["name"]: c.get("type") for c in columns}
    samples = await _sample_column_values(schema, table, col_names, est_rows)
    classes = _classify_columns(samples, col_types)
    date_ranges_skipped = False
    try:
        agg = await _aggregate(schema, table, col_names, classes, est_rows=est_rows)
    except Exception as exc:  # noqa: BLE001 — most often an invalid text date
        logger.info("aggregate retry without date ranges for %s.%s: %s", schema, table, exc)
        agg = await _aggregate(schema, table, col_names, classes, est_rows=est_rows,
                               date_ranges=False)
        date_ranges_skipped = True
    approx = bool(agg.get("approx"))
    scan_total = agg["total"]  # rows the aggregate actually scanned (sample if approx)
    # Real row count: the planner estimate when we sampled, else the exact scan count.
    total = int(est_rows) if (approx and est_rows) else scan_total
    ndist = await _pg_stats_distinct(schema, table)

    text_cols = [c for c in col_names if classes.get(c, {}).get("kind") in ("text", "empty")]
    top_by_col: dict[str, list[dict]] = {}
    for c in text_cols[:MAX_TEXT_COLS_TOPVALUES]:
        top_by_col[c] = await _top_values(schema, table, c, est_rows)

    columns_profile: dict[str, dict] = {}
    date_specs: dict[str, dict] = {}
    keyword_pool: list[str] = []
    for c in col_names:
        cls = classes.get(c, {})
        col_agg = agg["columns"].get(c, {})
        nn = col_agg.get("non_null", 0)
        raw_nd = ndist.get(c, 0.0)
        distinct = int(raw_nd) if raw_nd >= 0 else int(round(-raw_nd * total))
        distinct_ratio = (distinct / total) if total else 0.0
        # fill_rate is measured against what the aggregate scanned (the sample,
        # when approx), so nn and its denominator come from the same population.
        fill_den = scan_total
        top_vals = top_by_col.get(c, [])
        top_strings = [t["value"] for t in top_vals]
        entity = classify_entity_heuristic(
            c, is_numeric=cls.get("kind") == "numeric", date_fmt=cls.get("date_fmt"),
            top_values=top_strings, distinct_ratio=distinct_ratio, locality_names=locality_names)
        entry: dict = {
            "detected_kind": cls.get("kind"),
            "non_null": nn,
            "fill_rate": round(nn / fill_den, 4) if fill_den else 0.0,
            "distinct_est": distinct,
            "distinct_ratio": round(distinct_ratio, 4),
            "entity_guess": entity,
        }
        if cls.get("kind") == "numeric":
            entry.update({"min": col_agg.get("min"), "max": col_agg.get("max"),
                          "avg": col_agg.get("avg"), "numeric_rate": cls.get("numeric_rate")})
        elif cls.get("kind") == "date":
            entry.update({"min": col_agg.get("min"), "max": col_agg.get("max"),
                          "native": bool(cls.get("native")),
                          "date_format": cls.get("date_fmt")})
            # Only text-stored dates need a parse spec for phase-2 normalization;
            # native timestamp columns are already queryable as dates.
            if not cls.get("native") and cls.get("date_fmt"):
                date_specs[c] = cls.get("date_fmt")
        else:
            entry["top_values"] = top_vals
            keyword_pool.extend(samples.get(c, []))
        columns_profile[c] = entry

    sql_profile = {
        "row_count": total,
        "column_count": len(col_names),
        "geometry_columns": geom_cols,
        "columns": columns_profile,
        "keywords": extract_keywords(keyword_pool),
        "candidate_key": next(
            (c for c in col_names
             if columns_profile[c]["distinct_ratio"] >= 0.99 and columns_profile[c]["fill_rate"] >= 0.99),
            None),
    }
    if approx:
        # Ranges/fill for this (very large) table were computed over a sample.
        sql_profile["approx_ranges"] = True
        sql_profile["scanned_rows"] = scan_total
    if date_ranges_skipped:
        # A text date column held invalid values (bare year, 31/02, …); ranges
        # for text dates were skipped so the rest of the table still profiles.
        sql_profile["date_ranges_skipped"] = True
    if bad_name_cols:
        # Surface columns skipped for un-encodable names (ascii-safe rendering).
        sql_profile["unprofilable_columns"] = [
            n.encode("utf-8", "replace").decode("utf-8") for n in bad_name_cols]
    rec = {
        "schema_name": schema, "table_name": table, "title": title, "kind": kind,
        "row_count": total, "column_count": len(col_names),
        "signature": table_signature(total, columns),
        "sql_profile": sql_profile, "date_parse_specs": date_specs, "status": "sql_done",
    }
    await upsert_profile(rec)
    return rec


# ── Pilot selection ───────────────────────────────────────────────────────────
def _type_key(rec: dict) -> str:
    """A coarse 'type of dataset' bucket used to span all kinds in the pilot."""
    schema = rec.get("schema", "public")
    if schema != "public":
        return schema  # odata / knesset / idx
    return f"public:{rec.get('source_type') or rec.get('kind') or 'ckan'}"


def select_pilot(catalog: list[dict], n: int = 20) -> list[dict]:
    """Pick ~n tables spanning every type bucket (round-robin), skipping empties
    and OVER's own artifacts. Deterministic given the catalog order."""
    buckets: dict[str, list[dict]] = {}
    for rec in catalog:
        tbl = rec.get("table", "")
        if tbl.startswith("over_") or tbl == PROFILE_TABLE:
            continue
        if (rec.get("est_rows") or 0) == 0 and rec.get("schema") == "public":
            # allow it, but deprioritise: append it after non-empty ones
            pass
        buckets.setdefault(_type_key(rec), []).append(rec)
    # Sort each bucket: non-empty first, then by title for determinism.
    for b in buckets.values():
        b.sort(key=lambda r: (0 if (r.get("est_rows") or 0) > 0 else 1, r.get("title") or r.get("table")))
    picked: list[dict] = []
    keys = sorted(buckets.keys())
    i = 0
    while len(picked) < n and any(buckets.values()):
        k = keys[i % len(keys)]
        if buckets[k]:
            picked.append(buckets[k].pop(0))
        i += 1
        if i > n * len(keys) + len(keys):
            break
    return picked[:n]


# ── LLM enrichment ────────────────────────────────────────────────────────────
def llm_provider() -> str | None:
    """DeepSeek preferred, then Anthropic — mirrors ocal_enrich.ai_ner_provider."""
    if settings.deepseek_api_key:
        return "deepseek"
    if settings.anthropic_api_key:
        return "anthropic"
    return None


def llm_available() -> bool:
    return bool(llm_provider())


_ENRICH_PROMPT = """אתה מקטלג מאגרי מידע ממשלתיים בישראל. מקבל תקציר סטטיסטי \
של טבלה אחת (שם, מספר שורות, ולכל עמודה: הסוג שזוהה, שיעור מילוי, מונה ערכים \
ייחודיים, טווח min/max למספרים ותאריכים, ערכים שכיחים, וניחוש-ישות ראשוני).

החזר JSON יחיד (ללא טקסט מסביב) במבנה:
{
  "summary_he": "פסקה קצרה בעברית: מה המאגר מכיל, מה טווח הזמן (מתאריכי ה-min/max), וגודלו",
  "columns": {
     "<שם עמודה>": {
        "description_he": "משפט מה השדה מייצג",
        "semantic_type": "locality|municipality|person|corporation|date|amount|id|category|code|coordinate|free_text|other",
        "date_format_ok": true
     }
  },
  "tags": ["תגיות בעברית"],
  "keywords": ["מילות מפתח בולטות"]
}
כללים: אל תמציא מספרים — טווחים/מונים מגיעים מהקלט בלבד. סווג semantic_type לכל עמודה. \
אם עמודה נראית שם יישוב/רשות מקומית/אדם/תאגיד — סמן זאת. תמציתי."""


def _parse_json(raw: str) -> dict:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw).strip()
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        i, j = raw.find("{"), raw.rfind("}")
        if i >= 0 and j > i:
            try:
                obj = json.loads(raw[i:j + 1])
            except (ValueError, TypeError):
                return {}
        else:
            return {}
    return obj if isinstance(obj, dict) else {}


def _compact_for_llm(rec: dict, sample_rows: list[dict] | None) -> str:
    """Trim the SQL profile to the fields the LLM needs (keeps tokens down)."""
    prof = rec.get("sql_profile", {})
    cols = {}
    for name, c in (prof.get("columns") or {}).items():
        cols[name] = {
            "kind": c.get("detected_kind"),
            "fill_rate": c.get("fill_rate"),
            "distinct": c.get("distinct_est"),
            "min": c.get("min"), "max": c.get("max"),
            "top": [t["value"] for t in (c.get("top_values") or [])][:8],
            "guess": c.get("entity_guess", {}).get("guess"),
        }
    payload = {
        "table": rec.get("table_name"),
        "title": rec.get("title"),
        "rows": rec.get("row_count"),
        "keywords": [k["token"] for k in (prof.get("keywords") or [])][:20],
        "columns": cols,
        "sample": (sample_rows or [])[:5],
    }
    return json.dumps(payload, ensure_ascii=False)


async def _llm_call(user: str) -> dict:
    provider = llm_provider()
    if provider == "deepseek":
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url=_DEEPSEEK_BASE_URL)
        resp = await client.chat.completions.create(
            model=_DEEPSEEK_MODEL, temperature=0, max_tokens=4000,
            response_format={"type": "json_object"},
            messages=[{"role": "system", "content": _ENRICH_PROMPT},
                      {"role": "user", "content": user}],
        )
        return _parse_json(resp.choices[0].message.content or "")
    if provider == "anthropic":
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        resp = await client.messages.create(
            model=_ANTHROPIC_MODEL, max_tokens=4000,
            system=_ENRICH_PROMPT, messages=[{"role": "user", "content": user}])
        txt = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        return _parse_json(txt)
    return {}


async def enrich_profile(schema: str, table: str, db=None, *, sample_rows: list[dict] | None = None) -> dict:
    """Add the LLM enrichment layer on top of a stored SQL profile.

    Budget-guarded (llm_budget.reserve_llm_call) and no-op when no provider is
    configured. Returns the enrichment dict (empty on skip/failure)."""
    if not llm_available():
        logger.info("enrich_profile: no LLM provider configured; skipping")
        return {}
    rec = await get_profile(schema, table)
    if not rec:
        raise ValueError(f"no SQL profile for {schema}.{table} — run profile_table first")

    if db is not None:
        from app.services.llm_budget import reserve_llm_call
        if not await reserve_llm_call(db):
            logger.warning("enrich_profile: daily LLM budget exhausted; skipping %s.%s", schema, table)
            return {}

    user = _compact_for_llm(rec, sample_rows)
    try:
        result = await _llm_call(user)
    except Exception:  # noqa: BLE001
        logger.exception("enrich_profile LLM call failed for %s.%s", schema, table)
        return {}
    summary = result.get("summary_he")
    await save_enrichment(schema, table, result, summary)
    return result


# ── Orchestration ─────────────────────────────────────────────────────────────
async def _load_locality_names(db) -> set[str]:
    """Normalized locality names + aliases from the CBS gazetteer (main DB)."""
    try:
        from sqlalchemy import select
        from app.models.cbs_gazetteer import CbsGazetteer
        rows = (await db.execute(select(CbsGazetteer.name, CbsGazetteer.aliases))).all()
    except Exception:  # noqa: BLE001 — gazetteer may be empty/unloaded
        logger.info("locality gazetteer unavailable; entity heuristic runs without it")
        return set()
    names: set[str] = set()
    for name, aliases in rows:
        if name:
            names.add(_norm_place(name))
        for a in (aliases or []):
            if a:
                names.add(_norm_place(str(a)))
    return names


async def _run_over_records(db, records: list[dict], *, enrich: bool, force: bool,
                            use_public_budget: bool) -> dict:
    """Profile a list of catalog records (SQL always; LLM if ``enrich``). Shared
    engine for the pilot and the whole-catalog backfill. ``use_public_budget``
    routes the LLM call through the public cbs daily budget (True, for the
    anonymous-facing paths) or bypasses it (False, for trusted admin/worker runs)."""
    localities = await _load_locality_names(db)
    done, skipped, errors, enriched = [], [], [], 0
    for rec in records:
        schema, table = rec.get("schema", "public"), rec["table"]
        if table.startswith("over_") or table == PROFILE_TABLE:
            continue
        try:
            if not force:
                existing = await get_profile(schema, table)
                sig = table_signature(rec.get("est_rows"), rec.get("columns") or [])
                if existing and existing.get("signature") == sig and existing.get("status") in ("sql_done", "enriched"):
                    skipped.append(f"{schema}.{table}")
                    continue
            await profile_table(
                schema, table, columns=rec.get("columns"), est_rows=rec.get("est_rows"),
                locality_names=localities, title=rec.get("title"), kind=rec.get("kind"))
            done.append(f"{schema}.{table}")
            if enrich and llm_available():
                sample = await append_store.sample_rows(table, schema=schema, limit=5)
                res = await enrich_profile(schema, table, db if use_public_budget else None,
                                           sample_rows=sample.get("rows"))
                if res:
                    enriched += 1
        except Exception as exc:  # noqa: BLE001 — one bad table must not stop the run
            logger.exception("profiling failed for %s.%s", schema, table)
            errors.append({"table": f"{schema}.{table}", "error": str(exc)})
    return {"profiled": done, "skipped": skipped, "enriched": enriched, "errors": errors}


async def run_pilot(db, *, n: int = 20, enrich: bool = True, force: bool = False) -> dict:
    """Profile a diverse pilot of ~n tables spanning every source type."""
    from app.services.data_catalog import build_catalog
    await ensure_profile_table()
    catalog = await build_catalog(db, use_cache=False)
    pilot = select_pilot(catalog, n=n)
    res = await _run_over_records(db, pilot, enrich=enrich, force=force, use_public_budget=True)
    return {**res, "pilot_size": len(pilot), "buckets": sorted({_type_key(r) for r in pilot})}


async def run_all(db, *, enrich: bool = True, force: bool = False) -> dict:
    """Backfill: profile EVERY table in the catalog. Trusted admin action — LLM
    calls BYPASS the public cbs budget. Signature-skips unchanged tables unless
    ``force``. Run in the background (a full sweep is minutes-to-hours)."""
    from app.services.data_catalog import build_catalog
    await ensure_profile_table()
    catalog = await build_catalog(db, use_cache=False)
    res = await _run_over_records(db, catalog, enrich=enrich, force=force, use_public_budget=False)
    return {**res, "catalog_size": len(catalog)}


# ── Per-dataset auto-profiling (poll-pipeline hook) ───────────────────────────
# Fire-and-forget tasks are kept referenced here so the event loop does not GC
# them mid-run (the standard asyncio create_task pattern).
_bg_tasks: set = set()


async def profile_dataset(db, dataset, *, is_new: bool = False) -> dict:
    """Profile every NEON table of one dataset after a poll landed new data.

    The SQL layer (ranges, formats, entities) ALWAYS runs — that is the user's
    "re-profile after every update" requirement, and it is free. The LLM
    enrichment layer runs only when it can actually change: a brand-new dataset,
    a table with no profile yet, or a schema change (the column set differs).
    Best-effort and fully guarded — never raises into the poll pipeline."""
    from app.services import data_catalog
    from app.models.version_index import VersionIndex
    from sqlalchemy import select, desc

    if not data_catalog._dataset_is_neon(dataset):
        return {"skipped": "not a NEON-backed dataset"}
    await ensure_profile_table()

    lv = (await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == dataset.id)
        .order_by(desc(VersionIndex.version_number)).limit(1)
    )).scalar_one_or_none()
    maps = (lv.resource_mappings if lv else {}) or {}
    tables = data_catalog._tables_of(dataset, maps)
    localities = await _load_locality_names(db)

    profiled, enriched = [], []
    for t in tables:
        tbl = t.get("table")
        if not tbl:
            continue
        title = dataset.title
        if t.get("resource_name"):
            title = f"{title} — {t['resource_name']}"
        try:
            existing = await get_profile("public", tbl)
            prof = await profile_table("public", tbl, locality_names=localities,
                                       title=title, kind="dataset")
            profiled.append(tbl)
            new_cols = set(((prof.get("sql_profile") or {}).get("columns") or {}).keys())
            old_cols = set((((existing or {}).get("sql_profile") or {}).get("columns") or {}).keys())
            need_enrich = is_new or existing is None or new_cols != old_cols
            if (need_enrich and llm_available()
                    and getattr(settings, "profiler_auto_enrich", True)):
                sample = await append_store.sample_rows(tbl, schema="public", limit=5)
                # db=None ⇒ bypass the PUBLIC cbs daily budget: the worker is a
                # trusted caller, not an anonymous endpoint.
                res = await enrich_profile("public", tbl, None, sample_rows=sample.get("rows"))
                if res:
                    enriched.append(tbl)
        except Exception:  # noqa: BLE001 — one table must not break the rest
            logger.exception("profile_dataset: failed on %s", tbl)
    return {"profiled": profiled, "enriched": enriched}


async def _profile_after_poll_bg(dataset_id, is_new: bool) -> None:
    if not getattr(settings, "profiler_auto_enabled", True):
        return
    from app.database import async_session
    from app.models.tracked_dataset import TrackedDataset
    from sqlalchemy import select
    try:
        async with async_session() as db:
            ds = (await db.execute(
                select(TrackedDataset).where(TrackedDataset.id == dataset_id)
            )).scalar_one_or_none()
            if ds:
                res = await profile_dataset(db, ds, is_new=is_new)
                logger.info("auto-profile %s (new=%s): %s", dataset_id, is_new, res)
    except Exception:  # noqa: BLE001
        logger.exception("auto-profile bg failed for %s", dataset_id)


def kick_profile_after_poll(dataset_id, is_new: bool) -> None:
    """Schedule a background profile of a dataset whose poll just landed new data.

    Non-blocking and never raises — safe to call from the critical poll path.
    Does nothing if the profiler is disabled or no event loop is running."""
    if not getattr(settings, "profiler_auto_enabled", True):
        return
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # no loop (shouldn't happen in the async worker) — skip silently
    task = loop.create_task(_profile_after_poll_bg(dataset_id, is_new))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
