"""The cold-tier reader: phase 3 of the history storage plan.

docs/history-phase3-plan.md is the contract; this is its implementation. Since
phase 2, rows closed long ago can leave SQL for immutable Parquet parts on R2
(app/services/history_export.py, catalogued in ``app.history_parts``). A part
with ``deleted_from_sql=true`` holds rows that exist ONLY in R2. This module
lets the same read-only SQL see them again.

HOW A QUERY RUNS
----------------
DuckDB, in the app's own process, one fresh in-memory connection per query
(isolated memory, and ``interrupt()`` stops exactly one query):

1. ``validate_sql``: the consoles' own ``validate_readonly_sql`` (one SELECT /
   WITH, no write keywords), plus a DuckDB deny list: ``attach``, ``install``,
   ``load``, ``pragma``, ``set`` and friends, and every function that reads a
   file, a URL, a setting or a secret (``read_*``, ``*_scan``, ``glob``,
   ``current_setting``, ``duckdb_*``...). The Postgres consoles keep their list
   unchanged: ``"export"`` is a plausible column name there.
2. The hot side is attached as ``hot``: DuckDB's ``postgres`` extension with the
   console's READ-ONLY role (APPEND_READONLY_DATABASE_URL), so the engine cannot
   see more than the console does. Where the extension cannot be loaded, the
   rows are pulled with asyncpg instead (``ArrowHot``), capped at
   HISTORY_QUERY_HOT_MAX_ROWS. ``ensure_extensions`` decides which at boot.
3. For every catalogued table the SQL names (``extract_table_refs``), a view
   under the same name: for a table with deleted parts the UNION ALL of the hot
   table and ``read_parquet`` of those parts (``build_history_view``), for any
   other table a plain view of the hot one. Only ``deleted_from_sql=true`` parts:
   a part still in SQL is in both places and would count twice. Before the
   first view, ONE pass reads the footer of every part of every table
   (``_warm_metadata``, METADATA_THREADS in parallel): the connection caches
   Parquet and HTTP metadata, so the binds that follow (each DESCRIBE, the
   views, the query) never go back to R2 for a footer. Without it a union
   view over 180 parts read every footer about five times, one table at a
   time, serially: past the timeout even for count(*) (phase 4, 6.8/6.9).
4. ``_lock``: external access off except for the exact part URLs and the spill
   directory, configuration locked. Credentials live in TEMPORARY secrets, which
   DuckDB redacts, never in a setting or an ATTACH string a query could read.
5. The query, wrapped with a row cap, on a worker thread. The event loop
   enforces the timeout with ``conn.interrupt()``.

One query at a time (semaphore 1), up to HISTORY_QUERY_QUEUE waiting, beyond
that ``EngineBusy`` (429). ``memory_limit`` and a temp directory make a big
query spill rather than take the app down.

The result has the shape of ``append_store.run_readonly_sql`` (columns, fields,
rows, truncated, row_count) plus ``engine: "duckdb"``, ``cold_parts`` and
``elapsed_ms``, so every client keeps working.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import tempfile
import time
from contextlib import asynccontextmanager
import types
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable
from urllib.parse import parse_qsl, unquote, urlsplit

from app.config import settings
from app.services import append_store
from app.services.append_store import _qi

# Imported on first use, not with the module: the API layers import this
# module on every boot, and with the flag off the library must not load.
duckdb = None


def _duckdb():
    global duckdb
    if duckdb is None:
        import duckdb as _d
        duckdb = _d
    return duckdb


def _installed() -> bool:
    import importlib.util

    return duckdb is not None or importlib.util.find_spec("duckdb") is not None

logger = logging.getLogger(__name__)

UTC = timezone.utc
HOT = "hot"
FIRST_COL = "first_seen"
LAST_COL = append_store.VALIDITY_COL
HASH_COL = "row_hash"
THREADS = 2
# The footer pass is HTTP round trips (one HEAD + one GET per part), not CPU:
# 180 parts took 26 s at 2 threads and 7 s at 8 (phase 4, 6.9). The query
# itself runs at THREADS; memory_limit bounds both.
METADATA_THREADS = 8
_TS_COLS = {FIRST_COL, LAST_COL}
# PostGIS geometry does not survive the trip and is not in the Parquet parts;
# geometry_wkt, where a source has it, is ordinary text and stays.
_EXCLUDE = {"geom"}


class HistoryQueryError(ValueError):
    """A query the engine could not answer; the message is for the user."""


class EngineBusy(HistoryQueryError):
    """The engine is running one query and its queue is full (HTTP 429)."""


class HistoryTimeout(HistoryQueryError):
    """The query ran past HISTORY_QUERY_TIMEOUT_S and was interrupted."""


class HistoryUnavailable(HistoryQueryError):
    """The engine is off or could not start (see ``is_available``)."""


# ── state ────────────────────────────────────────────────────────────────────

def _new_stats() -> dict:
    return {"queries": 0, "ok": 0, "errors": 0, "timeouts": 0, "busy": 0,
            "last_error": None, "last_elapsed_ms": None, "last_at": None}


_stats: dict = _new_stats()
# Filled by ensure_extensions at boot. hot_mode: "attach" (the postgres
# extension) or "arrow" (asyncpg rows); None until checked.
_ext: dict = {"checked_at": None, "httpfs": None, "postgres": None, "hot_mode": None,
              "error": None}


class _Gate:
    """Semaphore 1 with a bounded queue. Rebinds to the running loop, so a
    process that runs several loops (the test suite) never shares one."""

    def __init__(self):
        self.active = 0
        self.waiting = 0
        self._sem: asyncio.Semaphore | None = None
        self._loop = None

    @asynccontextmanager
    async def admit(self, queue: int):
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            self._loop, self._sem = loop, asyncio.Semaphore(1)
            self.active = self.waiting = 0
        if (self._sem.locked() or self.active) and self.waiting >= max(0, int(queue)):
            _stats["busy"] += 1
            raise EngineBusy("מנוע ההיסטוריה עסוק בשאילתות אחרות; נסו שוב בעוד רגע")
        self.waiting += 1
        try:
            await self._sem.acquire()
        finally:
            self.waiting -= 1
        self.active += 1
        try:
            yield
        finally:
            self.active -= 1
            self._sem.release()


_gate = _Gate()


def reset_state() -> None:
    """Fresh counters and gate (tests)."""
    global _gate, _stats
    _gate = _Gate()
    _stats = _new_stats()


def is_available() -> tuple[bool, str]:
    """``(ok, reason)``. Off unless HISTORY_QUERY_ENABLED; then it needs DuckDB,
    the read-only role, object storage and the boot check of the extensions."""
    if not settings.history_query_enabled:
        return False, "disabled"
    if not _installed():
        return False, "duckdb is not installed"
    if not (settings.append_readonly_database_url or "").strip():
        return False, "APPEND_READONLY_DATABASE_URL is not set"
    if not (settings.s3_bucket and settings.s3_endpoint):
        return False, "object storage (R2_*) is not configured"
    if _ext["checked_at"] is None:
        return False, "extensions not checked yet"
    if _ext["httpfs"] != "ok":
        return False, f"httpfs: {_ext['httpfs']}"
    if _ext["hot_mode"] is None:
        return False, f"no way to read the hot tables: {_ext.get('error')}"
    return True, ""


def status() -> dict:
    ok, why = is_available()
    return {
        "enabled": settings.history_query_enabled,
        "available": ok,
        "reason": why or None,
        "duckdb_version": getattr(duckdb, "__version__", None) if duckdb else None,
        "extensions": {"httpfs": _ext["httpfs"], "postgres": _ext["postgres"],
                       "checked_at": _ext["checked_at"]},
        "hot_mode": _ext["hot_mode"],
        "active": _gate.active,
        "waiting": _gate.waiting,
        "queue": settings.history_query_queue,
        "memory_mb": settings.history_query_memory_mb,
        "threads": THREADS,
        "metadata_threads": METADATA_THREADS,
        "timeout_s": settings.history_query_timeout_s,
        "hot_max_rows": settings.history_query_hot_max_rows,
        "stats": dict(_stats),
    }


# ── pure helpers ─────────────────────────────────────────────────────────────

def _lit(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def _as_date(v) -> date:
    return v if isinstance(v, date) and not isinstance(v, datetime) else date.fromisoformat(str(v)[:10])


def _skeleton(sql: str) -> str:
    """Comments removed and single-quoted values blanked (see append_store)."""
    s = append_store._strip_sql_comments(sql)
    if append_store._LITERAL_SYNTAX_UNMODELLED.search(s):
        return s
    return append_store._strip_sql_comments(sql, blank_literals=True)


_QUOTED_IDENT = re.compile(r'"(?:[^"]|"")*"')
_DENY_WORDS = re.compile(
    r"\b(attach|detach|install|load|export|import|pragma|set|reset|checkpoint|use|copy"
    r"|insert|update|delete|drop|alter|create|truncate|grant|revoke|call|vacuum)\b",
    re.IGNORECASE)
# Anything that reads a file, a URL, a setting, a secret or the catalog of
# attached databases. DuckDB itself blocks the file/URL ones once external
# access is off (see _lock); this turns them into a clear message first.
_DENY_FUNCS = re.compile(
    r"\b(duckdb_\w+|pragma_\w+|read_\w+|\w+_scan|parquet_\w+|postgres_\w+|sqlite_\w+"
    r"|mysql_\w+|iceberg_\w+|delta_\w+|current_setting|getenv|which_secret|glob|query"
    r"|query_table|sniff_csv|load_aws_credentials|enable_\w+|disable_\w+)\s*\(",
    re.IGNORECASE)
_DENY_RELATIONS = re.compile(r"\b(duckdb_\w+|pragma_\w+)\b", re.IGNORECASE)


def validate_sql(sql: str) -> str:
    """The consoles' guards plus DuckDB's. Returns the statement or ValueError."""
    s = append_store.validate_readonly_sql(sql)
    skel = _skeleton(s)
    unquoted = skel.replace('"', "")
    if (_DENY_WORDS.search(_QUOTED_IDENT.sub('""', skel))
            or _DENY_FUNCS.search(unquoted) or _DENY_RELATIONS.search(unquoted)):
        raise ValueError("במנוע ההיסטוריה מותרת רק קריאה מטבלאות האתר: אסורות פקודות "
                         "הגדרה/טעינה ופונקציות שקוראות קבצים, כתובות או הגדרות")
    return s


_TOKEN = re.compile(r'"((?:[^"]|"")*)"|([A-Za-z_][A-Za-z0-9_$]*)')


def extract_table_refs(sql: str, known_tables: Iterable) -> list[str]:
    """Catalogued table names the statement mentions, in order, once each.

    ``known_tables`` holds names or ``(schema, name)`` pairs. A bare word folds
    to lower case, as Postgres does; a quoted one is taken as written. Names in
    comments and string literals do not count. A false positive only costs a
    view nobody reads."""
    names = {(k[1] if isinstance(k, (tuple, list)) else k) for k in known_tables}
    out: list[str] = []
    for m in _TOKEN.finditer(_skeleton(sql or "")):
        name = m.group(1).replace('""', '"') if m.group(1) is not None else m.group(2).lower()
        if name in names and name not in out:
            out.append(name)
    return out


def _cast(col: str) -> str:
    return "TIMESTAMPTZ" if col in _TS_COLS else "VARCHAR"


def build_history_view(table: str, parts: list[dict], hot_cols: list[str],
                       parquet_cols: list[str] | None = None, *, schema: str = "public",
                       url_of: Callable[[str], str] | None = None,
                       validity: str = "", cold: bool = False,
                       ident_col: str | None = None) -> str | None:
    """``CREATE VIEW`` uniting the hot table with its deleted parts, or None.

    Columns: the hot table's, then any only the parts have; a column one side
    lacks is a typed NULL there. Every source column is VARCHAR (that is how the
    parts store them), ``first_seen``/``last_seen`` TIMESTAMPTZ. ``geom`` is
    left out. ``validity`` (see validity_sql) narrows the whole union.

    Phase 4: ``cold`` (a table moved whole to ``full`` parts, empty in SQL)
    reads the parts alone, no hot side. A part flagged ``partial`` (a closed
    part whose rows the window is deleting, still ``deleted_from_sql=false``)
    joins as its own branch WITHOUT the rows still in SQL: an anti-join on
    ``(ident_col, last_seen)`` against the hot table, so each row shows once."""
    if not parts:
        return None
    url_of = url_of or _part_url
    ordered = sorted(parts, key=lambda p: (_as_date(p["closed_month"]), int(p["part"])))
    whole = [url_of(p["r2_key"]) for p in ordered if not p.get("partial")]
    partial = [url_of(p["r2_key"]) for p in ordered if p.get("partial")]
    hot = [] if cold else [c for c in hot_cols if c not in _EXCLUDE]
    pq_cols = [c for c in (parquet_cols if parquet_cols is not None else hot) if c not in _EXCLUDE]
    cols = hot + [c for c in pq_cols if c not in hot]

    def side(have: list[str]) -> str:
        return ", ".join(
            f"CAST({_qi(c)} AS {_cast(c)}) AS {_qi(c)}" if c in have
            else f"NULL::{_cast(c)} AS {_qi(c)}" for c in cols)

    def files(keys: list[str]) -> str:
        return f"read_parquet([{', '.join(_lit(k) for k in keys)}], union_by_name=true)"

    hot_ref = f"{HOT}.{_qi(schema)}.{_qi(table)}"
    branches = [] if cold else [f"SELECT {side(hot)} FROM {hot_ref}"]
    if whole:
        branches.append(f"SELECT {side(pq_cols)} FROM {files(whole)}")
    if partial:
        i = _qi(ident_col or HASH_COL)
        branches.append(
            f"SELECT {side(pq_cols)} FROM {files(partial)} c WHERE NOT EXISTS "
            f"(SELECT 1 FROM {hot_ref} h WHERE CAST(h.{i} AS VARCHAR) = CAST(c.{i} AS VARCHAR) "
            f'AND h."last_seen" = c."last_seen")')
    body = "\nUNION ALL\n".join(branches)
    if validity:
        body = f"SELECT * FROM (\n{body}\n) _h WHERE {validity}"
    return f"CREATE OR REPLACE VIEW {_qi(schema)}.{_qi(table)} AS\n{body}"


def validity_sql(*, as_of: datetime | None = None, current: bool = False,
                 has_last_seen: bool = True) -> str:
    """DuckDB twin of append_store.validity_where, with the moment inlined as a
    typed literal (views take no parameters). ``as_of`` must be a datetime."""
    if as_of is not None and current:
        raise ValueError("as_of and current are mutually exclusive")
    if current:
        return f"{_qi(LAST_COL)} IS NULL" if has_last_seen else ""
    if as_of is None:
        return ""
    if not isinstance(as_of, datetime):
        raise ValueError("as_of must be a datetime")
    at = as_of if as_of.tzinfo else as_of.replace(tzinfo=UTC)
    ts = f"TIMESTAMPTZ {_lit(at.astimezone(UTC).isoformat())}"
    if not has_last_seen:
        return f"{_qi(FIRST_COL)} <= {ts}"
    return (f"{_qi(FIRST_COL)} <= {ts} AND "
            f"({_qi(LAST_COL)} IS NULL OR {_qi(LAST_COL)} > {ts})")


def should_use_history(as_of: datetime | None, parts: list[dict], *, current: bool = False,
                       enabled: bool | None = None) -> bool:
    """Does answering need the rows that are only in R2?

    Never for the rows in force now (they are never exported), never without
    deleted parts. For a moment ``as_of``: only if some deleted part still held
    a row open after it, i.e. its ``last_seen_max`` is later; once every deleted
    row had closed, SQL alone is the exact answer and stays fast. With no moment
    (the whole history), whenever there are deleted parts. Off with the flag,
    unless ``enabled`` says otherwise (to tell the user what they are missing)."""
    on = settings.history_query_enabled if enabled is None else enabled
    if not on or current:
        return False
    deleted = [p for p in parts if p.get("deleted_from_sql") or p.get("partial")]
    if not deleted:
        return False
    if as_of is None:
        return True
    return any(p.get("last_seen_max") is None or p["last_seen_max"] > as_of for p in deleted)


_HINTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"~\*|!~|function with name ~|regexp", re.IGNORECASE),
     "ביטויים רגולריים בסגנון Postgres (~*, !~) לא נתמכים במנוע ההיסטוריה; השתמשו "
     "ב-regexp_matches(עמודה, 'ביטוי') (עם 'i' כפרמטר שלישי לחיפוש בלי רישיות)."),
    (re.compile(r"\bst_\w+|geometry", re.IGNORECASE),
     "פונקציות PostGIS (ST_*) לא זמין בהיסטוריה: הגאומטריה אינה בקבצי ההיסטוריה. "
     "geometry_wkt, אם קיימת, זמינה כטקסט."),
    (re.compile(r"INT128|uuid", re.IGNORECASE),
     "בתצוגת ההיסטוריה row_hash ושאר העמודות הם טקסט: השוו כטקסט (row_hash = '...') "
     "בלי ::uuid."),
    (re.compile(r"to_char|to_date|to_timestamp\(.*,", re.IGNORECASE),
     "במנוע ההיסטוריה (DuckDB) עיצוב ופענוח תאריכים נעשים ב-strftime / strptime."),
    (re.compile(r"Table with name .* does not exist", re.IGNORECASE),
     "במנוע ההיסטוריה זמינות הטבלאות שהשאילתה מזכירה בשמן המלא מתוך קטלוג האתר "
     "(בשאילתה של מאגר: טבלאות המאגר בלבד)."),
    (re.compile(r"Referenced column .* not found", re.IGNORECASE),
     "עמודות המקור בתצוגת ההיסטוריה הן טקסט, ו-geom אינה כלולה. עמודה שנוספה "
     "מאוחר יותר ריקה (NULL) בשורות הישנות."),
]

_DIALECT_REGEX = re.compile(r"(?<![~!])~(?![~*])")
DIALECT_NOTE = ("שימו לב: ב-DuckDB האופרטור ~ בודק התאמה מלאה של הביטוי, ולא חלקית "
                "כמו ב-Postgres. לחיפוש חלקי השתמשו ב-regexp_matches(עמודה, 'ביטוי').")


def duckdb_error_hint(exc) -> str:
    """The DuckDB message plus a Hebrew hint for the usual Postgres habits."""
    msg = (str(exc) or "").strip()
    import sys

    mod = sys.modules.get("duckdb")
    is_duck = mod is not None and isinstance(exc, mod.Error)
    if not is_duck:
        return msg
    first = msg.splitlines()[0] if msg else msg
    for pat, hint in _HINTS:
        if pat.search(first):
            return f"{first}\n{hint}"
    return f"{first}\n(השאילתה רצה על מנוע ההיסטוריה, DuckDB, שהדיאלקט שלו קרוב ל-Postgres אך אינו זהה.)"


def _field_type(t: str) -> str:
    u = str(t or "").upper()
    if u.startswith(("TIMESTAMP", "DATE", "TIME")):
        return "timestamp"
    if u in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT",
             "UINTEGER", "UBIGINT", "UHUGEINT"):
        return "int"
    if u in ("DOUBLE", "FLOAT", "REAL") or u.startswith("DECIMAL"):
        return "numeric"
    if u == "BOOLEAN":
        return "bool"
    return "text"


def _plain(v):
    return v if (v is None or isinstance(v, (str, int, float, bool))) else str(v)


# ── catalog ──────────────────────────────────────────────────────────────────

def _part_url(key: str) -> str:
    return f"s3://{settings.s3_bucket}/{key}"


async def _catalog_parts(table: str) -> list[dict]:
    """Every catalog row of ``table``, each with its dataset's ``window_days``."""
    from sqlalchemy import select

    from app.database import async_session
    from app.models.history_part import HistoryPart
    from app.models.tracked_dataset import TrackedDataset
    from app.services import history_export

    async with async_session() as db:
        rows = (await db.execute(
            select(HistoryPart, TrackedDataset.scraper_config)
            .join(TrackedDataset, TrackedDataset.id == HistoryPart.dataset_id, isouter=True)
            .where(HistoryPart.table_name == table))).all()
        return [{**history_export._part_dict(p),
                 "window_days": history_export.window_days_of(
                     types.SimpleNamespace(scraper_config=cfg))} for p, cfg in rows]


def _is_partial(p: dict, now: datetime) -> bool:
    """A closed part the window has begun deleting but that is not marked yet:
    its dataset has a window and its oldest row is past it (history_export.
    delete_exported deletes those rows and marks the part only when ALL are).
    The anti-join is exact either way; this only decides when to pay for it."""
    w = p.get("window_days")
    lmin = p.get("last_seen_min")
    return (not p.get("deleted_from_sql") and (p.get("kind") or "closed") == "closed"
            and bool(w) and lmin is not None and lmin < now - timedelta(days=int(w)))


async def cold_parts_for(table: str) -> list[dict]:
    """The parts of ``table`` with rows that are ONLY in R2, in (month, part)
    order: the deleted ones (closed or full), and the partially deleted closed
    ones flagged ``partial`` (read with an anti-join, build_history_view). A
    table moved to the cold tier has no partial parts: its SQL side is empty,
    and the full parts already hold those rows.
    Never raises: an unreadable catalog answers "none", i.e. "SQL is all"."""
    try:
        parts = await _catalog_parts(table)
    except Exception:  # noqa: BLE001
        logger.debug("history query: catalog read failed for %s", table, exc_info=True)
        return []
    now = datetime.now(UTC)
    cold = is_cold_spec(parts)
    out = []
    for p in parts:
        if p.get("deleted_from_sql"):
            out.append(p)
        elif not cold and _is_partial(p, now):
            out.append({**p, "partial": True})
    return sorted(out, key=lambda p: (_as_date(p["closed_month"]), int(p["part"])))


def is_cold_spec(parts: list[dict]) -> bool:
    """A table whose rows all live in full parts: read without a hot side."""
    return any(p.get("kind") == "full" and p.get("deleted_from_sql") for p in parts)


async def union_branches(view: str) -> list[tuple[str, str]] | None:
    """``[(table, label)]`` of a union view (union_views.py), or None."""
    from app.services import union_views

    found = union_views.spec_for_view(view)
    if not found:
        return None
    ds_id, spec = found
    from app.database import async_session

    async with async_session() as db:
        return await union_views._branches(db, ds_id, spec)


async def _ident_for(table: str) -> str:
    """The column a table dedups its open rows on (anti-join of partial parts)."""
    from app.services import history_export

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        found = await history_export._table_ident(conn, table)
    return found[0] if found else HASH_COL


# Tables (and union views over them) moved to the cold tier: empty in SQL. Held
# in memory so the /data console can warn about one without a catalog read on
# every query; refreshed at boot and after every tier move.
_cold_tables: set = set()


def cold_tables() -> set:
    return set(_cold_tables)


async def refresh_cold_tables() -> set:
    """Reload ``_cold_tables`` from the catalog. Never raises."""
    global _cold_tables
    try:
        from sqlalchemy import select

        from app.database import async_session
        from app.models.history_part import HistoryPart
        from app.services import union_views

        async with async_session() as db:
            names = {r[0] for r in (await db.execute(
                select(HistoryPart.table_name).distinct().where(
                    HistoryPart.kind == "full", HistoryPart.deleted_from_sql.is_(True)))).all()}
        for spec in union_views.UNION_VIEWS.values():
            branches = await union_branches(spec["view"]) or []
            if names & {t for t, _l in branches}:
                names.add(spec["view"])
        _cold_tables = names
    except Exception:  # noqa: BLE001
        logger.debug("history query: cold-table refresh failed", exc_info=True)
    return set(_cold_tables)


def build_union_view(view: str, label_column: str, branches: list,
                     columns: dict, *, schema: str = "public") -> str | None:
    """DuckDB twin of union_views.view_sql over the engine's own table views:
    UNION ALL of the branches with the label as a column. Pure."""
    skip = {"row_hash", "_row_hash", "geom"}
    order: list[str] = []
    for t, _l in branches:
        for c, _ty in columns.get(t, []):
            if c not in skip and c not in order:
                order.append(c)
    if not order:
        return None
    parts = []
    for t, label in branches:
        have = {c for c, _ty in columns.get(t, [])}
        if not have:
            continue
        cols = ", ".join(_qi(c) if c in have else f"NULL::{_cast(c)} AS {_qi(c)}" for c in order)
        parts.append(f"SELECT {_lit(label)}::VARCHAR AS {_qi(label_column)}, {cols} "
                     f"FROM {_qi(schema)}.{_qi(t)}")
    if not parts:
        return None
    return (f"CREATE OR REPLACE VIEW {_qi(schema)}.{_qi(view)} AS\n"
            + "\nUNION ALL\n".join(parts))


# ── the hot side ─────────────────────────────────────────────────────────────

def _pg_secret_parts(raw: str) -> tuple[dict, str]:
    """``(secret fields, non-secret libpq options)`` from the read-only URL."""
    from app.pg_ssl import host_is_internal

    u = urlsplit((raw or "").strip())
    fields = {
        "HOST": u.hostname or "",
        "PORT": str(u.port or 5432),
        "DATABASE": unquote((u.path or "/").lstrip("/")),
        "USER": unquote(u.username or ""),
        "PASSWORD": unquote(u.password or ""),
    }
    q = dict(parse_qsl(u.query))
    sslmode = q.get("sslmode") or ("disable" if host_is_internal(u.hostname or "") else "require")
    return fields, f"sslmode={sslmode}"


class AttachHot:
    """The hot tables through DuckDB's postgres extension, as the read-only role."""

    mode = "attach"

    async def prepare(self, specs: list[dict], *, as_of=None, current: bool = False) -> None:
        return None

    def setup(self, conn) -> None:
        fields, opts = _pg_secret_parts(settings.append_readonly_database_url)
        conn.execute("LOAD postgres")
        conn.execute("CREATE TEMPORARY SECRET hot_pg (TYPE postgres, "
                     + ", ".join(f"{k} {_lit(v)}" for k, v in fields.items()) + ")")
        conn.execute(f"ATTACH {_lit(opts)} AS {HOT} (TYPE postgres, SECRET hot_pg, READ_ONLY)")


class ArrowHot:
    """The hot tables pulled with asyncpg (the read-only pool) and attached as an
    in-memory ``hot`` database of the same shape. The fallback when the postgres
    extension cannot load; capped at HISTORY_QUERY_HOT_MAX_ROWS in all."""

    mode = "arrow"

    def __init__(self):
        self._tables: list[tuple[str, str, object]] = []
        self._rows = 0

    def _count(self, n: int) -> None:
        self._rows += int(n)
        cap = settings.history_query_hot_max_rows
        if self._rows > cap:
            raise HistoryQueryError(
                f"במצב הנוכחי של המנוע הטבלאות החמות נטענות לזיכרון, ועד {cap:,} שורות; "
                "צמצמו את השאילתה (as_of / טבלה אחת) או נסו שוב מאוחר יותר")

    def load(self, schema: str, table: str, cols: list[str], records: list) -> None:
        import pyarrow as pa

        self._count(len(records))
        data = {}
        for c in cols:
            vals = [r[c] for r in records]
            if c in _TS_COLS:
                data[c] = pa.array(vals, type=pa.timestamp("us", tz="UTC"))
            else:
                data[c] = pa.array([None if v is None else str(v) for v in vals], type=pa.string())
        self._tables.append((schema, table, pa.table(data)))

    async def prepare(self, specs: list[dict], *, as_of=None, current: bool = False) -> None:
        pool = await append_store.get_readonly_pool()
        cap = settings.history_query_hot_max_rows
        async with pool.acquire() as conn:
            for spec in specs:
                if spec.get("cold") or spec.get("union"):
                    continue  # no hot side: its rows are all in R2 / a view of views
                schema, table = spec["schema"], spec["table"]
                cols = [r["column_name"] for r in await conn.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
                    schema, table) if r["column_name"] not in _EXCLUDE]
                if not cols:
                    raise HistoryQueryError(f"הטבלה {schema}.{table} לא נמצאה")
                cond, params = "", []
                if schema == "public" and FIRST_COL in cols:
                    cond, params = append_store.validity_where(
                        as_of=as_of, current=current, has_last_seen=LAST_COL in cols)
                ref = f"{_qi(schema)}.{_qi(table)}"
                where = f" WHERE {cond}" if cond else ""
                async with conn.transaction(readonly=True):
                    n = await conn.fetchval(
                        f"SELECT count(*) FROM (SELECT 1 FROM {ref}{where} LIMIT {cap + 1}) _c",
                        *params)
                    if self._rows + int(n) > cap:
                        self._count(int(n))  # raises
                    recs = await conn.fetch(
                        f"SELECT {', '.join(_qi(c) for c in cols)} FROM {ref}{where}", *params)
                self.load(schema, table, cols, [dict(r) for r in recs])

    def setup(self, conn) -> None:
        conn.execute(f"ATTACH ':memory:' AS {HOT}")
        for schema, table, tbl in self._tables:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {HOT}.{_qi(schema)}")
            conn.register("__hq_arrow", tbl)
            try:
                conn.execute(f"CREATE TABLE {HOT}.{_qi(schema)}.{_qi(table)} AS "
                             "SELECT * FROM __hq_arrow")
            finally:
                conn.unregister("__hq_arrow")


class MemoryHot:
    """A hot side built by a function on the connection (tests)."""

    mode = "memory"

    def __init__(self, build: Callable):
        self._build = build

    async def prepare(self, specs: list[dict], *, as_of=None, current: bool = False) -> None:
        return None

    def setup(self, conn) -> None:
        self._build(conn)


def _hot_backend():
    return AttachHot() if _ext["hot_mode"] == "attach" else ArrowHot()


# ── the connection ───────────────────────────────────────────────────────────

def _open_connection(tmpdir: str | None = None):
    tmpdir = tmpdir or tempfile.mkdtemp(prefix="hq_")
    conn = _duckdb().connect(":memory:", config={
        "memory_limit": f"{int(settings.history_query_memory_mb)}MiB",
        "threads": THREADS,
        "temp_directory": tmpdir,
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    })
    conn.execute("SET TimeZone = 'UTC'")
    # A fresh connection per query, so a cache never outlives the parts it saw:
    # a footer is read once per query, not once per bind of the same part.
    conn.execute("SET parquet_metadata_cache = true")
    conn.execute("SET enable_http_metadata_cache = true")
    return conn


def _setup_r2(conn) -> None:
    conn.execute("LOAD httpfs")
    endpoint = re.sub(r"^https?://", "", settings.s3_endpoint or "").rstrip("/")
    conn.execute(
        "CREATE TEMPORARY SECRET r2 (TYPE s3, "
        f"KEY_ID {_lit(settings.s3_access_key)}, SECRET {_lit(settings.s3_secret_key)}, "
        f"ENDPOINT {_lit(endpoint)}, REGION {_lit(settings.s3_region or 'auto')}, "
        "URL_STYLE 'path')")


def _warm_metadata(conn, urls: list[str]) -> int:
    """Read the footer of every part once, METADATA_THREADS at a time, into
    the connection's caches; the query keeps THREADS. Returns the file count.
    Must run before ``_lock`` (the thread count is configuration)."""
    if not urls:
        return 0
    conn.execute(f"SET threads = {int(METADATA_THREADS)}")
    try:
        return int(conn.execute(
            f"SELECT count(*) FROM parquet_file_metadata([{', '.join(_lit(u) for u in urls)}])"
        ).fetchone()[0])
    finally:
        conn.execute(f"SET threads = {int(THREADS)}")


def _lock(conn, allowed: list[str]) -> None:
    """No file or URL but the given ones (and the spill directory); no further
    configuration changes for the life of the connection."""
    tmp = conn.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    if allowed:
        conn.execute(f"SET allowed_paths = [{', '.join(_lit(a) for a in allowed)}]")
    if tmp:
        conn.execute(f"SET allowed_directories = [{_lit(tmp.rstrip('/') + '/')}]")
    conn.execute("SET enable_external_access = false")
    conn.execute("SET lock_configuration = true")


def _describe(conn, ref: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in conn.execute(f"DESCRIBE {ref}").fetchall()]


def _prepare_views(conn, backend, specs: list[dict], *, as_of, current,
                   search_path: str | None) -> dict[str, list[tuple[str, str]]]:
    """Attach the hot side and create one view per table. Returns the view
    columns ``{table: [(name, type)]}``. Leaves the connection unlocked."""
    if any(p["url"].startswith("s3://") for s in specs for p in s["parts"]):
        _setup_r2(conn)
    _warm_metadata(conn, [p["url"] for s in specs for p in s["parts"]])
    backend.setup(conn)
    created: list[str] = []
    out: dict[str, list[tuple[str, str]]] = {}
    for spec in specs:
        if spec.get("union"):
            continue
        schema, table, parts = spec["schema"], spec["table"], spec["parts"]
        cold = bool(spec.get("cold"))
        hot_ref = f"{HOT}.{_qi(schema)}.{_qi(table)}"
        hot_cols = [] if cold else [c for c, _t in _describe(conn, hot_ref)]
        if schema not in created:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(schema)}")
            created.append(schema)
        pq_cols: list[str] = []
        if parts:
            urls = [p["url"] for p in parts]
            pq_cols = [c for c, _t in _describe(
                conn, f"SELECT * FROM read_parquet([{', '.join(_lit(u) for u in urls)}], "
                      "union_by_name=true)")]
        cond = ""
        if schema == "public" and FIRST_COL in (hot_cols or pq_cols):
            cond = validity_sql(as_of=as_of, current=current,
                                has_last_seen=bool(parts) or LAST_COL in hot_cols)
        if parts:
            by_key = {p["r2_key"]: p["url"] for p in parts}
            conn.execute(build_history_view(table, parts, hot_cols, pq_cols, schema=schema,
                                            url_of=by_key.__getitem__, validity=cond,
                                            cold=cold, ident_col=spec.get("ident")))
        else:
            sel = "*" if not (_EXCLUDE & set(hot_cols)) else (
                f"* EXCLUDE ({', '.join(_qi(c) for c in hot_cols if c in _EXCLUDE)})")
            conn.execute(f"CREATE OR REPLACE VIEW {_qi(schema)}.{_qi(table)} AS "
                         f"SELECT {sel} FROM {hot_ref}" + (f" WHERE {cond}" if cond else ""))
        out[table] = _describe(conn, f"{_qi(schema)}.{_qi(table)}")
    for spec in specs:
        if spec.get("union"):
            sql = build_union_view(spec["table"], spec["label_column"], spec["branches"], out,
                                   schema=spec["schema"])
            if sql:
                conn.execute(sql)
                out[spec["table"]] = _describe(conn, f"{_qi(spec['schema'])}.{_qi(spec['table'])}")
    order = [s.strip() for s in (search_path or "public").split(",") if s.strip()]
    path = [s for s in order if s in created] + [s for s in created if s not in order]
    conn.execute(f"SET search_path = {_lit(','.join(path + ['main']))}")
    return out


def _execute(conn, backend, specs: list[dict], sql, params: list, max_rows: int,
             as_of, current: bool, search_path: str | None) -> dict:
    """Runs on a worker thread. ``sql`` is a statement or, for the internal
    builders, a function of the view columns returning ``(statement, params)``."""
    views = _prepare_views(conn, backend, specs, as_of=as_of, current=current,
                           search_path=search_path)
    _lock(conn, [p["url"] for s in specs for p in s["parts"]])
    if callable(sql):
        sql, params = sql(views)
    wrapped = f"SELECT * FROM (\n{sql}\n) _q LIMIT {int(max_rows) + 1}"
    desc = conn.execute(f"DESCRIBE {wrapped}", params).fetchall()
    names = [d[0] for d in desc]
    types = [str(d[1]) for d in desc]
    final = wrapped
    # TIMESTAMPTZ out as text: the Python conversion would need pytz, and the
    # Postgres path hands timestamps to the client as strings anyway.
    if len(set(names)) == len(names) and any(t.startswith("TIMESTAMP WITH TIME ZONE")
                                             for t in types):
        proj = ", ".join(
            f"CAST({_qi(n)} AS VARCHAR) AS {_qi(n)}" if t.startswith("TIMESTAMP WITH TIME ZONE")
            else _qi(n) for n, t in zip(names, types))
        final = f"SELECT {proj} FROM ({wrapped}) _r"
    cur = conn.execute(final, params)
    cols = [d[0] for d in cur.description]
    recs = cur.fetchmany(int(max_rows) + 1)
    truncated = len(recs) > max_rows
    rows = [{c: _plain(v) for c, v in zip(cols, r)} for r in recs[:max_rows]]
    return {"columns": cols,
            "fields": [{"id": n, "type": _field_type(t)} for n, t in zip(names, types)],
            "rows": rows, "truncated": truncated, "row_count": len(rows)}


# ── running ──────────────────────────────────────────────────────────────────

async def _table_spec(schema: str, name: str) -> dict:
    parts = await cold_parts_for(name) if schema == "public" else []
    ident = await _ident_for(name) if any(p.get("partial") for p in parts) else None
    return {"schema": schema, "table": name, "cold": is_cold_spec(parts), "ident": ident,
            "parts": [{**p, "url": _part_url(p["r2_key"])} for p in parts]}


async def _specs(tables: Iterable) -> list[dict]:
    """One spec per table; a union view (union_views.py) expands into a spec
    per branch table plus a ``union`` spec built over them."""
    specs, seen, unions = [], set(), []
    for t in tables:
        schema, name = (t[0], t[1]) if isinstance(t, (tuple, list)) else ("public", t)
        branches = await union_branches(name) if schema == "public" else None
        if branches:
            from app.services import union_views

            found = union_views.spec_for_view(name)
            label = found[1]["label_column"] if found else "label"
            for bt, _label in branches:
                if ("public", bt) not in seen:
                    seen.add(("public", bt))
                    specs.append(await _table_spec("public", bt))
            unions.append({"schema": "public", "table": name, "union": True, "parts": [],
                           "branches": list(branches), "label_column": label})
            continue
        if (schema, name) in seen:
            continue
        seen.add((schema, name))
        specs.append(await _table_spec(schema, name))
    return specs + unions


async def _run_internal(sql, *, tables: Iterable, as_of=None, current: bool = False,
                        timeout_s: float | None = None, max_rows: int = 1000,
                        params: list | None = None, search_path: str | None = None) -> dict:
    if as_of is not None and current:
        raise HistoryQueryError("as_of and current are mutually exclusive")
    ok, why = is_available()
    if not ok:
        raise HistoryUnavailable(f"מנוע ההיסטוריה אינו זמין: {why}")
    timeout = float(timeout_s or settings.history_query_timeout_s)
    _stats["queries"] += 1
    async with _gate.admit(settings.history_query_queue):
        t0 = time.monotonic()
        try:
            specs = await _specs(tables)
            backend = _hot_backend()
            await backend.prepare(specs, as_of=as_of, current=current)
        except HistoryQueryError:
            _stats["errors"] += 1
            raise
        tmpdir = tempfile.mkdtemp(prefix="hq_")
        conn = _open_connection(tmpdir)

        def cleanup(_=None):
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            shutil.rmtree(tmpdir, ignore_errors=True)

        fut = asyncio.ensure_future(asyncio.to_thread(
            _execute, conn, backend, specs, sql, list(params or []), max_rows,
            as_of, current, search_path))
        try:
            done, _ = await asyncio.wait({fut}, timeout=timeout)
            if not done:
                conn.interrupt()
                try:
                    await fut
                except BaseException:  # noqa: BLE001, the interrupt is the expected end
                    pass
                _stats["timeouts"] += 1
                _stats["last_error"] = f"timeout after {timeout:g}s"
                raise HistoryTimeout(
                    f"השאילתה חרגה מ-{timeout:g} שניות במנוע ההיסטוריה והופסקה. "
                    "צמצמו אותה (WHERE על first_seen/last_seen, טבלה אחת, GROUP BY)")
            res = fut.result()
        except HistoryTimeout:
            raise
        except HistoryQueryError as e:
            _stats["errors"] += 1
            _stats["last_error"] = str(e)[:300]
            raise
        except Exception as e:  # noqa: BLE001, DuckDB errors go to the user
            _stats["errors"] += 1
            _stats["last_error"] = f"{type(e).__name__}: {e}"[:300]
            raise HistoryQueryError(duckdb_error_hint(e)) from e
        finally:
            if fut.done():
                cleanup()
            else:  # cancelled while waiting: stop the query, free it when it ends
                conn.interrupt()
                fut.add_done_callback(cleanup)
        elapsed = int((time.monotonic() - t0) * 1000)
        _stats["ok"] += 1
        _stats["last_elapsed_ms"] = elapsed
        _stats["last_at"] = datetime.now(UTC).isoformat()
        return {**res, "engine": "duckdb",
                "cold_parts": sum(len(s["parts"]) for s in specs), "elapsed_ms": elapsed}


async def run(sql: str, *, tables: Iterable, as_of: datetime | None = None,
              current: bool = False, timeout_s: float | None = None, max_rows: int = 1000,
              params: list | None = None, search_path: str | None = None) -> dict:
    """Run a user's read-only SELECT over the hot tables plus their cold parts.

    ``tables`` are the catalogued tables the statement names (plain names are
    ``public``; ``(schema, name)`` for the others). ``as_of`` / ``current``
    narrow every append table to the rows in force then / now."""
    s = validate_sql(sql)
    res = await _run_internal(s, tables=tables, as_of=as_of, current=current,
                              timeout_s=timeout_s, max_rows=max_rows, params=params,
                              search_path=search_path)
    if _DIALECT_REGEX.search(_skeleton(s)):
        res["dialect_note"] = DIALECT_NOTE
    return res


# ── /rows and /datastore_search on the engine ────────────────────────────────

def _view_cols(views: dict, table: str, validity: bool) -> list[str]:
    hidden = set(append_store._HIDDEN_COLS) | (set() if validity else {LAST_COL})
    return [c for c, _t in views.get(table, []) if c not in hidden]


def _latest_src(table: str, cols: list[str], key_col: str | None, order_col: str | None) -> str:
    if not key_col or key_col not in cols:
        return _qi(table)
    order = order_col if (order_col and order_col in cols) else (
        FIRST_COL if FIRST_COL in cols else key_col)
    return (f"(SELECT DISTINCT ON ({_qi(key_col)}) * FROM {_qi(table)} "
            f"ORDER BY {_qi(key_col)}, {_qi(order)} DESC NULLS LAST) AS {_qi(table)}")


def _paged(select_cols: str, src: str, where: str, order_by: str, limit: int, offset: int,
           *, distinct: bool = False) -> str:
    """One statement for the page AND the total: the total is a single-row
    side that is always there, so a page past the end still reports it."""
    sel = ("SELECT DISTINCT " if distinct else "SELECT ") + select_cols
    rn_order = order_by or "1"
    return (f"WITH f AS ({sel} FROM {src}{where}),\n"
            f"p AS (SELECT *, row_number() OVER (ORDER BY {rn_order}) AS __rn FROM f)\n"
            f"SELECT t.__total, p.* FROM (SELECT count(*) AS __total FROM f) t "
            f"LEFT JOIN p ON p.__rn > {int(offset)} AND p.__rn <= {int(offset) + int(limit)} "
            f"ORDER BY p.__rn")


def _split_page(res: dict, cols: list[str]) -> tuple[int, list[dict]]:
    rows = res["rows"]
    total = int(rows[0]["__total"]) if rows else 0
    out = [{c: r.get(c) for c in cols} for r in rows if r.get("__rn") is not None]
    return total, out


async def rows_page(table: str, *, parts: list[dict] | None = None, as_of=None,
                    current: bool = False, limit: int = 50, offset: int = 0,
                    sort: str | None = None, order: str = "desc", q: str | None = None,
                    filters: dict | None = None, latest_key: str | None = None,
                    latest_order: str | None = None) -> dict:
    """append_store.query's answer, read through the history view."""
    validity = as_of is not None or bool(current)
    limit = max(1, min(int(limit or 50), append_store.MAX_PAGE))
    offset = max(0, int(offset or 0))
    meta: dict = {}

    def build(views):
        cols = _view_cols(views, table, validity)
        if not cols:
            raise HistoryQueryError("No archived rows yet for this dataset")
        flt = {k: v for k, v in (filters or {}).items() if k in cols and v not in (None, "")}
        sort_col = sort if sort in cols else (FIRST_COL if FIRST_COL in cols else cols[0])
        direction = "ASC" if str(order).lower() == "asc" else "DESC"
        conds, params = [], []
        for term in append_store.search_terms(q):
            params.append(f"%{term}%")
            ph = f"${len(params)}"
            conds.append("(" + " OR ".join(f"CAST({_qi(c)} AS VARCHAR) ILIKE {ph}"
                                           for c in cols) + ")")
        for col, val in flt.items():
            params.append(f"%{val}%")
            conds.append(f"CAST({_qi(col)} AS VARCHAR) ILIKE ${len(params)}")
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        meta.update(cols=cols, sort=sort_col, order=direction.lower())
        src = _latest_src(table, cols, latest_key, latest_order)
        return _paged(", ".join(_qi(c) for c in cols), src, where,
                      f"{_qi(sort_col)} {direction} NULLS LAST", limit, offset), params

    res = await _run_internal(build, tables=[table], as_of=as_of, current=current,
                              max_rows=limit + 1)
    total, rows = _split_page(res, meta["cols"])
    return {"columns": meta["cols"], "rows": rows, "total": total, "limit": limit,
            "offset": offset, "sort": meta["sort"], "order": meta["order"],
            "engine": "duckdb", "cold_parts": res["cold_parts"], "elapsed_ms": res["elapsed_ms"]}


async def datastore_page(table: str, *, parts: list[dict] | None = None, as_of=None,
                         current: bool = False, fields: list[str] | None = None,
                         filters: dict | None = None, q: str | None = None,
                         sort: str | None = None, limit: int = 100, offset: int = 0,
                         distinct: bool = False, include_total: bool = True,
                         latest_key: str | None = None,
                         latest_order: str | None = None) -> dict:
    """append_store.datastore_search's answer, read through the history view."""
    validity = as_of is not None or bool(current)
    limit = max(0, min(int(limit if limit is not None else 100), append_store.MAX_PAGE))
    offset = max(0, int(offset or 0))
    meta: dict = {}

    def build(views):
        all_cols = _view_cols(views, table, validity)
        if not all_cols:
            raise HistoryQueryError("No archived rows yet for this dataset")
        colset = set(all_cols)
        sel = [c for c in (fields or all_cols) if c in colset] or all_cols
        conds, params = [], []
        for term in append_store.search_terms(q):
            params.append(f"%{term}%")
            ph = f"${len(params)}"
            conds.append("(" + " OR ".join(f"CAST({_qi(c)} AS VARCHAR) ILIKE {ph}"
                                           for c in all_cols) + ")")
        for col, val in (filters or {}).items():
            if col not in colset:
                continue
            if isinstance(val, list):
                phs = []
                for v in val:
                    params.append(None if v is None else str(v))
                    phs.append(f"${len(params)}")
                conds.append(f"CAST({_qi(col)} AS VARCHAR) IN ({', '.join(phs) or 'NULL'})")
            else:
                params.append(None if val is None else str(val))
                conds.append(f"CAST({_qi(col)} AS VARCHAR) = ${len(params)}")
        where = (" WHERE " + " AND ".join(conds)) if conds else ""
        order_by = append_store._parse_sort(sort, colset).replace(" ORDER BY ", "", 1) or (
            f"{_qi(FIRST_COL)} DESC" if FIRST_COL in colset else "")
        types = dict(views.get(table, []))
        meta.update(sel=sel, fields=[{"id": c, "type": _field_type(types.get(c))} for c in sel])
        src = _latest_src(table, all_cols, latest_key, latest_order)
        return _paged(", ".join(_qi(c) for c in sel), src, where, order_by, limit, offset,
                      distinct=distinct), params

    res = await _run_internal(build, tables=[table], as_of=as_of, current=current,
                              max_rows=max(limit, 1) + 1)
    total, records = _split_page(res, meta["sel"])
    return {"fields": meta["fields"], "records": records,
            "total": total if include_total else None, "limit": limit, "offset": offset,
            "engine": "duckdb", "cold_parts": res["cold_parts"], "elapsed_ms": res["elapsed_ms"]}


# ── routing helpers for the API layers ───────────────────────────────────────

NOTE_DISABLED = ("מנוע ההיסטוריה כבוי באתר כרגע, ולכן השאילתה רצה על SQL בלבד; "
                 "שורות שנמצאות רק בקבצי ההיסטוריה (history/files) לא נכללו.")
NOTE_NO_COLD = ("לטבלאות שבשאילתה אין שורות שנמצאות רק בקבצי ההיסטוריה, ולכן השאילתה "
                "רצה על SQL והתוצאה מלאה.")
NOTE_AS_OF = ("חלק מהשורות שהיו בתוקף ברגע הזה נמצאות רק בקבצי ההיסטוריה (history/files) "
              "ומנוע ההיסטוריה כבוי, ולכן הן לא נכללו בתשובה.")


NOTE_COLD = ("המאגר הועבר לשכבה הקרה (R2) והטבלה ב-SQL ריקה, אבל מנוע ההיסטוריה אינו "
             "זמין כרגע ({reason}), ולכן התשובה ריקה. השורות נמצאות בקבצי ההיסטוריה "
             "(history/files).")
NOTE_COLD_CONSOLE = ("הטבלה {tables} הועברה לשכבה הקרה (R2) וב-SQL היא ריקה: סמנו 'כולל "
                     "היסטוריה' כדי לקרוא את השורות שלה דרך מנוע ההיסטוריה.")


def engine_ready() -> bool:
    return settings.history_query_enabled and is_available()[0]


def _note_for_cold() -> str:
    if not settings.history_query_enabled:
        return NOTE_DISABLED
    return NOTE_UNAVAILABLE.format(reason=is_available()[1])


NOTE_UNAVAILABLE = ("מנוע ההיסטוריה אינו זמין כרגע ({reason}), ולכן השאילתה רצה על SQL "
                    "בלבד; שורות שנמצאות רק בקבצי ההיסטוריה (history/files) לא נכללו.")


async def route_sql(sql: str, *, tables: list, run_pg: Callable, search_path: str | None = None,
                    max_rows: int = 1000, timeout_s: float | None = None) -> dict:
    """``include_history`` for a SQL endpoint: the engine when a table the
    statement names has rows only in R2 and the engine is up, otherwise
    ``run_pg()`` with ``engine: "postgres"`` and a note that says why."""
    cold = False
    for t in tables:
        schema, name = (t[0], t[1]) if isinstance(t, (tuple, list)) else ("public", t)
        if schema != "public":
            continue
        names = [b for b, _l in (await union_branches(name) or [])] or [name]
        for n in names:
            if await cold_parts_for(n):
                cold = True
                break
        if cold:
            break
    if cold and engine_ready():
        return await run(sql, tables=tables, search_path=search_path, max_rows=max_rows,
                         timeout_s=timeout_s)
    res = await run_pg()
    res["engine"] = "postgres"
    res["history_note"] = _note_for_cold() if cold else NOTE_NO_COLD
    return res


def console_tables(sql: str, catalog: list[dict], search_path: str | None = None) -> list[tuple]:
    """``(schema, table)`` of every catalogued table the console SQL names. A
    name in two schemas resolves as the console's search_path would."""
    known = [(r["schema"], r["table"]) for r in catalog]
    order = [s.strip() for s in (search_path or "").split(",") if s.strip()]
    rank = {s: i for i, s in enumerate(order)}
    schema_of: dict[str, str] = {}
    for sch, tbl in sorted(known, key=lambda k: rank.get(k[0], len(rank))):
        schema_of.setdefault(tbl, sch)
    return [(schema_of[n], n) for n in extract_table_refs(sql, known)]


# ── boot and admin ───────────────────────────────────────────────────────────

def ensure_extensions() -> dict:
    """Install httpfs and postgres once, then decide how the hot side is read:
    ATTACH as the read-only role and run one catalog query AFTER the connection
    is locked (exactly as a user query would), else the asyncpg fallback.
    Blocking (network); the boot hook runs it on a thread."""
    _ext.update(checked_at=None, httpfs=None, postgres=None, hot_mode=None, error=None)
    if not _installed():
        _ext.update(checked_at=datetime.now(UTC).isoformat(), error="duckdb is not installed")
        return dict(_ext)
    for name in ("httpfs", "postgres"):
        conn = _duckdb().connect(":memory:")
        try:
            conn.execute(f"INSTALL {name}")
            conn.execute(f"LOAD {name}")
            _ext[name] = "ok"
        except Exception as e:  # noqa: BLE001
            _ext[name] = f"{type(e).__name__}: {e}"[:300]
            logger.warning("history query: extension %s unavailable: %s", name, e)
        finally:
            conn.close()
    mode, err = None, None
    if _ext["postgres"] == "ok" and (settings.append_readonly_database_url or "").strip():
        tmp = tempfile.mkdtemp(prefix="hq_")
        conn = _open_connection(tmp)
        try:
            AttachHot().setup(conn)
            _lock(conn, [])
            conn.execute(f"SELECT count(*) FROM {HOT}.information_schema.tables").fetchone()
            mode = "attach"
        except Exception as e:  # noqa: BLE001
            err = f"attach: {type(e).__name__}: {e}"[:300]
            logger.warning("history query: postgres attach failed, using asyncpg: %s", e)
        finally:
            conn.close()
            shutil.rmtree(tmp, ignore_errors=True)
    if mode is None and (settings.append_readonly_database_url or "").strip():
        mode = "arrow"
    _ext.update(checked_at=datetime.now(UTC).isoformat(), hot_mode=mode, error=err)
    logger.info("history query: httpfs=%s postgres=%s hot=%s", _ext["httpfs"],
                _ext["postgres"], mode)
    return dict(_ext)


def _rss_mb() -> float | None:
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except OSError:
        return None
    return None


async def smoke(table: str) -> dict:
    """End to end without any history part: ``count(*)`` of the existing
    Parquet mirror ``parquet/<table>.parquet`` on R2, and of the hot table, both
    through the engine (the hot one through ``hot`` in attach mode; in arrow
    mode by the read-only pool, since loading a big table is what the cap
    forbids). Under the same gate as user queries."""
    from app.services.parquet_export import parquet_key

    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table or ""):
        raise HistoryQueryError("table must be a plain table name")
    ok, why = is_available()
    if not ok:
        raise HistoryUnavailable(f"מנוע ההיסטוריה אינו זמין: {why}")
    url = _part_url(parquet_key(table))
    attach = _ext["hot_mode"] == "attach"
    async with _gate.admit(settings.history_query_queue):
        rss0 = _rss_mb()
        tmp = tempfile.mkdtemp(prefix="hq_")
        conn = _open_connection(tmp)

        def work() -> dict:
            out: dict = {}
            _setup_r2(conn)
            if attach:
                AttachHot().setup(conn)
            _lock(conn, [url])
            t = time.monotonic()
            out["parquet_rows"] = conn.execute(
                f"SELECT count(*) FROM read_parquet({_lit(url)})").fetchone()[0]
            out["parquet_ms"] = int((time.monotonic() - t) * 1000)
            if attach:
                t = time.monotonic()
                out["hot_rows"] = conn.execute(
                    f"SELECT count(*) FROM {HOT}.\"public\".{_qi(table)}").fetchone()[0]
                out["hot_ms"] = int((time.monotonic() - t) * 1000)
            return out

        fut = asyncio.ensure_future(asyncio.to_thread(work))
        try:
            done, _ = await asyncio.wait({fut}, timeout=float(settings.history_query_timeout_s) * 4)
            if not done:
                conn.interrupt()
                try:
                    await fut
                except BaseException:  # noqa: BLE001
                    pass
                raise HistoryTimeout("smoke test timed out")
            out = fut.result()
        except HistoryQueryError:
            raise
        except Exception as e:  # noqa: BLE001
            raise HistoryQueryError(duckdb_error_hint(e)) from e
        finally:
            if fut.done():
                conn.close()
                shutil.rmtree(tmp, ignore_errors=True)
            else:
                conn.interrupt()
        if not attach:
            pool = await append_store.get_readonly_pool()
            t = time.monotonic()
            async with pool.acquire() as c:
                out["hot_rows"] = int(await c.fetchval(f"SELECT count(*) FROM public.{_qi(table)}"))
            out["hot_ms"] = int((time.monotonic() - t) * 1000)
        return {"table": table, "parquet_key": parquet_key(table), "hot_mode": _ext["hot_mode"],
                **out, "equal": out.get("parquet_rows") == out.get("hot_rows"),
                "rss_mb_before": rss0, "rss_mb_after": _rss_mb()}
