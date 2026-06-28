"""Row-level APPEND archive in a dedicated Postgres (Neon) database.

Why this exists:
    data.gov.il datastore-backed datasets tracked as append_only need a place
    to accumulate rows over time — the source is a live window (flights board)
    or a huge slowly-growing registry (vehicles), and the history must be kept
    and queried by OVER. The old design wrote the accumulation into ODATA's
    CKAN datastore; ODATA's write path is down (500/502) and is being abandoned
    for R2. R2 is object storage (no upsert/queries), so it can't host a
    queryable growing table. This module is the answer: a dedicated Postgres
    (configured via APPEND_DATABASE_URL) holding one table per dataset.

What the DB gives us that the seen-set workaround didn't:
    - Dedup is the DB's job: a UNIQUE index + ``INSERT … ON CONFLICT DO NOTHING``
      replaces the ever-growing ``_appendonly_seen`` list carried in JSONB and
      re-serialized every poll (the write-amplification that forced the windowed
      seen-set for 15-min boards). No seen-set, no windowing needed.
    - ``first_seen`` is a column with ``DEFAULT now()`` — only newly-inserted
      rows get stamped; conflicting (already-seen) rows keep their original
      first_seen. Exactly the "timestamp of each addition" semantics.
    - Keyed datasets (vehicle registry) dedup on the key column (mispar_rechev);
      keyless datasets (flights) dedup on a row_hash of the whole row, so every
      distinct row STATE is captured once.

Connection: a lazily-created asyncpg pool with min_size=0 so the Neon compute
can scale to zero between polls (keeps cost down). All identifiers are quoted,
so Hebrew column names (e.g. החלטות ממשלה fields) work. Every value is stored as
text — archival robustness over typing; callers cast on query.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import ssl
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

# Postgres bind-parameter ceiling is 32767; stay well under it.
_MAX_PARAMS = 30000


def is_configured() -> bool:
    return bool(settings.append_database_url)


def _dsn() -> str:
    """Normalize the configured URL into a DSN asyncpg accepts.

    Neon hands out ``postgresql://…?sslmode=require&channel_binding=require``
    (and the SQLAlchemy ``+asyncpg`` suffix may be present). asyncpg takes the
    plain ``postgresql://`` scheme and gets SSL via the ``ssl`` kwarg, not query
    params — so strip the libpq-only params and the dialect suffix."""
    raw = settings.append_database_url.strip()
    u = urlsplit(raw)
    scheme = u.scheme.split("+", 1)[0] or "postgresql"
    q = [
        (k, v) for k, v in parse_qsl(u.query)
        if k.lower() not in ("sslmode", "channel_binding", "options")
    ]
    return urlunsplit((scheme, u.netloc, u.path, urlencode(q), ""))


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                ctx = ssl.create_default_context()
                _pool = await asyncpg.create_pool(
                    dsn=_dsn(),
                    ssl=ctx,
                    min_size=0,      # let Neon scale to zero between polls
                    max_size=5,
                    command_timeout=180,
                )
                logger.info("append_store: connection pool created")
    return _pool


def table_name(ds) -> str:
    """Stable, readable, unique table name for a tracked dataset.

    ``append_<sanitized ckan_name>_<id8>`` — the id suffix guarantees no
    collision between two datasets that share a ckan_name. Clamped to Postgres'
    63-char identifier limit."""
    base = re.sub(r"[^a-z0-9_]+", "_", (ds.ckan_name or "").lower()).strip("_") or "ds"
    sid = str(ds.id).replace("-", "")[:8]
    return f"append_{base}"[:54] + f"_{sid}"


def _qi(name: str) -> str:
    """Quote a SQL identifier (supports Hebrew/Unicode column names)."""
    return '"' + str(name).replace('"', '""') + '"'


def row_hash(row: dict, cols: list[str]) -> str:
    """SHA-256 over the row's column values (str-coerced, None→''), used as the
    dedup identity for keyless datasets. Matches version_detector._row_identity
    semantics so behavior is consistent."""
    canonical = json.dumps(
        {c: ("" if row.get(c) is None else str(row.get(c))) for c in cols},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_insert(
    table: str,
    cols: list[str],
    chunk: list[dict],
    *,
    key_col: str | None,
    keyless: bool,
) -> tuple[str, list]:
    """Build one multi-row ``INSERT … ON CONFLICT DO NOTHING`` for a chunk.

    Pure (no DB) so it's unit-testable. Dedups within the chunk by the conflict
    identity (so a single statement can't carry the same key twice). ``first_seen``
    is filled by ``now()`` inline (same value for the batch — "first seen this
    poll"); keyless rows also carry the computed ``row_hash``. Returns
    (sql, params); sql is "" when the chunk has nothing to insert."""
    insert_cols = list(cols) + ["first_seen"] + (["row_hash"] if keyless else [])
    conflict_target = "row_hash" if keyless else key_col
    values_sql: list[str] = []
    params: list = []
    seen_local: set[str] = set()
    p = 1
    for r in chunk:
        if keyless:
            ident = row_hash(r, cols)
        else:
            kv = r.get(key_col)
            ident = "" if kv is None else str(kv)
        if ident in seen_local:
            continue
        seen_local.add(ident)
        placeholders: list[str] = []
        for c in cols:
            v = r.get(c)
            params.append(None if v is None else str(v))
            placeholders.append(f"${p}")
            p += 1
        placeholders.append("now()")
        if keyless:
            params.append(ident)
            placeholders.append(f"${p}")
            p += 1
        values_sql.append("(" + ",".join(placeholders) + ")")
    if not values_sql:
        return "", []
    cols_sql = ",".join(_qi(c) for c in insert_cols)
    sql = (
        f"INSERT INTO {_qi(table)} ({cols_sql}) VALUES {','.join(values_sql)} "
        f"ON CONFLICT ({_qi(conflict_target)}) DO NOTHING"
    )
    return sql, params


def chunk_size_for(num_cols: int, keyless: bool) -> int:
    """Rows per INSERT so total bind params stay under the Postgres ceiling."""
    per_row = num_cols + (1 if keyless else 0)  # first_seen uses now(), no param
    return max(1, _MAX_PARAMS // max(1, per_row))


async def ensure_table(table: str, source_cols: list[str], *, key_col: str | None, keyless: bool) -> None:
    """Create the dataset's table if absent; otherwise add any newly-appeared
    source columns (schema drift). Then ensure the dedup UNIQUE index exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1",
            table,
        )
        existing = {r["column_name"] for r in rows}
        if not existing:
            cols_sql = ", ".join(f"{_qi(c)} text" for c in source_cols)
            hashcol = ', "row_hash" text' if keyless else ""
            try:
                await conn.execute(
                    f'CREATE TABLE IF NOT EXISTS {_qi(table)} '
                    f'({cols_sql}, "first_seen" timestamptz NOT NULL DEFAULT now(){hashcol})'
                )
            except (asyncpg.DuplicateTableError, asyncpg.UniqueViolationError):
                # CREATE TABLE IF NOT EXISTS isn't atomic against the pg_type
                # catalog: two concurrent polls of the same dataset can race and
                # one loses with a pg_type_typname duplicate. Benign — the table
                # now exists; fall through to the column/index reconciliation.
                pass
            existing = source_cols  # treat as present for the drift pass below
        else:
            for c in source_cols:
                if c not in existing:
                    await conn.execute(
                        f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS {_qi(c)} text'
                    )
            if keyless and "row_hash" not in existing:
                await conn.execute(
                    f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS "row_hash" text'
                )
        target = "row_hash" if keyless else key_col
        idx = (f"{table}_uq")[:63]
        await conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {_qi(idx)} ON {_qi(table)} ({_qi(target)})'
        )


async def append_rows(
    table: str,
    source_cols: list[str],
    rows: list[dict],
    *,
    key_col: str | None,
    keyless: bool,
) -> int:
    """Insert ``rows`` into ``table``, skipping ones already present (by key or
    row_hash). Returns the number actually inserted (parsed from the INSERT
    command tags). Assumes ensure_table has run for this (table, cols, mode)."""
    if not rows:
        return 0
    pool = await get_pool()
    size = chunk_size_for(len(source_cols), keyless)
    inserted = 0
    async with pool.acquire() as conn:
        for i in range(0, len(rows), size):
            sql, params = build_insert(
                table, source_cols, rows[i:i + size], key_col=key_col, keyless=keyless,
            )
            if not sql:
                continue
            status = await conn.execute(sql, *params)  # e.g. "INSERT 0 42"
            try:
                inserted += int(status.split()[-1])
            except (ValueError, IndexError):
                pass
    return inserted


# ── Content-diff mode (capture new AND changed rows, efficiently) ────────────
#
# For heavy registries (vehicles) where we want to record CHANGES to existing
# rows — not just new keys — the dedup identity is a hash of the WHOLE row, and
# inserts go through a COPY-staged set-based diff instead of millions of
# per-row ON CONFLICT probes. The hash is computed in SQL (md5 over the source
# columns) so the one-time backfill of existing rows and every ongoing poll use
# the exact same value — an unchanged row hashes identically and is skipped.

_HASH_SEP = "chr(31)"  # unit separator — won't appear in the text values


def _content_hash_expr(cols: list[str], alias: str = "") -> str:
    """SQL expression hashing the row's source columns (NULL→'') in a fixed
    order. Identical for the backfill (no alias) and the staging diff (alias)."""
    pfx = (alias + ".") if alias else ""
    parts = ",".join(f"coalesce({pfx}{_qi(c)}::text,'')" for c in cols)
    return f"md5(concat_ws({_HASH_SEP},{parts}))"


async def ensure_content_diff(table: str, source_cols: list[str], key_col: str | None) -> None:
    """One-time, idempotent, resumable migration of a keyed append table to
    content-diff mode: add a ``row_hash`` column, backfill it in committed
    batches (so a kill resumes), drop the old single-key unique index (which
    would block a changed row that reuses the same key), and add a UNIQUE index
    on ``row_hash``. After the migration every call is a few cheap no-ops."""
    pool = await get_pool()
    expr = _content_hash_expr(source_cols)
    async with pool.acquire() as conn:
        await conn.execute(f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS "row_hash" text')
    # Batched backfill — each batch is its own committed statement so an
    # interrupted migration resumes from WHERE row_hash IS NULL.
    while True:
        async with pool.acquire() as conn:
            tag = await conn.execute(
                f'UPDATE {_qi(table)} SET "row_hash" = {expr} '
                f'WHERE ctid IN (SELECT ctid FROM {_qi(table)} '
                f'WHERE "row_hash" IS NULL LIMIT 50000)'
            )
        try:
            done = int(tag.split()[-1])
        except (ValueError, IndexError):
            done = 0
        if done == 0:
            break
    async with pool.acquire() as conn:
        await conn.execute(f'DROP INDEX IF EXISTS {_qi((table + "_uq")[:63])}')
        await conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {_qi((table + "_hash_uq")[:63])} '
            f'ON {_qi(table)} ("row_hash")'
        )


async def append_diff(table: str, source_cols: list[str], rows: list[dict]) -> int:
    """Insert only NEW or CHANGED rows (by full-row hash) via a COPY-staged,
    set-based diff — one COPY + one INSERT…SELECT…ON CONFLICT per batch instead
    of thousands of per-row probes. Returns the number inserted. Requires
    ensure_content_diff to have run for this table."""
    if not rows:
        return 0
    pool = await get_pool()
    expr_s = _content_hash_expr(source_cols, alias="s")
    cols_q = ",".join(_qi(c) for c in source_cols)
    sel_q = ",".join("s." + _qi(c) for c in source_cols)
    records = [
        tuple(None if r.get(c) is None else str(r.get(c)) for c in source_cols)
        for r in rows
    ]
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                f'CREATE TEMP TABLE _stg (LIKE {_qi(table)}) ON COMMIT DROP'
            )
            await conn.copy_records_to_table("_stg", records=records, columns=source_cols)
            tag = await conn.execute(
                f'INSERT INTO {_qi(table)} ({cols_q}, "first_seen", "row_hash") '
                f'SELECT {sel_q}, now(), {expr_s} FROM _stg s '
                f'ON CONFLICT ("row_hash") DO NOTHING'
            )
    try:
        return int(tag.split()[-1])
    except (ValueError, IndexError):
        return 0


async def table_count(table: str) -> int:
    """Total rows currently in the dataset's archive table (0 if absent)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            return int(await conn.fetchval(f'SELECT count(*) FROM {_qi(table)}'))
        except asyncpg.UndefinedTableError:
            return 0


# ── Read side: powering the OVER "view & pull the archive" UI ────────────────
#
# All of the below validate every column name against the table's REAL columns
# (so a caller can't inject an identifier), parameterize every value, and quote
# identifiers — the table/column names are operator-controlled but the filter
# VALUES come from the public UI.

# Internal columns hidden from the UI (dedup bookkeeping, not data).
_HIDDEN_COLS = {"row_hash"}

# Hard ceiling on a single rows page, regardless of what the client asks.
MAX_PAGE = 500


async def user_columns(table: str) -> list[str]:
    """Ordered list of the table's user-facing columns (source columns +
    first_seen, with internal bookkeeping hidden). Empty if the table is gone."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
            table,
        )
    return [r["column_name"] for r in rows if r["column_name"] not in _HIDDEN_COLS]


def _build_where(
    cols: list[str], q: str | None, filters: dict[str, str], start_param: int,
) -> tuple[str, list]:
    """Build a WHERE clause: optional free-text ``q`` (ILIKE across every
    column) AND per-column ILIKE ``filters``. Only known columns are honored.
    Returns (clause_without_WHERE_or_empty, params)."""
    conds: list[str] = []
    params: list = []
    p = start_param
    colset = set(cols)
    if q:
        ph = f"${p}"  # one param, referenced by every column's ILIKE
        clause = " OR ".join(f"{_qi(c)}::text ILIKE {ph}" for c in cols)
        conds.append("(" + clause + ")")
        params.append(f"%{q}%")
        p += 1
    for col, val in filters.items():
        if col not in colset or val is None or val == "":
            continue
        conds.append(f"{_qi(col)}::text ILIKE ${p}")
        params.append(f"%{val}%")
        p += 1
    return (" WHERE " + " AND ".join(conds)) if conds else "", params


async def query(
    table: str,
    *,
    limit: int = 50,
    offset: int = 0,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    filters: dict[str, str] | None = None,
) -> dict:
    """Paginated, filtered read for the UI. Returns
    {columns, rows, total, limit, offset}. Validates sort/filter columns
    against the live schema; caps the page at MAX_PAGE."""
    cols = await user_columns(table)
    if not cols:
        return {"columns": [], "rows": [], "total": 0, "limit": limit, "offset": offset}
    filters = {k: v for k, v in (filters or {}).items() if k in cols}
    limit = max(1, min(int(limit or 50), MAX_PAGE))
    offset = max(0, int(offset or 0))
    sort_col = sort if (sort in cols) else ("first_seen" if "first_seen" in cols else cols[0])
    direction = "ASC" if str(order).lower() == "asc" else "DESC"

    where, params = _build_where(cols, q, filters, start_param=1)
    select_cols = ", ".join(_qi(c) for c in cols)
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = int(await conn.fetchval(f'SELECT count(*) FROM {_qi(table)}{where}', *params))
        rows = await conn.fetch(
            f'SELECT {select_cols} FROM {_qi(table)}{where} '
            f'ORDER BY {_qi(sort_col)} {direction} NULLS LAST '
            f'LIMIT ${len(params)+1} OFFSET ${len(params)+2}',
            *params, limit, offset,
        )
    return {
        "columns": cols,
        "rows": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort_col,
        "order": direction.lower(),
    }


async def iter_csv(
    table: str,
    *,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    filters: dict[str, str] | None = None,
):
    """Async generator yielding the filtered table as CSV (utf-8-sig) chunks,
    server-side cursor so even multi-million-row exports stay memory-bounded."""
    import csv as _csv
    import io as _io

    cols = await user_columns(table)
    if not cols:
        yield "﻿".encode("utf-8")
        return
    filters = {k: v for k, v in (filters or {}).items() if k in cols}
    sort_col = sort if (sort in cols) else ("first_seen" if "first_seen" in cols else cols[0])
    direction = "ASC" if str(order).lower() == "asc" else "DESC"
    where, params = _build_where(cols, q, filters, start_param=1)
    select_cols = ", ".join(_qi(c) for c in cols)
    sql = (
        f'SELECT {select_cols} FROM {_qi(table)}{where} '
        f'ORDER BY {_qi(sort_col)} {direction} NULLS LAST'
    )

    def _row_to_csv(values) -> bytes:
        buf = _io.StringIO()
        _csv.writer(buf).writerow(["" if v is None else str(v) for v in values])
        return buf.getvalue().encode("utf-8")

    # Header with BOM so Excel reads Hebrew correctly.
    head = _io.StringIO()
    _csv.writer(head).writerow(cols)
    yield ("﻿" + head.getvalue()).encode("utf-8")

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for rec in conn.cursor(sql, *params):
                yield _row_to_csv([rec[c] for c in cols])
