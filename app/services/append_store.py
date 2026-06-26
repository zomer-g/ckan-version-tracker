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
            await conn.execute(
                f'CREATE TABLE IF NOT EXISTS {_qi(table)} '
                f'({cols_sql}, "first_seen" timestamptz NOT NULL DEFAULT now(){hashcol})'
            )
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


async def table_count(table: str) -> int:
    """Total rows currently in the dataset's archive table (0 if absent)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            return int(await conn.fetchval(f'SELECT count(*) FROM {_qi(table)}'))
        except asyncpg.UndefinedTableError:
            return 0
