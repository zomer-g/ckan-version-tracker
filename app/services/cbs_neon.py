"""Mirror the CBS content index (``cbs_index``, main DB) into the NEON append
archive so it behaves like every other tabular tracked dataset.

Why this exists:
    The CBS index is crawled into ``cbs_index`` in the app's MAIN Postgres (see
    app/models/cbs_index.py) and served by its own /api/cbs/* API. But the
    site's "tracked datasets" collection, the /archive SQL console and the
    /api/append row API all read the DEDICATED NEON append DB
    (append_database_url) via app/services/append_store.py — a different
    database that can't see ``cbs_index``. To expose CBS as a first-class
    dataset (card + NEON SQL + append API) we keep a copy of the index rows in
    a NEON append table, kept fresh by:
      * a best-effort dual-write on every /api/cbs/ingest batch, and
      * a one-shot backfill (POST /api/cbs/sync-neon) for the existing rows.

Table shape:
    ``append_cbs_index_<id8>`` — the SAME name app/services/append_store.table_name
    derives for the synthetic CBS TrackedDataset (ckan_name ``cbs_index`` + the
    fixed dataset id below), so app/api/append.py resolves it with zero special
    casing. Every column is ``text`` (append-archive convention) except the
    ``first_seen timestamptz DEFAULT now()`` stamp. Unlike the generic
    append_store (append-only, ON CONFLICT DO NOTHING), CBS upserts keep the
    LATEST state per ``url`` (ON CONFLICT DO UPDATE) — the index is a catalog
    that gets re-crawled, so callers want current metadata; ``first_seen`` still
    records when the page first entered the archive.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import append_store

logger = logging.getLogger(__name__)

# Fixed identity of the synthetic CBS TrackedDataset (see migration 028). The
# append table name is derived from ckan_name + this id by
# append_store.table_name(); we hardcode the result so the dual-write and the
# public append endpoints agree without constructing an ORM object here.
CBS_DATASET_ID = "cb500000-cb50-4b50-8b50-cb50cb50cb50"
CBS_CKAN_ID = "cbs-index"
CBS_CKAN_NAME = "cbs_index"
CBS_APPEND_TABLE = "append_cbs_index_cb500000"  # append_store.table_name equivalent

# Columns mirrored into NEON, in insert order. A useful, queryable subset of
# cbs_index — full_text is intentionally excluded (huge, and search_vector isn't
# reproducible here); it stays queryable via /api/cbs/search.
COLUMNS = (
    "url", "title", "title_en", "summary", "section", "series", "item_type",
    "lang", "subject_tags", "geo_levels", "file_types", "file_links", "extra",
    "year_start", "year_end", "content_hash", "crawl_status", "last_crawled",
)
# Columns whose value is a list/dict → stored as JSON text.
_JSON_COLS = {"subject_tags", "geo_levels", "file_types", "file_links", "extra"}

_ensured = False


def is_configured() -> bool:
    return append_store.is_configured()


def _to_text(col: str, val) -> str | None:
    if val is None:
        return None
    if col in _JSON_COLS:
        return json.dumps(val, ensure_ascii=False)
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)


def _row_tuple(src: dict) -> tuple:
    return tuple(_to_text(c, src.get(c)) for c in COLUMNS)


async def ensure_table() -> None:
    """Create the CBS append table + its unique(url) index in NEON. Idempotent;
    the DDL runs once per process."""
    global _ensured
    if _ensured or not is_configured():
        return
    pool = await append_store.get_pool()
    defs = []
    for c in COLUMNS:
        defs.append(f'{append_store._qi(c)} text NOT NULL' if c == "url"
                    else f'{append_store._qi(c)} text')
    defs.append('"first_seen" timestamptz NOT NULL DEFAULT now()')
    create = f'CREATE TABLE IF NOT EXISTS {append_store._qi(CBS_APPEND_TABLE)} ({", ".join(defs)})'
    idx = append_store._index_name(CBS_APPEND_TABLE, "url")
    create_idx = (
        f'CREATE UNIQUE INDEX IF NOT EXISTS {append_store._qi(idx)} '
        f'ON {append_store._qi(CBS_APPEND_TABLE)} ("url")'
    )
    async with pool.acquire() as conn:
        await conn.execute(create)
        await conn.execute(create_idx)
    _ensured = True
    logger.info("cbs_neon: table %s ensured", CBS_APPEND_TABLE)


async def upsert_pages(pages: list[dict]) -> int:
    """Upsert a batch of CBS page dicts into NEON (latest-state per url).

    ``pages`` are dicts keyed like cbs_index columns (the ingest rows, or ORM
    rows in the backfill). Returns the number of rows written. No-op when the
    append DB isn't configured."""
    rows = [p for p in pages if p.get("url")]
    if not is_configured() or not rows:
        return 0
    await ensure_table()
    pool = await append_store.get_pool()
    n = len(COLUMNS)
    cols_sql = ", ".join(append_store._qi(c) for c in COLUMNS)
    update_sql = ", ".join(
        f'{append_store._qi(c)}=EXCLUDED.{append_store._qi(c)}'
        for c in COLUMNS if c != "url"
    )
    max_rows = max(1, append_store._MAX_PARAMS // n)
    total = 0
    async with pool.acquire() as conn:
        for i in range(0, len(rows), max_rows):
            chunk = rows[i:i + max_rows]
            value_groups = []
            params: list = []
            for r in chunk:
                ph = ", ".join(f"${len(params) + j + 1}" for j in range(n))
                value_groups.append(f"({ph})")
                params.extend(_row_tuple(r))
            sql = (
                f'INSERT INTO {append_store._qi(CBS_APPEND_TABLE)} ({cols_sql}) '
                f'VALUES {", ".join(value_groups)} '
                f'ON CONFLICT ("url") DO UPDATE SET {update_sql}'
            )
            await conn.execute(sql, *params)
            total += len(chunk)
    return total


async def backfill(db: AsyncSession, page_size: int = 500) -> int:
    """Copy every existing ``cbs_index`` row (main DB) into the NEON table.
    Run once after deploy via POST /api/cbs/sync-neon; safe to re-run."""
    if not is_configured():
        return 0
    from app.models.cbs_index import CbsIndex

    await ensure_table()
    offset = 0
    total = 0
    while True:
        result = await db.execute(
            select(CbsIndex).order_by(CbsIndex.id).limit(page_size).offset(offset)
        )
        batch = result.scalars().all()
        if not batch:
            break
        pages = [{c: getattr(r, c) for c in COLUMNS} for r in batch]
        total += await upsert_pages(pages)
        offset += page_size
        if len(batch) < page_size:
            break
    logger.info("cbs_neon: backfilled %d rows into %s", total, CBS_APPEND_TABLE)
    return total


async def backfill_if_empty() -> int:
    """Seed the NEON table from cbs_index the first time only — when the table
    has no rows yet. Called (non-blocking) at app startup so the CBS archive
    fills after the first deploy without any manual trigger. Idempotent and
    best-effort: any failure is logged, never raised (must not affect boot)."""
    if not is_configured():
        return 0
    try:
        await ensure_table()
        existing = await append_store.table_count(CBS_APPEND_TABLE)
        if existing and existing > 0:
            return 0
        from app.database import async_session
        async with async_session() as db:
            return await backfill(db)
    except Exception:  # noqa: BLE001 — startup mirror is advisory
        logger.warning("cbs_neon.backfill_if_empty failed", exc_info=True)
        return 0
