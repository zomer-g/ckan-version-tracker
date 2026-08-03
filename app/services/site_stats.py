"""Site-wide totals for the home page hero.

Three numbers the existing hero stats (datasets / versions / organizations) do
not convey — how much data גרסאות לעם actually holds:

  * ``tables``  — queryable SQL tables on /data (the whole console catalog:
                  dataset tables, the idx mirror, odata/ocal/over, knesset)
  * ``rows``    — their total row count (planner ESTIMATES, never COUNT(*):
                  the corpus reaches into the tens of millions and exact counts
                  would mean a full scan of every one of ~3,000 tables)
  * ``files``   — every downloadable file across every saved version

All three are expensive relative to a home-page load — the catalog alone costs
seconds on a cold cache, and the file count expands every version's
``resource_mappings`` JSONB — while none of them changes between polls. So the
whole triple is served from one process-local TTL cache, and each half fails
soft: a broken/slow half returns ``None`` for its numbers and the hero simply
does not render that stat, rather than 500ing the home page.
"""
from __future__ import annotations

import asyncio
import logging
import time

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import append_store, data_catalog

logger = logging.getLogger(__name__)

# Slow-moving numbers on the most-visited page: a long TTL, refreshed lazily by
# whoever asks first after it expires.
_TTL_SECONDS = 900.0
_cache: dict | None = None
_cache_at: float = 0.0
_lock = asyncio.Lock()

# Keys in ``resource_mappings`` whose values are NOT downloadable files —
# bookkeeping hashes, seen-sets, and NEON table names. Mirror of the skip lists
# in app/api/v1.py::_extract_version_resources and
# storage_client.enumerate_files; the definition of "a file" below (an ``r2:``
# marker or a ≥30-char ODATA resource id) is the same one those two use.
_NON_FILE_KEYS = (
    "_hashes", "_resource_ids", "_appendonly_seen", "_names", "_filedates",
    "_probes", "append_table", "_append_tables",
)
# Inlined rather than bound: a fixed server-side constant (asserted below), so
# there is nothing to inject, and no dependency on how the driver types a
# text[] parameter.
assert all(k.replace("_", "").isalnum() for k in _NON_FILE_KEYS)
_SKIP_LITERAL = ", ".join(f"'{k}'" for k in _NON_FILE_KEYS)

# One pass over version_index, expanding both the string-valued entries (a named
# resource) and the list-valued aggregates (_zip_parts, _geojson, _gpkg …), then
# de-duplicated per version exactly as the per-version file list is.
#
# Both type guards sit INSIDE the function argument, not in a WHERE clause: a
# LATERAL set-returning function is evaluated per input row BEFORE the filter
# runs, so jsonb_each/jsonb_array_elements over a non-object/non-array would
# error out ("cannot extract elements from a scalar") rather than be skipped.
_FILES_SQL = text(f"""
WITH entries AS (
    SELECT v.id AS version_id, e.value AS val
      FROM version_index v
      CROSS JOIN LATERAL jsonb_each(
          CASE WHEN jsonb_typeof(v.resource_mappings) = 'object'
               THEN v.resource_mappings ELSE '{{}}'::jsonb END) e
     WHERE e.key NOT IN ({_SKIP_LITERAL})
), vals AS (
    SELECT version_id, val #>> '{{}}' AS v
      FROM entries WHERE jsonb_typeof(val) = 'string'
    UNION ALL
    SELECT e.version_id, el #>> '{{}}'
      FROM entries e
      CROSS JOIN LATERAL jsonb_array_elements(
          CASE WHEN jsonb_typeof(e.val) = 'array'
               THEN e.val ELSE '[]'::jsonb END) el
     WHERE jsonb_typeof(el) = 'string'
)
SELECT count(*) FROM (
    SELECT DISTINCT version_id, v FROM vals
     WHERE v LIKE 'r2:%' OR length(v) >= 30) d
""")


async def get_site_stats(db: AsyncSession) -> dict:
    """``{tables, rows, files}`` — any value may be ``None`` if that half failed.

    Cached for _TTL_SECONDS; the lock keeps a cold cache from being rebuilt N
    times concurrently (the build is seconds long, the home page is not)."""
    global _cache, _cache_at
    cached, age = _cache, time.monotonic() - _cache_at
    if cached is not None and age < _TTL_SECONDS:
        return cached
    async with _lock:
        cached, age = _cache, time.monotonic() - _cache_at
        if cached is not None and age < _TTL_SECONDS:
            return cached
        built = await _build(db)
        _cache, _cache_at = built, time.monotonic()
        return built


async def _build(db: AsyncSession) -> dict:
    tables, rows = await _catalog_totals(db)
    # Files LAST: it is the only statement that can time out, and a timeout
    # poisons the session's transaction — anything after it would fail too.
    files = await _file_total(db)
    return {"tables": tables, "rows": rows, "files": files}


async def _catalog_totals(db: AsyncSession) -> tuple[int | None, int | None]:
    """(table count, total estimated rows) over the same catalog /data lists."""
    try:
        catalog = await data_catalog.build_catalog(db)
    except Exception:  # noqa: BLE001 — the hero must not depend on the console
        logger.warning("site_stats: catalog build failed", exc_info=True)
        return None, None
    try:
        est = await append_store.schema_row_estimates(
            sorted({r["schema"] for r in catalog if r.get("schema")}))
    except Exception:  # noqa: BLE001
        logger.warning("site_stats: row estimates failed", exc_info=True)
        est = {}
    total = 0
    for rec in catalog:
        # Prefer the live planner estimate; fall back to the count the catalog
        # already carries (knesset's exact total, an odata import's row count)
        # for anything pg_class reports as never-analyzed.
        n = est.get((rec.get("schema"), rec.get("table"))) or 0
        if n <= 0:
            n = int(rec.get("est_rows") or 0)
        total += max(0, n)
    return len(catalog), total


async def _file_total(db: AsyncSession) -> int | None:
    try:
        # Bounded on purpose: this is the one statement here that scans a whole
        # table, and a home-page request must not hang on it. Over the limit we
        # report nothing rather than block (the cache keeps the retry rare).
        await db.execute(text("SET LOCAL statement_timeout = 30000"))
        result = await db.execute(_FILES_SQL)
        return int(result.scalar() or 0)
    except Exception:  # noqa: BLE001 — a slow scan must not break the home page
        logger.warning("site_stats: file count failed", exc_info=True)
        return None
