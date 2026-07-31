"""Which physical NEON table(s) a dataset's archived rows live in.

The ORM-aware half of that question: it reads ``version_index.resource_mappings``
and hands the JSONB to ``append_store.tables_from_mappings``, which is the pure
function that interprets it. append_store itself stays ORM-free (asyncpg +
config only), hence the split.

THIS MODULE EXISTS BECAUSE THE ANSWER WAS BEING COMPUTED IN THREE PLACES. A
dataset can have one table PER RESOURCE — a CKAN dataset archived as
``append_db_multi`` (keyed by datastore resource id) or a scraper dataset
publishing several tabular resources per version (keyed by resource name) — and
of the three readers only data_catalog knew it:

  * app/api/append.py looked for the single-table ``append_table`` key and fell
    back to ``table_name(ds)`` — so all 7 public endpoints served all 7 such
    datasets as completely empty;
  * app/mcp/server.py's query_dataset_rows called ``table_name(ds)`` directly and
    never read the mappings at all, so it reported ``total: 0`` to models — and
    it stayed broken after the API was fixed, because it never went through it.

Both now call in here. A fourth reader that resolves this itself is a bug
waiting to be written.
"""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.version_index import VersionIndex
from app.services import append_store


async def resolve_tables(ds, db: AsyncSession) -> list[dict]:
    """``[{table, resource_id, resource_name}]`` for a dataset, first table first.

    Uses the most recent version that carries a NEON mapping — either shape —
    and falls back to the deterministic single-table name when no version has one
    (a classic append_only dataset, or an archive_neon dataset seeded
    retroactively before its first forward dual-write version exists)."""
    rows = (await db.execute(
        select(VersionIndex.resource_mappings)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc())
    )).all()
    for (mappings,) in rows:
        if mappings and (mappings.get("append_table") or mappings.get("_append_tables")):
            return append_store.tables_from_mappings(ds, mappings)
    return append_store.tables_from_mappings(ds, None)


def is_append_archive(ds) -> bool:
    """Whether this dataset accumulates rows in the append DB at all — a classic
    ``append_only`` dataset, or a full-snapshot one opted into a NEON archive."""
    from app.services.storage_client import dataset_archives_neon
    return ds.storage_mode == "append_only" or dataset_archives_neon(ds)
