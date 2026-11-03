"""Finding a scraped source's tables when each table is a dataset of its own.

Sources that used to publish several tables side by side in ONE dataset
(מכרזי רמ"י, הוצאות חברי הכנסת) now publish one dataset per table. A consumer
that looked a table up by its resource name inside one known dataset breaks on
that: each table now sits in its own dataset, as that dataset's primary
resource. This module answers "where is the table with THESE columns" instead.

A role is found by its column signature — the columns it must have, and any it
must not (a summary table's columns are a subset of the transactions', so the
summary role excludes a transaction-only column). Only tables of a dataset's
CURRENT version count, read from the /data catalog: when a dataset changes
shape its superseded tables stay in the schema until they are purged, and a
stale copy must never answer. Where two current tables match one role (the old
combined dataset still beside the new per-table one), a table that is its
dataset's ONLY table wins, then the larger.

``ckan_prefix`` narrows the search to one source (a manifest source's datasets
all carry ``<id>-scraper-`` as their ckan_id prefix), so another dataset that
happens to share the columns is never picked up.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from app.services import append_store

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Role:
    include: frozenset[str]
    exclude: frozenset[str] = frozenset()

    def matches(self, columns: set[str]) -> bool:
        return self.include <= columns and not (self.exclude & columns)


async def catalog_tables() -> dict[str, dict] | None:
    """``{table: {dataset_id, ckan_id, n_tables}}`` for the public tables of
    every dataset's current version, from the /data catalog (cached there).
    None when the catalog cannot be read (no main DB in this process)."""
    try:
        from app.database import async_session
        from app.services import data_catalog
        async with async_session() as db:
            recs = await data_catalog.build_catalog(db)
    except Exception:  # noqa: BLE001
        logger.debug("source_tables: catalog unavailable", exc_info=True)
        return None
    recs = [r for r in recs if r.get("schema") == "public" and r.get("dataset_id")]
    per_ds: dict[str, int] = {}
    for r in recs:
        per_ds[r["dataset_id"]] = per_ds.get(r["dataset_id"], 0) + 1
    return {r["table"]: {"dataset_id": r["dataset_id"], "ckan_id": r.get("ckan_id") or "",
                         "n_tables": per_ds[r["dataset_id"]]} for r in recs}


def pick(roles: dict[str, Role], columns_by_table: dict[str, set[str]],
         rank: dict[str, tuple]) -> dict[str, str]:
    """Pure: ``{role: table}`` — the highest-ranked table matching each role."""
    out: dict[str, str] = {}
    for table in sorted(columns_by_table):
        cols = columns_by_table[table]
        for role, sig in roles.items():
            if sig.matches(cols) and (role not in out
                                      or rank.get(table, ()) > rank.get(out[role], ())):
                out[role] = table
    return out


async def find(roles: dict[str, Role], *, ckan_prefix: str | None = None,
               catalog: dict[str, dict] | None = None) -> dict[str, dict]:
    """``{role: {schema, table, columns, dataset_id}}`` on this deployment.

    ``catalog`` may be passed in (tests, or a caller that already read it);
    otherwise it is read here. Without a catalog every append table counts and
    no dataset id is known — the degraded answer, not a wrong one."""
    raw = await append_store.schema_table_columns("public")
    cols = {t: {c["name"] if isinstance(c, dict) else c for c in cs}
            for t, cs in raw.items() if t.startswith("append_")}
    cols = {t: c for t, c in cols.items() if any(r.matches(c) for r in roles.values())}
    if catalog is None:
        catalog = await catalog_tables()
    if catalog is not None:
        cols = {t: c for t, c in cols.items() if t in catalog
                and (not ckan_prefix or catalog[t]["ckan_id"].startswith(ckan_prefix))}
    rank: dict[str, tuple] = {}
    for t in cols:
        try:
            est = await append_store.table_count_estimate(t)
        except Exception:  # noqa: BLE001
            est = 0
        rank[t] = (bool(catalog and catalog[t]["n_tables"] == 1), est)
    return {role: {"schema": "public", "table": t, "columns": sorted(cols[t]),
                   "dataset_id": (catalog or {}).get(t, {}).get("dataset_id")}
            for role, t in pick(roles, cols, rank).items()}
