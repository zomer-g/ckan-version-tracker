"""Which files of a CKAN package OVER already collects.

One rule, two callers. The public picker asks "what of this collection is
already here?" before the user ticks anything; the split-request handler asks
the same question again while creating datasets. If the two disagree the
picker offers a file the submit then refuses as a duplicate — which is exactly
the dead end this module exists to remove — so the claim logic lives here once
and both import it.

A CKAN package is a folder, not a table: its files are usually unrelated
tables on their own publishing rhythms, and each one becomes its OWN
TrackedDataset (own cadence, own versions page, own NEON table). "Collected"
is therefore a property of a single resource, not of the package.
"""

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset

logger = logging.getLogger(__name__)

# Only these states hold a resource. A REJECTED row is neither tracking nor a
# queued request — counting it would turn one rejection into an invisible
# tombstone blocking the whole package forever (the row shows up in no list, so
# the requester keeps being told the file is taken by something they can never
# find). Asking again is allowed; the admin can reject again.
HOLDING_STATES = ("active", "pending")


async def holders(db: AsyncSession, ckan_id: str) -> list[TrackedDataset]:
    """Every dataset currently holding resources of this CKAN package."""
    rows = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_id == ckan_id)
    )).scalars().all()
    return [d for d in rows if d.status in HOLDING_STATES]


def pinned_ids(ds: TrackedDataset, all_source_ids: set[str]) -> set[str]:
    """The resource ids ``ds`` holds.

    A row pinning nothing at all mirrors the WHOLE package (legacy "track
    all"), so its set is every resource at the source.
    """
    ids = set(ds.resource_ids or [])
    if ds.resource_id:
        ids.add(ds.resource_id)
    return ids or set(all_source_ids)


def is_combined_request(ds: TrackedDataset, pins: set[str]) -> bool:
    """A pending request for SEVERAL files at once.

    Such a row is not tracking anything — it is an earlier request for the same
    files at coarser granularity, and the split path supersedes it rather than
    treating it as a duplicate. The picker must therefore keep its files
    selectable, or the first "select all" submit locks the package forever.
    """
    return ds.status == "pending" and len(pins) > 1


def claims(
    rows: list[TrackedDataset],
    all_source_ids: set[str],
    *,
    skip: list[TrackedDataset] | None = None,
) -> dict[str, TrackedDataset]:
    """rid → the dataset already holding it. First holder wins.

    ``skip`` drops rows the caller has decided to supersede, so their files
    read as free.
    """
    skipped = {id(d) for d in (skip or [])}
    taken: dict[str, TrackedDataset] = {}
    for d in rows:
        if id(d) in skipped:
            continue
        for rid in pinned_ids(d, all_source_ids):
            taken.setdefault(rid, d)
    return taken


def describe(resources: list[dict], rows: list[TrackedDataset]) -> list[dict]:
    """Per-resource collection state, in source order.

    ``state`` is what the reader is told:

    * ``collected`` — an active dataset archives this file. Not selectable;
      the caller links to its versions page instead.
    * ``pending``   — a request for it is in the approval queue. Selectable
      only when it is a combined request the split path will supersede.
    * ``free``      — nobody holds it. This is what "can be added" means.
    """
    all_ids = {r["id"] for r in resources}
    taken = claims(rows, all_ids)
    out: list[dict] = []
    for res in resources:
        rid = res["id"]
        holder = taken.get(rid)
        entry: dict = {
            "id": rid,
            "name": res.get("name") or rid,
            "format": (res.get("format") or "").upper() or None,
            # Enough to tell a live file from the frozen historical copies a
            # publisher leaves lying around — the whole reason someone opens
            # this list is to pick the one that still updates.
            "last_modified": res.get("last_modified") or res.get("created"),
            "size": res.get("size"),
            "datastore_active": bool(res.get("datastore_active")),
        }
        if holder is None:
            entry.update(state="free", selectable=True)
        else:
            combined = is_combined_request(
                holder, pinned_ids(holder, all_ids)
            )
            entry.update(
                state="pending" if holder.status == "pending" else "collected",
                selectable=combined,
                dataset_id=str(holder.id),
                dataset_title=holder.title,
            )
        out.append(entry)
    return out
