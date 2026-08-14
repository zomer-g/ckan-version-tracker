"""Whether a tracked dataset may be served to an anonymous caller.

Most public surfaces filter `status IN ('active','pending')`, so a dataset in
any other state drops out of them for free. The **by-id** endpoints do not: they
load a row by primary key and 404 only if it is missing, so a dataset that is
absent from every list is still fully readable — including its versions and its
files — to anyone holding the UUID. For a dataset that is merely `duplicate` or
`failed` that is intentional (a bookmarked link keeps working).

For `hidden` it is not. `hidden` means *deliberately unpublished*, so the by-id
paths have to be told, and this module is where that is said once.

Note what this deliberately does NOT do: it does not restrict the by-id paths to
the public statuses. That would 404 duplicates and failed datasets too, changing
behaviour well beyond the request and breaking existing links. It excludes
exactly the one status that means "do not publish this".
"""
from __future__ import annotations

from fastapi import HTTPException

#: Served by the public lists.
PUBLIC_STATUSES = ("active", "pending")

#: Tracked and polled, but not published — see migration 063.
HIDDEN_STATUS = "hidden"


def is_hidden(ds) -> bool:
    return ds is not None and getattr(ds, "status", None) == HIDDEN_STATUS


def require_visible(ds, *, detail: str = "Dataset not found"):
    """404 a hidden dataset, exactly as if it did not exist.

    The 404 is the point: a 403 would confirm that the id is real."""
    if is_hidden(ds):
        raise HTTPException(status_code=404, detail=detail)
    return ds


async def require_visible_version(db, version, *, detail: str = "Version not found"):
    """Same, reached through a version's parent dataset."""
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset

    if version is None:
        return version
    ds = (await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.id == version.tracked_dataset_id)
    )).scalar_one_or_none()
    if is_hidden(ds):
        raise HTTPException(status_code=404, detail=detail)
    return version
