"""The home page's tracked-dataset list, one page at a time.

The home page used to download the whole public catalog (GET /api/datasets:
~1,850 rows, 2.3MB of JSON) just to show 24 cards and three hero numbers. On
anything slower than a fast desktop line that held the list and the top of the
hero on "—" / "טוען..." for many seconds.

This module keeps a slim copy of that catalog in process memory — only the
fields a home card renders, plus a lower-cased search haystack — and answers
each request with one page of it, already ordered (pinned datasets first, the
Knesset committees folded into one tile) and filtered (the in-site keyword
search). The hero's datasets / versions / organizations totals come with it,
computed over the same rows the old client-side reduce summed.

Freshness: the catalog changes when a dataset is approved or deleted and when a
poll writes a version, never per request. A TTL of a minute keeps it close;
past the TTL the stale copy is still served while one background task rebuilds
it, so no visitor waits on the multi-second build except on a cold process.
The pinned list is NOT cached — it is one tiny row, read per request, so an
admin's pin shows on their very next load.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.organization import Organization
from app.models.page_content import PageContent
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex

logger = logging.getLogger(__name__)

PAGE_SIZE = 24
# Keep in sync with HomePage.tsx: the hundreds of per-committee datasets carry
# this tag and are shown as ONE tile linking to /knesset when not searching.
KNESSET_COMMITTEE_TAG = "ועדות כנסת"
FEATURED_KEY = "featured_datasets"

_TTL_SECONDS = 60.0
_cache: dict | None = None
_cache_at: float = 0.0
_lock = asyncio.Lock()
_refresh_task: asyncio.Task | None = None


def _haystack(card: dict) -> str:
    """Lower-cased searchable text — the same fields the client used to match."""
    return " ".join(
        s for s in (
            card["title"],
            card["organization_title"],
            card["organization"],
            card["ckan_id"],
            card["source_url"],
            *(t["name"] for t in card["tags"]),
        ) if s
    ).lower()


async def _build(db: AsyncSession) -> dict:
    # Same row set as the public catalog (app/api/datasets.py list_tracked), so
    # the hero totals do not change meaning.
    from app.api.datasets import _build_source_url

    result = await db.execute(
        select(TrackedDataset, Organization)
        .options(selectinload(TrackedDataset.tags))
        .outerjoin(Organization, TrackedDataset.organization_id == Organization.id)
        .where(
            TrackedDataset.status.in_(["active", "pending"]),
            TrackedDataset.ckan_name.notlike("knesset-committee-single-%"),
        )
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = result.unique().all()
    count_result = await db.execute(
        select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
        .group_by(VersionIndex.tracked_dataset_id)
    )
    version_counts = dict(count_result.all())

    cards: list[dict] = []
    orgs: set[str] = set()
    versions = 0
    for ds, org in rows:
        tags = [{"id": str(t.id), "name": t.name} for t in ds.tags]
        card = {
            "id": str(ds.id),
            "ckan_id": ds.ckan_id,
            "ckan_name": ds.ckan_name,
            "title": ds.title,
            "organization": ds.organization,
            "organization_id": str(ds.organization_id) if ds.organization_id else None,
            "organization_title": org.title if org else None,
            "odata_dataset_id": ds.odata_dataset_id,
            "poll_interval": ds.poll_interval,
            "version_count": version_counts.get(ds.id, 0),
            "resource_name": None,
            "source_type": ds.source_type or "ckan",
            "source_url": _build_source_url(ds),
            "tags": tags,
        }
        versions += card["version_count"]
        org_key = card["organization_id"] or ds.organization
        if org_key:
            orgs.add(org_key)
        cards.append({
            "card": card,
            "hay": _haystack(card),
            "committee": any(t["name"] == KNESSET_COMMITTEE_TAG for t in tags),
        })
    return {
        "entries": cards,
        "stats": {"datasets": len(cards), "versions": versions, "organizations": len(orgs)},
    }


async def _refresh_in_background() -> None:
    global _cache, _cache_at
    from app.database import async_session

    try:
        async with async_session() as db:
            built = await _build(db)
        _cache, _cache_at = built, time.monotonic()
    except Exception:  # noqa: BLE001 — keep serving the stale copy
        logger.warning("home_catalog: background refresh failed", exc_info=True)


async def get_catalog(db: AsyncSession) -> dict:
    """The cached slim catalog; built inline only when there is none at all."""
    global _cache, _cache_at, _refresh_task
    if _cache is not None:
        if time.monotonic() - _cache_at >= _TTL_SECONDS and (
            _refresh_task is None or _refresh_task.done()
        ):
            _refresh_task = asyncio.create_task(_refresh_in_background())
        return _cache
    async with _lock:
        if _cache is None:
            _cache, _cache_at = await _build(db), time.monotonic()
        return _cache


async def featured_ids(db: AsyncSession) -> list[str]:
    """Admin-pinned dataset ids, in pin order (see app/api/page_content.py)."""
    try:
        value = (await db.execute(
            select(PageContent.value).where(
                PageContent.page == "home",
                PageContent.lang == "he",
                PageContent.key == FEATURED_KEY,
            )
        )).scalar_one_or_none()
        ids = json.loads(value or "[]")
    except Exception:  # noqa: BLE001 — an unreadable pin list just means no pins
        logger.warning("home_catalog: featured list unreadable", exc_info=True)
        return []
    return [i for i in ids if isinstance(i, str)] if isinstance(ids, list) else []


def page_of(catalog: dict, featured: list[str], q: str, page: int) -> dict:
    """One page of the home list, ordered and sliced exactly as the page shows it.

    Searching: every query word must appear in the haystack (order-free), and
    the committees surface individually. Not searching: pinned datasets lead in
    pin order, and the committees fold into one tile that takes the first slot
    of page 1 — a slot, so every page still holds PAGE_SIZE tiles.
    """
    entries = catalog["entries"]
    tokens = q.strip().lower().split()
    committee = None
    if tokens:
        shown = [e["card"] for e in entries if all(t in e["hay"] for t in tokens)]
    else:
        rest = [e["card"] for e in entries if not e["committee"]]
        folded = [e["card"] for e in entries if e["committee"]]
        if folded:
            committee = {
                "count": len(folded),
                "versions": sum(c["version_count"] for c in folded),
            }
        if featured:
            by_id = {c["id"]: c for c in rest}
            head = [by_id[i] for i in featured if i in by_id]
            pinned = {c["id"] for c in head}
            rest = head + [c for c in rest if c["id"] not in pinned]
        shown = rest

    offset = 1 if committee else 0
    tiles = len(shown) + offset
    page_count = max(1, -(-tiles // PAGE_SIZE))
    page = min(max(1, page), page_count)
    slot = (page - 1) * PAGE_SIZE
    items = shown[max(0, slot - offset):max(0, slot + PAGE_SIZE - offset)]
    return {
        "items": items,
        "total": len(shown),
        "page": page,
        "page_size": PAGE_SIZE,
        "committee": committee,
        "stats": catalog["stats"],
    }
