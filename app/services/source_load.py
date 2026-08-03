"""Per-source load control: how many workers may scrape one upstream at once.

The queue's priority bands (app/models/scrape_task.py) decide WHICH task goes
out next. They say nothing about how many go out at once *to the same site* —
so a GovMap sweep or a 38-dataset munidata batch can put the whole fleet on one
upstream, which is both rude to that server and a good way to get throttled or
blocked while every other source waits.

This module adds the missing axis: a cap, per source, on concurrently RUNNING
tasks. One worker runs one task at a time, so "running tasks for source X" is
exactly "workers on source X".

WHAT A SOURCE IS
────────────────
The upstream site, which is what a rate limit is really about — not the
publishing ministry (many ministries, one data.gov.il) and not the dataset kind.
The server can derive it from columns it already has: every scraper dataset's
``ckan_id`` is ``"<source>-scraper-<something>"`` (the same prefix the worker's
source manifests are keyed by, see app/models/source_registry.py), and
everything else is its ``source_type``. Live catalog, for scale:

    govmap 866 · ckan 93 · govil 65 · munidata 38 · idf 11 · registries 10 ·
    mevaker 9 · mavat 4 · mankal 4 · jda 3 · knesset/eden/avodata/health 2 ·
    ykpubdata/workagreements/emun/servicescompass/hatzav/cbs 1

``source_key`` (Python, for counting and display) and ``source_filter`` (SQL,
for excluding a saturated source from the claim query) are two directions of
one rule, so they are defined together here and pinned against each other in
tests/test_source_load.py. Deriving the key in SQL instead would need
``split_part``, which the SQLite test suite doesn't have.
"""
from __future__ import annotations

import logging

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.scrape_task import ScrapeTask
from app.models.source_limit import SourceLimit
from app.models.tracked_dataset import TrackedDataset

logger = logging.getLogger(__name__)

_SCRAPER_MARK = "-scraper-"


def source_key(ckan_id: str | None, source_type: str | None) -> str:
    """The upstream this dataset is collected from.

    Scraper datasets carry it in the ckan_id prefix; everything else is its
    source_type ("govmap", "ckan", "cbs"). Never returns "" — an unclassifiable
    row lands in "unknown" so it can still be seen and capped, rather than
    silently escaping every limit.
    """
    cid = (ckan_id or "").strip()
    if _SCRAPER_MARK in cid:
        prefix = cid.split(_SCRAPER_MARK, 1)[0].strip()
        if prefix:
            return prefix
    return (source_type or "").strip() or "unknown"


def source_filter(key: str):
    """SQL twin of :func:`source_key` — "this dataset belongs to source `key`".

    Built from LIKE alone so it runs on both Postgres and the SQLite test DB,
    and so the claim query stays a plain indexable predicate rather than a
    function call over every candidate row.
    """
    scraper_prefix = f"{key}{_SCRAPER_MARK}%"
    if key == "unknown":
        # Neither a scraper prefix nor a usable source_type.
        return and_(
            TrackedDataset.ckan_id.notlike(f"%{_SCRAPER_MARK}%"),
            or_(TrackedDataset.source_type.is_(None), TrackedDataset.source_type == ""),
        )
    return or_(
        TrackedDataset.ckan_id.like(scraper_prefix),
        and_(
            TrackedDataset.ckan_id.notlike(f"%{_SCRAPER_MARK}%"),
            TrackedDataset.source_type == key,
        ),
    )


async def running_by_source(db: AsyncSession) -> dict[str, int]:
    """How many workers are on each source right now."""
    rows = await db.execute(
        select(TrackedDataset.ckan_id, TrackedDataset.source_type)
        .join(ScrapeTask, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "running")
    )
    counts: dict[str, int] = {}
    for ckan_id, source_type in rows.all():
        k = source_key(ckan_id, source_type)
        counts[k] = counts.get(k, 0) + 1
    return counts


async def limits(db: AsyncSession) -> dict[str, int]:
    """Configured caps, keyed by source. Absent = uncapped.

    Fails OPEN, and this is the important part: ``saturated_sources`` runs on
    every worker poll, so if this query raises — the table missing because the
    app deployed ahead of its migration is the realistic case — an uncaught
    error here would 500 the claim path and stop the ENTIRE fleet from getting
    work. A load-control feature must never be able to do that. Losing the caps
    degrades to the behaviour we had before they existed; losing dispatch is an
    outage. The warning is what makes the degradation visible, and it heals by
    itself the moment the table appears.

    The rollback is not optional: a failed statement poisons the transaction, so
    without it the claim query immediately after would fail too — turning the
    fail-open back into the outage it exists to prevent.
    """
    try:
        rows = await db.execute(select(SourceLimit.source_key, SourceLimit.max_workers))
        return {k: v for k, v in rows.all()}
    except SQLAlchemyError as e:  # noqa: BLE001 — see docstring: dispatch outranks caps
        await db.rollback()
        logger.warning(
            "source_limits unreadable (%s: %s) — dispatching with no per-source "
            "caps until it is back", type(e).__name__, e,
        )
        return {}


async def saturated_sources(db: AsyncSession) -> dict[str, tuple[int, int]]:
    """Sources that must not be handed another task, as {key: (running, cap)}.

    ``running >= cap`` and not ``==``, because a cap lowered while work is in
    flight leaves a source legitimately over it: running tasks are never
    preempted (that would throw away a scrape mid-way), so a lowered cap takes
    effect by starving new claims until the excess drains.
    """
    caps = await limits(db)
    if not caps:
        return {}
    running = await running_by_source(db)
    return {
        key: (running.get(key, 0), cap)
        for key, cap in caps.items()
        if running.get(key, 0) >= cap
    }


async def source_load(db: AsyncSession) -> list[dict]:
    """One row per source for the admin panel: size, live load, and the cap."""
    ds_rows = await db.execute(
        select(TrackedDataset.ckan_id, TrackedDataset.source_type, TrackedDataset.is_active)
    )
    datasets: dict[str, int] = {}
    active: dict[str, int] = {}
    for ckan_id, source_type, is_act in ds_rows.all():
        k = source_key(ckan_id, source_type)
        datasets[k] = datasets.get(k, 0) + 1
        if is_act:
            active[k] = active.get(k, 0) + 1

    task_rows = await db.execute(
        select(ScrapeTask.status, TrackedDataset.ckan_id, TrackedDataset.source_type)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status.in_(["pending", "running"]))
    )
    running: dict[str, int] = {}
    pending: dict[str, int] = {}
    for status, ckan_id, source_type in task_rows.all():
        k = source_key(ckan_id, source_type)
        bucket = running if status == "running" else pending
        bucket[k] = bucket.get(k, 0) + 1

    caps = await limits(db)
    keys = set(datasets) | set(caps)
    out = [
        {
            "source_key": k,
            "datasets": datasets.get(k, 0),
            "active_datasets": active.get(k, 0),
            "running": running.get(k, 0),
            "pending": pending.get(k, 0),
            "max_workers": caps.get(k),
        }
        for k in keys
    ]
    # Busiest first — the panel exists to answer "who is eating the fleet".
    out.sort(key=lambda r: (-r["running"], -r["pending"], -r["datasets"], r["source_key"]))
    return out
