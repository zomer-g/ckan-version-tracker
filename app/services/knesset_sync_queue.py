"""The Knesset mirror's sync, as a task for the worker fleet.

knesset.gov.il geo-blocks this server, so the mirror (app/services/knesset_db)
is fed by a worker in Israel: the server queues ONE task, a worker claims it,
fetches the ODATA pages the plan asks for and posts them back
(app/api/worker.py, /api/worker/knesset-db/*). Same shape as the geocoding
queue (app/services/geocode_queue.py): a hidden tracked dataset whose
``scraper_config["kind"]`` is the worker's switch, and a task that carries the
run's parameters.

Cadence (owner's call, 2026-10-31): daily for what is new, weekly for what was
updated after the fact — see ``knesset_db.MODE_*``. The 3-minute in-process
tick is gone; it only made sense for the initial multi-million-row load.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.models.scrape_task import PRIORITY_MANUAL, PRIORITY_ROUTINE, ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.services import knesset_db

logger = logging.getLogger(__name__)

KIND = "knesset_db_sync"
DATASET_SLUG = "knesset-db-sync"
WEEKLY_DAY = 5          # Saturday (Monday = 0): the weekly look-back runs instead of the daily
OVERDUE = timedelta(hours=26)


async def ensure_dataset(db) -> TrackedDataset:
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name == DATASET_SLUG)
    )).scalar_one_or_none()
    if ds is not None:
        return ds
    ds = TrackedDataset(
        ckan_id=f"over-{DATASET_SLUG}",
        ckan_name=DATASET_SLUG,
        title="מסד הנתונים של הכנסת — סנכרון ODATA דרך הוורקר",
        organization="knesset.gov.il",
        source_type="scraper",
        source_url=knesset_db.PARLIAMENT_BASE,
        # A working ledger, not a dataset: the mirror itself is the `knesset`
        # schema. Hidden from the public lists like the geocoding row.
        status="hidden",
        # No poll job: the daily/weekly crons queue the work (poll_interval 0
        # means "not polled", see scheduler.add_poll_job).
        poll_interval=0,
        scraper_config={"kind": KIND},
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    logger.info("knesset sync: created tracked dataset %s", ds.id)
    return ds


async def outstanding(db) -> ScrapeTask | None:
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name == DATASET_SLUG)
    )).scalar_one_or_none()
    if ds is None:
        return None
    return (await db.execute(
        select(ScrapeTask).where(ScrapeTask.tracked_dataset_id == ds.id,
                                 ScrapeTask.status.in_(("pending", "running")))
        .limit(1)
    )).scalar_one_or_none()


def mode_for(now: datetime) -> str:
    from zoneinfo import ZoneInfo
    local = now.astimezone(ZoneInfo("Asia/Jerusalem"))
    return knesset_db.MODE_WEEKLY if local.weekday() == WEEKLY_DAY else knesset_db.MODE_DAILY


async def enqueue(db, mode: str, *, tables: list[str] | None = None,
                  actor: str = "scheduler") -> dict:
    """Queue one run, unless one is already waiting or running."""
    if mode not in knesset_db.MODES:
        raise ValueError(f"mode must be one of {knesset_db.MODES}")
    existing = await outstanding(db)
    if existing is not None:
        return {"queued": False, "reason": f"task {existing.id} still {existing.status}"}
    ds = await ensure_dataset(db)
    label = "שבועי — עדכונים בדיעבד" if mode == knesset_db.MODE_WEEKLY else "יומי — מה שחדש"
    task = ScrapeTask(
        tracked_dataset_id=ds.id, status="pending",
        priority=PRIORITY_MANUAL if actor != "scheduler" else PRIORITY_ROUTINE,
        phase="queued", params={"mode": mode, "tables": tables or None, "actor": actor},
        message=(f"סנכרון מסד הכנסת ({label})"
                 + (f": {', '.join(tables)}" if tables else ""))[:500],
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    logger.info("knesset sync: queued %s task %s (%s)", mode, task.id, actor)
    return {"queued": True, "task_id": str(task.id), "mode": mode}


async def last_synced_at() -> datetime | None:
    """The newest table sync in the mirror, or None (never / unreadable)."""
    try:
        pool = await knesset_db.append_store.get_pool()
        async with pool.acquire() as conn:
            return await conn.fetchval(
                f"SELECT max(last_synced_at) FROM {knesset_db._qtable('sync_state')}")
    except Exception:  # noqa: BLE001 — schema not there yet, DB down: unknown
        return None


async def scheduled() -> None:
    """The daily cron: weekly look-back on Saturday, otherwise what is new."""
    if not knesset_db.is_configured():
        return
    from app.database import async_session
    try:
        async with async_session() as db:
            res = await enqueue(db, mode_for(datetime.now(timezone.utc)))
        logger.info("knesset sync: %s", res)
    except Exception:  # noqa: BLE001 — never kill the scheduler job
        logger.exception("knesset sync: could not queue the run")


async def catch_up() -> None:
    """After boot: queue a daily run if the mirror is more than a day behind
    (the process was down at the cron's hour, or this is the first deploy)."""
    if not knesset_db.is_configured():
        return
    last = await last_synced_at()
    if last is not None and datetime.now(timezone.utc) - last < OVERDUE:
        return
    from app.database import async_session
    try:
        async with async_session() as db:
            res = await enqueue(db, knesset_db.MODE_DAILY)
        logger.info("knesset sync catch-up (last sync %s): %s", last, res)
    except Exception:  # noqa: BLE001
        logger.exception("knesset sync catch-up failed")
