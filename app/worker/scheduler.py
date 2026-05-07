import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select

from app.database import async_session
from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.worker.poll_job import poll_dataset

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def cleanup_stuck_scrape_tasks() -> None:
    """Periodically mark 'running' scrape tasks as failed when the worker
    has stopped sending progress updates. Runs independently of worker polls,
    so stuck tasks get cleaned even when no worker is online.

    Liveness is determined solely by heartbeat (updated_at): if the worker
    is still posting progress, the task is alive — long-but-healthy scrapes
    (e.g. tens of thousands of attachments behind a slow upstream) are fine.
    """
    now = datetime.now(timezone.utc)
    heartbeat_cutoff = now - timedelta(minutes=10)
    async with async_session() as db:
        result = await db.execute(
            select(ScrapeTask).where(
                ScrapeTask.status == "running",
                ScrapeTask.updated_at < heartbeat_cutoff,
            )
        )
        stuck = result.scalars().all()
        if not stuck:
            return
        for task in stuck:
            age_min = int((now - task.created_at).total_seconds() / 60) if task.created_at else 0
            hb_min = int((now - task.updated_at).total_seconds() / 60) if task.updated_at else age_min
            task.status = "failed"
            task.phase = "timeout"
            task.error = (
                f"Task auto-reset by scheduler: no heartbeat for {hb_min} min "
                f"(task age {age_min} min) — worker likely crashed"
            )
            task.completed_at = now
            logger.warning(
                "Scheduler auto-reset stuck task %s (age=%dmin, no heartbeat for %dmin)",
                task.id, age_min, hb_min,
            )
        await db.commit()


async def init_scheduler() -> None:
    """Load all active tracked datasets and schedule their poll jobs."""
    async with async_session() as db:
        result = await db.execute(
            select(TrackedDataset).where(
                TrackedDataset.is_active.is_(True),
                TrackedDataset.status == "active",
            )
        )
        datasets = result.scalars().all()

        for ds in datasets:
            add_poll_job(
                str(ds.id), ds.poll_interval,
                last_polled_at=ds.last_polled_at,
            )
            logger.info(
                "Scheduled poll for %s every %ds (last=%s)",
                ds.ckan_name, ds.poll_interval, ds.last_polled_at,
            )

    # Periodic cleanup of stuck scrape tasks (every 5 min)
    scheduler.add_job(
        cleanup_stuck_scrape_tasks,
        trigger=IntervalTrigger(minutes=5),
        id="cleanup_stuck_scrape_tasks",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))


def add_poll_job(
    dataset_id: str,
    interval_seconds: int,
    last_polled_at: datetime | None = None,
) -> None:
    """Add or replace a poll job for a dataset.

    Anchors the schedule to `last_polled_at + interval_seconds` (or
    `now` when never polled), not "now + interval". This matters on
    Render: every deploy/restart re-runs init_scheduler, and the bare
    IntervalTrigger(seconds=N) starts counting from registration time.
    For datasets configured with weekly/monthly intervals — and a
    deploy cadence faster than that — the timer never accumulates
    enough wall-clock to fire. Anchoring to last_polled_at means
    restarts preserve the "next fire" decision: if the dataset is
    already overdue we fire immediately on startup; otherwise we wait
    just the remaining time.
    """
    now = datetime.now(timezone.utc)
    if last_polled_at is None:
        # Brand-new dataset (never polled). Fire on next scheduler tick.
        start_date = now
    else:
        candidate = last_polled_at + timedelta(seconds=interval_seconds)
        # If overdue, fire on next tick; otherwise fire at the computed
        # time. APScheduler refuses start_date in the past for
        # IntervalTrigger, so we clamp to now.
        start_date = candidate if candidate > now else now

    scheduler.add_job(
        poll_dataset,
        trigger=IntervalTrigger(
            seconds=interval_seconds,
            start_date=start_date,
        ),
        id=f"poll_{dataset_id}",
        args=[dataset_id],
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )


def remove_poll_job(dataset_id: str) -> None:
    """Remove a poll job for a dataset."""
    job_id = f"poll_{dataset_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)


def shutdown_scheduler() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)
