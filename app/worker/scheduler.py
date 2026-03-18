import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import select

from app.database import async_session
from app.models.tracked_dataset import TrackedDataset
from app.worker.poll_job import poll_dataset

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def init_scheduler() -> None:
    """Load all active tracked datasets and schedule their poll jobs."""
    async with async_session() as db:
        result = await db.execute(
            select(TrackedDataset).where(TrackedDataset.is_active.is_(True))
        )
        datasets = result.scalars().all()

        for ds in datasets:
            add_poll_job(str(ds.id), ds.poll_interval)
            logger.info("Scheduled poll for %s every %ds", ds.ckan_name, ds.poll_interval)

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))


def add_poll_job(dataset_id: str, interval_seconds: int) -> None:
    """Add or replace a poll job for a dataset."""
    scheduler.add_job(
        poll_dataset,
        trigger=IntervalTrigger(seconds=interval_seconds),
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
