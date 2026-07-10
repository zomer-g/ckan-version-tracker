import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from app.database import async_session
from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.worker.poll_job import poll_dataset, resume_interrupted_appends
from app.worker.datastore_push_runner import (
    cleanup_stuck_push_jobs,
    drain_one_job,
)
from app.worker.drive_export_runner import (
    cleanup_stuck_drive_exports,
    drain_one_drive_export,
)

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
            # GovMap coverage datasets are driven ONLY by the twice-daily
            # coverage rollout (below), never by a per-dataset poll job — skip
            # them here so we don't also fire them on their own interval (and
            # never mass-fire the whole catalog on a deploy).
            if (ds.scraper_config or {}).get("coverage_managed"):
                continue
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

    # Durable datastore-push queue runner. Drains one pending job per
    # 30s tick — keeps RSS bounded because the push itself is the
    # heavy work and we never want two of them racing on the same
    # dyno. ``max_instances=1`` is the safety belt (overlapping fires
    # would otherwise stack up if a tick takes >30s).
    scheduler.add_job(
        drain_one_job,
        trigger=IntervalTrigger(seconds=30),
        id="drain_datastore_push_queue",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    # And the matching stuck-job rescuer. Same pattern as
    # cleanup_stuck_scrape_tasks above.
    scheduler.add_job(
        cleanup_stuck_push_jobs,
        trigger=IntervalTrigger(minutes=5),
        id="cleanup_stuck_push_jobs",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    # Resume driver for interrupted giant append polls. A full content-diff
    # scan of a multi-million-row datastore dataset can't finish in one
    # web-process invocation (a dyno recycle/deploy kills it partway); the
    # delta archiver checkpoints seed_offset so this driver re-runs the poll to
    # resume from the checkpoint, one dataset per tick, until the scan
    # completes and clears the checkpoint. ``max_instances=1`` guarantees we
    # never run two heavy streams at once (RSS/OOM safety). 3-minute cadence:
    # long enough that a still-running resume isn't nagged, short enough that a
    # killed scan restarts promptly.
    scheduler.add_job(
        resume_interrupted_appends,
        trigger=IntervalTrigger(minutes=3),
        id="resume_interrupted_appends",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # Durable Drive-export queue runner + its stuck-job rescuer. Same
    # one-job-per-tick shape as the datastore-push queue above; the work is
    # IO-bound (R2 → dyno → Drive) so it doesn't block other scheduler jobs.
    scheduler.add_job(
        drain_one_drive_export,
        trigger=IntervalTrigger(seconds=30),
        id="drain_drive_export_queue",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        cleanup_stuck_drive_exports,
        trigger=IntervalTrigger(minutes=5),
        id="cleanup_stuck_drive_exports",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    # GovMap full-coverage rollout: a TOP-UP queue for the worker fleet. Every
    # 3 min the tick refills the active coverage tasks up to
    # GOVMAP_COVERAGE_CONCURRENCY (default 6 — the operator runs 4 OVER
    # workers on 3 machines; a small buffer above the fleet size keeps a
    # finishing worker claiming instantly), never-triggered layers first,
    # then stalest. 10-min ticks left the fleet idle most of each window:
    # small layers finish in 1-3 min, so 4 workers drained the batch and
    # waited ~7 min for the next one (observed ~17 layers/h vs the fleet's
    # ~60/h capacity). The tick is cheap (two SELECTs) and self-skips at the
    # concurrency target. See app/services/govmap_coverage.py.
    from app.services.govmap_coverage import scrape_next_layer
    scheduler.add_job(
        scrape_next_layer,
        trigger=IntervalTrigger(minutes=3),
        id="govmap_coverage_rollout",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # Knesset ODATA mirror: advance the sync within a per-tick time budget
    # (initial full load of ~3M rows spans many ticks; each tick checkpoints,
    # then it settles into 12h incremental refreshes). Memory-safe: at most
    # ~1000 x 100-column rows are held between INSERT batches. max_instances=1
    # + the module's own sync_lock keep it single-flight even against the
    # admin's manual trigger. No-op when the append DB / feature is off.
    from app.config import settings as _settings
    from app.services.knesset_db import sync_tick as _knesset_sync_tick

    async def knesset_db_sync_job() -> None:
        try:
            await _knesset_sync_tick(
                budget_seconds=_settings.knesset_db_tick_budget_seconds,
                sync_interval_hours=_settings.knesset_db_sync_interval_hours,
            )
        except Exception:  # noqa: BLE001 — never let a feed flap kill the job
            logger.exception("knesset_db sync tick failed")

    scheduler.add_job(
        knesset_db_sync_job,
        trigger=IntervalTrigger(minutes=3),
        id="knesset_db_sync",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
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
    fires immediately when overdue / never polled), not "now + interval".
    This matters on Render: every deploy/restart re-runs init_scheduler,
    and a bare IntervalTrigger(seconds=N) starts counting from
    registration time. For datasets configured with weekly/monthly
    intervals — and a deploy cadence faster than that — the timer never
    accumulates enough wall-clock to fire.

    Computing start_date for the "fire immediately" case is fiddly.
    APScheduler's IntervalTrigger.get_next_fire_time has two branches:
      - start_date > now → next_fire = start_date (exact)
      - else → next_fire = start_date + ceil(diff/interval) * interval

    The "else" branch is unusable for "fire on next tick" — float drift
    between add_job and the trigger evaluation means diff is tiny but
    positive, so ceil rounds up to 1, and next_fire ends up start_date
    + one full interval. Confirmed empirically: weekly datasets we
    intended to fire immediately got "next_run = deploy_time + 7 days"
    and never ran.

    Use the deterministic "future" branch instead: start_date =
    now + 1s. Then start_date > now strictly, next_fire = start_date,
    and the trigger fires on the next scheduler tick (~1s later).
    Subsequent fires anchor off previous_fire_time, so the chosen
    1-second offset doesn't propagate into the cadence.
    """
    now = datetime.now(timezone.utc)
    interval = timedelta(seconds=interval_seconds)
    immediate = now + timedelta(seconds=1)
    if last_polled_at is None:
        # Brand-new dataset — fire on next tick.
        start_date = immediate
    else:
        candidate = last_polled_at + interval
        if candidate > now:
            # Not overdue — fire at the natural time.
            start_date = candidate
        else:
            # Overdue — fire on next tick.
            start_date = immediate

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
