import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select

from app.config import settings
from app.database import async_session
from app.models.scrape_task import INTERRUPTED_MESSAGE, PHASE_INTERRUPTED, ScrapeTask
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
            # An interrupted run, not a failed scrape — see PHASE_INTERRUPTED.
            task.status = "failed"
            task.phase = PHASE_INTERRUPTED
            task.error = INTERRUPTED_MESSAGE.format(hb=hb_min, age=age_min)
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
        # MMM document catalog → knesset.mmm_documents. Version-gated: a no-op
        # single SELECT until the MMM dataset publishes a new version.
        try:
            from app.services import knesset_mmm_db
            if _settings.knesset_db_enabled and _settings.append_database_url:
                await knesset_mmm_db.sync_if_due()
        except Exception:  # noqa: BLE001
            logger.exception("knesset_mmm_db sync failed")

    scheduler.add_job(
        knesset_db_sync_job,
        trigger=IntervalTrigger(minutes=3),
        id="knesset_db_sync",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # Index-CSV mirror → the `idx` schema, so /data can search inside the
    # collections. Version-gated: `pending` is three SELECTs and an in-memory
    # diff, so a tick where nothing new landed costs almost nothing — which is
    # the normal case (GovMap polls every 90 days). A small per-tick chunk keeps
    # each run's memory to ONE streaming CSV at a time and lets a large backfill
    # advance steadily in the background instead of in one long blocking job.
    async def index_mirror_sync_job() -> None:
        if not _settings.append_database_url:
            return
        from app.database import async_session
        from app.services import index_mirror
        try:
            async with async_session() as db:
                await index_mirror.sync_due(db, limit=_settings.index_mirror_chunk)
        except Exception:  # noqa: BLE001 — never let one bad CSV kill the job
            logger.exception("index_mirror sync tick failed")
        # Geometry backfill: bring already-mirrored layers up to the current
        # PostGIS setting without waiting for a new version to land on each one
        # (GovMap polls every 90 days, so that wait is effectively forever).
        #
        # Deliberately its own try: a backfill problem must not be mistaken for
        # a sync problem, and vice versa. Self-terminating — candidates are
        # layers MISSING the column, so once the corpus is converted every tick
        # costs one cheap catalog query and stops. All the work is database-side.
        try:
            s = await index_mirror.backfill_geometry(
                limit=_settings.index_mirror_geom_backfill_chunk)
            if s.get("converted"):
                logger.info("index_mirror geometry backfill: converted=%d remaining=%d",
                            s["converted"], s.get("remaining"))
        except Exception:  # noqa: BLE001 — never let it kill the sync job
            logger.exception("index_mirror geometry backfill tick failed")

    if settings.index_mirror_enabled:
        scheduler.add_job(
            index_mirror_sync_job,
            trigger=IntervalTrigger(minutes=_settings.index_mirror_interval_minutes),
            id="index_mirror_sync",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
        )

    # Column captions: lift each GovMap layer's Hebrew field dictionary out of
    # the documentation bundle already archived on its latest version, so /data
    # can label machine-named columns. Version-gated and chunked like the mirror
    # above — once a layer is ingested at its current version the tick skips it,
    # so the steady state is two queries and the backfill advances a slice at a
    # time instead of pulling ~810 ZIPs in one run.
    async def column_alias_job() -> None:
        if not _settings.append_database_url:
            return
        from app.database import async_session
        from app.services import column_aliases
        try:
            async with async_session() as db:
                r = await column_aliases.refresh(db, limit=40)
            if r.get("columns"):
                logger.info("column_aliases tick: %s", r)
        except Exception:  # noqa: BLE001 — a bad bundle must not kill the job
            logger.exception("column_aliases refresh tick failed")

    if settings.index_mirror_enabled:
        scheduler.add_job(
            column_alias_job,
            trigger=IntervalTrigger(minutes=10),
            id="column_aliases",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
        )

    # The site's own index (public.over_datasets / over_dataset_files): what each
    # dataset holds, how big, in which formats, and whether it is queryable here.
    # Wholly derived — every refresh replaces it — so the cadence is a freshness
    # choice, not a correctness one. Hourly: nothing in it moves faster than a
    # poll landing, and the build is one pass over the catalog plus two grouped
    # queries.
    async def site_index_job() -> None:
        if not _settings.append_database_url:
            return
        from app.database import async_session
        from app.services import site_index
        try:
            async with async_session() as db:
                await site_index.refresh(db)
        except Exception:  # noqa: BLE001 — a derived index must never kill a tick
            logger.exception("site_index refresh tick failed")

    scheduler.add_job(
        site_index_job,
        trigger=IntervalTrigger(minutes=60),
        id="site_index",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )

    # Admin "dataset sizes" cache: one package_show per active dataset on the
    # odata mirror, fanned out with a small concurrency cap (see
    # app/api/admin.py _compute_dataset_sizes). Used to run inline from the
    # /admin/dataset-sizes route on a stale-cache hit — that let it land at
    # an unpredictable moment (whenever an admin loaded the page), which
    # sometimes coincided with other memory-heavy work already in flight and
    # contributed to 512MB-dyno OOM crashes. Now it's just another scheduled
    # job like the ones above: predictable cadence, max_instances=1, never
    # triggered by request traffic.
    from app.api.admin import refresh_dataset_sizes_job
    scheduler.add_job(
        refresh_dataset_sizes_job,
        trigger=IntervalTrigger(
            minutes=20,
            # Populate the cache a couple minutes after boot rather than
            # waiting the full 20 — but not immediately, so it doesn't
            # compete with the poll-job registration burst just above.
            start_date=datetime.now(timezone.utc) + timedelta(minutes=2),
        ),
        id="refresh_dataset_sizes",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=300,
    )

    # Scheduled targeted sampling. A source too big to re-read whole says so in
    # its manifest (``sampling.schedule = {"new": 604800}``) and OVER keeps that
    # cadence for it — the weekly "which files opened since we last looked",
    # alongside the far rarer full pass the poll job already runs.
    #
    # Why this is not just a shorter poll_interval: on the register that forced
    # it (Jerusalem building licensing) a full pass is 48 hours and re-reads
    # 100k files, while the walk that finds the ~15 genuinely new ones is under
    # a minute. Running the former weekly would spend a fifth of the fleet's
    # year re-reading files nobody claimed had changed.
    #
    # The tick is far shorter than any cadence it serves, and each dataset's own
    # last-queued time is what decides — so a deploy can never reset a weekly
    # run into never firing (the trap add_poll_job documents), and a missed tick
    # costs at most one tick.
    async def sampling_schedule_job() -> None:
        from app.models.scrape_task import PRIORITY_ROUTINE
        from app.services import sampling_runs

        now = datetime.now(timezone.utc)
        async with async_session() as db:
            datasets = (await db.execute(
                select(TrackedDataset).where(
                    TrackedDataset.is_active.is_(True),
                    TrackedDataset.status == "active",
                    TrackedDataset.source_type == "scraper",
                )
            )).scalars().all()
            for ds in datasets:
                # A source can want several cadences at once, and they are not
                # one per mode: the Jerusalem register wants its numbering
                # walked weekly, its publication clocks read every three days
                # and its year's movers read weekly. Each is due on its own
                # clock, and only ONE can be queued per tick because a dataset
                # holds at most one active task — the rest come round again.
                for run in sampling_runs.scheduled_runs(ds):
                    last = await sampling_runs.last_run_at(
                        db, ds.id, run["mode"], run["group"])
                    if last and (now - last).total_seconds() < run["interval"]:
                        continue
                    try:
                        _task, summary, _p = await sampling_runs.queue_run(
                            ds, db, mode=run["mode"], group=run["group"],
                            priority=PRIORITY_ROUTINE, actor="scheduler",
                            reaim=False, note="דגימה מתוזמנת",
                        )
                    except sampling_runs.SamplingBusy:
                        break      # dataset is occupied — every run here waits
                    except sampling_runs.SamplingError as e:
                        logger.warning("Scheduled %s for %s refused: %s",
                                       run["key"], ds.ckan_name, e)
                        continue
                    except Exception:  # noqa: BLE001 — one bad dataset, not the tick
                        logger.exception("Scheduled %s for %s failed",
                                         run["key"], ds.ckan_name)
                        await db.rollback()
                        continue
                    logger.info("Queued scheduled %s for %s (every %ds) — %s",
                                run["key"], ds.ckan_name, run["interval"], summary)
                    break          # one run per dataset per tick

    scheduler.add_job(
        sampling_schedule_job,
        trigger=IntervalTrigger(
            minutes=30,
            # A few minutes after boot, clear of the poll-job registration
            # burst — but soon enough that a dataset whose cadence came due
            # while the dyno was down does not wait another half hour.
            start_date=datetime.now(timezone.utc) + timedelta(minutes=3),
        ),
        id="sampling_schedule",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=600,
    )

    # Auto-discovery: every N hours, map the full data.gov.il catalog and
    # onboard ONE random untracked dataset as a NEON-archived tracked dataset.
    # Off unless AUTO_DISCOVER_ENABLED is set. max_instances=1 + the job's own
    # duplicate guard keep it single-flight. See app/services/auto_discovery.py.
    if settings.auto_discover_enabled:
        from app.services.auto_discovery import discover_and_onboard_one
        scheduler.add_job(
            discover_and_onboard_one,
            trigger=IntervalTrigger(hours=settings.auto_discover_interval_hours),
            id="auto_discover",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=600,
        )
        logger.info(
            "Auto-discovery enabled — onboarding one dataset every %.1fh",
            settings.auto_discover_interval_hours,
        )

    # יומן לעם (Ocal) diary auto-import: discover new diary resources on
    # odata.org.il and onboard a bounded few per tick into the ocal DB (reuses
    # odata_import's parser). No-op unless OCAL_DATABASE_URL is set and the
    # feature flag is on. Rejected candidates are recorded so it converges.
    async def ocal_import_job() -> None:
        from app.services import ocal_db, ocal_import
        if not (settings.ocal_import_enabled and ocal_db.is_configured()):
            return
        try:
            auto = await ocal_import.get_automation_settings()
            if not auto.get("auto_scan_enabled", True):
                return  # runtime kill-switch from the admin automation tab
            r = await ocal_import.scan_once(trigger="scheduler")
            if r.get("imported") or r.get("skipped"):
                logger.info("ocal_import tick: %s", r)
        except Exception:  # noqa: BLE001 — never let a bad scan kill the job
            logger.exception("ocal_import scan tick failed")

    if settings.ocal_import_enabled:
        scheduler.add_job(
            ocal_import_job,
            trigger=IntervalTrigger(hours=settings.ocal_import_interval_hours),
            id="ocal_import",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=600,
        )
        logger.info("Ocal diary auto-import enabled — every %.1fh",
                    settings.ocal_import_interval_hours)

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
