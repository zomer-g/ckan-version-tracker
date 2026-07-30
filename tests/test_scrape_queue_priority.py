"""The scrape queue's priority band: one queue, two kinds of work.

Context: on 2026-07-30 the GovMap scraper began capturing far more fields per
feature (plus per-layer symbology), which makes every previously-captured layer
stale — a ~859-layer re-scrape spanning about a day. The queue used to drain
strictly FIFO, so dumping that backfill in would have parked every routine poll
behind it. These tests pin the three behaviours that make sharing one queue safe:

  1. claim order — priority band first, oldest-first within a band;
  2. banding    — only the epoch backfill is de-prioritized, and only when the
                  epoch is the sole reason the layer is due;
  3. promotion  — a dataset already queued in the backfill band jumps bands when
                  a human clicks "דגום", since the one-active-task-per-dataset
                  index means we cannot just queue a second, higher task.

In-memory SQLite, driven by asyncio.run — matching the repo's dependency-light
test style (see tests/test_auth_codes.py).
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api.worker import next_pending_task_q  # noqa: E402
from app.models.scrape_task import (  # noqa: E402
    PRIORITY_BACKFILL,
    PRIORITY_COVERAGE,
    PRIORITY_MANUAL,
    PRIORITY_ROUTINE,
    ScrapeTask,
)
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.services.govmap_coverage import engine_epoch, trigger_priority  # noqa: E402
from app.worker.poll_job import _create_scrape_task  # noqa: E402

NOW = datetime(2026, 7, 31, 9, 0, tzinfo=timezone.utc)
EPOCH = datetime(2026, 7, 30, 13, 0, tzinfo=timezone.utc)   # 16:00 Israel time
REFRESH_CUTOFF = NOW - timedelta(days=90)


def _run(coro):
    return asyncio.run(coro)


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        # TrackedDataset eager-loads its tags, so the tag tables have to exist
        # even though nothing here uses them.
        for table in (Tag.__table__, dataset_tags,
                      TrackedDataset.__table__, ScrapeTask.__table__):
            await conn.run_sync(lambda c, t=table: t.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _dataset(name: str) -> TrackedDataset:
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=f"govmap-{name}", ckan_name=name, title=name,
        organization="govmap.gov.il", source_type="govmap",
        source_url=f"https://www.govmap.gov.il/?lay={name}",
        scraper_config={"kind": "govmap", "layer_id": name},
    )


# ── 1. claim order ────────────────────────────────────────────────────────

def test_routine_poll_beats_an_older_backfill_task():
    """The whole point: a backfill task queued yesterday must not be handed out
    ahead of a routine poll queued a second ago."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            backfill_ds, routine_ds = _dataset("100"), _dataset("200")
            db.add_all([backfill_ds, routine_ds])
            db.add_all([
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=backfill_ds.id,
                           status="pending", priority=PRIORITY_BACKFILL,
                           created_at=NOW - timedelta(hours=20)),
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=routine_ds.id,
                           status="pending", priority=PRIORITY_ROUTINE,
                           created_at=NOW),
            ])
            await db.commit()

            task, ds = (await db.execute(next_pending_task_q())).first()
            assert ds.id == routine_ds.id, "routine poll must be claimed first"
            assert task.priority == PRIORITY_ROUTINE
    _run(go())


def test_oldest_first_within_a_band():
    """Priority reorders bands, not the queue inside one — FIFO still holds
    among equals, so no routine poll can be starved by a newer routine poll."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            older, newer = _dataset("1"), _dataset("2")
            db.add_all([older, newer])
            db.add_all([
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=newer.id,
                           status="pending", priority=PRIORITY_ROUTINE, created_at=NOW),
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=older.id,
                           status="pending", priority=PRIORITY_ROUTINE,
                           created_at=NOW - timedelta(minutes=5)),
            ])
            await db.commit()

            _, ds = (await db.execute(next_pending_task_q())).first()
            assert ds.id == older.id
    _run(go())


def test_manual_trigger_outranks_everything_pending():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            manual, routine = _dataset("3"), _dataset("4")
            db.add_all([manual, routine])
            db.add_all([
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=routine.id,
                           status="pending", priority=PRIORITY_ROUTINE,
                           created_at=NOW - timedelta(hours=1)),
                ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=manual.id,
                           status="pending", priority=PRIORITY_MANUAL, created_at=NOW),
            ])
            await db.commit()

            _, ds = (await db.execute(next_pending_task_q())).first()
            assert ds.id == manual.id
    _run(go())


def test_running_tasks_are_never_claimed():
    """Guards the status filter — a busy queue must not re-hand-out live work."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            ds = _dataset("5")
            db.add(ds)
            db.add(ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds.id,
                              status="running", priority=PRIORITY_MANUAL, created_at=NOW))
            await db.commit()
            assert (await db.execute(next_pending_task_q())).first() is None
    _run(go())


# ── 2. banding ────────────────────────────────────────────────────────────

def test_pre_epoch_layer_is_backfill_band():
    """Scraped recently but before the new engine shipped: fresh data, missing
    the new fields. Re-scrape it — but only when the queue is idle."""
    assert trigger_priority(EPOCH - timedelta(hours=2), EPOCH, REFRESH_CUTOFF) == PRIORITY_BACKFILL


def test_post_epoch_layer_is_not_backfill():
    """Already captured by the new engine — it isn't due at all, and if some
    other rule makes it due it is ordinary coverage work, not catch-up."""
    assert trigger_priority(EPOCH + timedelta(hours=1), EPOCH, REFRESH_CUTOFF) == PRIORITY_COVERAGE


def test_never_scraped_layer_keeps_the_coverage_band():
    """A layer with NO data is a real gap in coverage; it must not be demoted
    behind a backfill of layers that already have data."""
    assert trigger_priority(None, EPOCH, REFRESH_CUTOFF) == PRIORITY_COVERAGE


def test_layer_due_on_the_routine_cycle_keeps_the_coverage_band():
    """Stale by the ordinary 90-day rule (and therefore also pre-epoch). It
    would be scraped today regardless of the epoch, so the epoch must not
    demote it."""
    assert trigger_priority(REFRESH_CUTOFF - timedelta(days=5), EPOCH, REFRESH_CUTOFF) == PRIORITY_COVERAGE


def test_no_epoch_configured_means_no_backfill_band():
    """With the backfill switched off, every trigger is ordinary coverage."""
    assert trigger_priority(EPOCH - timedelta(days=1), None, REFRESH_CUTOFF) == PRIORITY_COVERAGE


def test_engine_epoch_parses_and_fails_soft(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "govmap_engine_epoch", "2026-07-30T13:00:00Z")
    assert engine_epoch() == EPOCH

    # Naive input is read as UTC rather than crashing on a tz-aware comparison.
    monkeypatch.setattr(settings, "govmap_engine_epoch", "2026-07-30T13:00:00")
    assert engine_epoch() == EPOCH

    # A typo in an env var must disable the backfill, not kill the tick.
    monkeypatch.setattr(settings, "govmap_engine_epoch", "yesterday-ish")
    assert engine_epoch() is None

    monkeypatch.setattr(settings, "govmap_engine_epoch", "")
    assert engine_epoch() is None


# ── 3. promotion ──────────────────────────────────────────────────────────

def test_manual_poll_promotes_a_queued_backfill_task():
    """Only ONE active task per dataset is allowed, so a manual trigger for a
    layer already sitting in the backfill queue has to raise that task's band —
    otherwise the click does nothing until the whole backfill drains."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            ds = _dataset("52")
            db.add(ds)
            await db.commit()

            await _create_scrape_task(ds, db, priority=PRIORITY_BACKFILL)
            await _create_scrape_task(ds, db, priority=PRIORITY_MANUAL)

            tasks = (await db.execute(
                ScrapeTask.__table__.select().where(ScrapeTask.tracked_dataset_id == ds.id)
            )).all()
            assert len(tasks) == 1, "must not create a second active task"
            assert tasks[0].priority == PRIORITY_MANUAL
    _run(go())


def test_backfill_never_demotes_a_waiting_manual_task():
    """The reverse direction must be a no-op: a human is waiting on that task."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            ds = _dataset("53")
            db.add(ds)
            await db.commit()

            await _create_scrape_task(ds, db, priority=PRIORITY_MANUAL)
            await _create_scrape_task(ds, db, priority=PRIORITY_BACKFILL)

            tasks = (await db.execute(
                ScrapeTask.__table__.select().where(ScrapeTask.tracked_dataset_id == ds.id)
            )).all()
            assert len(tasks) == 1
            assert tasks[0].priority == PRIORITY_MANUAL
    _run(go())


def test_a_running_task_is_left_alone():
    """A claimed task is already out at a worker; rewriting its band would be
    noise at best. The poll is still recorded, no second task is created."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            ds = _dataset("54")
            db.add(ds)
            db.add(ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds.id,
                              status="running", priority=PRIORITY_BACKFILL, created_at=NOW))
            await db.commit()

            await _create_scrape_task(ds, db, priority=PRIORITY_MANUAL)

            tasks = (await db.execute(
                ScrapeTask.__table__.select().where(ScrapeTask.tracked_dataset_id == ds.id)
            )).all()
            assert len(tasks) == 1
            assert tasks[0].priority == PRIORITY_BACKFILL
    _run(go())


def test_default_band_is_routine():
    """Every existing caller passes no priority — they must stay routine."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            ds = _dataset("55")
            db.add(ds)
            await db.commit()
            await _create_scrape_task(ds, db)

            tasks = (await db.execute(
                ScrapeTask.__table__.select().where(ScrapeTask.tracked_dataset_id == ds.id)
            )).all()
            assert tasks[0].priority == PRIORITY_ROUTINE
    _run(go())
