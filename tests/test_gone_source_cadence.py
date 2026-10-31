"""A source the publisher REMOVED is polled quarterly, not on its routine cadence.

~30 GovMap datasets carry ``source_gone_at`` — the layer is no longer in
GovMap's catalog — and each was failing its daily/weekly poll with the same
certain verdict ("layer 452 is not in the catalog and returned 0 features").
They stay active and readable (the archive matters most for a removed source),
and they are still re-asked, because layer ids get renumbered and come back;
just every 90 days. A version landing clears ``source_gone_at``, and the
cadence must come back with it.

Both drivers are pinned: the per-dataset poll job, and the coverage rollout
that drives ``coverage_managed`` layers instead.
"""
import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.worker import scheduler as sch  # noqa: E402

DAY = 86400
NOW = datetime.now(timezone.utc)


def _ds(**kw):
    base = dict(id=uuid.uuid4(), poll_interval=DAY, last_polled_at=NOW - timedelta(hours=1),
                source_gone_at=None, scraper_config={}, is_active=True, status="active")
    base.update(kw)
    return SimpleNamespace(**base)


def _drop(ds):
    """In tests the scheduler is never started, and an unstarted APScheduler
    keeps a re-added job as a SECOND pending entry (replace_existing only
    applies once it runs), so get_job would return the old one. Production
    replaces in place; here the old job is dropped first."""
    sch.remove_poll_job(str(ds.id))


@pytest.fixture
def job_of():
    made = []

    def _get(ds):
        made.append(ds)
        return sch.scheduler.get_job(f"poll_{ds.id}")
    yield _get
    for ds in made:
        sch.remove_poll_job(str(ds.id))


# ── the per-dataset poll job ──────────────────────────────────────────────────
def test_a_gone_source_gets_the_quarterly_cadence(job_of):
    ds = _ds(source_gone_at=NOW - timedelta(days=3))
    sch.schedule_dataset(ds)
    job = job_of(ds)
    assert job.trigger.interval_length == sch.GONE_SOURCE_INTERVAL_SECONDS == 90 * DAY
    # Anchored to the last poll, not fired now: a deploy must not re-ask
    # every dead layer at once.
    assert job.trigger.start_date >= ds.last_polled_at + timedelta(days=89)


def test_a_present_source_keeps_its_own_cadence(job_of):
    ds = _ds()
    sch.schedule_dataset(ds)
    assert job_of(ds).trigger.interval_length == DAY


def test_gone_never_makes_a_slow_dataset_faster(job_of):
    ds = _ds(poll_interval=365 * DAY, source_gone_at=NOW)
    sch.schedule_dataset(ds)
    assert job_of(ds).trigger.interval_length == 365 * DAY


def test_an_unpolled_gone_dataset_stays_unpolled(job_of):
    ds = _ds(poll_interval=0, source_gone_at=NOW)
    sch.schedule_dataset(ds)
    assert job_of(ds) is None


def test_a_gone_source_is_not_pinned_to_its_hour(job_of):
    ds = _ds(source_gone_at=NOW, scraper_config={"poll_at_hour": 3})
    sch.schedule_dataset(ds)
    assert job_of(ds).trigger.start_date >= ds.last_polled_at + timedelta(days=89)


def test_cadence_returns_when_the_source_comes_back(job_of):
    """The push path clears source_gone_at and re-schedules; the routine
    interval must take effect then, not at the next deploy."""
    ds = _ds(source_gone_at=NOW - timedelta(days=40))
    sch.schedule_dataset(ds)
    assert job_of(ds).trigger.interval_length == 90 * DAY
    ds.source_gone_at = None
    _drop(ds)
    sch.reschedule_after_gone_change(ds)
    assert job_of(ds).trigger.interval_length == DAY


def test_marking_gone_reschedules_onto_the_long_cadence(job_of):
    ds = _ds()
    sch.schedule_dataset(ds)
    ds.source_gone_at = NOW
    _drop(ds)
    sch.reschedule_after_gone_change(ds)
    assert job_of(ds).trigger.interval_length == 90 * DAY


def test_a_coverage_managed_dataset_never_gets_a_job_from_this(job_of):
    """The rollout is its only driver (init_scheduler skips it too)."""
    ds = _ds(source_gone_at=None, scraper_config={"coverage_managed": True})
    sch.reschedule_after_gone_change(ds)
    assert job_of(ds) is None


# ── the coverage rollout ──────────────────────────────────────────────────────
def test_rollout_skips_a_gone_layer_until_its_quarterly_recheck(monkeypatch):
    """The engine epoch makes every pre-epoch layer due at once — including
    dead ones, whose every attempt is the same failure. A gone layer is due
    only once GONE_LAYER_RECHECK_DAYS have passed since its last trigger."""
    from app.config import settings
    from app.models.govmap_coverage import GovmapCoverage
    from app.models.scrape_task import ScrapeTask
    from app.models.tag import Tag, dataset_tags
    from app.models.tracked_dataset import TrackedDataset
    from app.services import govmap_coverage as gc
    import app.worker.poll_job as pj

    def _tracked(name, gone):
        return TrackedDataset(
            id=uuid.uuid4(), ckan_id=f"govmap-{name}", ckan_name=name, title=name,
            organization="govmap.gov.il", source_type="govmap",
            source_url=f"https://www.govmap.gov.il/?lay={name}",
            scraper_config={"kind": "govmap", "layer_id": name, "coverage_managed": True},
            source_gone_at=(NOW - timedelta(days=30)) if gone else None)

    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            for table in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                          ScrapeTask.__table__, GovmapCoverage.__table__):
                await conn.run_sync(lambda c, t=table: t.create(c))
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as db:
            for lid, gone, ago in (("452", True, 10),     # gone, recent → skip
                                   ("453", True, 100),    # gone, quarter passed → due
                                   ("500", False, 10)):   # present, pre-epoch → due
                ds = _tracked(lid, gone)
                db.add(ds)
                db.add(GovmapCoverage(layer_id=lid, tracked_dataset_id=ds.id,
                                      last_triggered_at=NOW - timedelta(days=ago)))
            # Never captured, no dataset: the NOT IN must not swallow it.
            db.add(GovmapCoverage(layer_id="999", last_triggered_at=None))
            await db.commit()

        triggered = []

        async def fake_poll(ds_id, priority=None):
            triggered.append(ds_id)

        async def fake_ensure(db, row):
            if row.tracked_dataset_id:
                return SimpleNamespace(id=row.tracked_dataset_id)
            ds = _tracked(f"new{row.layer_id}", False)
            db.add(ds)
            await db.flush()
            row.tracked_dataset_id = ds.id
            return ds

        monkeypatch.setattr(gc, "async_session", Session)
        monkeypatch.setattr(gc, "_ensure_dataset", fake_ensure)
        monkeypatch.setattr(pj, "poll_dataset", fake_poll)
        monkeypatch.setattr(settings, "govmap_coverage_concurrency", 10)
        monkeypatch.setattr(settings, "govmap_new_layer_slots", 0)
        # An epoch after every trigger above: all of them "due" by the epoch.
        monkeypatch.setattr(settings, "govmap_engine_epoch",
                            (NOW - timedelta(days=1)).isoformat())

        # SQLite hands timestamps back naive, which trigger_priority (written
        # for Postgres' aware ones) cannot compare; the band is not what this
        # test is about.
        monkeypatch.setattr(gc, "trigger_priority", lambda *a: 0)
        try:
            res = await gc.scrape_next_layer()
        finally:
            await engine.dispose()   # else aiosqlite's thread outlives a failure
        return sorted(t["layer_id"] for t in res["triggered"])

    assert asyncio.run(go()) == ["453", "500", "999"]
