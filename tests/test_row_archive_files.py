"""A dataset archived as rows still owes its FILES to someone.

rail_stat (2026-10-01): its two datastore tables streamed to the SQL archive,
and the branch that did it recorded "checked, nothing blocked" — so its
RAIL_STAT_SHP and RAIL_STAT_KML were never fetched by anyone. ~40 packages had
the same hole. Beside it, lrt_stat's files were "delivered" as 0 bytes during
the 2026-09-27 burst, and the requeue for those ran only inside the catalog
watch, which does not read ministry_of_transport.
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.config import settings  # noqa: E402
from app.models.scrape_task import ScrapeTask  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services import blocked_resources as br  # noqa: E402
from app.worker import poll_job  # noqa: E402

MOD = "2026-07-12T09:11:53.584040"

PKG = {"metadata_modified": MOD, "resources": [
    {"id": "shp", "name": "RAIL_STAT_SHP", "format": "ZIP",
     "url": "https://e.data.gov.il/rail_stat.zip"},
    {"id": "kml", "name": "RAIL_STAT_KML", "format": "ZIP",
     "url": "https://e.data.gov.il/rail_stat_kmz.zip"},
    {"id": "csv", "name": "RAIL_STAT_CSV", "format": "CSV",
     "url": "https://e.data.gov.il/rail_stat.csv", "datastore_active": True},
    {"id": "meta", "name": "מטאדאטה", "format": "XLSX",
     "url": "https://e.data.gov.il/meta.xlsx", "datastore_active": True},
]}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def Session(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://")
    S = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _create():
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      VersionIndex.__table__, ScrapeTask.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))

    _run(_create())
    monkeypatch.setattr(poll_job, "async_session", S)
    monkeypatch.setattr(settings, "ckan_blocked_files_enabled", True)
    return S


def _dataset(config, **kw):
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id="pkg", ckan_name="rail_stat", title="תחנות",
        source_type="ckan", poll_interval=7776000, is_active=True,
        status="active", storage_mode="full_snapshot",
        source_url="https://data.gov.il/dataset/rail_stat",
        last_modified=MOD, scraper_config=config, **kw,
    )


async def _assess(S, ds, versions=()):
    async with S() as db:
        db.add(ds)
        for v in versions:
            db.add(v)
        await db.commit()
        await poll_job._assess_files_beside_row_archive(ds, PKG, MOD, db)
        await db.commit()
    # poll_dataset hands the files over only when the poll is done.
    queued = poll_job._files_after_poll.pop(str(ds.id), None)
    if queued:
        await poll_job._queue_blocked_files_task(*queued)
    async with S() as db:
        tasks = (await db.execute(select(ScrapeTask))).scalars().all()
    return tasks


def test_only_files_count_as_files():
    assert [r["id"] for r in br.file_resources(PKG["resources"])] == ["shp", "kml"]
    assert br.file_resources([{"id": "x", "url": ""}]) == []


def test_a_row_archived_dataset_hands_its_files_to_a_worker(Session):
    # The exact state rail_stat was left in: assessed, "nothing blocked".
    ds = _dataset({"archive_neon": True, "blocked_resources": []})
    tasks = _run(_assess(Session, ds))

    assert [e["id"] for e in br.stored(ds)] == ["shp", "kml"]
    assert len(tasks) == 1
    assert tasks[0].params["kind"] == poll_job.BLOCKED_FILES_KIND
    assert [e["id"] for e in tasks[0].params["blocked_resources"]] == ["shp", "kml"]
    assert "ממתין לגירוד" in (ds.last_error or "")


def test_tracked_subset_is_respected(Session):
    ds = _dataset({"archive_neon": True}, resource_ids=["csv", "kml"])
    tasks = _run(_assess(Session, ds))
    assert [e["id"] for e in br.stored(ds)] == ["kml"]
    assert [e["id"] for e in tasks[0].params["blocked_resources"]] == ["kml"]


def test_files_delivered_for_real_are_not_asked_again(Session):
    stamp = {"fetched_at": "2026-09-28T00:00:00", "fetched_modified": MOD,
             "fetched_version": 1}
    ds = _dataset({"archive_neon": True, "blocked_resources": [
        {"id": "shp", "name": "RAIL_STAT_SHP", "format": "ZIP", "url": "u", **stamp},
        {"id": "kml", "name": "RAIL_STAT_KML", "format": "ZIP", "url": "u", **stamp},
    ]})
    v1 = VersionIndex(
        id=uuid.uuid4(), tracked_dataset_id=ds.id, version_number=1,
        metadata_modified=MOD, change_summary={"scrape_metadata": {"blocked_files": {
            "resources": [{"resource_id": "shp", "status": "features"},
                          {"resource_id": "kml", "status": "raw_only", "bytes": 578}]}}},
    )
    tasks = _run(_assess(Session, ds, [v1]))
    assert tasks == []
    assert br.pending(br.stored(ds)) == []


def test_files_that_arrived_empty_are_asked_again(Session):
    # lrt_stat: worker v1 holds three 0-byte files, stamped as fetched.
    stamp = {"fetched_at": "2026-09-27T14:35:00", "fetched_modified": MOD,
             "fetched_version": 1}
    ds = _dataset({"archive_neon": True, "blocked_resources": [
        {"id": "shp", "name": "RAIL_STAT_SHP", "format": "ZIP", "url": "u", **stamp},
        {"id": "kml", "name": "RAIL_STAT_KML", "format": "ZIP", "url": "u", **stamp},
    ]})
    v1 = VersionIndex(
        id=uuid.uuid4(), tracked_dataset_id=ds.id, version_number=1,
        metadata_modified=MOD, change_summary={"scrape_metadata": {"blocked_files": {
            "resources": [
                {"resource_id": "shp", "status": "raw_only",
                 "reason": "unrecognised container — archived as a file"},
                {"resource_id": "kml", "status": "raw_only", "bytes": 578}]}}},
    )
    tasks = _run(_assess(Session, ds, [v1]))
    assert [e["id"] for e in br.pending(br.stored(ds))] == ["shp"]
    assert [e["id"] for e in tasks[0].params["blocked_resources"]] == ["shp"]


def test_a_single_tracked_file_is_left_to_the_inline_download(Session):
    """r2+neon over one FILE downloads it inline and assesses it there."""
    ds = _dataset({"archive_neon": True}, resource_ids=["shp"])

    async def _go():
        async with Session() as db:
            db.add(ds)
            await db.commit()
            return await poll_job._assess_files_beside_row_archive(ds, PKG, MOD, db)

    assert _run(_go()) is False
    assert not br.assessed(ds)


# ---------------------------------------------------------------------------
# The daily retry reaches every package, paced and capped
# (2026-10-01: 47 of 77 re-polled datasets got empty files in one burst).
# ---------------------------------------------------------------------------

def test_the_daily_retry_takes_the_oldest_missing_files_and_skips_busy_ones(Session, monkeypatch):
    from datetime import datetime, timezone

    from app.services import catalog_watch

    entry = {"id": "shp", "name": "SHP", "format": "ZIP", "url": "u"}
    done = {**entry, "fetched_at": "x", "fetched_modified": MOD, "fetched_version": 1}
    old = _dataset({"blocked_resources": [entry]})
    old.last_polled_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new = _dataset({"blocked_resources": [entry]})
    new.last_polled_at = datetime(2026, 9, 1, tzinfo=timezone.utc)
    busy = _dataset({"blocked_resources": [entry]})
    complete = _dataset({"blocked_resources": [done]})
    other = _dataset(None)

    async def _seed():
        async with Session() as db:
            for d in (old, new, busy, complete, other):
                db.add(d)
            db.add(ScrapeTask(tracked_dataset_id=busy.id, status="pending",
                              phase="queued", params={}))
            await db.commit()
    _run(_seed())

    polled = []

    async def _poll(ds_id, **kw):
        polled.append(ds_id)

    monkeypatch.setattr(catalog_watch, "async_session", Session)
    monkeypatch.setattr(poll_job, "poll_dataset", _poll)
    monkeypatch.setattr(settings, "catalog_watch_blocked_retry_limit", 1)
    monkeypatch.setattr(settings, "catalog_watch_blocked_retry_gap_s", 0)

    out = _run(catalog_watch._retry_blocked_everywhere(set()))
    assert polled == [str(old.id)]
    assert out["waiting"] == 1          # `new`, left for tomorrow

    polled.clear()
    monkeypatch.setattr(settings, "catalog_watch_blocked_retry_limit", 10)
    _run(catalog_watch._retry_blocked_everywhere({str(old.id)}))
    assert polled == [str(new.id)]      # not busy, not complete, not skipped


def test_the_worker_is_asked_only_after_the_poll_is_done(Session, monkeypatch):
    """A worker asked at the start of the poll pushed its version while the
    poll was still streaming tables; both took the same version number and the
    poll died on the unique key (2026-10-01, three datasets)."""
    ds = _dataset({"archive_neon": True})
    order = []

    async def _poll(dataset_id, force=False, priority=None):
        async with Session() as db:
            db.add(ds)
            await db.commit()
            await poll_job._assess_files_beside_row_archive(ds, PKG, MOD, db)
            await db.commit()
        async with Session() as db:
            order.append(("during", len((await db.execute(select(ScrapeTask))).scalars().all())))

    monkeypatch.setattr(poll_job, "_poll_dataset", _poll)
    poll_job._active_polls.clear()
    _run(poll_job.poll_dataset(str(ds.id), force=True))

    async def _count():
        async with Session() as db:
            return len((await db.execute(select(ScrapeTask))).scalars().all())
    assert order == [("during", 0)], "no task may exist while the poll is running"
    assert _run(_count()) == 1, "the files are handed over once it is done"
