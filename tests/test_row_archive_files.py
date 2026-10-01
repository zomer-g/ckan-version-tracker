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
