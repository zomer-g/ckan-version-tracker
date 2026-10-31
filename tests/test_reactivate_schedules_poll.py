"""Re-activating a paused dataset must register its poll job at once.

init_scheduler registers only datasets that are is_active at boot, so a paused
dataset has no job at all. The PATCH used to re-register jobs only on an
interval change: flipping is_active back on left the dataset unpolled until a
later restart happened to re-read it. Found investigating the 137 Knesset-23/24
committee datasets paused (is_active=false) since 2026-07-11.
"""
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.worker import scheduler as sch  # noqa: E402

LAST = datetime.now(timezone.utc) - timedelta(days=112)
_ENGINES: list = []


@pytest_asyncio.fixture(autouse=True)
async def _dispose_engines():
    # An undisposed aiosqlite connection keeps its worker thread alive, and a
    # failing assertion's traceback holds it — the run then never exits.
    yield
    while _ENGINES:
        await _ENGINES.pop().dispose()


async def _session_with(ds):
    engine = create_async_engine("sqlite+aiosqlite://")
    _ENGINES.append(engine)
    async with engine.begin() as conn:
        for t in (Organization.__table__, Tag.__table__, TrackedDataset.__table__,
                  dataset_tags, VersionIndex.__table__):
            await conn.run_sync(lambda c, t=t: t.create(c))
    S = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with S() as db:
        db.add(ds)
        await db.commit()
    return S


def _committee(is_active):
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id="knesset-scraper-knesset-committee-single-2213-x",
        ckan_name="knesset-committee-single-2213-x", title="ועדה (כנסת 23)",
        source_type="scraper", poll_interval=604800, is_active=is_active,
        status="active", storage_mode="full_snapshot",
        source_url="https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=Id eq 2213",
        last_polled_at=LAST)


async def _patch(S, ds_id, monkeypatch, **body):
    from app.api import datasets as api
    added = []
    monkeypatch.setattr(sch, "add_poll_job",
                        lambda dsid, interval, **kw: added.append((dsid, interval, kw)))
    async with S() as db:
        await api.update_tracked(str(ds_id), api.UpdateRequest(**body), user=None, db=db)
    return added


@pytest.mark.asyncio
async def test_reactivating_registers_the_poll_job(monkeypatch):
    ds = _committee(is_active=False)
    S = await _session_with(ds)
    added = await _patch(S, ds.id, monkeypatch, is_active=True)
    assert len(added) == 1
    dsid, interval, kw = added[0]
    assert dsid == str(ds.id) and interval == 604800
    # Anchored on the real last poll, so an overdue dataset fires on the next tick.
    assert kw["last_polled_at"].replace(tzinfo=timezone.utc) == LAST


@pytest.mark.asyncio
async def test_an_unrelated_edit_of_an_active_dataset_does_not_reschedule(monkeypatch):
    ds = _committee(is_active=True)
    S = await _session_with(ds)
    assert await _patch(S, ds.id, monkeypatch, is_active=True) == []
    assert await _patch(S, ds.id, monkeypatch, title="שם חדש") == []


@pytest.mark.asyncio
async def test_pausing_does_not_register_a_job(monkeypatch):
    ds = _committee(is_active=True)
    S = await _session_with(ds)
    assert await _patch(S, ds.id, monkeypatch, is_active=False, poll_interval=86400) == []
