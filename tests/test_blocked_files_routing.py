"""Files the web process cannot fetch are handed to a worker that can.

data.gov.il's wall needs a real headful browser. CKAN is polled INLINE, in the
web process, where Playwright cannot run — so the inline poll keeps archiving
everything it can reach and queues the remainder for the worker fleet.

The routing is additive, and these tests are mostly about the ways it must NOT
disturb the poll it hangs off:

  * it runs in its own session, because a duplicate insert loses to
    `uq_scrape_tasks_active_per_dataset` and that IntegrityError, raised inside
    the poll's session, would abort the whole commit — losing the archive to a
    bookkeeping row;
  * it does not re-queue on every cadence for a wall that is not moving;
  * it is off by default, because a task whose `kind` no worker can claim would
    fail, and a failed task is no longer active, so the next poll would queue
    another one — a treadmill across all 13 affected datasets.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.config import settings  # noqa: E402
from app.models.scrape_task import ScrapeTask  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.worker import poll_job  # noqa: E402

_DS = uuid.uuid4()

ENTRIES = [
    {"id": "r1", "name": "גושים", "format": "ZIP",
     "url": "https://e.data.gov.il/gushim.zip"},
    {"id": "r2", "name": "נחלים", "format": "SHP",
     "url": "https://e.data.gov.il/nahalim.zip"},
]


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def db(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _create():
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      VersionIndex.__table__, ScrapeTask.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))
        async with Session() as s:
            s.add(TrackedDataset(
                id=_DS, ckan_id="pkg-1", ckan_name="nahalim", title="נחלים",
                source_type="ckan", poll_interval=7776000, is_active=True,
                status="active", storage_mode="full_snapshot",
                source_url="https://data.gov.il/dataset/nahalim",
                last_modified="2024-01-01T00:00:00",
            ))
            await s.commit()

    _run(_create())
    monkeypatch.setattr(poll_job, "async_session", Session)
    monkeypatch.setattr(settings, "ckan_blocked_files_enabled", True)
    return Session


def _tasks(Session):
    async def _go():
        async with Session() as s:
            from sqlalchemy import select
            return (await s.execute(select(ScrapeTask))).scalars().all()
    return _run(_go())


def _ds(Session):
    async def _go():
        async with Session() as s:
            return await s.get(TrackedDataset, _DS)
    return _run(_go())


def test_a_task_carries_the_work_list_in_its_own_params(db):
    """params is merged over scraper_config in the /poll response, so the
    worker gets the list with no migration and no new endpoint."""
    _run(poll_job._queue_blocked_files_task(_ds(db), ENTRIES))
    tasks = _tasks(db)
    assert len(tasks) == 1
    p = tasks[0].params
    assert p["kind"] == poll_job.BLOCKED_FILES_KIND
    assert [r["id"] for r in p["blocked_resources"]] == ["r1", "r2"]
    assert p["blocked_resources"][1]["url"].endswith("nahalim.zip")
    assert tasks[0].status == "pending"


def test_nothing_blocked_queues_nothing(db):
    _run(poll_job._queue_blocked_files_task(_ds(db), []))
    assert _tasks(db) == []


def test_it_does_not_requeue_while_one_is_already_waiting(db):
    """The wall is not moving. One task per dataset, not one per cadence."""
    for _ in range(3):
        _run(poll_job._queue_blocked_files_task(_ds(db), ENTRIES))
    assert len(_tasks(db)) == 1


def test_the_switch_actually_stops_it(monkeypatch, db):
    """Now ON by default, since govil-scraper dispatches the kind. The switch
    stays because the failure it guards against is ugly: a task no worker can
    claim fails, and a failed task is no longer active, so the next poll queues
    another — a treadmill across every affected dataset. If the worker side
    ever regresses, this is the one lever that stops the bleeding."""
    monkeypatch.setattr(settings, "ckan_blocked_files_enabled", False)
    _run(poll_job._queue_blocked_files_task(_ds(db), ENTRIES))
    assert _tasks(db) == []


def test_a_failure_to_queue_never_reaches_the_caller(monkeypatch, db):
    """It hangs off a poll that has already done the real work. Losing the
    archive because a bookkeeping row would not insert is not a trade anyone
    would make."""
    def _boom(*a, **k):
        raise RuntimeError("database is on fire")

    monkeypatch.setattr(poll_job, "async_session", _boom)
    _run(poll_job._queue_blocked_files_task(_ds(db), ENTRIES))  # must not raise
