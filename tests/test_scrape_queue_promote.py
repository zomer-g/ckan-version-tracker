"""Hand-promoting one waiting task to the head of the scrape queue.

Context: the queue's bands (PRIORITY_* in app/models/scrape_task.py) sort work
by KIND — routine poll, manual "דגום", coverage, backfill. That is the right
default, but it leaves no answer to "the queue is backed up and I need THIS row
now, ahead of the other routine polls". POST /api/admin/scrape-tasks/{id}/promote
is that answer: it moves one pending task into PRIORITY_PROMOTED, a band nothing
scheduled ever writes, so a hand-picked row cannot be joined there by a job.

These tests pin what the button promises and — just as important — what it does
not: it reorders the QUEUE, so a running task keeps its worker, and the reported
`ahead` / `running` counts are what the panel tells the admin instead of a bare
"done".

Bare FastAPI app + in-memory SQLite, in the repo's dependency-light style (see
tests/test_admin_datasets_page.py).
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.admin import router as admin_router  # noqa: E402
from app.api.worker import next_pending_task_q  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.scrape_task import (  # noqa: E402
    PRIORITY_BACKFILL,
    PRIORITY_MANUAL,
    PRIORITY_PROMOTED,
    PRIORITY_ROUTINE,
    ScrapeTask,
)
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

_TABLES = [
    Organization.__table__,
    User.__table__,
    TrackedDataset.__table__,
    Tag.__table__,
    dataset_tags,
    ScrapeTask.__table__,
]

NOW = datetime(2026, 8, 2, 9, 0, tzinfo=timezone.utc)


def _ds(name):
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=f"id-{name}", ckan_name=f"name-{name}", title=name,
        poll_interval=3600, is_active=True, status="active", source_type="ckan",
    )


@pytest.fixture()
def env():
    """(TestClient, Session, {title: task_id}) over a small realistic queue."""
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ids = {}

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            # A loaded queue: one task out at a worker, three waiting — an old
            # routine poll, a newer one, and a backfill parked at the bottom.
            spec = [
                ("בעבודה",  "running", PRIORITY_ROUTINE, timedelta(minutes=30)),
                ("ותיק",    "pending", PRIORITY_ROUTINE, timedelta(hours=3)),
                ("חדש",     "pending", PRIORITY_ROUTINE, timedelta(minutes=2)),
                ("השלמה",   "pending", PRIORITY_BACKFILL, timedelta(hours=20)),
            ]
            for title, status, prio, age in spec:
                ds = _ds(title)
                db.add(ds)
                task = ScrapeTask(
                    id=uuid.uuid4(), tracked_dataset_id=ds.id, status=status,
                    priority=prio, created_at=NOW - age,
                )
                db.add(task)
                ids[title] = str(task.id)
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    yield TestClient(app), Session, ids


def _next_title(Session):
    """The dataset the worker would actually be handed, via the claim query."""
    async def go():
        async with Session() as db:
            row = (await db.execute(next_pending_task_q())).first()
            return None if row is None else row[1].title
    return asyncio.run(go())


def test_promoted_task_is_claimed_next(env):
    """The whole point: a task queued two minutes ago goes out before a routine
    poll that has been waiting three hours, and before everything else."""
    client, Session, ids = env
    assert _next_title(Session) == "ותיק"  # oldest routine, pre-promotion

    r = client.post(f"/api/admin/scrape-tasks/{ids['חדש']}/promote")
    assert r.status_code == 200, r.text
    assert r.json()["priority"] == PRIORITY_PROMOTED
    assert _next_title(Session) == "חדש"


def test_response_reports_position_and_worker_load(env):
    """Promotion reorders the queue; it cannot take a worker off a scrape
    already in flight. The panel says so only because the API reports it."""
    client, _, ids = env
    body = client.post(f"/api/admin/scrape-tasks/{ids['השלמה']}/promote").json()
    assert body["status"] == "promoted"
    assert body["ahead"] == 0        # genuinely first among pending
    assert body["running"] == 1      # ...but one worker is busy, so not instant


def test_promotion_outranks_a_manual_trigger(env):
    """A row picked by hand out of the queue beats a "דגום" click from an hour
    ago — otherwise the button silently fails to do what it says."""
    client, Session, ids = env

    async def add_manual():
        async with Session() as db:
            ds = _ds("דגום ידני")
            db.add(ds)
            db.add(ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds.id,
                              status="pending", priority=PRIORITY_MANUAL,
                              created_at=NOW - timedelta(hours=1)))
            await db.commit()
    asyncio.run(add_manual())
    assert _next_title(Session) == "דגום ידני"

    client.post(f"/api/admin/scrape-tasks/{ids['ותיק']}/promote")
    assert _next_title(Session) == "ותיק"


def test_second_promotion_does_not_displace_the_first(env):
    """Inside the band the ordinary oldest-first rule still holds, and `ahead`
    admits it rather than promising a second "next"."""
    client, Session, ids = env
    client.post(f"/api/admin/scrape-tasks/{ids['ותיק']}/promote")
    body = client.post(f"/api/admin/scrape-tasks/{ids['חדש']}/promote").json()

    assert body["ahead"] == 1
    assert _next_title(Session) == "ותיק"


def test_promotion_can_be_undone(env):
    """Undo matters: promoting a heavy layer by mistake would otherwise hold a
    worker for an hour once claimed."""
    client, Session, ids = env
    client.post(f"/api/admin/scrape-tasks/{ids['חדש']}/promote")
    assert _next_title(Session) == "חדש"

    r = client.post(f"/api/admin/scrape-tasks/{ids['חדש']}/promote",
                    json={"promote": False})
    assert r.status_code == 200, r.text
    assert r.json() == {"status": "restored", "priority": PRIORITY_ROUTINE,
                        "ahead": 1, "running": 1}
    assert _next_title(Session) == "ותיק"


def test_running_task_cannot_be_promoted(env):
    """It is already out at a worker — raising its band would change nothing,
    so say that instead of returning a success the admin will misread."""
    client, _, ids = env
    r = client.post(f"/api/admin/scrape-tasks/{ids['בעבודה']}/promote")
    assert r.status_code == 400
    assert "running" in r.json()["detail"]


def test_unknown_task_is_404(env):
    client, _, _ = env
    r = client.post(f"/api/admin/scrape-tasks/{uuid.uuid4()}/promote")
    assert r.status_code == 404
