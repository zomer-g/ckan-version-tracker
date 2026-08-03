"""Per-worker drain: take one machine out of rotation to update its code.

The operator sequence this exists for is: pause a worker → wait until it is
holding nothing → restart it on new code → un-pause. So the behaviours that
matter are that pausing does NOT touch the task already running (a GovMap layer
can be an hour in), that a paused worker is handed nothing on its next poll, and
that "safe to restart" is reported rather than left for a human to infer.

Bare FastAPI app + in-memory SQLite, in the repo's dependency-light style.
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
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.admin import router as admin_router  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.scrape_task import PRIORITY_ROUTINE, ScrapeTask  # noqa: E402
from app.models.source_limit import SourceLimit  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.models.worker_node import WorkerNode  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services.worker_fleet import (  # noqa: E402
    OFFLINE_AFTER, _as_utc, fleet, touch_worker, worker_key,
)

_TABLES = [
    Organization.__table__, User.__table__, TrackedDataset.__table__,
    Tag.__table__, dataset_tags, ScrapeTask.__table__, SourceLimit.__table__,
    WorkerNode.__table__,
    # Dispatch builds its response from the dataset's version history.
    VersionIndex.__table__,
]

NOW = datetime.now(timezone.utc)


def _run(coro):
    return asyncio.run(coro)


async def _session_factory(with_workers_table=True):
    engine = create_async_engine("sqlite+aiosqlite://")
    tables = _TABLES if with_workers_table else [t for t in _TABLES if t is not WorkerNode.__table__]
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=tables)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ── identity ──────────────────────────────────────────────────────────────

def test_explicit_id_beats_ip():
    """Two workers behind one NAT share an IP — keying on it would let a pause
    of one silently drain the other."""
    assert worker_key("box-a#1234", "10.0.0.5") == "box-a#1234"
    assert worker_key(None, "10.0.0.5") == "ip:10.0.0.5"
    assert worker_key("", "unknown") == "unknown"


# ── registration + heartbeat ──────────────────────────────────────────────

def test_first_poll_registers_the_worker_unpaused():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            node = await touch_worker(
                db, worker_id="box-a#1234", worker_ip="10.0.0.5",
                worker_version="abc123", worker_upstream="current",
            )
            assert node is not None and node.paused is False
            assert node.worker_version == "abc123"
    _run(go())


def test_heartbeat_writes_are_throttled_but_a_code_change_is_not():
    """A worker polls ~1/s; writing every poll costs compute for precision
    nobody reads. A version change is the exception — a worker restarted on new
    code must not look stale."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await touch_worker(db, worker_id="box-a", worker_ip="10.0.0.5",
                               worker_version="old", worker_upstream="behind")
            # SQLite drops tzinfo on read where Postgres keeps it, so compare
            # through the same normaliser the service uses.
            node = await db.get(WorkerNode, "box-a")
            first_seen = _as_utc(node.last_seen_at)

            # Same code, immediately after: no write.
            await touch_worker(db, worker_id="box-a", worker_ip="10.0.0.5",
                               worker_version="old", worker_upstream="behind")
            node = await db.get(WorkerNode, "box-a")
            assert _as_utc(node.last_seen_at) == first_seen

            # Restarted on new code: written through at once.
            await touch_worker(db, worker_id="box-a", worker_ip="10.0.0.5",
                               worker_version="new", worker_upstream="current")
            node = await db.get(WorkerNode, "box-a")
            assert node.worker_version == "new"
            assert node.worker_upstream == "current"
            assert _as_utc(node.last_seen_at) > first_seen
    _run(go())


def test_missing_workers_table_does_not_stop_dispatch():
    """Same rule as the source caps: fleet bookkeeping runs on every poll, so it
    must never be what stops the fleet from getting work."""
    async def go():
        Session = await _session_factory(with_workers_table=False)
        async with Session() as db:
            assert await touch_worker(db, worker_id="box-a", worker_ip="10.0.0.5") is None
            # Session still usable — a poisoned transaction would break the claim.
            assert (await db.scalars(select(ScrapeTask))).all() == []
    _run(go())


# ── the drain, through the API ────────────────────────────────────────────

@pytest.fixture()
def env():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            ds = TrackedDataset(
                id=uuid.uuid4(), ckan_id="layer-52", ckan_name="layer-52",
                title="שכבת GovMap 52", source_type="govmap", is_active=True,
            )
            db.add(ds)
            # busy-box holds a long scrape; idle-box has nothing; gone-box
            # stopped reporting an hour ago.
            db.add(ScrapeTask(
                id=uuid.uuid4(), tracked_dataset_id=ds.id, status="running",
                priority=PRIORITY_ROUTINE, created_at=NOW - timedelta(minutes=50),
                updated_at=NOW - timedelta(seconds=20),
                worker_id="busy-box", worker_ip="10.0.0.1", phase="scraping", progress=61,
            ))
            db.add_all([
                WorkerNode(worker_key="busy-box", worker_id="busy-box",
                           worker_ip="10.0.0.1", worker_version="abc",
                           worker_upstream="current", last_seen_at=NOW),
                WorkerNode(worker_key="idle-box", worker_id="idle-box",
                           worker_ip="10.0.0.2", worker_version="abc",
                           worker_upstream="current", last_seen_at=NOW),
                WorkerNode(worker_key="gone-box", worker_id="gone-box",
                           worker_ip="10.0.0.3", worker_version="old",
                           last_seen_at=NOW - timedelta(hours=1)),
            ])
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
    yield TestClient(app), Session


def _workers(client):
    r = client.get("/api/admin/workers")
    assert r.status_code == 200, r.text
    return {w["worker_key"]: w for w in r.json()["workers"]}


def test_fleet_lists_what_each_machine_is_doing(env):
    client, _ = env
    w = _workers(client)
    assert set(w) == {"busy-box", "idle-box", "gone-box"}
    assert w["busy-box"]["current_task"]["dataset_title"] == "שכבת GovMap 52"
    assert w["idle-box"]["current_task"] is None
    assert w["gone-box"]["offline"] is True
    assert w["busy-box"]["offline"] is False


def test_pausing_a_busy_worker_leaves_its_task_running(env):
    """The entire point: you pause to update code, not to throw away 50 minutes
    of scraping."""
    client, Session = env
    r = client.put("/api/admin/workers/busy-box/pause", json={"paused": True})
    assert r.status_code == 200, r.text

    async def check():
        async with Session() as db:
            tasks = (await db.scalars(select(ScrapeTask))).all()
            assert [t.status for t in tasks] == ["running"], "must not touch live work"
    _run(check())

    w = _workers(client)["busy-box"]
    assert w["paused"] is True
    assert w["drained"] is False, "still holding a task — not safe to restart yet"


def test_drained_is_the_signal_that_it_is_safe_to_restart(env):
    client, _ = env
    client.put("/api/admin/workers/idle-box/pause", json={"paused": True})
    w = _workers(client)["idle-box"]
    assert w["paused"] is True and w["drained"] is True


def test_a_paused_worker_is_handed_no_task(env):
    """The mechanism itself — poll_for_task returns 204 for a paused machine."""
    client, Session = env
    client.put("/api/admin/workers/idle-box/pause", json={"paused": True})

    async def check():
        async with Session() as db:
            node = await touch_worker(db, worker_id="idle-box", worker_ip="10.0.0.2")
            assert node.paused is True, "the poll path sees the pause"
            # And an untouched worker is unaffected.
            other = await touch_worker(db, worker_id="busy-box", worker_ip="10.0.0.1")
            assert other.paused is False
    _run(check())


def test_pause_is_reversible(env):
    client, _ = env
    client.put("/api/admin/workers/idle-box/pause", json={"paused": True})
    r = client.put("/api/admin/workers/idle-box/pause", json={"paused": False})
    assert r.status_code == 200
    w = _workers(client)["idle-box"]
    assert w["paused"] is False and w["paused_by"] is None


def test_forgetting_a_paused_worker_is_refused(env):
    """Deleting the row would silently un-pause the machine on its next poll —
    the opposite of what someone clicking "forget" on a drained worker expects."""
    client, _ = env
    client.put("/api/admin/workers/gone-box/pause", json={"paused": True})
    r = client.delete("/api/admin/workers/gone-box")
    assert r.status_code == 400

    client.put("/api/admin/workers/gone-box/pause", json={"paused": False})
    assert client.delete("/api/admin/workers/gone-box").status_code == 200
    assert "gone-box" not in _workers(client)


def test_unknown_worker_is_404(env):
    client, _ = env
    assert client.put("/api/admin/workers/nope/pause", json={"paused": True}).status_code == 404
    assert client.delete("/api/admin/workers/nope").status_code == 404


def test_offline_window_matches_the_task_heartbeat_window(env):
    """A worker going quiet and its task being auto-failed should be reported on
    the same clock, or the panel contradicts the queue."""
    assert OFFLINE_AFTER == timedelta(minutes=10)


# ── the drain, through /api/worker/poll ───────────────────────────────────
#
# The tests above prove the flag is stored and reported. This one proves the
# flag actually does the thing the button promises: a paused machine is handed
# nothing, and the task it did not get is still there for someone else.

@pytest.fixture()
def worker_client(monkeypatch):
    from app.api import worker as worker_api
    from app.config import settings

    monkeypatch.setattr(settings, "worker_api_key", "workerkey")
    monkeypatch.setattr(settings, "worker_version_check_enabled", False)

    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            ds = TrackedDataset(
                id=uuid.uuid4(), ckan_id="jda-scraper-tenders", ckan_name="jda-t",
                title="מכרזי הרשות לפיתוח ירושלים", source_type="scraper", is_active=True,
                source_url="https://jda.gov.il/tenders",
            )
            db.add(ds)
            db.add(ScrapeTask(
                id=uuid.uuid4(), tracked_dataset_id=ds.id, status="pending",
                priority=PRIORITY_ROUTINE, created_at=NOW - timedelta(minutes=5),
            ))
            db.add(WorkerNode(
                worker_key="paused-box", worker_id="paused-box", worker_ip="10.0.0.7",
                last_seen_at=NOW, paused=True,
            ))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)
    app.dependency_overrides[get_db] = _db
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False), Session


def _poll(client, worker_id):
    return client.get("/api/worker/poll", headers={
        "Authorization": "Bearer workerkey", "X-Worker-Id": worker_id,
        "X-Worker-Version": "a" * 40,
    })


def test_paused_worker_polls_and_gets_nothing(worker_client):
    client, Session = worker_client
    r = _poll(client, "paused-box")
    assert r.status_code == 204, r.text

    async def check():
        async with Session() as db:
            tasks = (await db.scalars(select(ScrapeTask))).all()
            assert [t.status for t in tasks] == ["pending"], \
                "the task must stay in the queue for another worker"
    _run(check())


def test_the_same_task_goes_to_an_unpaused_worker(worker_client):
    """Guards against the drain quietly blocking the whole fleet instead of one
    machine — the failure mode that would look like 'scraping just stopped'."""
    client, Session = worker_client
    assert _poll(client, "paused-box").status_code == 204

    r = _poll(client, "other-box")
    assert r.status_code == 200, r.text
    assert r.json()["source_url"] == "https://jda.gov.il/tenders"

    async def check():
        async with Session() as db:
            task = (await db.scalars(select(ScrapeTask))).one()
            assert task.status == "running"
            assert task.worker_id == "other-box"
            # ...and the machine that just showed up registered itself.
            assert (await db.get(WorkerNode, "other-box")) is not None
    _run(check())


def test_resuming_lets_the_worker_take_work_again(worker_client):
    client, Session = worker_client
    assert _poll(client, "paused-box").status_code == 204

    async def unpause():
        async with Session() as db:
            node = await db.get(WorkerNode, "paused-box")
            node.paused = False
            await db.commit()
    _run(unpause())

    assert _poll(client, "paused-box").status_code == 200


def test_fleet_reads_an_empty_table_without_error():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            assert await fleet(db) == []
    _run(go())
