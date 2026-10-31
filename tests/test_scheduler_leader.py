"""Leader election: one scheduler across overlapping containers.

An xhostd deploy runs the new container beside the old one for ~15 s, and both
used to start a scheduler (app/worker/leader.py). The lock is a session-level
Postgres advisory lock on a dedicated direct connection; here the connection is
a fake that answers pg_try_advisory_lock from a script.
"""
import asyncio
import inspect

import pytest

from app.config import settings
from app.worker import leader as L


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(L, "RETRY_S", 0)
    monkeypatch.setattr(L, "KEEPALIVE_S", 0)
    monkeypatch.setattr(L, "state", L._State())
    monkeypatch.setattr(settings, "database_url",
                        "postgresql+asyncpg://u:p@db.internal:5432/over")
    monkeypatch.setattr(settings, "database_url_direct", "")


class FakeConn:
    def __init__(self, answers, *, die_after_pings=None):
        self.answers = list(answers)
        self.closed = False
        self.pings = 0
        self.die_after_pings = die_after_pings
        self.sql: list[str] = []

    async def fetchval(self, sql, *args, **kw):
        self.sql.append(sql)
        if "pg_try_advisory_lock" in sql:
            assert args == (L.LOCK_KEY,)
            return self.answers.pop(0)
        if sql == "SELECT 1":
            self.pings += 1
            if self.die_after_pings is not None and self.pings > self.die_after_pings:
                raise ConnectionResetError("gone")
            return 1
        return True

    def is_closed(self):
        return self.closed

    async def close(self):
        self.closed = True

    def terminate(self):
        self.closed = True


# ── where the lock may live ───────────────────────────────────────────────────

def test_no_election_without_postgres(monkeypatch):
    monkeypatch.setattr(settings, "database_url", "sqlite+aiosqlite:///:memory:")
    assert L.lock_dsn() is None


def test_no_election_through_a_pooler(monkeypatch):
    """A session lock through a transaction-mode PgBouncer sits on whatever
    server connection the pooler lent for one statement."""
    monkeypatch.setattr(settings, "database_url",
                        "postgresql+asyncpg://u:p@ep-x-pooler.neon.tech/over")
    assert L.lock_dsn() is None


def test_the_lock_goes_to_the_direct_address(monkeypatch):
    monkeypatch.setattr(settings, "database_url",
                        "postgresql+asyncpg://u:p@pgbouncer.internal:6432/over")
    monkeypatch.setattr(settings, "database_url_direct",
                        "postgresql+asyncpg://u:p@db.internal:5432/over?sslmode=disable")
    dsn = L.lock_dsn()
    assert dsn.startswith("postgresql://u:p@db.internal:5432/over")
    assert "sslmode" not in dsn and "+asyncpg" not in dsn


def test_lock_key_is_a_signed_bigint():
    assert 0 < L.LOCK_KEY < 2 ** 63


# ── the election ──────────────────────────────────────────────────────────────

def _run_until(coro_fn, cond, timeout=2):
    async def go():
        task = asyncio.create_task(coro_fn())
        for _ in range(int(timeout * 1000)):
            if cond():
                break
            await asyncio.sleep(0.001)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    asyncio.run(go())


def test_without_a_lock_address_the_scheduler_starts_at_once(monkeypatch, caplog):
    monkeypatch.setattr(settings, "database_url", "sqlite+aiosqlite:///:memory:")
    started = []

    async def on_leader():
        started.append(True)

    with caplog.at_level("WARNING"):
        asyncio.run(L.run(on_leader, connect=None))
    assert started == [True]
    assert L.state.status == L.FALLBACK and L.holds_scheduler()
    assert "no leader election" in caplog.text


def test_the_follower_waits_until_the_old_instance_lets_go(monkeypatch):
    conn = FakeConn([False, False, False, True])
    seen = []
    real_set = L._set
    monkeypatch.setattr(L, "_set", lambda s: (seen.append(s), real_set(s)))
    started = []

    async def connect(dsn):
        return conn

    async def on_leader():
        started.append(True)

    _run_until(lambda: L.run(on_leader, connect=connect), lambda: bool(started))
    assert started == [True] and conn.answers == []
    assert seen[0] == L.WAITING and seen[-1] == L.LEADER
    assert L.state.conn is conn and not conn.closed, "the lock connection is held"


def test_unreachable_database_at_boot_falls_back_with_a_warning(caplog):
    started = []

    async def connect(dsn):
        raise OSError("connection refused")

    async def on_leader():
        started.append(True)

    with caplog.at_level("WARNING"):
        asyncio.run(asyncio.wait_for(L.run(on_leader, connect=connect), 2))
    assert started == [True] and L.state.status == L.FALLBACK
    assert "WITHOUT leader election" in caplog.text


def test_a_lost_lock_pauses_then_resumes():
    conns = [FakeConn([True], die_after_pings=1), FakeConn([False, True])]
    events = []

    async def connect(dsn):
        return conns.pop(0)

    _run_until(
        lambda: L.run(lambda: events.append("start"), connect=connect,
                      on_lost=lambda: events.append("lost"),
                      on_regained=lambda: events.append("regained")),
        lambda: "regained" in events)
    assert events == ["start", "lost", "regained"]
    assert L.state.status == L.LEADER


def test_release_unlocks_and_closes():
    conn = FakeConn([True])

    async def go():
        async def connect(dsn):
            return conn
        L.start(lambda: None, connect=connect)
        for _ in range(1000):
            if L.state.status == L.LEADER:
                break
            await asyncio.sleep(0.001)
        await L.release()

    asyncio.run(go())
    assert "SELECT pg_advisory_unlock($1)" in conn.sql
    assert conn.closed and L.state.conn is None and L.state.status == L.OFF


def test_release_while_waiting_stops_the_wait():
    conn = FakeConn([False] * 10_000)
    started = []

    async def go():
        async def connect(dsn):
            return conn
        L.start(lambda: started.append(True), connect=connect)
        await asyncio.sleep(0.01)
        await L.release()

    asyncio.run(go())
    assert started == [] and conn.closed


# ── wiring ────────────────────────────────────────────────────────────────────

def test_boot_does_not_wait_for_the_lock():
    """launch.sh must answer HTTP within 120 s; the old container may hold the
    lock longer. The lifespan hands the scheduler to a background election."""
    from app import main as M
    src = inspect.getsource(M.lifespan)
    assert "leader.start(_start_writers" in src
    assert "await init_scheduler()" not in src
    assert "await leader.release()" in src
    assert "await init_scheduler()" in inspect.getsource(M._start_writers)


def test_healthz_reports_the_leader_state():
    from fastapi.testclient import TestClient

    from app import main as M
    body = TestClient(M.app).get("/healthz").json()
    assert body["scheduler_running"] is False
    assert body["scheduler_leader"] == L.OFF


def test_pause_and_resume_are_noops_on_a_stopped_scheduler():
    from app import main as M
    M._pause_scheduler()
    M._resume_scheduler()
