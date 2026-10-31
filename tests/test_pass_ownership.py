"""The pass sweep may only drop passes whose owner is dead.

2026-10-31: an xhostd deploy runs the new container beside the old one for
~15 s. The new one's boot sweep dropped every ``_loader.pass_*`` table, among
them the one the old container was still streaming into, and that load died
("relation _loader.pass_… does not exist"). A pass now has a row in
``_loader.live_passes`` with a heartbeat, and the sweep drops only stale ones
(app/services/append_store.py, "Who owns a pass table").

No live Postgres in the suite: the SQL is checked as text, the flow against a
fake connection that records what it was asked.
"""
import asyncio
import contextlib

import pytest

from app.services import append_store as A


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    monkeypatch.setattr(A, "_live_passes", set())
    monkeypatch.setattr(A, "_registry_ready", True)
    monkeypatch.setattr(A, "_heartbeat_task", None)


class FakeConn:
    def __init__(self, rows=None, still_stale=True, locked=()):
        self.rows = rows or []
        self.still_stale = still_stale
        self.locked = set(locked)
        self.log: list[tuple] = []
        self.in_txn = False

    @contextlib.asynccontextmanager
    async def _txn(self):
        self.in_txn = True
        self.log.append(("BEGIN",))
        try:
            yield
        except Exception:
            self.log.append(("ROLLBACK",))
            raise
        else:
            self.log.append(("COMMIT",))
        finally:
            self.in_txn = False

    def transaction(self):
        return self._txn()

    async def execute(self, sql, *args, **kw):
        self.log.append(("execute", sql, args, self.in_txn))
        if sql.startswith("DROP TABLE") and any(n in sql for n in self.locked):
            import asyncpg
            raise asyncpg.exceptions.LockNotAvailableError("locked")
        return "OK"

    async def fetch(self, sql, *args, **kw):
        self.log.append(("fetch", sql, args))
        return self.rows

    async def fetchval(self, sql, *args, **kw):
        self.log.append(("fetchval", sql, args, self.in_txn))
        if sql.startswith("DELETE") and self.still_stale:
            return args[0]
        return None

    def sql(self, kind=None):
        return [e[1] for e in self.log if len(e) > 1 and (kind is None or e[0] == kind)]


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def _acq(self):
        yield self.conn

    def acquire(self):
        return self._acq()


def _use(monkeypatch, conn):
    async def get_pool():
        return FakePool(conn)
    monkeypatch.setattr(A, "get_pool", get_pool)


# ── the decision ──────────────────────────────────────────────────────────────

def test_sweep_decision_every_arm():
    d = A.sweep_decision
    assert d(registered=True, stale=True, own=False) == "drop"
    assert d(registered=True, stale=False, own=False) == "keep"
    # Made by older code (the old container during the first rollout): its
    # owner cannot be judged, so register it and judge it on a later round.
    assert d(registered=False, stale=False, own=False) == "adopt"
    # Our own pass is never dropped by our own sweep, whatever the registry says.
    assert d(registered=True, stale=True, own=True) == "keep"
    assert d(registered=False, stale=False, own=True) == "keep"


def test_the_registry_is_not_itself_a_pass_table():
    """The sweep lists ``pass\\_%``; the registry living in the same schema must
    never match it, or the sweep would drop it."""
    assert not A._LIVE_TABLE.startswith(A._PASS_PREFIX)
    assert A._PASS_SCHEMA == "_loader"


def test_the_stale_window_dwarfs_the_heartbeat():
    assert A.PASS_STALE_S >= 10 * A.PASS_HEARTBEAT_S
    # The loop runs often enough that an adopted table goes on the next round.
    assert A.PASS_SWEEP_INTERVAL_S >= A.PASS_STALE_S


def test_candidates_are_one_snapshot_with_db_clock_staleness():
    sql = A.sweep_candidates_sql()
    assert 'LEFT JOIN "_loader"."live_passes" r' in sql
    assert "now() - make_interval(secs => $3)" in sql
    # A row without a heartbeat comparison result is not stale (COALESCE).
    assert "COALESCE(" in sql
    ddl = " ".join(A.registry_sql())
    assert 'CREATE TABLE IF NOT EXISTS "_loader"."live_passes"' in ddl
    assert '"heartbeat" timestamptz NOT NULL DEFAULT now()' in ddl


# ── the sweep ─────────────────────────────────────────────────────────────────

def _row(name, registered, stale):
    return {"tablename": name, "registered": registered, "stale": stale}


def test_sweep_drops_only_stale_registered_passes(monkeypatch):
    A._live_passes.add("pass_mine")
    conn = FakeConn(rows=[
        _row("pass_dead", True, True),
        _row("pass_live", True, False),     # the old container, mid-load
        _row("pass_legacy", False, False),  # pre-registry code
        _row("pass_mine", True, True),
    ])
    _use(monkeypatch, conn)
    assert asyncio.run(A.sweep_abandoned_passes()) == 1
    drops = [s for s in conn.sql("execute") if s.startswith("DROP TABLE")]
    assert drops == ['DROP TABLE IF EXISTS "_loader"."pass_dead"']
    adopts = [e for e in conn.log if e[0] == "execute" and e[1].startswith("INSERT")]
    assert len(adopts) == 1 and adopts[0][2] == ("pass_legacy", A._UNCLAIMED_OWNER)
    assert "ON CONFLICT" in adopts[0][1]


def test_the_drop_rechecks_staleness_in_the_same_transaction(monkeypatch):
    conn = FakeConn(rows=[_row("pass_dead", True, True)])
    _use(monkeypatch, conn)
    asyncio.run(A.sweep_abandoned_passes())
    i = conn.log.index(("BEGIN",))
    txn = conn.log[i:conn.log.index(("COMMIT",), i) + 1]
    kinds = [e[1][:20] for e in txn[1:-1]]
    assert kinds[0].startswith("SET LOCAL lock_timeo")
    assert kinds[1].startswith("DELETE FROM")
    assert kinds[2].startswith("DROP TABLE")
    delete = txn[2][1]
    assert '"heartbeat" < now() - make_interval(secs => $2)' in delete


def test_a_heartbeat_landing_before_the_drop_wins(monkeypatch):
    conn = FakeConn(rows=[_row("pass_dead", True, True)], still_stale=False)
    _use(monkeypatch, conn)
    assert asyncio.run(A.sweep_abandoned_passes()) == 0
    assert not [s for s in conn.sql("execute") if s.startswith("DROP TABLE")]


def test_a_table_locked_by_a_loader_is_kept(monkeypatch):
    conn = FakeConn(rows=[_row("pass_busy", True, True)], locked={"pass_busy"})
    _use(monkeypatch, conn)
    assert asyncio.run(A.sweep_abandoned_passes()) == 0
    assert ("ROLLBACK",) in conn.log


def test_sweep_never_raises(monkeypatch):
    async def boom():
        raise OSError("db down")
    monkeypatch.setattr(A, "get_pool", boom)
    assert asyncio.run(A.sweep_abandoned_passes()) == 0


# ── the owner's side ──────────────────────────────────────────────────────────

def test_start_creates_table_and_registry_row_in_one_transaction(monkeypatch):
    conn = FakeConn()
    started = []
    monkeypatch.setattr(A, "_ensure_heartbeat", lambda: started.append(True))
    t = asyncio.run(A.PassTracker("append_x", ident_col="row_hash", token="abc").start(conn))
    stmts = [e for e in conn.log if e[0] == "execute"]
    assert stmts[-2][1] == 'CREATE TABLE "_loader"."pass_abc" ("ident" text)'
    assert stmts[-1][1].startswith('INSERT INTO "_loader"."live_passes"')
    assert stmts[-1][2] == ("pass_abc", A._OWNER)
    assert stmts[-1][3] and stmts[-2][3], "both inside the transaction"
    assert t.pass_name in A._live_passes and started == [True]


def test_abandon_drops_the_row_and_forgets_the_pass(monkeypatch):
    conn = FakeConn()
    _use(monkeypatch, conn)
    A._live_passes.add("pass_abc")
    asyncio.run(A.PassTracker("append_x", ident_col="row_hash", token="abc").abandon())
    stmts = [e[1] for e in conn.log if e[0] == "execute"]
    assert stmts == ['DROP TABLE IF EXISTS "_loader"."pass_abc"',
                     'DELETE FROM "_loader"."live_passes" WHERE "name" = $1']
    assert "pass_abc" not in A._live_passes


def test_abandon_forgets_the_pass_even_when_the_drop_fails(monkeypatch):
    """Without beats, a table that could not be dropped goes stale and the sweep
    takes it; if we kept beating for it, it would live forever."""
    async def boom():
        raise OSError("db down")
    monkeypatch.setattr(A, "get_pool", boom)
    A._live_passes.add("pass_abc")
    asyncio.run(A.PassTracker("append_x", ident_col="row_hash", token="abc").abandon())
    assert "pass_abc" not in A._live_passes


def test_heartbeat_beats_while_passes_live_then_stops(monkeypatch):
    monkeypatch.setattr(A, "PASS_HEARTBEAT_S", 0)
    A._live_passes.update({"pass_a", "pass_b"})
    beats: list[list[str]] = []

    async def beat(names):
        beats.append(names)
        if len(beats) == 1:
            raise OSError("blip")  # a failed beat is retried, not fatal
        if len(beats) == 3:
            A._live_passes.clear()
        return set(names)

    monkeypatch.setattr(A, "_beat", beat)
    asyncio.run(asyncio.wait_for(A._heartbeat_loop(), 2))
    assert beats[0] == ["pass_a", "pass_b"] and len(beats) == 3


def test_heartbeat_reports_a_pass_swept_from_under_it(monkeypatch, caplog):
    monkeypatch.setattr(A, "PASS_HEARTBEAT_S", 0)
    A._live_passes.add("pass_a")
    calls = []

    async def beat_once(names):
        calls.append(names)
        if len(calls) > 1:
            A._live_passes.clear()
        return set()

    monkeypatch.setattr(A, "_beat", beat_once)
    with caplog.at_level("ERROR"):
        asyncio.run(asyncio.wait_for(A._heartbeat_loop(), 2))
    assert "swept while still in use" in caplog.text


def test_the_sweep_runs_on_the_leader_not_at_every_boot():
    import inspect

    from app import main as M
    assert "sweep_abandoned_passes" not in inspect.getsource(M.lifespan)
    assert "sweep_loop" in inspect.getsource(M._start_writers)
