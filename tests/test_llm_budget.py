"""Unit tests for the GLOBAL daily LLM-call budget (anti-abuse cost guard).

Exercises app.services.llm_budget.reserve_llm_call against a fake async session
so no real Postgres is needed — the DB does the atomic increment+ceiling in one
UPSERT, and these tests pin the surrounding decision logic: the reservation is
allowed iff the UPSERT returns a row, the feature can be disabled two ways, and
a DB error fails OPEN (never takes the feature down).
"""
import asyncio
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.config import settings
from app.services import llm_budget


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def first(self):
        return self._row


class _FakeSession:
    """Minimal stand-in for AsyncSession. `row` is what the UPSERT's
    RETURNING yields: a tuple when under budget, None when at/over it."""

    def __init__(self, row=(1,), raise_on_execute=False):
        self._row = row
        self._raise = raise_on_execute
        self.committed = False
        self.rolled_back = False
        self.last_params = None

    async def execute(self, stmt, params=None):
        self.last_params = params
        if self._raise:
            raise RuntimeError("boom")
        return _FakeResult(self._row)

    async def commit(self):
        self.committed = True

    async def rollback(self):
        self.rolled_back = True


def _run(coro):
    return asyncio.run(coro)


def _configure(enabled=True, budget=2000):
    settings.llm_budget_enabled = enabled
    settings.cbs_ask_daily_budget = budget


def test_allows_when_upsert_returns_row():
    _configure(enabled=True, budget=2000)
    s = _FakeSession(row=(7,))
    assert _run(llm_budget.reserve_llm_call(s)) is True
    assert s.committed is True
    assert s.last_params == {"budget": 2000}


def test_blocks_when_upsert_returns_none():
    _configure(enabled=True, budget=2000)
    s = _FakeSession(row=None)  # WHERE calls < budget matched nothing → over budget
    assert _run(llm_budget.reserve_llm_call(s)) is False
    assert s.committed is True


def test_disabled_by_flag_skips_db():
    _configure(enabled=False, budget=1)
    s = _FakeSession(row=None)
    assert _run(llm_budget.reserve_llm_call(s)) is True
    # Never touched the DB when disabled.
    assert s.committed is False
    assert s.last_params is None
    _configure(enabled=True, budget=2000)


def test_zero_budget_disables():
    _configure(enabled=True, budget=0)
    s = _FakeSession(row=None)
    assert _run(llm_budget.reserve_llm_call(s)) is True
    assert s.last_params is None
    _configure(enabled=True, budget=2000)


def test_db_error_fails_open():
    _configure(enabled=True, budget=2000)
    s = _FakeSession(raise_on_execute=True)
    # A bookkeeping failure must not break the feature — allow the call.
    assert _run(llm_budget.reserve_llm_call(s)) is True
    assert s.rolled_back is True


# ── endpoint-level: the cap survives IP rotation ────────────────────────────
# The whole point of this budget (vs the per-IP 20/minute limiter) is that it is
# NOT keyed by client IP, so spoofing X-Forwarded-For / CF-Connecting-IP — the
# exact bypass that --proxy-headers makes worth worrying about — buys nothing.


def _ask_client(monkeypatch, exhausted: bool):
    """A /api/cbs/ask + /resolve app whose global budget is either exhausted or
    not, with a provider configured so the budget gate is reached."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from slowapi import _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded

    from app.api import cbs_ask
    from app.database import get_db
    from app.rate_limit import limiter

    # A provider must look configured, else the gate short-circuits to 503.
    monkeypatch.setattr(cbs_ask, "_provider", lambda: "deepseek")
    # Stand in for the real DB-backed reservation.
    async def _reserve(db):
        return not exhausted
    monkeypatch.setattr(cbs_ask, "reserve_llm_call", _reserve)
    # If the budget ever fails to block, this makes the test fail loudly
    # instead of silently attempting a real paid LLM call.
    async def _boom(db, q):
        raise AssertionError("LLM was invoked despite an exhausted budget")
    if exhausted:
        monkeypatch.setattr(cbs_ask, "_parse_question", _boom)

    async def _fake_db():
        yield object()

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(cbs_ask.router)
    app.dependency_overrides[get_db] = _fake_db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def test_ask_is_429_when_global_budget_exhausted(monkeypatch):
    c = _ask_client(monkeypatch, exhausted=True)
    r = c.post("/api/cbs/ask", json={"q": "כמה תושבים בחיפה"})
    assert r.status_code == 429
    assert r.headers.get("Retry-After") == "3600"


def test_resolve_is_429_when_global_budget_exhausted(monkeypatch):
    c = _ask_client(monkeypatch, exhausted=True)
    r = c.post("/api/cbs/resolve", json={"q": "כמה תושבים בחיפה"})
    assert r.status_code == 429


def test_exhausted_budget_cannot_be_bypassed_by_rotating_ips(monkeypatch):
    """Rotating the client IP resets the per-IP limiter's bucket but must NOT
    grant a single extra LLM call once the global daily cap is spent."""
    c = _ask_client(monkeypatch, exhausted=True)
    for i in range(30):
        spoofed = f"203.0.113.{i}"
        r = c.post(
            "/api/cbs/ask",
            json={"q": "test"},
            headers={
                "X-Forwarded-For": spoofed,
                "CF-Connecting-IP": spoofed,
            },
        )
        assert r.status_code == 429, f"IP {spoofed} slipped through: {r.status_code}"


def test_ask_proceeds_when_budget_available(monkeypatch):
    """Sanity: the gate is not blanket-blocking — an in-budget call gets past it
    (and then fails downstream on the fake DB, i.e. not a 429)."""
    c = _ask_client(monkeypatch, exhausted=False)
    r = c.post("/api/cbs/ask", json={"q": "test"})
    assert r.status_code != 429
