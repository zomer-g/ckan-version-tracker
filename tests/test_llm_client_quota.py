"""Each caller's daily share of the LLM budget.

The SQL runs against a real Postgres named by OVER_TEST_PG_DSN (skipped
without one); the wiring tests need none.
"""
import asyncio
import os
from types import SimpleNamespace

import pytest

from app.config import settings
from app.services import llm_budget

DSN = os.environ.get("OVER_TEST_PG_DSN")


def test_client_key_prefers_the_user_and_never_stores_the_ip(monkeypatch):
    monkeypatch.setattr(settings, "jwt_secret_key", "k" * 40)
    req = SimpleNamespace(headers={"x-forwarded-for": "8.8.8.8"},
                          client=SimpleNamespace(host="10.0.0.1"))
    assert llm_budget.client_key(req, SimpleNamespace(id="abc")) == "u:abc"
    key = llm_budget.client_key(req)
    assert key.startswith("ip:") and "8.8.8.8" not in key
    other = SimpleNamespace(headers={"x-forwarded-for": "9.9.9.9"}, client=None)
    assert llm_budget.client_key(other) != key


def test_per_client_cap_is_checked_before_the_global_counter(monkeypatch):
    calls = []

    async def client_call(db, key, budget):
        calls.append(("client", key, budget))
        return False

    monkeypatch.setattr(llm_budget, "_reserve_client_call", client_call)
    monkeypatch.setattr(settings, "llm_client_daily_calls", 3)
    monkeypatch.setattr(settings, "cbs_ask_daily_budget", 1000)
    # No db access past the per-client refusal: the global counter is untouched.
    assert asyncio.run(llm_budget.reserve_llm_call(object(), client="ip:x")) is False
    assert calls == [("client", "ip:x", 3)]


@pytest.mark.skipif(not DSN, reason="OVER_TEST_PG_DSN not set")
def test_upsert_stops_at_the_caller_budget():
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    async def run():
        engine = create_async_engine(DSN.replace("postgresql://", "postgresql+asyncpg://"))
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS llm_client_daily_usage"))
            await conn.execute(text(
                "CREATE TABLE llm_client_daily_usage (day date NOT NULL, "
                "client_key varchar(80) NOT NULL, calls integer NOT NULL DEFAULT 0, "
                "updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (day, client_key))"))
        try:
            async with AsyncSession(engine) as db:
                got = [await llm_budget._reserve_client_call(db, "ip:a", 3) for _ in range(5)]
                other = await llm_budget._reserve_client_call(db, "ip:b", 3)
            return got, other
        finally:
            async with engine.begin() as conn:
                await conn.execute(text("DROP TABLE llm_client_daily_usage"))
            await engine.dispose()

    got, other = asyncio.run(run())
    assert got == [True, True, True, False, False]
    assert other is True
