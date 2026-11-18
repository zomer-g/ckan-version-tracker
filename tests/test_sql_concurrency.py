"""The per-IP cap on free-SQL queries running at once (app/sql_concurrency.py).

Pins: four run together, a fifth waits for a slot and then runs, a fifth that
waits too long or finds the queue full gets 429 with Retry-After, IPs do not
share slots, idle IPs leave no state behind — and the four free-SQL endpoints
carry both the gate and the 120/minute allowance.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import asyncio
from contextlib import asynccontextmanager

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute

from app import sql_concurrency as G
from app.config import settings

REAL_KEY = G.sql_client_key  # the fixture below swaps it out


@pytest.fixture(autouse=True)
def _gate(monkeypatch):
    # The "request" is the IP itself; the real key function is tested elsewhere.
    monkeypatch.setattr(G, "sql_client_key", lambda request: request)
    G._slots.clear()
    yield
    G._slots.clear()


slot = asynccontextmanager(G.sql_slot)


async def _hold(ip, started, release):
    async with slot(ip):
        started.append(ip)
        await release.wait()


def test_four_run_together_and_a_fifth_waits_for_a_slot():
    async def go():
        started, release = [], asyncio.Event()
        held = [asyncio.create_task(_hold("1.1.1.1", started, release)) for _ in range(4)]
        await asyncio.sleep(0)
        assert len(started) == 4
        fifth_ran = asyncio.Event()

        async def fifth():
            async with slot("1.1.1.1"):
                fifth_ran.set()

        t = asyncio.create_task(fifth())
        await asyncio.sleep(0.05)
        assert not fifth_ran.is_set()  # all four slots are taken
        release.set()
        await asyncio.wait_for(t, 1)
        assert fifth_ran.is_set()
        await asyncio.gather(*held)

    asyncio.run(go())


def test_waiting_too_long_is_a_429_with_retry_after(monkeypatch):
    monkeypatch.setattr(G, "WAIT_SECONDS", 0.05)

    async def go():
        started, release = [], asyncio.Event()
        held = [asyncio.create_task(_hold("1.1.1.1", started, release)) for _ in range(4)]
        await asyncio.sleep(0)
        with pytest.raises(HTTPException) as e:
            async with slot("1.1.1.1"):
                pass
        assert e.value.status_code == 429
        assert e.value.headers["Retry-After"] == str(G.RETRY_AFTER_SECONDS)
        release.set()
        await asyncio.gather(*held)

    asyncio.run(go())


def test_a_full_queue_is_refused_at_once(monkeypatch):
    monkeypatch.setattr(G, "MAX_WAITING_PER_IP", 1)

    async def go():
        started, release = [], asyncio.Event()
        held = [asyncio.create_task(_hold("1.1.1.1", started, release)) for _ in range(5)]  # 4 run, 1 waits
        await asyncio.sleep(0)
        with pytest.raises(HTTPException) as e:
            async with slot("1.1.1.1"):
                pass
        assert e.value.status_code == 429
        release.set()
        await asyncio.gather(*held)
        assert len(started) == 5

    asyncio.run(go())


def test_ips_do_not_share_slots_and_leave_no_state():
    async def go():
        started, release = [], asyncio.Event()
        held = [asyncio.create_task(_hold("1.1.1.1", started, release)) for _ in range(4)]
        await asyncio.sleep(0)
        async with slot("2.2.2.2"):  # another client is not queued behind the first
            pass
        release.set()
        await asyncio.gather(*held)

    asyncio.run(go())
    assert G._slots == {}


def test_a_failing_query_frees_its_slot():
    async def go():
        for _ in range(G.MAX_RUNNING_PER_IP + 1):
            with pytest.raises(RuntimeError):
                async with slot("1.1.1.1"):
                    raise RuntimeError("statement timeout")
        async with slot("1.1.1.1"):
            pass

    asyncio.run(go())
    assert G._slots == {}


def test_the_free_sql_endpoints_carry_the_gate_and_the_new_rate():
    import app.main as M
    from app.rate_limit import limiter

    wanted = {
        ("POST", "/api/append/{dataset_id}/sql"),
        ("GET", "/api/append/{dataset_id}/datastore_search_sql"),
        ("POST", "/api/tables/sql"),
        ("POST", "/api/knesset-db/sql"),
    }
    found = set()
    for r in M.app.routes:
        if not isinstance(r, APIRoute):
            continue
        for method in r.methods:
            if (method, r.path) in wanted:
                found.add((method, r.path))
                deps = {d.call for d in r.dependant.dependencies}
                assert G.sql_slot in deps, r.path
                name = f"{r.endpoint.__module__}.{r.endpoint.__name__}"
                limits = [str(l.limit) for l in limiter._route_limits.get(name, [])]
                assert any("120 per 1 minute" in l for l in limits), (r.path, limits)
    assert found == wanted, wanted - found


# ── SQL_PROXY_TOKEN: a trusted site forwarding its visitors ──────────────────

PROXY = "p" * 40


class _Req:
    def __init__(self, headers):
        self.headers = {k.lower(): v for k, v in headers.items()}


def test_the_proxy_key_is_the_forwarded_visitor(monkeypatch):
    monkeypatch.setattr(settings, "sql_proxy_token", PROXY)
    monkeypatch.setattr(G, "client_ip_key", lambda request: "9.9.9.9")
    auth = {"Authorization": "Bearer " + PROXY}
    assert REAL_KEY(_Req({**auth, "X-Over-Client": "visitorAAAA1234"})) == "proxy:visitorAAAA1234"
    assert REAL_KEY(_Req(auth)) == "proxy"                                   # no id: one shared bucket
    assert REAL_KEY(_Req({**auth, "X-Over-Client": "bad id!"})) == "proxy"   # malformed id ignored
    assert REAL_KEY(_Req({"Authorization": "Bearer wrong", "X-Over-Client": "visitorAAAA1234"})) == "9.9.9.9"
    assert REAL_KEY(_Req({"X-Over-Client": "visitorAAAA1234"})) == "9.9.9.9"  # an id alone is not trusted


def test_an_unconfigured_proxy_token_matches_nothing(monkeypatch):
    monkeypatch.setattr(settings, "sql_proxy_token", "")
    monkeypatch.setattr(G, "client_ip_key", lambda request: "9.9.9.9")
    assert REAL_KEY(_Req({"Authorization": "Bearer ", "X-Over-Client": "visitorAAAA1234"})) == "9.9.9.9"
