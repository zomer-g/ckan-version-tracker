"""The dispatch gate must not strand a fleet behind a pin nobody bumped.

The worker repo is private, so this server cannot look up its upstream SHA
(GitHub's commits API answers 404 without a token). That used to be papered
over with a SHA hardcoded in config.py which an operator had to bump on every
worker deploy — and the day it was forgotten, every worker in the fleet was
refused while running the newest code.

Freshness is now self-reported: the worker compares HEAD to origin/<branch>,
which it already fetches to self-update, and sends X-Worker-Upstream. These
tests pin who is let through, including the workers too old to send the header
at all.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import worker as worker_api
from app.config import settings
from app.database import get_db
from app.rate_limit import limiter


WORKER_SHA = "51ef400406507f701f2332bad8e3f8f5c1cbdedc"


class _EmptyDB:
    """No pending tasks — the gate is what's under test, not dispatch.

    Also stands in for the fleet bookkeeping the poll does before the gate
    (app/services/worker_fleet.touch_worker): `get` finds no existing machine,
    so the worker registers itself via `add`. Nothing here is asserted on — the
    stub just has to answer the same calls a real session would.
    """

    # poll_for_task takes the fleet-wide claim lock before it looks for a task,
    # and _acquire_claim_lock reads db.bind.dialect.name to decide whether the
    # backend even has advisory locks. Without a bind the whole endpoint raised
    # AttributeError → 500, which _refused() read as "not refused" — so every
    # dispatch assertion in this file passed on a crash. Naming a non-Postgres
    # dialect takes the same branch the SQLite test DB does: lock granted, no
    # SQL issued.
    class _Bind:
        class dialect:
            name = "sqlite"

    bind = _Bind()

    async def get(self, model, key):
        return None

    def add(self, obj):
        pass

    async def rollback(self):
        pass

    async def execute(self, stmt):
        class _Result:
            def scalars(self):
                class _S:
                    def all(self_inner):
                        return []
                return _S()

            def scalar_one_or_none(self):
                return None

            def first(self):
                return None

            # source_load.limits() reads the per-source caps straight off the
            # result (no .scalars()); with none configured every source is
            # uncapped, which is the state this file wants.
            def all(self):
                return []
        return _Result()

    async def commit(self):
        pass


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")
    monkeypatch.setattr(settings, "worker_version_check_enabled", True)
    # The default since the pin was removed.
    monkeypatch.setattr(settings, "worker_required_version", "")

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)

    async def _db():
        yield _EmptyDB()

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _poll(client, **headers):
    base = {"Authorization": "Bearer workerkey", "X-Worker-Version": WORKER_SHA}
    return client.get("/api/worker/poll", headers={**base, **headers})


def _refused(response) -> bool:
    """Did the gate turn this worker away?

    A refusal is a 200 carrying ``outdated``; a worker that gets past the gate
    sees 204 here, because the stub queue is empty. Anything else — a 500 most
    of all — is neither, and must be loud: read as a plain boolean it would
    come back False and every ``assert not _refused(...)`` below would pass on
    a crash, which is exactly what used to happen.
    """
    assert response.status_code in (200, 204), (
        f"poll returned {response.status_code}, which is neither a refusal nor "
        f"a dispatch: {response.text[:200]}"
    )
    return response.status_code == 200 and (response.json() or {}).get("outdated")


def test_a_current_worker_is_dispatched_to(client):
    assert not _refused(_poll(client, **{"X-Worker-Upstream": "current"}))


def test_a_worker_that_says_it_is_behind_is_refused(client):
    response = _poll(client, **{"X-Worker-Upstream": "behind"})
    assert _refused(response)
    assert "behind origin/" in response.json()["message"]


def test_a_worker_that_cannot_tell_is_dispatched_to(client):
    """No git, a detached checkout, a fetch that failed — none of these mean
    the code is stale, and refusing would strand the worker for good."""
    assert not _refused(_poll(client, **{"X-Worker-Upstream": "unknown"}))


def test_a_worker_too_old_to_report_is_dispatched_to(client):
    """Workers predating the header must keep working; they self-update to a
    build that does report."""
    assert not _refused(_poll(client))


def test_no_github_lookup_is_attempted_without_a_pin(client, monkeypatch):
    """The repo is private, so every lookup 404s — and doing one per poll
    would burn the unauthenticated 60/hour rate limit for nothing."""
    async def _boom(*a, **kw):
        raise AssertionError("the gate must not query GitHub without a pin")

    monkeypatch.setattr(worker_api, "get_required_worker_sha", _boom)
    monkeypatch.setattr(worker_api, "get_required_engine_hash", _boom)
    assert not _refused(_poll(client, **{"X-Worker-Upstream": "current"}))
    assert not _refused(_poll(client))  # header-less worker too


# --- the emergency override -------------------------------------------------


@pytest.fixture
def pinned(client, monkeypatch):
    monkeypatch.setattr(settings, "worker_required_version", WORKER_SHA)

    async def _no_engine_hash(*a, **kw):
        return None

    monkeypatch.setattr(worker_api, "get_required_engine_hash", _no_engine_hash)
    return client


def test_pin_lets_the_matching_worker_through(pinned):
    assert not _refused(_poll(pinned, **{"X-Worker-Upstream": "current"}))


def test_pin_refuses_a_different_sha_even_if_it_claims_to_be_current(pinned):
    """The override exists to freeze the fleet on a known-good commit while a
    bad one is reverted — a worker's own opinion must not defeat it."""
    response = _poll(pinned, **{"X-Worker-Version": "0" * 40,
                                "X-Worker-Upstream": "current"})
    assert _refused(response)
    assert "git SHA mismatch" in response.json()["message"]
