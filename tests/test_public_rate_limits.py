"""The public v1 / versions API must never expose an unlimited heavy endpoint.

app/api/v1.py and app/api/versions.py are anonymous, unauthenticated surfaces
that run real work per request (list_datasets does count + join +
selectinload(tags) + a version-count subquery; diff_versions fetches two ODATA
metadata snapshots and diffs them). They used to carry no @limiter.limit at all.

These tests pin two invariants:
  * EVERY route on both routers has a rate limit registered — so a future
    endpoint added without a decorator fails CI instead of shipping unlimited.
  * `offset` is bounded, because deep OFFSET is O(offset) in Postgres.

No Postgres needed: get_db is overridden with a session that fails fast, which
is enough to exercise the limiter/validation layers that run before the handler
body.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.utils import MAX_API_OFFSET
from app.api.v1 import router as v1_router
from app.api.versions import router as versions_router
from app.database import get_db
from app.rate_limit import limiter


class _DeadSession:
    """Stand-in for AsyncSession whose every query fails immediately, so these
    tests never touch (or wait on) a real database."""

    async def execute(self, *a, **k):
        raise RuntimeError("no database in unit tests")


async def _fake_db():
    yield _DeadSession()


def _client() -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(v1_router)
    app.include_router(versions_router)
    app.dependency_overrides[get_db] = _fake_db
    # Each test starts from a clean per-IP bucket.
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _routes():
    for router in (v1_router, versions_router):
        for route in router.routes:
            if isinstance(route, APIRoute):
                yield route


def _limit_key(route: APIRoute) -> str:
    fn = route.endpoint
    return f"{fn.__module__}.{fn.__name__}"


def test_every_public_route_has_a_rate_limit():
    """No endpoint on the public v1 / versions routers may be unlimited."""
    unlimited = [
        f"{sorted(r.methods)} {r.path}"
        for r in _routes()
        if not limiter._route_limits.get(_limit_key(r))
    ]
    assert unlimited == [], f"endpoints missing @limiter.limit: {unlimited}"


@pytest.mark.parametrize(
    "name, ceiling",
    [
        # The two the audit called out as expensive get a tighter bucket than
        # the ordinary 60/minute reads.
        ("app.api.v1.list_datasets", 30),
        ("app.api.versions.diff_versions", 20),
        ("app.api.v1.get_tag", 30),
    ],
)
def test_heavy_endpoints_have_a_lower_ceiling(name, ceiling):
    limits = limiter._route_limits[name]
    assert limits, f"{name} has no limit"
    amount = limits[0].limit.amount
    assert amount == ceiling, f"{name} allows {amount}/min, expected {ceiling}"


def test_limiter_actually_rejects_over_the_ceiling():
    """End-to-end proof the decorator is wired (slowapi resolves `request`):
    the 21st call to the 20/minute diff endpoint is refused."""
    c = _client()
    codes = [c.get("/api/diff?from=x&to=y").status_code for _ in range(22)]
    assert codes.count(429) == 2, codes
    # ...and the refusal happens only after the ceiling, not before.
    assert 429 not in codes[:20], codes[:20]


@pytest.mark.parametrize("offset", [MAX_API_OFFSET + 1, 10 ** 9, -1])
def test_offset_outside_bounds_is_rejected(offset):
    """Deep OFFSET is O(offset) in Postgres — over the cap must be a clean 422,
    never a silent clamp that would quietly return the wrong page."""
    c = _client()
    assert c.get(f"/api/v1/datasets?offset={offset}").status_code == 422


def test_offset_at_the_cap_is_accepted():
    """The bound is inclusive: exactly MAX_API_OFFSET still validates (it gets
    past validation and fails later on the dead DB, i.e. not a 422)."""
    c = _client()
    assert c.get(f"/api/v1/datasets?offset={MAX_API_OFFSET}").status_code != 422
