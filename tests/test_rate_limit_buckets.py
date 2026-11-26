"""Per-minute limits count per ROUTE, not per URL, and signed-in callers get a tier.

On 25.11 one address made 126,746 calls to /api/nadlan/parcel/{gush}/{helka}/
source/gazetteer in nine hours without one 429: slowapi's default key_style
("url") gave every gush/helka its own fresh 60/minute bucket. These tests pin:
  * two different ids on one route share one bucket per caller;
  * a signed-in caller (valid Bearer) is counted per user, at the multiple;
  * the 429 says when to come back and where bulk data lives;
  * every nadlan route also counts against one family-wide bucket.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from fastapi import APIRouter, FastAPI, Request
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded

from app.api.nadlan import NADLAN_PER_CALLER
from app.api.nadlan import router as nadlan_router
from app.auth.security import create_access_token
from app.config import settings
from app.rate_limit import (BULK_HINT, limiter, rate_limit_exceeded_handler,
                            rate_limit_key, scale_limit)

router = APIRouter(prefix="/t")


@router.get("/item/{item_id}")
@limiter.limit("3/minute")
async def _item(request: Request, item_id: int):
    return {"id": item_id}


@router.get("/other/{item_id}")
@limiter.limit("3/minute")
async def _other(request: Request, item_id: int):
    return {"id": item_id}


def _client() -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(router)
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def test_different_ids_on_one_route_share_one_bucket():
    c = _client()
    codes = [c.get(f"/t/item/{i}").status_code for i in range(1, 6)]
    assert codes == [200, 200, 200, 429, 429], codes


def test_each_route_keeps_its_own_bucket():
    c = _client()
    for i in range(3):
        assert c.get(f"/t/item/{i}").status_code == 200
    assert c.get("/t/item/99").status_code == 429
    assert c.get("/t/other/1").status_code == 200


def test_signed_in_caller_gets_the_multiple_in_its_own_bucket(monkeypatch):
    monkeypatch.setattr(settings, "signed_in_rate_multiplier", 2)
    c = _client()
    # The address's own bucket is used up...
    for i in range(3):
        assert c.get(f"/t/item/{i}").status_code == 200
    assert c.get("/t/item/9").status_code == 429
    # ...but a signed-in user behind it is counted apart, at 2 x 3.
    auth = {"Authorization": f"Bearer {create_access_token('user-1')}"}
    codes = [c.get(f"/t/item/{i}", headers=auth).status_code for i in range(10, 17)]
    assert codes == [200] * 6 + [429], codes


def test_an_invalid_token_is_counted_by_address():
    c = _client()
    bad = {"Authorization": "Bearer not-a-token"}
    for i in range(3):
        assert c.get(f"/t/item/{i}", headers=bad).status_code == 200
    assert c.get("/t/item/9").status_code == 429


def test_rate_limit_key():
    from starlette.requests import Request as R

    def req(headers):
        return R({"type": "http", "method": "GET", "path": "/", "client": ("203.0.113.9", 1),
                  "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()]})

    assert rate_limit_key(req({})) == "203.0.113.9"
    assert rate_limit_key(req({"Authorization": "Bearer junk"})) == "203.0.113.9"
    tok = create_access_token("42")
    assert rate_limit_key(req({"Authorization": f"Bearer {tok}"})) == "u:42"


def test_429_says_when_and_where_bulk_data_lives():
    c = _client()
    for i in range(3):
        c.get(f"/t/item/{i}")
    r = c.get("/t/item/50")
    assert r.status_code == 429
    assert int(r.headers["Retry-After"]) >= 1
    body = r.json()
    assert body["bulk"] == BULK_HINT
    assert "download.csv" in body["bulk"]["en"] and "Bearer" in body["bulk"]["he"]
    assert "IP" in body["detail"]


def test_scale_limit():
    assert scale_limit("60/minute", 10) == "600/minute"
    assert scale_limit("6/hour;2/minute", 3) == "18/hour;6/minute"
    assert scale_limit("60/minute", 1) == "60/minute"


def test_every_nadlan_route_counts_against_the_family_bucket():
    missing = []
    for r in nadlan_router.routes:
        if isinstance(r, APIRoute):
            name = f"{r.endpoint.__module__}.{r.endpoint.__name__}"
            if NADLAN_PER_CALLER not in limiter.declared_shared.get(name, []):
                missing.append(r.path)
    assert missing == [], missing


def test_every_route_in_the_app_is_keyed_by_endpoint():
    """The one setting the 25.11 sweep slipped through."""
    assert limiter._key_style == "endpoint"
