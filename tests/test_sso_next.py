"""Signing in from a SQL console returns the reader to the console, not to /admin.

The console's sign-in panel links to /api/auth/sso/google?next=<the page>. The
destination rides through Google in `state` and comes back with the one-time
code. These tests pin the round trip and, more importantly, that `next` can
never become an open redirect.
"""
import asyncio
import base64
import os
import sys
import types
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")

import httpx  # noqa: E402
import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

import app.api.oauth as oauth  # noqa: E402
from app.api.oauth import router as oauth_router  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.auth_code import AuthCode  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

CONSOLE = "/data?sql=SELECT%201"


# ── the rule ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    (CONSOLE, CONSOLE),
    ("/knesset", "/knesset"),
    ("/data#results", "/data#results"),
    ("https://evil.example/", None),
    ("//evil.example/steal", None),
    ("/\\evil.example", None),
    ("javascript:alert(1)", None),
    ("/data\r\nSet-Cookie: x=1", None),
    ("/admin/login?code=abc", None),
    ("/api/auth/sso/google", None),
    ("", None),
    (None, None),
])
def test_only_a_path_on_this_site_is_a_destination(raw, expected):
    assert oauth._safe_next(raw) == expected


def test_an_overlong_destination_keeps_its_page_and_drops_the_query():
    assert oauth._safe_next("/data?sql=" + "x" * 5000) == "/data"


def test_state_carries_the_destination_and_refuses_a_tampered_one():
    assert oauth._next_from_state(oauth._state_with_next(CONSOLE)) == CONSOLE
    forged = "abc." + base64.urlsafe_b64encode(b"https://evil.example/").decode().rstrip("=")
    assert oauth._next_from_state(forged) is None
    assert oauth._next_from_state("a-plain-random-state") is None
    assert oauth._next_from_state("abc.***not-base64***") is None
    assert "." not in oauth._state_with_next(None)


# ── the round trip over HTTP ─────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeGoogle:
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, *a, **k):
        return _FakeResp({"access_token": "ya29.fake"})

    async def get(self, url, *a, **k):
        return _FakeResp({"email": "reader@example.com", "name": "Reader"})


async def _client():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: User.__table__.create(c))
        await conn.run_sync(lambda c: AuthCode.__table__.create(c))
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _get_db():
        async with SessionLocal() as s:
            yield s

    application = FastAPI()
    application.state.limiter = limiter
    application.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    application.include_router(oauth_router)
    application.dependency_overrides[get_db] = _get_db
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=application),
                             base_url="http://test", follow_redirects=False)


def _google_state(location: str) -> str:
    return parse_qs(urlsplit(location).query)["state"][0]


def test_sign_in_from_the_console_comes_back_to_the_console(monkeypatch):
    async def go():
        monkeypatch.setattr(settings, "google_client_id", "cid")
        monkeypatch.setattr(settings, "google_client_secret", "csec")
        monkeypatch.setattr(oauth, "httpx", types.SimpleNamespace(AsyncClient=_FakeGoogle))
        async with await _client() as c:
            start = await c.get("/api/auth/sso/google", params={"next": CONSOLE})
            assert start.status_code in (302, 307)
            state = _google_state(start.headers["location"])
            assert oauth._next_from_state(state) == CONSOLE

            back = await c.get("/api/auth/sso/google/callback", params={"code": "g", "state": state})
            loc = back.headers["location"]
            assert loc.startswith("/admin/login?code=")
            assert parse_qs(urlsplit(loc).query)["next"] == [CONSOLE]

            code = parse_qs(urlsplit(loc).query)["code"][0]
            ex = await c.post("/api/auth/sso/exchange", json={"code": code})
            assert ex.status_code == 200
    asyncio.run(go())


def test_a_hostile_next_never_leaves_the_site(monkeypatch):
    async def go():
        monkeypatch.setattr(settings, "google_client_id", "cid")
        monkeypatch.setattr(settings, "google_client_secret", "csec")
        monkeypatch.setattr(oauth, "httpx", types.SimpleNamespace(AsyncClient=_FakeGoogle))
        async with await _client() as c:
            start = await c.get("/api/auth/sso/google", params={"next": "//evil.example/steal"})
            state = _google_state(start.headers["location"])
            assert "." not in state

            forged = "abc." + base64.urlsafe_b64encode(b"https://evil.example/").decode().rstrip("=")
            back = await c.get("/api/auth/sso/google/callback", params={"code": "g", "state": forged})
            assert "next=" not in back.headers["location"]
            assert "evil" not in back.headers["location"]
    asyncio.run(go())


def test_sign_in_from_the_login_page_still_goes_to_admin(monkeypatch):
    async def go():
        monkeypatch.setattr(settings, "google_client_id", "cid")
        monkeypatch.setattr(settings, "google_client_secret", "csec")
        monkeypatch.setattr(oauth, "httpx", types.SimpleNamespace(AsyncClient=_FakeGoogle))
        async with await _client() as c:
            start = await c.get("/api/auth/sso/google")
            state = _google_state(start.headers["location"])
            back = await c.get("/api/auth/sso/google/callback", params={"code": "g", "state": state})
            assert "next=" not in back.headers["location"]
    asyncio.run(go())
