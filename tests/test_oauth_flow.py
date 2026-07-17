"""HTTP-level integration test for the OAuth login + Drive flows, proving the
security fix: NO JWT ever appears in a URL query string, and both flows still
work end-to-end.

Mounts only the oauth + auth routers on a bare FastAPI app (no lifespan /
scheduler), backs them with an in-memory SQLite DB, and mocks Google's token +
userinfo endpoints. Driven by asyncio.run + httpx.ASGITransport so no
pytest-asyncio is needed.
"""
import asyncio
import os
import sys
import types
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

import app.api.oauth as oauth  # noqa: E402
from app.api.auth import router as auth_router  # noqa: E402
from app.api.oauth import router as oauth_router  # noqa: E402
from app.auth.security import decode_access_token  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.auth_code import AuthCode  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402


# ── Fake Google (httpx.AsyncClient stand-in used inside oauth.py) ────────────
class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeGoogle:
    """Returns a token on POST (token exchange) and profile on GET (userinfo).
    The token payload includes a refresh_token so the Drive flow succeeds."""

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, *a, **k):
        return _FakeResp({"access_token": "ya29.fake", "refresh_token": "1//refresh-fake"})

    async def get(self, url, *a, **k):
        return _FakeResp({"email": "admin@example.com", "name": "Admin"})


def _make_app(SessionLocal):
    async def _get_db():
        async with SessionLocal() as s:
            yield s

    application = FastAPI()
    application.state.limiter = limiter
    application.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    application.include_router(oauth_router)
    application.include_router(auth_router)
    application.dependency_overrides[get_db] = _get_db
    return application


async def _setup():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: User.__table__.create(c))
        await conn.run_sync(lambda c: AuthCode.__table__.create(c))
    SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return SessionLocal


def _client(application):
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://test", follow_redirects=False)


# ── Login flow ───────────────────────────────────────────────────────────────

def test_login_callback_puts_no_jwt_in_url_and_exchange_works():
    async def go():
        settings.google_client_id = "cid"
        settings.google_client_secret = "csec"
        oauth.httpx = types.SimpleNamespace(AsyncClient=_FakeGoogle)  # patch only oauth's ref

        SessionLocal = await _setup()
        application = _make_app(SessionLocal)

        async with _client(application) as c:
            # 1) The SSO callback must redirect with a one-time ?code=, NOT a JWT.
            r = await c.get("/api/auth/sso/google/callback", params={"code": "goog-auth-code"})
            assert r.status_code in (302, 307), r.text
            loc = r.headers["location"]
            assert loc.startswith("/admin/login?code=")
            assert "sso_token" not in loc
            one_time = loc.split("code=", 1)[1]
            # The value in the URL must NOT be a decodable JWT.
            assert decode_access_token(one_time) is None

            # 2) Swapping the code returns a real JWT in the POST body.
            r2 = await c.post("/api/auth/sso/exchange", json={"code": one_time})
            assert r2.status_code == 200, r2.text
            token = r2.json()["token"]
            assert decode_access_token(token) is not None

            # 3) The code is single-use — a replay fails.
            r3 = await c.post("/api/auth/sso/exchange", json={"code": one_time})
            assert r3.status_code == 400

            # 4) The minted JWT authenticates /me.
            me = await c.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
            assert me.status_code == 200
            assert me.json()["email"] == "admin@example.com"

    asyncio.run(go())


def test_exchange_rejects_garbage_code():
    async def go():
        SessionLocal = await _setup()
        application = _make_app(SessionLocal)
        async with _client(application) as c:
            r = await c.post("/api/auth/sso/exchange", json={"code": "nope"})
            assert r.status_code == 400

    asyncio.run(go())


# ── Refresh flow ─────────────────────────────────────────────────────────────

def test_refresh_requires_auth_and_returns_new_token():
    async def go():
        SessionLocal = await _setup()
        # Seed a user + a valid token for them.
        uid = uuid.uuid4()
        async with SessionLocal() as db:
            db.add(User(
                id=uid, email="u@example.com", hashed_password="x",
                display_name="U", is_active=True, is_admin=False,
            ))
            await db.commit()
        from app.auth.security import create_access_token
        token = create_access_token(str(uid))

        application = _make_app(SessionLocal)
        async with _client(application) as c:
            # No auth → 401/403.
            r = await c.post("/api/auth/refresh")
            assert r.status_code in (401, 403)
            # With a valid token → a fresh valid token.
            r2 = await c.post("/api/auth/refresh", headers={"Authorization": f"Bearer {token}"})
            assert r2.status_code == 200, r2.text
            fresh = r2.json()["token"]
            assert decode_access_token(fresh) == str(uid)

    asyncio.run(go())


# ── Drive flow ───────────────────────────────────────────────────────────────

def test_drive_connect_returns_authorize_url_with_opaque_state_no_jwt():
    async def go():
        settings.google_client_id = "cid"
        settings.google_client_secret = "csec"
        oauth.httpx = types.SimpleNamespace(AsyncClient=_FakeGoogle)

        SessionLocal = await _setup()
        # Seed an admin.
        uid = uuid.uuid4()
        async with SessionLocal() as db:
            db.add(User(
                id=uid, email="admin@example.com", hashed_password="x",
                display_name="Admin", is_active=True, is_admin=True,
            ))
            await db.commit()
        from app.auth.security import create_access_token
        admin_token = create_access_token(str(uid))

        application = _make_app(SessionLocal)
        async with _client(application) as c:
            # connect requires admin auth (JWT in the header, not the URL).
            unauth = await c.post("/api/auth/sso/google/drive/connect", json={"next": "/admin"})
            assert unauth.status_code in (401, 403)

            r = await c.post(
                "/api/auth/sso/google/drive/connect",
                json={"next": "/admin/datasets/x"},
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert r.status_code == 200, r.text
            authorize_url = r.json()["authorize_url"]
            # The admin JWT must NOT be embedded anywhere in the Google URL.
            assert admin_token not in authorize_url
            # Pull the opaque state code out and confirm it's not a JWT.
            state = authorize_url.split("state=", 1)[1].split("&", 1)[0]
            assert decode_access_token(state) is None

            # The callback consumes the state code, stores the refresh token, and
            # bounces back to `next` with drive=connected.
            cb = await c.get(
                "/api/auth/sso/google/drive/callback",
                params={"code": "goog-auth-code", "state": state},
            )
            assert cb.status_code in (302, 307), cb.text
            assert cb.headers["location"] == "/admin/datasets/x?drive=connected"

        # The refresh token was persisted on the admin.
        async with SessionLocal() as db:
            u = (await db.get(User, uid))
            assert u.google_refresh_token == "1//refresh-fake"

    asyncio.run(go())


def test_drive_callback_rejects_unknown_state():
    async def go():
        settings.google_client_id = "cid"
        SessionLocal = await _setup()
        application = _make_app(SessionLocal)
        async with _client(application) as c:
            cb = await c.get(
                "/api/auth/sso/google/drive/callback",
                params={"code": "x", "state": "bogus-state"},
            )
            assert cb.status_code in (302, 307)
            assert cb.headers["location"] == "/admin/login?error=drive_unauthorized"

    asyncio.run(go())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
