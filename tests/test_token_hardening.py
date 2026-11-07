"""Token hardening: site tokens name their audience, sessions have a maximum
age, the MCP state has an audience of its own, and MCP refresh tokens are
single-use with reuse detection."""
import asyncio
import time
import uuid
from datetime import datetime, timedelta, timezone

import jwt
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.auth import security
from app.config import settings
from app.database import Base
from app.mcp import oauth
from app.models.mcp import ApiUser, McpOauthClient, McpRefreshToken
from app.models.user import User  # noqa: F401 — api_users.invited_by FK target

SECRET = "s" * 40


@pytest.fixture(autouse=True)
def _secret(monkeypatch):
    monkeypatch.setattr(settings, "jwt_secret_key", SECRET)
    monkeypatch.setattr(oauth, "mcp_jwt_secret", lambda: SECRET)


# ── site tokens ──────────────────────────────────────────────────────────────

def test_site_token_round_trip_and_audience():
    tok = security.create_access_token("u1")
    assert security.decode_access_token(tok) == "u1"
    assert jwt.decode(tok, SECRET, algorithms=["HS256"], audience="over-site")["auth_time"]


def test_mcp_tokens_are_not_site_tokens():
    mcp_access = jwt.encode({"sub": "u1", "aud": "over-mcp", "exp": int(time.time()) + 60},
                            SECRET, algorithm="HS256")
    no_aud = jwt.encode({"sub": "u1", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")
    assert security.decode_access_token(mcp_access) is None
    assert security.decode_access_token(no_aud) is None


def test_refresh_keeps_sign_in_time_and_caps_session(monkeypatch):
    monkeypatch.setattr(settings, "jwt_max_session_hours", 24)
    old = int(time.time()) - 25 * 3600
    claims = security.decode_access_claims(security.create_access_token("u1", auth_time=old))
    assert claims["auth_time"] == old
    assert security.session_too_old(claims)
    fresh = security.decode_access_claims(security.create_access_token("u1"))
    assert not security.session_too_old(fresh)
    assert security.session_too_old({})          # no auth_time: refuse


# ── MCP state ────────────────────────────────────────────────────────────────

def test_mcp_state_without_its_audience_is_refused():
    class _Req:
        query_params = {"code": "c", "state": jwt.encode(
            {"client_id": "x", "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")}
        headers = {"host": "over.org.il"}
    resp = asyncio.run(oauth.google_callback(_Req(), db=None))
    assert resp.status_code == 400


# ── MCP refresh rotation ─────────────────────────────────────────────────────

_TABLES = [ApiUser.__table__, McpOauthClient.__table__, McpRefreshToken.__table__]


class _Req:
    headers = {"host": "over.org.il", "x-forwarded-proto": "https"}
    url = type("U", (), {"scheme": "https", "netloc": "over.org.il"})()


@pytest.fixture()
def db():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    user_id, client_id = uuid.uuid4(), uuid.uuid4()

    async def setup():
        from app.models.user import User as U
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[U.__table__, *_TABLES])
        async with Session() as s:
            s.add(ApiUser(id=user_id, email="a@x.org", is_active=True))
            s.add(McpOauthClient(client_id=client_id, client_name="c", redirect_uris=[],
                                 grant_types=[], response_types=[]))
            await s.commit()
    asyncio.run(setup())
    s = Session()
    s.ids = (str(user_id), str(client_id))
    yield s
    asyncio.run(s.close())
    asyncio.run(engine.dispose())


def _claims(tok):
    return jwt.decode(tok, SECRET, algorithms=["HS256"], audience="over-mcp")


def test_refresh_is_single_use_and_reuse_revokes_family(db):
    uid, cid = db.ids

    async def run():
        first = _claims(await oauth._issue_refresh(db, _Req(), uid, cid))
        fam, err = await oauth._spend_refresh(db, first)
        assert err is None and fam == first["fam"]
        second = _claims(await oauth._issue_refresh(db, _Req(), uid, cid, family=fam))
        # Replay of the spent first token inside the grace window: allowed.
        assert (await oauth._spend_refresh(db, first))[1] is None
        # ...and after it: the family is revoked, the live token with it.
        row = await db.get(McpRefreshToken, first["jti"])
        row.used_at = datetime.now(timezone.utc) - timedelta(
            seconds=oauth.REFRESH_REUSE_GRACE_SECONDS + 5)
        await db.commit()
        assert (await oauth._spend_refresh(db, first))[1]
        assert (await oauth._spend_refresh(db, second))[1] == "refresh token revoked or unknown"

    asyncio.run(run())


def test_legacy_refresh_without_jti_starts_a_tracked_chain(db):
    uid, _ = db.ids
    assert asyncio.run(oauth._spend_refresh(db, {"sub": uid})) == (None, None)


def test_refresh_for_another_user_is_refused(db):
    uid, cid = db.ids

    async def run():
        c = _claims(await oauth._issue_refresh(db, _Req(), uid, cid))
        c["sub"] = str(uuid.uuid4())
        return await oauth._spend_refresh(db, c)

    assert asyncio.run(run())[1]
