"""Unit tests for the one-time auth-code store — the secret-free replacement
for putting a JWT in a URL query string (app/services/auth_codes.py).

Runs against an in-memory SQLite DB (the model uses the dialect-portable
sa.Uuid type), driven by asyncio.run so no pytest-asyncio is required — matching
the repo's dependency-light test style.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.database import Base  # noqa: E402
from app.models.auth_code import AuthCode  # noqa: E402
from app.services import auth_codes  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        # Create only the auth_codes table (avoid pulling every model's DDL).
        await conn.run_sync(lambda c: AuthCode.__table__.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ── hashing ──────────────────────────────────────────────────────────────

def test_hash_is_deterministic_and_not_the_code():
    h1 = auth_codes._hash("abc123")
    h2 = auth_codes._hash("abc123")
    assert h1 == h2
    assert h1 != "abc123"
    assert len(h1) == 64  # sha256 hex
    assert auth_codes._hash("abc123") != auth_codes._hash("abc124")


# ── issue → consume round-trip ───────────────────────────────────────────

def test_issue_then_consume_returns_identity():
    async def go():
        Session = await _session_factory()
        uid = uuid.uuid4()
        async with Session() as db:
            code = await auth_codes.issue_code(
                db, uid, auth_codes.PURPOSE_LOGIN, ttl_seconds=120
            )
            assert code and "sso_token" not in code
        async with Session() as db:
            consumed = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_LOGIN)
        assert consumed is not None
        assert consumed.user_id == str(uid)
        assert consumed.next_path is None

    _run(go())


def test_drive_code_carries_next_path():
    async def go():
        Session = await _session_factory()
        uid = uuid.uuid4()
        async with Session() as db:
            code = await auth_codes.issue_code(
                db, uid, auth_codes.PURPOSE_DRIVE,
                next_path="/admin/datasets/x?tab=y", ttl_seconds=600,
            )
        async with Session() as db:
            consumed = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_DRIVE)
        assert consumed is not None
        assert consumed.next_path == "/admin/datasets/x?tab=y"

    _run(go())


# ── single-use ───────────────────────────────────────────────────────────

def test_code_is_single_use():
    async def go():
        Session = await _session_factory()
        uid = uuid.uuid4()
        async with Session() as db:
            code = await auth_codes.issue_code(
                db, uid, auth_codes.PURPOSE_LOGIN, ttl_seconds=120
            )
        async with Session() as db:
            first = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_LOGIN)
        async with Session() as db:
            second = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_LOGIN)
        assert first is not None
        assert second is None  # replay denied

    _run(go())


# ── purpose isolation ────────────────────────────────────────────────────

def test_wrong_purpose_is_rejected():
    async def go():
        Session = await _session_factory()
        uid = uuid.uuid4()
        async with Session() as db:
            code = await auth_codes.issue_code(
                db, uid, auth_codes.PURPOSE_LOGIN, ttl_seconds=120
            )
        # A login code must not be usable as a drive code (or vice-versa).
        async with Session() as db:
            wrong = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_DRIVE)
        assert wrong is None
        # ...and after the failed cross-purpose attempt it still works for login.
        async with Session() as db:
            right = await auth_codes.consume_code(db, code, auth_codes.PURPOSE_LOGIN)
        assert right is not None

    _run(go())


# ── expiry ───────────────────────────────────────────────────────────────

def test_expired_code_is_rejected_and_consumed():
    async def go():
        Session = await _session_factory()
        uid = uuid.uuid4()
        # Insert a row that is already expired.
        async with Session() as db:
            db.add(AuthCode(
                code_hash=auth_codes._hash("expired-code"),
                user_id=uid,
                purpose=auth_codes.PURPOSE_LOGIN,
                next_path=None,
                expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
            ))
            await db.commit()
        async with Session() as db:
            consumed = await auth_codes.consume_code(db, "expired-code", auth_codes.PURPOSE_LOGIN)
        assert consumed is None
        # Even expired, it was deleted (single presentation), so it stays gone.
        async with Session() as db:
            again = await auth_codes.consume_code(db, "expired-code", auth_codes.PURPOSE_LOGIN)
        assert again is None

    _run(go())


def test_unknown_and_empty_codes_return_none():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            assert await auth_codes.consume_code(db, "", auth_codes.PURPOSE_LOGIN) is None
            assert await auth_codes.consume_code(db, "nope", auth_codes.PURPOSE_LOGIN) is None

    _run(go())


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
