"""One-time, short-lived auth codes — the secret-free replacement for putting a
JWT in a URL query string.

Two OAuth callbacks used to leak the admin JWT through the URL:
  * SSO login  — ``/admin/login?sso_token=<JWT>``
  * Drive connect — the whole JWT embedded in Google's ``state`` param

Both surfaces (Referer, browser history, proxy/Render/Cloudflare access logs)
retain query strings, so a 24h admin session token was routinely written to
places it must never appear. Instead the callback mints a random code here and
the URL carries only that opaque code:

  * login  — SPA POSTs the code to /api/auth/sso/exchange and gets a fresh JWT
  * drive  — Google echoes the code back as ``state``; we use it only to
             attribute the refresh token to the right admin (no JWT involved)

Codes are single-use (deleted the instant they are consumed) and time-boxed.
Only the SHA-256 of the code is persisted, so a DB read never yields a code
that could be replayed. See app/models/auth_code.py.
"""
import hashlib
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.auth_code import AuthCode

# Purposes.
PURPOSE_LOGIN = "login"
PURPOSE_DRIVE = "drive"

# TTLs. Login is a fast SPA POST that follows the redirect in the same tab, so
# seconds are plenty. Drive round-trips through Google's consent screen (the
# admin may read it), so it gets a longer, still-short window.
LOGIN_TTL_SECONDS = 120
DRIVE_TTL_SECONDS = 600


def _hash(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()


@dataclass
class ConsumedCode:
    user_id: str
    next_path: str | None


async def issue_code(
    db: AsyncSession,
    user_id,
    purpose: str,
    *,
    next_path: str | None = None,
    ttl_seconds: int,
) -> str:
    """Mint a one-time code for ``user_id`` and return the RAW code (the only
    time it exists in plaintext). Persists just its SHA-256."""
    code = secrets.token_urlsafe(32)
    now = datetime.now(timezone.utc)
    row = AuthCode(
        code_hash=_hash(code),
        user_id=user_id,
        purpose=purpose,
        next_path=next_path,
        expires_at=now + timedelta(seconds=ttl_seconds),
    )
    db.add(row)
    # Opportunistic housekeeping: drop rows that have already expired so the
    # table can't accumulate. Cheap (indexed) and bounded.
    await db.execute(delete(AuthCode).where(AuthCode.expires_at < now))
    await db.commit()
    return code


async def consume_code(db: AsyncSession, code: str, purpose: str) -> ConsumedCode | None:
    """Look up ``code`` for ``purpose``, delete it (single-use), and return the
    attached identity — or None if unknown, wrong purpose, or expired.

    The row is deleted whether or not it had expired, so a leaked code is inert
    after one presentation regardless of timing.
    """
    if not code:
        return None
    row = (
        await db.execute(
            select(AuthCode).where(
                AuthCode.code_hash == _hash(code),
                AuthCode.purpose == purpose,
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None

    expires_at = row.expires_at
    next_path = row.next_path
    user_id = str(row.user_id)

    # Single-use: consume it now, before validating expiry.
    await db.delete(row)
    await db.commit()

    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        return None

    return ConsumedCode(user_id=user_id, next_path=next_path)
