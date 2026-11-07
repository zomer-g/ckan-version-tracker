import logging
import time
from datetime import datetime, timedelta, timezone

import jwt

from app.config import settings

logger = logging.getLogger(__name__)

# Every site token names its audience, and decoding requires it. The MCP tokens
# are signed with the same secret unless MCP_JWT_SECRET is set; they carry
# aud=over-mcp (and the Google-roundtrip state none), so without this check the
# two kinds were kept apart only by which claims each happened to lack.
SITE_AUDIENCE = "over-site"


def create_access_token(subject: str, auth_time: int | None = None) -> str:
    """A site JWT for ``subject``.

    ``auth_time`` is when the user actually signed in. A refresh carries it
    forward unchanged, which is what lets /api/auth/refresh refuse a session
    older than ``jwt_max_session_hours`` however often it was slid.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": subject,
        "aud": SITE_AUDIENCE,
        "iat": int(now.timestamp()),
        "auth_time": int(auth_time if auth_time is not None else now.timestamp()),
        "exp": now + timedelta(minutes=settings.jwt_expiry_minutes),
    }
    return jwt.encode(payload, settings.get_jwt_secret(), algorithm=settings.jwt_algorithm)


def decode_access_claims(token: str) -> dict | None:
    """The claims of a valid site token, or None."""
    try:
        return jwt.decode(token, settings.get_jwt_secret(),
                          algorithms=[settings.jwt_algorithm], audience=SITE_AUDIENCE)
    except jwt.PyJWTError:
        return None


def decode_access_token(token: str) -> str | None:
    claims = decode_access_claims(token)
    return claims.get("sub") if claims else None


def session_too_old(claims: dict, now: float | None = None) -> bool:
    """True when the sign-in behind ``claims`` is older than the session cap."""
    auth_time = claims.get("auth_time")
    if not isinstance(auth_time, (int, float)):
        return True
    max_age = int(settings.jwt_max_session_hours) * 3600
    return (now if now is not None else time.time()) - auth_time > max_age
