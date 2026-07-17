import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class AuthCode(Base):
    """Short-lived, single-use one-time codes exchanged for a JWT.

    Replaces putting the admin JWT (or, in the Drive flow, the whole token
    embedded in Google's ``state``) into a URL query string — where it would
    leak through Referer headers, browser history, and proxy/Render/Cloudflare
    logs. Instead the OAuth callback mints a random code here; the SPA (login)
    or Google (Drive ``state``) carries only that opaque code, and it is
    swapped server-side for a fresh JWT / used to attribute the refresh token.

    Rows are single-use (deleted on consume) and time-boxed (``expires_at``).
    Only the SHA-256 of the code is stored, so a DB read never yields a usable
    code. See app/services/auth_codes.py.
    """

    __tablename__ = "auth_codes"

    # SHA-256 hex of the raw code (never the raw code itself).
    code_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    # sa.Uuid renders as native UUID on Postgres (matching the migration) and is
    # portable to SQLite for tests. No FK to users on purpose — codes are
    # ephemeral and self-cleaning; a stale user_id just fails the consume lookup.
    user_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, index=True)
    # "login" (exchange for a JWT) | "drive" (identify the connecting admin).
    purpose: Mapped[str] = mapped_column(String(16), nullable=False)
    # Drive flow only: the SPA path to bounce back to after consent.
    next_path: Mapped[str | None] = mapped_column(String(512))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
