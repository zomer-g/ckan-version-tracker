import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    # DEPRECATED and not authoritative. Admin is derived per request from the
    # ADMIN_EMAILS platform secret (app/auth/dependencies.is_admin); nothing
    # reads this column to decide anything, and nothing writes it. Kept only so
    # the migration does not have to rewrite the table.
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    oauth_provider: Mapped[str | None] = mapped_column(String(50))
    # Google OAuth refresh token, stored when an admin connects Drive
    # (the "export to Drive" feature). Lets the runner mint a fresh
    # access token offline. NULL = Drive not connected for this user.
    google_refresh_token: Mapped[str | None] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
