"""MCP server models: api_users allow-list + OAuth clients/codes + usage log.

Mirrors alembic migration 022. See app/mcp/ for the server + OAuth flow.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Identity, Integer, String, Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


class ApiUser(Base):
    """Closed-beta invite list — the real MCP access gate."""
    __tablename__ = "api_users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str | None] = mapped_column(Text)
    google_id: Mapped[str | None] = mapped_column(Text, unique=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    invited_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    tier: Mapped[str] = mapped_column(String(20), nullable=False, default="beta")  # beta|free|pro
    monthly_quota: Mapped[int | None] = mapped_column(Integer)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now, onupdate=_now)


class McpOauthClient(Base):
    """Dynamically-registered OAuth client (RFC 7591). Public (PKCE) by default."""
    __tablename__ = "mcp_oauth_clients"

    client_id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    client_secret_hash: Mapped[str | None] = mapped_column(Text)
    client_name: Mapped[str] = mapped_column(Text, nullable=False)
    redirect_uris: Mapped[list] = mapped_column(JSONB, nullable=False)
    grant_types: Mapped[list] = mapped_column(JSONB, nullable=False)
    response_types: Mapped[list] = mapped_column(JSONB, nullable=False)
    token_endpoint_auth_method: Mapped[str] = mapped_column(Text, nullable=False, default="none")
    scope: Mapped[str] = mapped_column(Text, nullable=False, default="mcp")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class McpOauthCode(Base):
    """Short-lived single-use PKCE authorization code (10-min TTL)."""
    __tablename__ = "mcp_oauth_codes"

    code: Mapped[str] = mapped_column(Text, primary_key=True)
    client_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("mcp_oauth_clients.client_id", ondelete="CASCADE"), nullable=False)
    api_user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("api_users.id", ondelete="CASCADE"), nullable=False)
    redirect_uri: Mapped[str] = mapped_column(Text, nullable=False)
    code_challenge: Mapped[str] = mapped_column(Text, nullable=False)
    code_challenge_method: Mapped[str] = mapped_column(Text, nullable=False, default="S256")
    scope: Mapped[str] = mapped_column(Text, nullable=False, default="mcp")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class McpUsageEvent(Base):
    """Append-only log of every MCP tool call (for analytics / future billing)."""
    __tablename__ = "mcp_usage_events"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    api_user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("api_users.id", ondelete="CASCADE"), nullable=False)
    client_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("mcp_oauth_clients.client_id", ondelete="SET NULL"))
    mcp_session_id: Mapped[str | None] = mapped_column(Text)
    tool_name: Mapped[str] = mapped_column(Text, nullable=False)
    request_params: Mapped[dict | None] = mapped_column(JSONB)
    result_count: Mapped[int | None] = mapped_column(Integer)
    result_bytes: Mapped[int | None] = mapped_column(Integer)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ok")
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
