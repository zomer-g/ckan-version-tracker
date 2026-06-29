"""MCP server: api_users allow-list + OAuth clients/codes + usage events

Ports the MCP+OAuth schema from the sibling Ocal project to OVER. The
``api_users`` table is the closed-beta access gate (only invited emails may
connect via MCP); ``mcp_oauth_clients`` + ``mcp_oauth_codes`` back the OAuth
2.1 + PKCE flow (Dynamic Client Registration + short-lived auth codes);
``mcp_usage_events`` is an append-only log of every tool call.

Revision ID: 022
Revises: 021
Create Date: 2026-06-29
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "022"
down_revision: Union[str, None] = "021"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── api_users: the invite allow-list (the real MCP access gate) ──
    op.create_table(
        "api_users",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("email", sa.Text(), nullable=False, unique=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("google_id", sa.Text(), nullable=True, unique=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column(
            "invited_by", sa.UUID(),
            sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
        ),
        sa.Column("tier", sa.String(20), nullable=False, server_default="beta"),
        sa.Column("monthly_quota", sa.Integer(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("tier IN ('beta','free','pro')", name="ck_api_users_tier"),
    )

    # ── mcp_oauth_clients: Dynamic Client Registration (RFC 7591) ──
    op.create_table(
        "mcp_oauth_clients",
        sa.Column("client_id", sa.UUID(), primary_key=True),
        sa.Column("client_secret_hash", sa.Text(), nullable=True),  # NULL = public (PKCE-only)
        sa.Column("client_name", sa.Text(), nullable=False),
        sa.Column("redirect_uris", sa.JSON(), nullable=False),
        sa.Column("grant_types", sa.JSON(), nullable=False),
        sa.Column("response_types", sa.JSON(), nullable=False),
        sa.Column("token_endpoint_auth_method", sa.Text(), nullable=False, server_default="none"),
        sa.Column("scope", sa.Text(), nullable=False, server_default="mcp"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # ── mcp_oauth_codes: short-lived PKCE authorization codes (10-min TTL) ──
    op.create_table(
        "mcp_oauth_codes",
        sa.Column("code", sa.Text(), primary_key=True),
        sa.Column("client_id", sa.UUID(), sa.ForeignKey("mcp_oauth_clients.client_id", ondelete="CASCADE"), nullable=False),
        sa.Column("api_user_id", sa.UUID(), sa.ForeignKey("api_users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("code_challenge", sa.Text(), nullable=False),
        sa.Column("code_challenge_method", sa.Text(), nullable=False, server_default="S256"),
        sa.Column("scope", sa.Text(), nullable=False, server_default="mcp"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_mcp_oauth_codes_expires", "mcp_oauth_codes", ["expires_at"])

    # ── mcp_usage_events: append-only log of every tool call ──
    op.create_table(
        "mcp_usage_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), primary_key=True),
        sa.Column("api_user_id", sa.UUID(), sa.ForeignKey("api_users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("client_id", sa.UUID(), sa.ForeignKey("mcp_oauth_clients.client_id", ondelete="SET NULL"), nullable=True),
        sa.Column("mcp_session_id", sa.Text(), nullable=True),
        sa.Column("tool_name", sa.Text(), nullable=False),
        sa.Column("request_params", sa.JSON(), nullable=True),
        sa.Column("result_count", sa.Integer(), nullable=True),
        sa.Column("result_bytes", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(20), nullable=False, server_default="ok"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("idx_mcp_usage_events_user_time", "mcp_usage_events", ["api_user_id", "created_at"])


def downgrade() -> None:
    op.drop_index("idx_mcp_usage_events_user_time", table_name="mcp_usage_events")
    op.drop_table("mcp_usage_events")
    op.drop_index("idx_mcp_oauth_codes_expires", table_name="mcp_oauth_codes")
    op.drop_table("mcp_oauth_codes")
    op.drop_table("mcp_oauth_clients")
    op.drop_table("api_users")
