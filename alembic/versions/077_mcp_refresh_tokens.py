"""auth.mcp_refresh_tokens: single-use MCP refresh tokens.

One row per issued refresh token (its ``jti``), grouped by ``family`` (one
family per sign-in). A refresh spends its row and issues the next; a spent
token presented again revokes the family. See app/models/mcp.McpRefreshToken.

Lives in ``auth``, which has no PUBLIC grant (migration 065), so the public
console role cannot read it; main.SENSITIVE_TABLES names it for the boot proof.

Revision ID: 077
Revises: 076
Create Date: 2026-11-07
"""
from alembic import op
import sqlalchemy as sa

revision = "077"
down_revision = "076"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "mcp_refresh_tokens",
        sa.Column("jti", sa.Text(), primary_key=True),
        sa.Column("family", sa.Text(), nullable=False),
        sa.Column("api_user_id", sa.UUID(),
                  sa.ForeignKey("auth.api_users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("client_id", sa.UUID(),
                  sa.ForeignKey("auth.mcp_oauth_clients.client_id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        schema="auth",
    )
    op.create_index("ix_mcp_refresh_tokens_family", "mcp_refresh_tokens", ["family"],
                    schema="auth")
    op.create_index("ix_mcp_refresh_tokens_expires_at", "mcp_refresh_tokens",
                    ["expires_at"], schema="auth")


def downgrade() -> None:
    op.drop_index("ix_mcp_refresh_tokens_expires_at", table_name="mcp_refresh_tokens",
                  schema="auth")
    op.drop_index("ix_mcp_refresh_tokens_family", table_name="mcp_refresh_tokens",
                  schema="auth")
    op.drop_table("mcp_refresh_tokens", schema="auth")
