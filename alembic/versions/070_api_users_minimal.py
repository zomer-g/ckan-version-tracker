"""MCP users: keep the email only. Drop name and google_id.

Revision ID: 070
Revises: 069
Create Date: 2026-10-04

Anyone who completes Google SSO on an MCP server gets an ``auth.api_users``
row. Until now that row also kept the Google display name and the Google
account id, neither of which anything read: the gate matches on the email,
the admin list shows the email, usage is keyed by our own uuid. Personal data
that serves no function is only a liability, so both columns go, together
with the data already in them. app/mcp/oauth.py stops asking Google for the
``profile`` scope in the same change, so nothing arrives to be stored.

The downgrade recreates the columns empty: the values are deliberately not
recoverable.
"""
import sqlalchemy as sa
from alembic import op

revision = "070"
down_revision = "069"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("api_users", "google_id", schema="auth")
    op.drop_column("api_users", "name", schema="auth")


def downgrade() -> None:
    op.add_column("api_users", sa.Column("name", sa.Text(), nullable=True), schema="auth")
    op.add_column("api_users", sa.Column("google_id", sa.Text(), nullable=True), schema="auth")
    op.create_unique_constraint("api_users_google_id_key", "api_users", ["google_id"], schema="auth")
