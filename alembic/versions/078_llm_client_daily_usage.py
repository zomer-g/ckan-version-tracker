"""app.llm_client_daily_usage: each caller's daily share of the LLM budget.

One row per (day, caller). The caller key is ``u:<user id>`` for a signed-in
user or ``ip:<HMAC of the address>`` for an anonymous one; the address itself
is never stored. See app/services/llm_budget.reserve_llm_call.

Revision ID: 078
Revises: 077
Create Date: 2026-11-07
"""
from alembic import op

revision = "078"
down_revision = "077"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.llm_client_daily_usage (
            day         date         NOT NULL,
            client_key  varchar(80)  NOT NULL,
            calls       integer      NOT NULL DEFAULT 0,
            updated_at  timestamptz  NOT NULL DEFAULT now(),
            PRIMARY KEY (day, client_key)
        )
    """)
    op.execute("REVOKE ALL ON TABLE app.llm_client_daily_usage FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.llm_client_daily_usage")
