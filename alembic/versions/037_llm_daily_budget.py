"""Global daily budget counter for paid-LLM calls (public CBS NL endpoints).

/api/cbs/ask and /api/cbs/resolve invoke a paid LLM on every request. The
per-IP request limiter throttles one client but an attacker rotating IPs could
still drive unbounded spend. This table is a SINGLE global counter, one row per
calendar day, keyed only by the day — never by IP — so the app can enforce a
hard daily ceiling on total LLM spend that survives restarts and can't be reset
by X-Forwarded-For rotation. See app/models/llm_budget.py +
app/services/llm_budget.py.

Revision ID: 037
Revises: 036
Create Date: 2026-07-17
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "037"
down_revision: Union[str, None] = "036"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "llm_daily_usage",
        sa.Column("day", sa.Date(), primary_key=True),
        sa.Column("calls", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("llm_daily_usage")
