"""Track LLM TOKENS, not just calls, on the global daily budget.

Migration 037 capped the public LLM endpoints by call count, which was the right
guard for /api/cbs/ask: every call there sends a fixed, small prompt, so calls
and spend are proportional.

The free-text query feature (/api/nl/query) breaks that assumption. Its prompt
carries a retrieved slice of the semantic model, so one call can cost anywhere
from a few hundred to several thousand input tokens, and its output includes
thinking tokens — which are billed as output, at 5x the input rate. A call-count
ceiling therefore bounds the number of requests but NOT the bill: at 2,000
calls/day the same cap is worth roughly $8 or roughly $130 depending on model
and prompt size. That is not a budget.

So the budget row now also accumulates tokens, and the reservation checks an
output-token ceiling alongside the call ceiling. Output is the gate because it
dominates the cost (5x input on every current model) and because it is the part
an attacker can inflate — a question engineered to trigger long reasoning costs
far more than one that does not, while counting as exactly one call either way.
Input tokens are recorded for observability only.

Both columns default to 0 and are updated after each call, so an existing row
for today keeps working and nothing needs backfilling.

Revision ID: 050
Revises: 049
Create Date: 2026-08-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "050"
down_revision: Union[str, None] = "049"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "llm_daily_usage",
        sa.Column("input_tokens", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.add_column(
        "llm_daily_usage",
        sa.Column("output_tokens", sa.BigInteger(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("llm_daily_usage", "output_tokens")
    op.drop_column("llm_daily_usage", "input_tokens")
