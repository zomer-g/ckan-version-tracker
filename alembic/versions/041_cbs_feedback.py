"""User like/dislike feedback on CBS searches.

One row per vote. Feeds the admin feedback report (GET /api/cbs/feedback/report)
that ranks queries by dislikes so search quality can be improved where it hurts.
No PII — only the query text, what was shown, and the vote.

Revision ID: 041
Revises: 040
Create Date: 2026-07-19
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "041"
down_revision: Union[str, None] = "040"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cbs_feedback",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("query", sa.Text(), nullable=False),
        # 'ask' (natural-language /resolve) | 'advanced' (keyword /search)
        sa.Column("mode", sa.String(12), nullable=False),
        # +1 like, -1 dislike.
        sa.Column("vote", sa.SmallInteger(), nullable=False),
        # For ask mode: the resolve answer_type shown (guidance/data_file/…).
        sa.Column("answer_type", sa.String(24)),
        # The primary result the user was rating, when there was one.
        sa.Column("top_url", sa.Text()),
        # 'web' | 'extension' — which surface the vote came from.
        sa.Column("source", sa.String(16)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    # The report groups by the normalized query; index it for the aggregation.
    op.create_index("ix_cbs_feedback_query", "cbs_feedback", ["query"])
    op.create_index("ix_cbs_feedback_created", "cbs_feedback", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_cbs_feedback_created", table_name="cbs_feedback")
    op.drop_index("ix_cbs_feedback_query", table_name="cbs_feedback")
    op.drop_table("cbs_feedback")
