"""Click-through log for the guided explorer, and a learned-synonym store.

THE PROBLEM THIS SOLVES: the explorer's quality metric (recall@5) is currently
measured against ~21 hand-written gold cases — enough to catch regressions,
small enough to over-fit, and blind to what people actually type. Meanwhile the
biggest damage in this feature's history came from shipping "improvements"
without a measurement loop.

Every use of the explorer is a labelled example waiting to be kept: the user
types a description, sees a shortlist, and PICKS one. The pick is ground truth
produced by the person who knows what they meant. Two tables:

  nl_suggest_log — one row per search: the text, how many suggestions came
    back, the top suggestion, and (updated later) which one was picked at which
    rank. From this: recall-in-the-wild (was anything picked? at what rank?),
    the no-pick rate (the real out-of-scope signal), and a stream of real
    questions to grow the benchmark gold set from.

  nl_synonyms — words the scorer should treat as naming a dataset, adopted BY
    AN ADMIN from the log (typed "גירושין", picked the "התגרשו" table ⇒ the
    pair is a synonym candidate). This is the systematic fix for Hebrew
    morphology: the consonant-skeleton guess catches a pair once, the admin
    promotes it, and from then on it is an exact match. Adoption is manual on
    purpose — auto-learning from clicks would let one user's misclick teach the
    scorer a wrong association with no one noticing.

PRIVACY: search text is user-authored, same rules as nl_query_log — app DB
only (never the publicly-queryable append DB), admin-only endpoints, prunable
(index on created_at).

Revision ID: 052
Revises: 051
Create Date: 2026-08-02
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "052"
down_revision: Union[str, None] = "051"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "nl_suggest_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("query", sa.Text(), nullable=False),
        sa.Column("suggestions_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("approximate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("top_table", sa.String(255)),
        # Filled in by /api/nl/picked when the user chooses. NULL = no pick,
        # which is itself the signal: either nothing relevant was offered, or
        # the user bounced. rank is 1-based position in the list they saw.
        sa.Column("picked_table", sa.String(255)),
        sa.Column("picked_rank", sa.Integer()),
        sa.Column("picked_approximate", sa.Boolean()),
        sa.Column("picked_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_nl_suggest_log_created_at", "nl_suggest_log", ["created_at"])

    op.create_table(
        "nl_synonyms",
        sa.Column("id", sa.Integer(), primary_key=True),
        # The word as the user typed it (normalized by the scorer's tokenizer
        # at read time, so store the surface form).
        sa.Column("word", sa.String(120), nullable=False),
        sa.Column("table_key", sa.String(255), nullable=False),
        # 'admin' (adopted from the log) for now; leaves room for 'curated'.
        sa.Column("source", sa.String(32), nullable=False, server_default="admin"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.UniqueConstraint("word", "table_key", name="uq_nl_synonyms_word_table"),
    )


def downgrade() -> None:
    op.drop_table("nl_synonyms")
    op.drop_index("ix_nl_suggest_log_created_at", table_name="nl_suggest_log")
    op.drop_table("nl_suggest_log")
