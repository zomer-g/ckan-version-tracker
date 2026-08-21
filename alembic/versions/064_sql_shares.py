"""Short links for shared /data console queries

The console carried the whole query in the URL (`?q=` base64). A real query
made a kilobyte-long link, and past the 4,000-character encoded cap the share
button produced no link at all — it degraded to copying raw SQL. This table
stores the query server-side so the link's length stops tracking the query's:
`/s/AbC12345` resolves to the saved view.

Rows are permanent (a pasted link must not rot) and deduped by content_hash, so
re-sharing the same view reuses its slug instead of growing the table.

See app/models/sql_share.py and app/services/sql_shares.py.

Revision ID: 064
Revises: 063
Create Date: 2026-08-21
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "064"
down_revision: Union[str, None] = "063"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "sql_shares",
        sa.Column("slug", sa.String(16), primary_key=True),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("sql_text", sa.Text(), nullable=False),
        sa.Column("params", sa.String(2048), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("view_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_viewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Unique, not just indexed: the dedup path relies on the DB to settle a race
    # between two concurrent shares of the same query (ON CONFLICT DO NOTHING).
    op.create_index(
        "ix_sql_shares_content_hash", "sql_shares", ["content_hash"], unique=True
    )


def downgrade() -> None:
    op.drop_index("ix_sql_shares_content_hash", table_name="sql_shares")
    op.drop_table("sql_shares")
