"""Append-only activity log for the dataset/scrape lifecycle

One immutable row per event (requested / approved / rejected / queued /
started / completed / failed) so the admin can read the full history of a
tracked dataset including the error message on failed steps. See
app/models/activity_log.py.

Revision ID: 021
Revises: 020
Create Date: 2026-06-28
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "021"
down_revision: Union[str, None] = "020"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "activity_log",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "tracked_dataset_id",
            sa.UUID(),
            sa.ForeignKey("tracked_datasets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("dataset_title", sa.String(1000), nullable=True),
        sa.Column("source_type", sa.String(20), nullable=True),
        sa.Column("event", sa.String(40), nullable=False),
        sa.Column("status", sa.String(20), nullable=False, server_default="info"),
        sa.Column("message", sa.String(500), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("actor", sa.String(255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index(
        "ix_activity_log_tracked_dataset_id", "activity_log", ["tracked_dataset_id"]
    )
    op.create_index("ix_activity_log_created_at", "activity_log", ["created_at"])
    op.create_index(
        "ix_activity_log_ds_created", "activity_log", ["tracked_dataset_id", "created_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_activity_log_ds_created", table_name="activity_log")
    op.drop_index("ix_activity_log_created_at", table_name="activity_log")
    op.drop_index("ix_activity_log_tracked_dataset_id", table_name="activity_log")
    op.drop_table("activity_log")
