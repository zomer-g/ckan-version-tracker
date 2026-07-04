"""GovMap coverage inventory (throttled full-coverage rollout)

One row per GovMap vector layer (859 as of 2026-07). A twice-daily scheduler
job walks this table (never-triggered first, then stalest) and scrapes the next
layer when the worker is idle, so coverage of the whole catalog builds up
without overloading GovMap or the single worker. See
app/models/govmap_coverage.py + app/services/govmap_coverage.py.

Revision ID: 025
Revises: 024
Create Date: 2026-07-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "025"
down_revision: Union[str, None] = "024"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "govmap_coverage",
        sa.Column("layer_id", sa.String(32), primary_key=True),
        sa.Column("caption", sa.String(500), nullable=True),
        sa.Column("layer_kind", sa.Integer(), nullable=True),
        sa.Column("complexity", sa.Integer(), nullable=True),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "tracked_dataset_id",
            sa.UUID(),
            sa.ForeignKey("tracked_datasets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("last_triggered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    # The scheduler's round-robin pick orders by (last_triggered_at NULLS FIRST,
    # sort_order) — index it so the pick stays cheap as the table fills.
    op.create_index(
        "ix_govmap_coverage_pick",
        "govmap_coverage",
        ["last_triggered_at", "sort_order"],
    )


def downgrade() -> None:
    op.drop_index("ix_govmap_coverage_pick", table_name="govmap_coverage")
    op.drop_table("govmap_coverage")
