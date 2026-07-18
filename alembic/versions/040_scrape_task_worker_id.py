"""Explicit worker identity, so co-located workers are distinguishable.

Migration 039 added ``worker_ip``, but the IP alone can't tell two workers
apart when they sit behind the same NAT / public IP (several machines in one
office, or several processes on one box). The worker now sends an explicit
``X-Worker-Id`` header (``hostname#short`` or the OVER_WORKER_ID override);
store it here. Preferred over the IP for display; IP stays as the fallback for
older workers that don't send the header.

Nullable — pending tasks and rows from before this change carry NULL.

Revision ID: 040
Revises: 039
Create Date: 2026-07-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "040"
down_revision: Union[str, None] = "039"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "scrape_tasks",
        sa.Column("worker_id", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scrape_tasks", "worker_id")
