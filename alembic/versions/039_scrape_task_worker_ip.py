"""Record which worker machine runs each scrape task.

The admin scrape queue couldn't show which machine holds a task when several
workers run in parallel — the assigning IP was only ever stuffed into the
mutable ``message`` field, which the first progress report immediately
overwrote. Add a dedicated ``worker_ip`` column that the worker API sets on
assignment and refreshes on every progress report.

Nullable — pending tasks (not yet claimed) and any historical rows carry NULL.

Revision ID: 039
Revises: 038
Create Date: 2026-07-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "039"
down_revision: Union[str, None] = "038"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "scrape_tasks",
        sa.Column("worker_ip", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scrape_tasks", "worker_ip")
