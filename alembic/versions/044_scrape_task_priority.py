"""Priority band on the scrape queue, so a bulk backfill can't starve routine polls.

``scrape_tasks`` was drained strictly FIFO (``ORDER BY created_at``). That is
fine while the queue only ever holds a handful of routine polls, but it breaks
the moment we enqueue a whole-catalog re-scrape: the GovMap coverage backfill
(~859 layers) would sit at the head of the queue for a day and every routine
poll created behind it would wait its turn.

``priority`` (higher wins, then oldest-first within a band) lets the two kinds
of work share one queue: routine polls keep the default 100, the bulk backfill
is enqueued at 0 and is therefore only ever claimed when nothing routine is
pending. Bands are defined in app/models/scrape_task.py.

Existing rows get 100 (the routine default) — the pre-change behaviour, since
before this migration everything in the queue WAS routine work.

The index matches the claim query in app/api/worker.py exactly
(status filter, then priority DESC, created_at ASC).

Revision ID: 044
Revises: 043
Create Date: 2026-07-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "044"
down_revision: Union[str, None] = "043"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "scrape_tasks",
        sa.Column(
            "priority",
            sa.SmallInteger(),
            nullable=False,
            server_default=sa.text("100"),
        ),
    )
    # Partial index on the only status the claim query scans. Keeps the index
    # tiny (pending is normally a handful of rows) while covering the exact
    # ORDER BY the worker's FOR UPDATE SKIP LOCKED claim uses.
    op.create_index(
        "ix_scrape_tasks_pending_priority",
        "scrape_tasks",
        [sa.text("priority DESC"), sa.text("created_at ASC")],
        postgresql_where=sa.text("status = 'pending'"),
    )


def downgrade() -> None:
    op.drop_index("ix_scrape_tasks_pending_priority", table_name="scrape_tasks")
    op.drop_column("scrape_tasks", "priority")
