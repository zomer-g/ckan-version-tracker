"""At most one active (pending/running) scrape task per dataset

poll_job._create_scrape_task guards against duplicates with a check-then-act
("is there already a pending/running task?" → INSERT), which races under
concurrent polls: two polls both read "none" and both INSERT, producing two
identical scrapes of the same dataset (observed live on GovMap layer 52 —
two 2h+ scrapes running side by side). The in-process single-flight guard on
poll_dataset closes the single-dyno window; this partial unique index is the
DB-level backstop for the cross-process case.

Creating a UNIQUE index fails if duplicates already exist, so we first collapse
any current duplicates: keep the newest active task per dataset, mark the rest
failed. Then create the index.

Revision ID: 023
Revises: 022
Create Date: 2026-07-01
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "023"
down_revision: Union[str, None] = "022"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) Collapse existing duplicates so the unique index can be created. Keep
    #    the most-recently-created active task per dataset (it's furthest along
    #    or the freshest attempt); fail the older siblings.
    op.execute(
        """
        UPDATE scrape_tasks SET
            status = 'failed',
            phase = 'cancelled',
            error = 'auto-deduped by migration 023 (concurrent duplicate scrape)',
            completed_at = now()
        WHERE id IN (
            SELECT id FROM (
                SELECT id, row_number() OVER (
                    PARTITION BY tracked_dataset_id
                    ORDER BY created_at DESC, id DESC
                ) AS rn
                FROM scrape_tasks
                WHERE status IN ('pending', 'running')
            ) ranked
            WHERE ranked.rn > 1
        )
        """
    )

    # 2) DB-level backstop: at most one active task per dataset.
    op.create_index(
        "uq_scrape_tasks_active_per_dataset",
        "scrape_tasks",
        ["tracked_dataset_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'running')"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_scrape_tasks_active_per_dataset",
        table_name="scrape_tasks",
    )
