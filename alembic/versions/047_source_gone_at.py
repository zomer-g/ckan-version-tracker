"""Mark datasets whose source has been removed at the publisher

A GovMap sweep can end three ways that all look like "0 features": the catalog
was unreachable, the catalog lists the layer but the sweep saw none, or the
catalog was read successfully and the layer id is simply not in it. The scraper
already separates them — only the third produces "GovMap layer <id> is not in
the catalog and returned 0 features", and the first two produce an explicitly
TRANSIENT message instead. So that one string is a verdict worth recording,
not a guess.

Measured on prod 2026-08-01 while re-running the 124 layers that the new-engine
rollout had failed: 22 of the 79 remaining failures are this — layer ids retired
or renumbered in GovMap's 2026 rebuild. No number of re-scrapes will fix them,
and until now nothing on the site said so: the dataset simply looked like it had
stopped updating.

`source_gone_at` records the FIRST such detection and is cleared whenever a
version lands again (see app/api/worker.py). It is deliberately not a `status`
value — a dataset whose source is gone is the case where the archive matters
most, so it must stay listed, readable and downloadable. Hiding it (as
status='duplicate' does) would bury the only remaining copy.

The backfill takes each dataset's NEWEST terminal task and marks the dataset
only if that task carries the verdict — a layer that failed this way once and
was later scraped successfully is left alone.

Revision ID: 047
Revises: 046
Create Date: 2026-08-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "047"
down_revision: Union[str, None] = "046"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Keep in sync with app/api/worker.py's _SOURCE_GONE_MARKER.
_MARKER = "is not in the catalog and returned 0"


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("source_gone_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_tracked_datasets_source_gone_at",
        "tracked_datasets",
        ["source_gone_at"],
        postgresql_where=sa.text("source_gone_at IS NOT NULL"),
    )

    # Backfill from the newest terminal task per dataset.
    op.execute(
        f"""
        WITH newest AS (
            SELECT DISTINCT ON (tracked_dataset_id)
                   tracked_dataset_id, status, error, completed_at
            FROM scrape_tasks
            WHERE status IN ('completed', 'failed')
            ORDER BY tracked_dataset_id, created_at DESC
        )
        UPDATE tracked_datasets t
        SET source_gone_at = COALESCE(n.completed_at, now())
        FROM newest n
        WHERE t.id = n.tracked_dataset_id
          AND n.status = 'failed'
          AND n.error LIKE '%{_MARKER}%'
        """
    )


def downgrade() -> None:
    op.drop_index("ix_tracked_datasets_source_gone_at", table_name="tracked_datasets")
    op.drop_column("tracked_datasets", "source_gone_at")
