"""Put 'רשימת האגודות השיתופיות' on the r2+neon plan + self-firing repoll

Dataset 59360419 (moital/cooperative, single datastore-active CSV resource,
~7.2k rows) archives files to R2 but its rows never reach NEON because the
storage plan lacks ``archive_neon``. All routing preconditions already hold
(exactly one tracked resource, datastore_active=True), so the fix is the flag
plus making the next poll actually run and stream:

  1. merge {"storage_backend":"r2","archive_neon":true} into scraper_config —
     the r2+neon plan (see datasets.apply_storage_target);
  2. NULL last_modified — the documented force-repoll lever: bypasses both the
     unchanged-metadata short-circuit and the version-already-exists skip
     (poll_job "forced_repoll"), since the source metadata hasn't changed
     since today's version 1;
  3. NULL last_polled_at — on this deploy's restart init_scheduler treats the
     dataset as never-polled and fires immediately (its 30-day interval would
     otherwise delay the first NEON stream by a month).

Same pattern as migrations 030/031 (עמותות). Idempotent.

Revision ID: 032
Revises: 031
Create Date: 2026-07-08
"""
from typing import Sequence, Union

from alembic import op


revision: str = "032"
down_revision: Union[str, None] = "031"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DATASET_ID = "59360419-13ac-4a8e-8dce-f6f6b89a3beb"


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config = COALESCE(scraper_config, '{{}}'::jsonb)
                             || '{{"storage_backend": "r2", "archive_neon": true}}'::jsonb,
            last_modified = NULL,
            last_polled_at = NULL
        WHERE id = '{_DATASET_ID}'
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config = scraper_config - 'archive_neon'
        WHERE id = '{_DATASET_ID}'
        """
    )
