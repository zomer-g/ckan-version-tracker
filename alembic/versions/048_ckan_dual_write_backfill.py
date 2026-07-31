"""Give the file-only data.gov.il datasets their NEON half

A CKAN dataset is tracked because its ROWS matter — that's what /data queries.
But approval derived the storage plan from the global file backend, so every
data.gov.il dataset approved without an explicit choice was pinned to "r2":
files archived, nothing in the append DB, no SQL console, and no sign in the
UI that half the plan was missing. The default is fixed in the same deploy
(app/api/admin.py approve_request + the admin create path).

This migration repairs the batch that made the gap visible: the 17 per-file
datasets of "תוצאות בחירות - ועדת הבחירות המרכזית לכנסת" (ckan_id
26f9fa06-fcd7-4173-8df5-65797b63e857), created 2026-08-01 by the first
split-resources request. Each pins exactly one resource, which is the shape
the dual write wants.

Scoped to that package on purpose. 62 active CKAN datasets currently sit at
"r2"; flipping them all here would queue a full datastore stream for each on
its next poll, and that bill is the user's call, not a migration's.

Sets scraper_config.archive_neon = true and pins storage_backend = 'r2'
(explicit, so a later change to the global default can't silently move these).
The next poll routes each datastore-active resource through the streaming
archiver, which writes the NEON table AND keeps the R2 file snapshot. Idempotent:
only rows that don't already archive to NEON are touched.

Revision ID: 048
Revises: 047
Create Date: 2026-08-01
"""
from typing import Sequence, Union

from alembic import op


revision: str = "048"
down_revision: Union[str, None] = "047"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_CKAN_ID = "26f9fa06-fcd7-4173-8df5-65797b63e857"


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config =
                COALESCE(scraper_config, '{{}}'::jsonb)
                || '{{"archive_neon": true, "storage_backend": "r2"}}'::jsonb,
            updated_at = NOW()
        WHERE ckan_id = '{_CKAN_ID}'
          AND source_type = 'ckan'
          AND status = 'active'
          AND COALESCE(scraper_config->>'archive_neon', 'false') <> 'true'
          AND COALESCE(scraper_config->>'storage_backend', 'r2') = 'r2'
        """
    )


def downgrade() -> None:
    # Back to file-only. The NEON tables a poll may have written are left
    # alone — dropping archived rows is not a schema concern.
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config = scraper_config - 'archive_neon',
            updated_at = NOW()
        WHERE ckan_id = '{_CKAN_ID}'
          AND source_type = 'ckan'
        """
    )
