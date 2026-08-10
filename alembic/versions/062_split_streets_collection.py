"""Give the streets CSV its own dataset instead of merging it into the XML one

Dataset e3a63f81 tracks one file of the data.gov.il package
``israel-streets-synom``: the 47MB XML (98d231af). The package publishes 22
files, and the live CSV (bf185c7f — datastore-active, 9.3MB, refreshed monthly)
was added to that same dataset's ``resource_ids`` — the only route the admin UI
offered, and the wrong shape:

  * one dataset, one poll cadence, one versions page for two files that the
    publisher refreshes on different days;
  * the NEON/SQL path wants a SINGLE tracked resource per dataset — with two
    (one of which has no datastore at all) the CSV's rows do not become a
    queryable table, which is the whole reason to track a CSV;
  * "רשימת רחובות" then reads as one archive when it is two.

So: pin e3a63f81 back to the XML alone, and open a separate dataset for the
CSV — active, r2+neon, monthly — which is what the request path now produces
for every file picked out of a collection.

The new row is created directly rather than left pending: this file was already
chosen deliberately, and it is a re-shaping of an existing decision, not a new
request to review.

One-off data fix. Idempotent (the insert is guarded on the resource already
having a dataset), and safe to run before or after the next poll of either row.

Revision ID: 062
Revises: 061
Create Date: 2026-08-10
"""
from typing import Sequence, Union

from alembic import op


revision: str = "062"
down_revision: Union[str, None] = "061"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_XML_DATASET = "e3a63f81-b8fb-45c6-8366-57fb2bd99088"
_XML_RESOURCE = "98d231af-67c7-4554-b54d-69b933c52b0d"
_CSV_DATASET = "bf185c7f-0000-4000-8000-57fb2bd99088"  # fixed, so this is idempotent
_CSV_RESOURCE = "bf185c7f-1a4e-4662-88c5-fa118a244bda"
_CKAN_ID = "785ad9fb-6da6-426d-b5ea-b8e36febbc8a"
_CKAN_NAME = "israel-streets-synom"
_CSV_TITLE = "רשימת רחובות בישראל - קובץ עם סינונימיים — קובץ CSV מתעדכן"
# r2+neon, in the keys the code actually reads (see apply_storage_target):
# storage_backend pins the file destination, archive_neon is the tabular half.
# The CSV is datastore-active, so this is the dataset that becomes a queryable
# table on /data — the reason to track a CSV at all.
_CSV_CONFIG = '{"storage_backend": "r2", "archive_neon": true}'
# Monthly: the publisher refreshes this file on the 2nd of each month. The XML
# row keeps its own (quarterly) cadence — having each file on the rhythm its
# publisher actually uses is the point of splitting them.
_CSV_INTERVAL = 2592000


def upgrade() -> None:
    # The XML dataset goes back to the one file it was opened for. last_modified
    # is nulled so the next poll rebuilds the snapshot against the narrowed set
    # instead of skipping on unchanged metadata (see poll_job.forced_repoll).
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET resource_ids = '["{_XML_RESOURCE}"]'::jsonb,
            resource_id = '{_XML_RESOURCE}',
            last_modified = NULL,
            last_error = NULL
        WHERE id = '{_XML_DATASET}'
        """
    )

    # ...and the CSV becomes its own dataset. Guarded: if the file already has
    # one (e.g. it was requested through the collection picker before this
    # deploy), leave that row alone rather than creating a second.
    op.execute(
        f"""
        INSERT INTO tracked_datasets (
            id, ckan_id, ckan_name, resource_id, resource_ids, title,
            organization, organization_id, poll_interval, is_active, status,
            source_type, source_url, scraper_config, storage_mode,
            created_at, updated_at
        )
        SELECT
            '{_CSV_DATASET}',
            '{_CKAN_ID}',
            '{_CKAN_NAME}',
            '{_CSV_RESOURCE}',
            '["{_CSV_RESOURCE}"]'::jsonb,
            '{_CSV_TITLE}',
            src.organization,
            src.organization_id,
            {_CSV_INTERVAL},
            TRUE,
            'active',
            'ckan',
            'https://data.gov.il/he/datasets/population_authority/{_CKAN_NAME}/{_CSV_RESOURCE}',
            '{_CSV_CONFIG}'::jsonb,
            'full_snapshot',
            NOW(),
            NOW()
        FROM tracked_datasets src
        WHERE src.id = '{_XML_DATASET}'
          AND NOT EXISTS (
              SELECT 1 FROM tracked_datasets d
              WHERE d.ckan_id = '{_CKAN_ID}'
                AND d.status IN ('active', 'pending')
                AND d.resource_ids @> '["{_CSV_RESOURCE}"]'::jsonb
          )
        """
    )


def downgrade() -> None:
    op.execute(f"DELETE FROM tracked_datasets WHERE id = '{_CSV_DATASET}'")
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET resource_ids = '["{_XML_RESOURCE}", "{_CSV_RESOURCE}"]'::jsonb
        WHERE id = '{_XML_DATASET}'
        """
    )
