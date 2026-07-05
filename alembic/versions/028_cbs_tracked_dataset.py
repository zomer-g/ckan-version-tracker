"""Register the CBS content index as a first-class tracked dataset.

Inserts a synthetic ``tracked_datasets`` row (source_type ``cbs``,
``append_only``, storage plan ``neon``) so the CBS index shows up as a card in
the tracked-datasets collection, and a single ``version_index`` row whose
``resource_mappings.append_table`` points at the NEON append table
(``append_cbs_index_cb500000`` — the name app/services/append_store.table_name
derives for this dataset). The NEON table itself lives in the SEPARATE append
DB and is created/populated by app/services/cbs_neon.py (ingest dual-write +
POST /api/cbs/sync-neon) — not here, since alembic only migrates the main DB.

``is_active=false`` so the poll scheduler never tries to poll it (CBS is fed by
the worker's /api/cbs/ingest, not by OVER's poller); ``status='active'`` so the
public listing + /api/v1 still surface it. See app/services/cbs_neon.py.

Revision ID: 028
Revises: 027
Create Date: 2026-07-05
"""
from typing import Sequence, Union

from alembic import op


revision: str = "028"
down_revision: Union[str, None] = "027"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DATASET_ID = "cb500000-cb50-4b50-8b50-cb50cb50cb50"
_VERSION_ID = "cb500001-cb50-4b50-8b50-cb50cb50cb50"
_APPEND_TABLE = "append_cbs_index_cb500000"


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO tracked_datasets
          (id, ckan_id, ckan_name, title, organization, source_type, source_url,
           scraper_config, storage_mode, poll_interval, is_active, status,
           created_at, updated_at)
        SELECT
          '%(id)s', 'cbs-index', 'cbs_index',
          'הלשכה המרכזית לסטטיסטיקה (למ"ס) — אינדקס תוכן',
          'הלשכה המרכזית לסטטיסטיקה', 'cbs', 'https://www.cbs.gov.il',
          '{"storage_backend": "neon", "append_key": "url"}'::jsonb,
          'append_only', 86400, false, 'active', now(), now()
        WHERE NOT EXISTS (
          SELECT 1 FROM tracked_datasets WHERE ckan_id = 'cbs-index'
        )
        """ % {"id": _DATASET_ID}
    )
    op.execute(
        """
        INSERT INTO version_index
          (id, tracked_dataset_id, version_number, metadata_modified, detected_at,
           change_summary, resource_mappings, source)
        SELECT
          '%(vid)s', '%(id)s', 1,
          to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS'), now(),
          '{"type": "append_db", "key": "url", "note": "CBS content index mirrored to NEON"}'::jsonb,
          '{"append_table": "%(table)s"}'::jsonb,
          'append_db'
        WHERE NOT EXISTS (
          SELECT 1 FROM version_index WHERE tracked_dataset_id = '%(id)s'
        )
        """ % {"vid": _VERSION_ID, "id": _DATASET_ID, "table": _APPEND_TABLE}
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM version_index WHERE tracked_dataset_id = '%s'" % _DATASET_ID
    )
    op.execute("DELETE FROM tracked_datasets WHERE ckan_id = 'cbs-index'")
