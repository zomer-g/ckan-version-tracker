"""Give the REST of the file-only data.gov.il datasets their NEON half

048 fixed one package and said why it stopped there: "62 active CKAN datasets
currently sit at 'r2'; flipping them all here would queue a full datastore
stream for each on its next poll, and that bill is the user's call, not a
migration's." This is that call, made explicitly. 42 remain.

What the bill actually is, measured on prod 2026-08-05 before writing this: all
42 carry status='active', but only 15 have is_active=true — the other 27 are
paused and will not poll, so flipping them costs nothing today and simply means
their plan is already right whenever they resume. Of the 15 that do poll, most
are monthly or quarterly single-version datasets; the weekly ones are מספרי
רישוי של כלי רכב פרטיים, מאגר כוח אדם ברשויות הרווחה and מאגר תלונות חופש המידע.

This flips the PLAN, which is forward-looking: the next poll of each dataset
routes its datastore-active resources through the streaming archiver, writing
the NEON table while keeping the R2 file snapshot. It does NOT backfill the
164 versions already sitting in R2 — replaying snapshots needs R2 reads and can
run for minutes per dataset, which is an operation, not a schema change. Run
POST /api/admin/datasets/{id}/seed-neon (apply=false first for the plan) on any
dataset whose history matters; it replays with the historical first_seen.

Same predicate shape as 048, minus the single-package scope and plus an
upload_mode guard: a 'local_only' dataset derives to "local", not "r2", and
048's COALESCE(storage_backend,'r2') would have swept one in. Idempotent —
rows that already archive to NEON are not touched.

Revision ID: 059
Revises: 058
Create Date: 2026-08-05
"""
from typing import Sequence, Union

from alembic import op


revision: str = "059"
down_revision: Union[str, None] = "058"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# The elections package 048 already converted. Only downgrade cares: those rows
# satisfy this migration's predicate as well, and undoing 059 must not undo 048.
_MIGRATION_048_CKAN_ID = "26f9fa06-fcd7-4173-8df5-65797b63e857"

# Mirrors app.api.datasets.storage_target_of deriving exactly "r2": no
# local_only override, no NEON half already, and a file destination that is R2
# either explicitly or by the global default (settings.storage_backend = "r2").
_DERIVES_TO_R2 = """
          AND COALESCE(scraper_config->>'upload_mode', '') <> 'local_only'
          AND COALESCE(scraper_config->>'archive_neon', 'false') <> 'true'
          AND COALESCE(scraper_config->>'storage_backend', 'r2') = 'r2'
"""


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config =
                COALESCE(scraper_config, '{{}}'::jsonb)
                || '{{"archive_neon": true, "storage_backend": "r2"}}'::jsonb,
            updated_at = NOW()
        WHERE source_type = 'ckan'
          AND status = 'active'
          {_DERIVES_TO_R2}
        """
    )


def downgrade() -> None:
    # Back to file-only for the datasets this migration converted. Narrower than
    # upgrade's inverse in two ways, both deliberate: it drops archive_neon only
    # where the plan is exactly the r2+neon this wrote (a dataset an admin has
    # since moved to odata+neon or neon keeps the plan they chose), and it skips
    # 048's package — those rows match this predicate too, and downgrading 059
    # must not silently undo its predecessor. Rows already written to NEON are
    # left alone; dropping archived data is not a schema concern.
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET scraper_config = scraper_config - 'archive_neon',
            updated_at = NOW()
        WHERE source_type = 'ckan'
          AND status = 'active'
          AND ckan_id <> '{_MIGRATION_048_CKAN_ID}'
          AND scraper_config->>'archive_neon' = 'true'
          AND scraper_config->>'storage_backend' = 'r2'
        """
    )
