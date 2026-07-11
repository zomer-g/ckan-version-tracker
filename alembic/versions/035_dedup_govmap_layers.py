"""Merge duplicate govmap datasets — one dataset per GovMap layer id

The coverage rollout's _ensure_dataset reused an existing dataset only on an
EXACT source_url match ("?lay=N"), but manually-added govmap datasets carry
extra URL params ("?c=...&z=...&lay=N") — so the rollout created a second
(sometimes third) dataset for layers that were already tracked. ~20 layers
ended up duplicated, and the public search showed the same layer 2-3 times
(e.g. תחנות הידרומטריות ×3, all layer 227195).

Fix here (the code fix — adopt-by-layer-id — ships in the same deploy):
  * For every govmap layer id tracked by >1 dataset, keep the one with the
    most versions (tie → oldest) and demote the rest to status='duplicate'
    (hidden from the public list, which filters status IN (active,pending)),
    is_active=false, with a scraper_config.duplicate_of pointer back to the
    keeper. Their versions stay reachable — nothing is deleted.
  * Point govmap_coverage.tracked_dataset_id at the keeper and mark it
    coverage_managed (poll_interval=90d) so the rollout drives its refreshes.
  * Cancel the losers' PENDING scrape tasks (running ones finish harmlessly
    on the hidden dataset).

Revision ID: 035
Revises: 034
Create Date: 2026-07-11
"""
from typing import Sequence, Union

from alembic import op


revision: str = "035"
down_revision: Union[str, None] = "034"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TEMP TABLE _gov_ranked AS
        SELECT id, lid, nv, created_at,
               row_number() OVER (PARTITION BY lid ORDER BY nv DESC, created_at ASC) AS rn,
               count(*)     OVER (PARTITION BY lid) AS cnt
        FROM (
            SELECT t.id, t.created_at,
                   COALESCE(t.scraper_config->>'layer_id',
                            substring(t.source_url from '[?&]lay=([0-9]+)')) AS lid,
                   (SELECT count(*) FROM version_index v
                     WHERE v.tracked_dataset_id = t.id) AS nv
            FROM tracked_datasets t
            WHERE t.source_type = 'govmap'
              AND t.status IN ('active', 'pending')
        ) g
        WHERE g.lid IS NOT NULL
        """
    )

    # Demote the losers (every duplicate beyond the keeper).
    op.execute(
        """
        UPDATE tracked_datasets t
        SET status = 'duplicate',
            is_active = false,
            scraper_config = coalesce(t.scraper_config, '{}'::jsonb)
                || jsonb_build_object(
                       'duplicate_of',
                       (SELECT k.id::text FROM _gov_ranked k
                         WHERE k.lid = r.lid AND k.rn = 1))
        FROM _gov_ranked r
        WHERE t.id = r.id AND r.rn > 1
        """
    )

    # Cancel losers' queued (not yet claimed) scrape tasks.
    op.execute(
        """
        UPDATE scrape_tasks st
        SET status = 'failed',
            error = 'בוטל: המאגר אוחד לתוך מאגר קיים לאותה שכבה (כפילות)',
            completed_at = now()
        FROM _gov_ranked r
        WHERE r.rn > 1 AND st.tracked_dataset_id = r.id AND st.status = 'pending'
        """
    )

    # Re-point the coverage inventory at the keepers of merged groups.
    op.execute(
        """
        UPDATE govmap_coverage gc
        SET tracked_dataset_id = r.id
        FROM _gov_ranked r
        WHERE r.rn = 1 AND r.cnt > 1 AND gc.layer_id = r.lid
        """
    )

    # Keepers that inherited a coverage link become coverage-managed: the
    # rollout (not the per-dataset scheduler) drives their refreshes.
    op.execute(
        """
        UPDATE tracked_datasets t
        SET scraper_config = coalesce(t.scraper_config, '{}'::jsonb)
                || '{"coverage_managed": true}'::jsonb,
            poll_interval = 7776000
        FROM _gov_ranked r
        WHERE r.rn = 1 AND r.cnt > 1 AND t.id = r.id
          AND EXISTS (SELECT 1 FROM govmap_coverage gc WHERE gc.layer_id = r.lid)
        """
    )

    op.execute("DROP TABLE _gov_ranked")


def downgrade() -> None:
    # Reactivating the demoted duplicates would recreate the confusion this
    # migration removes; restore manually from scraper_config.duplicate_of
    # pointers if ever needed.
    pass
