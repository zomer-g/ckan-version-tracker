"""Merge the GovMap duplicates created after migration 035

035 merged ~20 layers that were tracked twice and shipped the adopt-by-layer-id
fix alongside it — but only the coverage rollout and the bulk-add path learned to
match on identity. The admin single-add branch kept comparing `source_url` to the
pasted string, so a link carrying the viewport and several layers
("?c=183604.69,572861.25&lay=228014,228013,228012&z=6") did not look like the
stored "?lay=228014" and a second dataset for that layer was created anyway.

Measured on prod 2026-07-31: of 897 govmap datasets, 28 layer ids are tracked by
more than one dataset. 22 of those groups are already resolved — 035 demoted the
extras. Six are not, and both copies are active:

    228012 קווי גובה 25 סמ      228013 קווי גובה 50 סמ
    228014 קווי גובה 1 מטר       400    מתקני ספורט
    405    עמדות טעינה רב קו     234343 שטחי אש

Each is scraped twice on its own schedule — for the contour layers that is
~94,000 features per redundant pass — and the public list shows the same layer
twice with its history split between the two.

Same treatment as 035, and deliberately the same query so it stays idempotent:
partition the ACTIVE/PENDING govmap datasets by layer id, keep the one with the
most versions (tie → oldest, i.e. the one that has been tracked longest), demote
the rest to status='duplicate' with a `duplicate_of` pointer. Nothing is deleted —
the demoted datasets' versions stay reachable by id, which matters here because
some of them hold the only copy of a scrape (see the code fix in
app/api/datasets.py shipping in the same deploy).

Cancelling the losers' PENDING scrape tasks is not housekeeping in this round:
GovMap is being re-swept for the offset-paging repair, and a queued task on a
hidden dataset spends that budget on a layer nobody will read.

Revision ID: 046
Revises: 045
Create Date: 2026-07-31
"""
from typing import Sequence, Union

from alembic import op


revision: str = "046"
down_revision: Union[str, None] = "045"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TEMP TABLE _gov_ranked2 AS
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

    op.execute(
        """
        UPDATE tracked_datasets t
        SET status = 'duplicate',
            is_active = false,
            scraper_config = coalesce(t.scraper_config, '{}'::jsonb)
                || jsonb_build_object(
                       'duplicate_of',
                       (SELECT k.id::text FROM _gov_ranked2 k
                         WHERE k.lid = r.lid AND k.rn = 1))
        FROM _gov_ranked2 r
        WHERE t.id = r.id AND r.rn > 1
        """
    )

    op.execute(
        """
        UPDATE scrape_tasks st
        SET status = 'failed',
            error = 'בוטל: המאגר אוחד לתוך מאגר קיים לאותה שכבה (כפילות)',
            completed_at = now()
        FROM _gov_ranked2 r
        WHERE r.rn > 1 AND st.tracked_dataset_id = r.id AND st.status = 'pending'
        """
    )

    # Keep the coverage inventory pointing at the surviving dataset, so the
    # rollout re-scrapes the layer that is actually published.
    op.execute(
        """
        UPDATE govmap_coverage gc
        SET tracked_dataset_id = r.id
        FROM _gov_ranked2 r
        WHERE r.rn = 1 AND r.cnt > 1 AND gc.layer_id = r.lid
        """
    )

    op.execute("DROP TABLE _gov_ranked2")


def downgrade() -> None:
    # Same as 035: reactivating the demoted copies restores the duplication
    # this removes. The `duplicate_of` pointers are on record if a specific
    # merge ever has to be undone by hand.
    pass
