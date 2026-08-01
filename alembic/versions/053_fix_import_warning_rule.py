"""Withdraw the engine-downgrade warning; flag the one layer we measured

052 inferred a faulty import from an engine downgrade — the latest version came
from the `quadtree` fallback after an earlier one used `spatial-analysis`.
Checked against the data on prod the same day, that rule was wrong in both
directions:

  * It flagged גני ילדים, whose v5 (spatial-analysis) and v6 (quadtree) hold the
    identical 20,465 POINT features. A kindergarten layer IS points. Nothing
    degraded; only the engine changed.
  * It did NOT flag קווי גובה 50 ס"מ — the case the feature was built for —
    because that layer has only ever run on quadtree, so there was no downgrade
    to detect, even though its 93,436 contour LINES had just become 93,866
    points at a row count matching the source exactly.

A warning that fires on healthy data and stays silent on erased data is worse
than no warning, so the inference is withdrawn (app/api/worker.py loses it in
the same deploy). What remains is the channel that can actually be right: the
worker declares `scrape_metadata.quality_warning`, because the scraper is the
only side that parses geometry. Until its geometry gate ships, warnings come
from measurement, not from guessing.

This migration therefore clears every warning 052 wrote, then sets ONE — on
layer 228013 — from a measured fact rather than an inference.

Revision ID: 053
Revises: 052
Create Date: 2026-08-01
"""
from typing import Sequence, Union

from alembic import op


revision: str = "053"
down_revision: Union[str, None] = "052"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_MEASURED = (
    "נמדד ב-1.8.2026: הגרסה האחרונה מכילה 93,866 רשומות — בדיוק כמספר שהמקור "
    "מצהיר עליו — אך כולן נשמרו כנקודות, במקום 93,436 קווי מתאר שהיו בגרסה "
    "הקודמת. הספירה מלאה והצורות שגויות. מומלץ להסתמך על הגרסה הקודמת עד "
    "לסריקה מתוקנת."
)


def upgrade() -> None:
    op.execute(
        "UPDATE tracked_datasets SET import_warning = NULL, import_warning_at = NULL "
        "WHERE import_warning IS NOT NULL"
    )
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET import_warning = '{_MEASURED}',
            import_warning_at = now()
        WHERE source_type = 'govmap'
          AND ckan_name LIKE 'govmap-228013-%'
          AND status = 'active'
        """
    )


def downgrade() -> None:
    op.execute(
        "UPDATE tracked_datasets SET import_warning = NULL, import_warning_at = NULL "
        "WHERE import_warning IS NOT NULL"
    )
