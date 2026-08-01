"""Flag datasets whose latest version may not have imported faithfully

A scrape can satisfy every count we check and still produce something a reader
would be misled by. Measured 1.8.2026: קווי גובה 50 ס"מ (layer 228013) published
93,866 features against 93,866 declared by the source — a perfect completeness
score — having turned 93,436 contour LINES into points. Row counts cannot see
that, the archive looked healthy, and the degraded version is what the public
got. A contour layer reduced to points is not a quality dip; it is the layer
erased, silently.

`import_warning` carries a reader-facing reason to distrust the latest version;
`import_warning_at` records when it was raised. NULL means no known problem.

The rule this backfills — and that push-version applies going forward — is an
ENGINE DOWNGRADE: the newest version came from the `quadtree` fallback while an
earlier one came from `spatial-analysis`. That is exactly what happened above,
and it is computable from what we already store. It is deliberately narrow: the
scraper is the only side that can see geometry, so the durable fix is the
geometry gate there, and the worker can also declare a warning outright via
`scrape_metadata.quality_warning`. This gives the site something honest to say
in the meantime rather than presenting a hollowed-out layer as current.

A dataset that has only ever used quadtree is NOT flagged — quadtree produced
correct polygons for most layers, and flagging those would make the warning
noise, which is worse than no warning at all.

Revision ID: 052
Revises: 051
Create Date: 2026-08-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "052"
down_revision: Union[str, None] = "051"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_MSG = (
    "הגרסה האחרונה נסרקה במנוע הנפילה (quadtree) אחרי שגרסאות קודמות נסרקו "
    "במנוע המלא, ולכן ייתכן שהגאומטריה בה מנוונת — למשל קווים או פוליגונים "
    "שנשמרו כנקודות. מספר הרשומות עשוי להיות תקין ובכל זאת הצורות שגויות."
)


def upgrade() -> None:
    op.add_column("tracked_datasets", sa.Column("import_warning", sa.Text(), nullable=True))
    op.add_column("tracked_datasets",
                  sa.Column("import_warning_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index(
        "ix_tracked_datasets_import_warning",
        "tracked_datasets", ["import_warning_at"],
        postgresql_where=sa.text("import_warning_at IS NOT NULL"),
    )
    op.execute(
        f"""
        WITH eng AS (
            SELECT tracked_dataset_id AS ds,
                   version_number,
                   change_summary->'scrape_metadata'->>'engine' AS engine,
                   row_number() OVER (PARTITION BY tracked_dataset_id
                                      ORDER BY version_number DESC) AS rn
            FROM version_index
        ),
        latest AS (SELECT ds, engine FROM eng WHERE rn = 1),
        ever_full AS (
            SELECT DISTINCT ds FROM eng WHERE engine = 'spatial-analysis' AND rn > 1
        )
        UPDATE tracked_datasets t
        SET import_warning = '{_MSG}',
            import_warning_at = now()
        FROM latest l
        JOIN ever_full f ON f.ds = l.ds
        WHERE t.id = l.ds AND l.engine = 'quadtree'
        """
    )


def downgrade() -> None:
    op.drop_index("ix_tracked_datasets_import_warning", table_name="tracked_datasets")
    op.drop_column("tracked_datasets", "import_warning_at")
    op.drop_column("tracked_datasets", "import_warning")
