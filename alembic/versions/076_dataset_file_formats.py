"""File formats per dataset, and GIN indexes for the search facets.

Adds ``tracked_datasets.file_formats`` — the format codes (csv, xlsx, geojson…)
of the files the dataset's versions hold — and ``file_formats_at``, when they
were last derived. Filled and kept current by
app/services/dataset_facets.sync_file_formats (a scheduler job: NULL means
"not derived yet", so the first runs backfill every dataset).

Both facet columns get a GIN index (jsonb_path_ops: containment only), which is
the operator the search filters use (``@>``) on the home list, /api/v1/datasets
and the MCP's search_datasets.

Revision ID: 076
Revises: 075
Create Date: 2026-11-02
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "076"
down_revision = "075"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("file_formats", JSONB(), nullable=False, server_default=sa.text("'[]'::jsonb")),
        schema="app",
    )
    op.add_column(
        "tracked_datasets",
        sa.Column("file_formats_at", sa.DateTime(timezone=True), nullable=True),
        schema="app",
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_tracked_datasets_field_flags "
        "ON app.tracked_datasets USING gin (field_flags jsonb_path_ops)"
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_tracked_datasets_file_formats "
        "ON app.tracked_datasets USING gin (file_formats jsonb_path_ops)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS app.ix_tracked_datasets_file_formats")
    op.execute("DROP INDEX IF EXISTS app.ix_tracked_datasets_field_flags")
    op.drop_column("tracked_datasets", "file_formats_at", schema="app")
    op.drop_column("tracked_datasets", "file_formats", schema="app")
