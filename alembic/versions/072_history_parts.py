"""History parts: the catalog of closed append rows exported to Parquet on R2.

Revision ID: 072
Revises: 071
Create Date: 2026-10-16

Phase 2 of docs/history-storage-strategy.md (plan: docs/history-phase2-plan.md).
One row per immutable Parquet file under ``history/<dsid8>/<table>/closed_month=
YYYY-MM/part-NNNN.parquet``, written by app/services/history_export.py only after
the file was read back from R2 and its content hash matched the SQL side.

In schema `app` BY NAME, like every app table since 066: the public SQL
console's role reads `public`, and this is bookkeeping, not published data.
The FK is RESTRICT on purpose: a part with deleted_from_sql=true is the only
record of rows that now live nowhere but R2.
"""
from alembic import op

revision = "072"
down_revision = "071"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.history_parts (
            id               uuid PRIMARY KEY,
            dataset_id       uuid NOT NULL
                             REFERENCES app.tracked_datasets (id) ON DELETE RESTRICT,
            table_name       text NOT NULL,
            closed_month     date NOT NULL,
            part             integer NOT NULL,
            r2_key           text NOT NULL UNIQUE,
            rows             bigint NOT NULL,
            bytes            bigint NOT NULL,
            content_hash     text NOT NULL,
            first_seen_min   timestamptz,
            first_seen_max   timestamptz,
            last_seen_min    timestamptz,
            last_seen_max    timestamptz,
            deleted_from_sql boolean NOT NULL DEFAULT false,
            deleted_at       timestamptz,
            created_at       timestamptz NOT NULL DEFAULT now(),
            UNIQUE (table_name, closed_month, part)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_history_parts_dataset "
               "ON app.history_parts (dataset_id)")
    op.execute("REVOKE ALL ON TABLE app.history_parts FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.history_parts")
