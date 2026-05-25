"""Durable queue for datastore-ingest pushes

FastAPI's BackgroundTasks were dying on Render dyno recycles mid-push,
leaving large ODATA resources with datastore_active=false and broken
Download buttons. This table is the queue that replaces them: each
upload-csv call writes one row, and an APScheduler periodic task
drains the table by running the push (with /tmp recovery from the
already-uploaded .csv.gz file if needed).

See app/models/datastore_push_job.py for the full schema rationale,
and app/worker/datastore_push_runner.py for the worker loop.

Revision ID: 018
Revises: 017
Create Date: 2026-05-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "018"
down_revision: Union[str, None] = "017"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datastore_push_jobs",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "tracked_dataset_id",
            sa.UUID(),
            sa.ForeignKey("tracked_datasets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("resource_id", sa.String(64), nullable=False),
        sa.Column("csv_path", sa.String(512), nullable=False),
        sa.Column(
            "csv_is_gzipped_in_source",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("fields_json", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default="pending",
        ),
        sa.Column(
            "attempts", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "rows_pushed", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("total_rows", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    # Index for the runner's "next pending job" query and for the admin
    # UI's "what's currently running / failed" filter.
    op.create_index(
        "ix_datastore_push_jobs_status",
        "datastore_push_jobs",
        ["status"],
    )
    op.create_index(
        "ix_datastore_push_jobs_resource_id",
        "datastore_push_jobs",
        ["resource_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_datastore_push_jobs_resource_id", table_name="datastore_push_jobs"
    )
    op.drop_index(
        "ix_datastore_push_jobs_status", table_name="datastore_push_jobs"
    )
    op.drop_table("datastore_push_jobs")
