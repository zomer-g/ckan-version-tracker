"""Add Drive export: users.google_refresh_token + drive_export_jobs queue

Backs the admin "export a version's files to Google Drive" feature.
users.google_refresh_token holds the offline token captured when an
admin connects Drive; drive_export_jobs is the durable queue an
APScheduler tick drains (mirrors datastore_push_jobs).

See app/models/drive_export_job.py and
app/worker/drive_export_runner.py.

Revision ID: 019
Revises: 018
Create Date: 2026-06-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "019"
down_revision: Union[str, None] = "018"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("google_refresh_token", sa.String(512), nullable=True),
    )

    op.create_table(
        "drive_export_jobs",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column(
            "version_id",
            sa.UUID(),
            sa.ForeignKey("version_index.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "tracked_dataset_id",
            sa.UUID(),
            sa.ForeignKey("tracked_datasets.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "user_id",
            sa.UUID(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("folder_id", sa.String(128), nullable=False),
        sa.Column("folder_label", sa.String(512), nullable=True),
        sa.Column(
            "status", sa.String(20), nullable=False, server_default="pending"
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "total_files", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "completed_files", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("current_file", sa.String(512), nullable=True),
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
    op.create_index(
        "ix_drive_export_jobs_status", "drive_export_jobs", ["status"]
    )


def downgrade() -> None:
    op.drop_index("ix_drive_export_jobs_status", table_name="drive_export_jobs")
    op.drop_table("drive_export_jobs")
    op.drop_column("users", "google_refresh_token")
