"""Initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(255), unique=True, nullable=False, index=True),
        sa.Column("hashed_password", sa.String(255), nullable=False),
        sa.Column("display_name", sa.String(255), nullable=False),
        sa.Column("is_active", sa.Boolean(), default=True),
        sa.Column("is_admin", sa.Boolean(), default=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "tracked_datasets",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("ckan_id", sa.String(255), unique=True, nullable=False),
        sa.Column("ckan_name", sa.String(255), nullable=False),
        sa.Column("title", sa.String(1000), nullable=False),
        sa.Column("organization", sa.String(255)),
        sa.Column("odata_dataset_id", sa.String(255)),
        sa.Column("poll_interval", sa.Integer(), default=3600),
        sa.Column("is_active", sa.Boolean(), default=True),
        sa.Column("last_polled_at", sa.DateTime(timezone=True)),
        sa.Column("last_modified", sa.String(50)),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "version_index",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "tracked_dataset_id",
            UUID(as_uuid=True),
            sa.ForeignKey("tracked_datasets.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("metadata_modified", sa.String(50), nullable=False),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("odata_metadata_resource_id", sa.String(255)),
        sa.Column("change_summary", JSONB),
        sa.Column("resource_mappings", JSONB),
        sa.UniqueConstraint("tracked_dataset_id", "version_number"),
    )
    op.create_index(
        "idx_versions_dataset",
        "version_index",
        ["tracked_dataset_id", sa.text("version_number DESC")],
    )


def downgrade() -> None:
    op.drop_table("version_index")
    op.drop_table("tracked_datasets")
    op.drop_table("users")
