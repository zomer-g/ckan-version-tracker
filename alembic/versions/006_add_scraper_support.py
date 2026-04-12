"""Add scraper support: source_type, source_url, scraper_config on tracked_datasets + scrape_tasks table

Revision ID: 006
Revises: 005
Create Date: 2026-04-12
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new columns to tracked_datasets
    op.add_column("tracked_datasets", sa.Column("source_type", sa.String(20), server_default="ckan", nullable=False))
    op.add_column("tracked_datasets", sa.Column("source_url", sa.String(1000), nullable=True))
    op.add_column("tracked_datasets", sa.Column("scraper_config", JSONB, nullable=True))

    # Create scrape_tasks table
    op.create_table(
        "scrape_tasks",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("tracked_dataset_id", sa.UUID(), sa.ForeignKey("tracked_datasets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", sa.String(20), server_default="pending", nullable=False),
        sa.Column("phase", sa.String(50), nullable=True),
        sa.Column("progress", sa.Integer(), server_default="0", nullable=False),
        sa.Column("message", sa.String(500), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("scrape_tasks")
    op.drop_column("tracked_datasets", "scraper_config")
    op.drop_column("tracked_datasets", "source_url")
    op.drop_column("tracked_datasets", "source_type")
