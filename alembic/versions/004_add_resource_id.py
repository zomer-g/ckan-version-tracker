"""Add resource_id column to tracked_datasets and update unique constraint

Revision ID: 004
Revises: 003
Create Date: 2026-04-11
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("tracked_datasets", sa.Column("resource_id", sa.String(255), nullable=True))
    # Remove old unique constraint on ckan_id (set in 001_initial)
    op.drop_constraint("tracked_datasets_ckan_id_key", "tracked_datasets", type_="unique")
    # Add composite unique on (ckan_id, resource_id) to prevent duplicates
    op.create_unique_constraint("uq_tracked_ckan_resource", "tracked_datasets", ["ckan_id", "resource_id"])


def downgrade() -> None:
    op.drop_constraint("uq_tracked_ckan_resource", "tracked_datasets", type_="unique")
    op.drop_column("tracked_datasets", "resource_id")
    op.create_unique_constraint("tracked_datasets_ckan_id_key", "tracked_datasets", ["ckan_id"])
