"""Add status column to tracked_datasets

Revision ID: 003
Revises: 002
Create Date: 2026-04-11
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("status", sa.String(20), nullable=False, server_default="active"),
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "status")
