"""Make created_by nullable for anonymous tracking requests

Revision ID: 005
Revises: 004
Create Date: 2026-04-11
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("tracked_datasets", "created_by", nullable=True)


def downgrade() -> None:
    op.alter_column("tracked_datasets", "created_by", nullable=False)
