"""Add oauth_provider to users

Revision ID: 002
Revises: 001
Create Date: 2026-03-18
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("oauth_provider", sa.String(50), nullable=True))


def downgrade() -> None:
    op.drop_column("users", "oauth_provider")
