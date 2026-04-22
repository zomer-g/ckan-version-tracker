"""Add parent_id to organizations for sub-unit hierarchy

Revision ID: 011
Revises: 010
Create Date: 2026-04-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("parent_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        "fk_organizations_parent_id",
        "organizations",
        "organizations",
        ["parent_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_organizations_parent_id",
        "organizations",
        ["parent_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_organizations_parent_id", table_name="organizations")
    op.drop_constraint("fk_organizations_parent_id", "organizations", type_="foreignkey")
    op.drop_column("organizations", "parent_id")
