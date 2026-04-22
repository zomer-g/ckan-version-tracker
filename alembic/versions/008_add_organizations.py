"""Add organizations table + organization_id FK on tracked_datasets

Revision ID: 008
Revises: 007
Create Date: 2026-04-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "008"
down_revision: Union[str, None] = "007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "organizations",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False, unique=True),
        sa.Column("title", sa.String(500), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("image_url", sa.String(1000), nullable=True),
        sa.Column("data_gov_il_id", sa.String(255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_organizations_name", "organizations", ["name"], unique=True)

    op.add_column(
        "tracked_datasets",
        sa.Column("organization_id", sa.UUID(), nullable=True),
    )
    op.create_foreign_key(
        "fk_tracked_datasets_organization_id",
        "tracked_datasets",
        "organizations",
        ["organization_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_tracked_datasets_organization_id",
        "tracked_datasets",
        ["organization_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_tracked_datasets_organization_id", table_name="tracked_datasets")
    op.drop_constraint("fk_tracked_datasets_organization_id", "tracked_datasets", type_="foreignkey")
    op.drop_column("tracked_datasets", "organization_id")
    op.drop_index("ix_organizations_name", table_name="organizations")
    op.drop_table("organizations")
