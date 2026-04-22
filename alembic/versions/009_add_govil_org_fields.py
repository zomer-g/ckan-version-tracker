"""Add gov.il landing-page fields to organizations

Revision ID: 009
Revises: 008
Create Date: 2026-04-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "009"
down_revision: Union[str, None] = "008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("organizations", sa.Column("gov_il_url_name", sa.String(255), nullable=True))
    op.add_column("organizations", sa.Column("gov_il_logo_url", sa.String(1000), nullable=True))
    op.add_column("organizations", sa.Column("external_website", sa.String(1000), nullable=True))
    op.add_column("organizations", sa.Column("org_type", sa.Integer(), nullable=True))
    op.create_index(
        "ix_organizations_gov_il_url_name",
        "organizations",
        ["gov_il_url_name"],
    )


def downgrade() -> None:
    op.drop_index("ix_organizations_gov_il_url_name", table_name="organizations")
    op.drop_column("organizations", "org_type")
    op.drop_column("organizations", "external_website")
    op.drop_column("organizations", "gov_il_logo_url")
    op.drop_column("organizations", "gov_il_url_name")
