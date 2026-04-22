"""Store gov.il office UUIDs on organizations to match scraper datasets

Revision ID: 010
Revises: 009
Create Date: 2026-04-22
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "010"
down_revision: Union[str, None] = "009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column("gov_il_office_ids", JSONB, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("organizations", "gov_il_office_ids")
