"""CBS index: add extra JSONB column

Holds additional structured taxonomy that doesn't warrant a first-class column
(interval/frequency, keywords, gathering_method, article_type, surveys,
publisher, languages). First-class facets (subject/geo/year/file_type) keep
their own columns + indexes. See app/models/cbs_index.py.

Revision ID: 027
Revises: 026
Create Date: 2026-07-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "027"
down_revision: Union[str, None] = "026"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("cbs_index", sa.Column("extra", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("cbs_index", "extra")
