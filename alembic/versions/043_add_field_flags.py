"""Per-dataset content field-flags metadata.

Adds ``tracked_datasets.field_flags`` — a JSONB bag of boolean metadata flags
describing what KINDS of columns the dataset's tracked table contains, e.g.
``{"has_locality": true}``. These are dataset METADATA (alongside title/source),
NOT rows inside the data and NOT tag objects. Computed by
app/services/field_flags.py from the live column list and merged additively, so
new flags can be added later without a migration and without clobbering old ones.

Revision ID: 043
Revises: 042
Create Date: 2026-07-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "043"
down_revision: Union[str, None] = "042"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("field_flags", JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "field_flags")
