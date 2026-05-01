"""Per-dataset multi-resource selection + new-resource alerts

Adds two JSONB columns to tracked_datasets:

- `resource_ids`: list of CKAN resource IDs to actually mirror. NULL keeps
  the legacy "track every resource on the source" behavior so existing
  rows are unaffected. New CKAN datasets must specify at least one ID.

- `new_resources_at_source`: list of {id,name,format} dicts populated by
  the poll job when it sees resources at data.gov.il that aren't in the
  tracked set. The admin UI surfaces these so they can be added or
  dismissed; staying out-of-band rather than reusing last_error keeps
  alerts and failures separate.

Revision ID: 015
Revises: 014
Create Date: 2026-04-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "015"
down_revision: Union[str, None] = "014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("resource_ids", JSONB(), nullable=True),
    )
    op.add_column(
        "tracked_datasets",
        sa.Column("new_resources_at_source", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "new_resources_at_source")
    op.drop_column("tracked_datasets", "resource_ids")
