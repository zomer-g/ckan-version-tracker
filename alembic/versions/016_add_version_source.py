"""Track which archive pipeline produced each version

Adds a `source` column to version_index to label whether a version was
created by the new harvest pipeline (CKAN ckanext-harvest + CAS blob
storage on the receiving CKAN) or by the legacy snapshot pipeline that
ran fully inside this FastAPI worker.

The column powers two things:

- The download endpoint routes the redirect URL based on `source`
  (legacy versions live as plain CKAN resources on odata.org.il; harvest
  versions live behind the activity-tied download URL provided by
  ckanext-versions).
- The poll job tries the harvest pipeline first; on any failure it
  silently falls back to the legacy pipeline. The `source` value is the
  audit trail of which path actually wrote each row.

All pre-existing rows were produced by the legacy path; default them to
"legacy" both server-side and as a column default so the migration is
safe to re-run.

Revision ID: 016
Revises: 015
Create Date: 2026-05-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "version_index",
        sa.Column("source", sa.String(length=16), nullable=False, server_default="legacy"),
    )


def downgrade() -> None:
    op.drop_column("version_index", "source")
