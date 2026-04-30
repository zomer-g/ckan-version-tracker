"""Per-dataset storage mode: full_snapshot vs. append_only

Adds:
- storage_mode (default 'full_snapshot')
- appendonly_resource_id (the shared odata resource that append datasets keep
  appending into; populated lazily on the first append-mode poll)

Also flips the government-decisions row to append_only at upgrade time, so the
behavior the user expected is finally true. The append_key is left unset on
purpose — until a user supplies the actual key column name, the runtime falls
back to per-row hashing, which is correct (just slower).

Revision ID: 013
Revises: 012
Create Date: 2026-04-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "013"
down_revision: Union[str, None] = "012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column(
            "storage_mode",
            sa.String(20),
            server_default="full_snapshot",
            nullable=False,
        ),
    )
    op.add_column(
        "tracked_datasets",
        sa.Column("appendonly_resource_id", sa.String(255), nullable=True),
    )

    # Flip the government-decisions tracked dataset to append-only.
    # Match is intentionally broad — title may be Hebrew ("החלטות הממשלה"),
    # ckan_name may carry the slug ("decisions" / "memshala"), and the
    # source_url for gov.il dynamic collectors usually contains "decisions".
    op.execute(
        """
        UPDATE tracked_datasets
        SET storage_mode = 'append_only'
        WHERE lower(coalesce(ckan_name,  '')) LIKE '%decisions%'
           OR lower(coalesce(ckan_name,  '')) LIKE '%memshala%'
           OR lower(coalesce(title,      '')) LIKE '%decisions%'
           OR coalesce(title,            '')  LIKE '%החלטות%'
           OR lower(coalesce(source_url, '')) LIKE '%decisions%'
           OR lower(coalesce(source_url, '')) LIKE '%memshala%'
        """
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "appendonly_resource_id")
    op.drop_column("tracked_datasets", "storage_mode")
