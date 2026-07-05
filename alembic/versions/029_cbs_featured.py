"""Admin-curated featured (pinned) CBS pages.

One row per page an admin pins; the CBS page surfaces these as quick-access
cards on the default (unsearched) view. ``url`` matches ``cbs_index.url`` but
carries no hard FK on purpose — a pin whose page later leaves the index just
renders no card (the featured endpoint inner-joins cbs_index). See
app/models/cbs_featured.py + app/api/cbs.py.

Revision ID: 029
Revises: 028
Create Date: 2026-07-05
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "029"
down_revision: Union[str, None] = "028"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cbs_featured",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    # One pin per page.
    op.create_index("ix_cbs_featured_url", "cbs_featured", ["url"], unique=True)
    # Card ordering.
    op.create_index("ix_cbs_featured_order", "cbs_featured", ["sort_order"])


def downgrade() -> None:
    op.drop_table("cbs_featured")
