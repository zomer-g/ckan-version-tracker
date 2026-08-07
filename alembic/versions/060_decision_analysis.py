"""Admin-editable analysis of a government decision, with a publish gate

Backs the "החלטת ממשלה 1933" analysis page (/rationale/1933). The content ships
as a bundled default in app/data/decision_1933.py; this table stores the admin's
edited version of the whole document as one JSONB blob that overrides it, plus
the `published` flag that decides whether the public may see the page at all.

No row = unpublished + bundled default, which is exactly the state a fresh
deploy should be in: the page exists but nobody outside the admin panel can
reach it until someone flips the switch.

See app/models/decision_analysis.py and app/api/decision_analysis.py.

Revision ID: 060
Revises: 059
Create Date: 2026-08-07
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "060"
down_revision: Union[str, None] = "059"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "decision_analysis",
        sa.Column("key", sa.String(40), primary_key=True),
        sa.Column(
            "published", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column("doc", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("updated_by", sa.String(255), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("decision_analysis")
