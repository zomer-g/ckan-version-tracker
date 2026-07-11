"""Admin-editable text overrides for static pages (About / Rationale)

The About and Rationale pages ship their copy in the frontend i18n bundles
(frontend/src/i18n/he.json + en.json, under the "about"/"rationale" keys). Those
are baked in at build time, so fixing a typo used to mean a code change + a full
redeploy. This table is a thin OVERRIDE layer: one row per
(page, lang, key) that an admin has edited. The public pages fetch these at
runtime and merge them over the bundled defaults, so edits go live with no
deploy. A key with no row here simply falls back to the bundled default.

See app/models/page_content.py and app/api/page_content.py.

Revision ID: 034
Revises: 033
Create Date: 2026-07-11
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "034"
down_revision: Union[str, None] = "033"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "page_content",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("page", sa.String(40), nullable=False),
        sa.Column("lang", sa.String(8), nullable=False),
        sa.Column("key", sa.String(80), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_by", sa.String(255), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("page", "lang", "key", name="uq_page_content_plk"),
    )
    op.create_index("ix_page_content_page", "page_content", ["page"])


def downgrade() -> None:
    op.drop_index("ix_page_content_page", table_name="page_content")
    op.drop_table("page_content")
