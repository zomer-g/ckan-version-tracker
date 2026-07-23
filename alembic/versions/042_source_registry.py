"""Declarative source registry — new scraper sources without an OVER deploy.

Each row is one manifest pushed by the GOVSCRAPER worker
(POST /api/worker/sources/sync). The manifest carries everything OVER needs
to onboard a source it has no code for: URL regexes, page types, title
templates, default scraper_config, poll cadence, NEON eligibility, and the
display badge. See app/services/source_registry.py for the schema.

Revision ID: 042
Revises: 041
Create Date: 2026-07-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "042"
down_revision: Union[str, None] = "041"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "source_registry",
        # The manifest id — also the scraper_config "kind" and the
        # "<id>-scraper-" ckan_id prefix.
        sa.Column("id", sa.String(40), primary_key=True),
        sa.Column("manifest", postgresql.JSONB(), nullable=False),
        # sha256 of the canonical manifest JSON; lets sync skip unchanged rows.
        sa.Column("manifest_hash", sa.String(64), nullable=False),
        # Kill switch: a disabled source stops classifying pasted URLs.
        # Already-tracked datasets keep polling.
        sa.Column(
            "enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        # Which worker build last pushed this manifest (diagnostics only).
        sa.Column("worker_version", sa.String(64)),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
    )


def downgrade() -> None:
    op.drop_table("source_registry")
