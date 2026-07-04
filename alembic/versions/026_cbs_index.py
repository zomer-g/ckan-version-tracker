"""CBS (cbs.gov.il) site content index

One row per crawled page on the Israeli Central Bureau of Statistics website.
Populated by the govil-scraper worker (Playwright DOM crawl, HEAD-only file
sizing — no downloads) via POST /api/cbs/ingest, read by the frontend + future
MCP via GET /api/cbs/*. Full-text search uses a STORED generated tsvector
(``search_vector``, 'simple' config — Hebrew has no dedicated TS config) with a
GIN index; titles also get a pg_trgm GIN index for substring/ILIKE matching.
See app/models/cbs_index.py + app/api/cbs.py.

Revision ID: 026
Revises: 025
Create Date: 2026-07-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "026"
down_revision: Union[str, None] = "025"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cbs_index",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("lang", sa.String(4), nullable=True),
        sa.Column("section", sa.String(64), nullable=True),
        sa.Column("series", sa.String(200), nullable=True),
        sa.Column("item_type", sa.String(32), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("title_en", sa.Text(), nullable=True),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("subject_tags", postgresql.JSONB(), nullable=True),
        sa.Column("year_start", sa.Integer(), nullable=True),
        sa.Column("year_end", sa.Integer(), nullable=True),
        sa.Column("geo_levels", postgresql.JSONB(), nullable=True),
        sa.Column("file_links", postgresql.JSONB(), nullable=True),
        sa.Column("file_types", postgresql.JSONB(), nullable=True),
        sa.Column("full_text", sa.Text(), nullable=True),
        sa.Column("content_hash", sa.String(64), nullable=True),
        sa.Column("crawl_status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("crawl_error", sa.Text(), nullable=True),
        sa.Column(
            "first_seen",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("last_crawled", sa.DateTime(timezone=True), nullable=True),
    )

    # Natural key — one row per URL (upsert target for the worker).
    op.create_index("ix_cbs_index_url", "cbs_index", ["url"], unique=True)
    # Common filter columns.
    op.create_index("ix_cbs_index_section", "cbs_index", ["section"])
    op.create_index("ix_cbs_index_item_type", "cbs_index", ["item_type"])
    op.create_index("ix_cbs_index_years", "cbs_index", ["year_start", "year_end"])
    op.create_index("ix_cbs_index_status", "cbs_index", ["crawl_status"])

    # Containment filters on the JSONB arrays (subject_tags @> '["..."]',
    # file_types @> '["xlsx"]', geo_levels @> '["locality"]').
    op.execute(
        "CREATE INDEX ix_cbs_index_subjects ON cbs_index "
        "USING gin (subject_tags jsonb_path_ops)"
    )
    op.execute(
        "CREATE INDEX ix_cbs_index_file_types ON cbs_index "
        "USING gin (file_types jsonb_path_ops)"
    )
    op.execute(
        "CREATE INDEX ix_cbs_index_geo ON cbs_index "
        "USING gin (geo_levels jsonb_path_ops)"
    )

    # Full-text search: STORED generated tsvector over the text fields. 'simple'
    # config (no stemming) — Postgres ships no Hebrew TS config, and 'simple'
    # still tokenises + lowercases, which is what we want for Hebrew.
    op.execute(
        "ALTER TABLE cbs_index ADD COLUMN search_vector tsvector "
        "GENERATED ALWAYS AS ("
        "  to_tsvector('simple', "
        "    coalesce(title,'') || ' ' || coalesce(title_en,'') || ' ' || "
        "    coalesce(summary,'') || ' ' || coalesce(full_text,'')"
        "  )"
        ") STORED"
    )
    op.execute("CREATE INDEX ix_cbs_index_search ON cbs_index USING gin (search_vector)")

    # Substring / ILIKE matching on titles (autocomplete, partial words).
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.execute(
        "CREATE INDEX ix_cbs_index_title_trgm ON cbs_index "
        "USING gin (title gin_trgm_ops)"
    )


def downgrade() -> None:
    op.drop_table("cbs_index")
