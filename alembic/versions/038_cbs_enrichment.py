"""CBS index enrichment columns + locality gazetteer.

The ultimate-search-interface plan (Lamas repo) maps the seven dimensions
benchmark users actually search by onto metadata the index lacked. These
columns are DERIVED server-side from fields the crawler already stores
(app/services/cbs_enrich.py) — no re-crawl:

* ``product_form``     — user-facing "what do I get": data_file / gis_layer /
                         puf / generator / dashboard / api / database /
                         publication / methodology.
* ``freq``             — time-axis unit (שנתי / רבעוני / חודשי …), promoted
                         out of extra.interval so it can be filtered.
* ``source_op``        — named collection operation (מפקד אוכלוסין, סקר כוח
                         אדם, מרשם דירות…).
* ``data_vintage``     — year of the DATA (title-parsed), vs year of
                         publication; drives honest recency.
* ``geo_vintage``      — boundary vintage (א"ס 2011 vs 2022 / אזורי סקר) —
                         the recurring join trap.
* ``geo_coverage``     — inclusion threshold ("יישובים 5,000+ תושבים בלבד").
* ``series_key`` / ``edition_year`` / ``is_latest_edition`` — yearly editions
  of the same product share a key; "latest edition only" becomes a real
  filter and old editions stop shadowing current ones.
* ``metrics`` / ``cuts`` — JSONB lists of measure types (avg/median/pct…) and
  population breakdowns (age/gender/ses…) the page offers.

``cbs_gazetteer`` holds the locality registry (from the CBS bycode file):
code ↔ name ↔ English name ↔ district/subdistrict/municipal status/SES —
powering place-name resolution ("בית שמש" → locality) and the advanced tab's
entity autocomplete.

Revision ID: 038
Revises: 037
Create Date: 2026-07-17
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


revision: str = "038"
down_revision: Union[str, None] = "037"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("cbs_index", sa.Column("product_form", sa.String(24)))
    op.add_column("cbs_index", sa.Column("freq", sa.String(16)))
    op.add_column("cbs_index", sa.Column("source_op", sa.String(64)))
    op.add_column("cbs_index", sa.Column("data_vintage", sa.Integer()))
    op.add_column("cbs_index", sa.Column("geo_vintage", sa.String(32)))
    op.add_column("cbs_index", sa.Column("geo_coverage", sa.String(120)))
    op.add_column("cbs_index", sa.Column("series_key", sa.Text()))
    op.add_column("cbs_index", sa.Column("edition_year", sa.Integer()))
    op.add_column(
        "cbs_index",
        sa.Column("is_latest_edition", sa.Boolean(), nullable=False,
                  server_default=sa.text("true")),
    )
    op.add_column("cbs_index", sa.Column("metrics", JSONB()))
    op.add_column("cbs_index", sa.Column("cuts", JSONB()))

    op.create_index("ix_cbs_index_product_form", "cbs_index", ["product_form"])
    op.create_index("ix_cbs_index_freq", "cbs_index", ["freq"])
    op.create_index("ix_cbs_index_source_op", "cbs_index", ["source_op"])
    # series_key is looked up both alone (edition history) and with the year
    # (recompute of is_latest_edition after a backfill/crawl).
    op.create_index("ix_cbs_index_series", "cbs_index", ["series_key", "edition_year"])

    op.create_table(
        "cbs_gazetteer",
        # CBS locality code (סמל יישוב) — the national key.
        sa.Column("code", sa.Integer(), primary_key=True, autoincrement=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("name_en", sa.Text()),
        # Alternate spellings people actually type (תל אביב for תל אביב-יפו…).
        sa.Column("aliases", JSONB()),
        sa.Column("district", sa.Text()),        # מחוז
        sa.Column("subdistrict", sa.Text()),     # נפה
        sa.Column("municipal_status", sa.Text()),  # עירייה / מועצה מקומית / אזורית
        sa.Column("regional_council", sa.Text()),
        sa.Column("population", sa.Integer()),
        sa.Column("ses_cluster", sa.Integer()),  # אשכול חברתי-כלכלי
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_cbs_gazetteer_name", "cbs_gazetteer", ["name"])


def downgrade() -> None:
    op.drop_table("cbs_gazetteer")
    for ix in ("ix_cbs_index_series", "ix_cbs_index_source_op",
               "ix_cbs_index_freq", "ix_cbs_index_product_form"):
        op.drop_index(ix, table_name="cbs_index")
    for col in ("cuts", "metrics", "is_latest_edition", "edition_year",
                "series_key", "geo_coverage", "geo_vintage", "data_vintage",
                "source_op", "freq", "product_form"):
        op.drop_column("cbs_index", col)
