"""CBS (cbs.gov.il) site content index.

One row per crawled page on the Israeli Central Bureau of Statistics website.
The CBS site is a SharePoint app behind a WAF that allowlists only its own REST
calls (SharePoint Search REST is blocked) and renders all content client-side,
so the GOV SCRAPER worker crawls it with a real browser (Playwright) and
extracts per-page metadata from the rendered DOM:

* ``title`` / ``title_en`` — page heading.
* ``subject_tags`` — SharePoint managed-metadata terms (the site's own topic
  taxonomy, e.g. "מדדי מחירים").
* ``file_links`` — every linked xlsx/pdf/csv/zip with its label, and size /
  last-modified obtained via a HEAD request. **Files are never downloaded** —
  the index catalogs what exists, not the bytes.
* ``year_start`` / ``year_end`` — the time span the page/its files cover,
  parsed from titles + link labels.
* ``geo_levels`` — geographic granularity the data is broken down by
  (locality / municipality / district / subdistrict / national).

The worker pushes batches here via ``POST /api/cbs/ingest`` (worker-key auth).
The frontend and a future dedicated MCP read this table (``GET /api/cbs/*``)
to let users navigate the otherwise-hard-to-search CBS site. Full-text search
uses a DB-side generated ``search_vector`` tsvector (see migration 026), not an
ORM column. See app/api/cbs.py and the ``cbs`` engine in the govil-scraper repo.
"""
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CbsIndex(Base):
    __tablename__ = "cbs_index"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    # Canonical absolute URL of the page — the natural key (unique). Kept as the
    # click-through target for the search UI.
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    lang: Mapped[str | None] = mapped_column(String(4))  # "he" | "en"

    # First path segment after the language ("publications", "mediarelease",
    # "subjects", "cbsNewBrand"...). The coarse area of the site.
    section: Mapped[str | None] = mapped_column(String(64))
    # Publication series / sub-area from the URL (e.g. "Madad").
    series: Mapped[str | None] = mapped_column(String(200))
    # publication | media_release | table | tool | subject | page
    item_type: Mapped[str | None] = mapped_column(String(32))

    title: Mapped[str | None] = mapped_column(Text)
    title_en: Mapped[str | None] = mapped_column(Text)
    summary: Mapped[str | None] = mapped_column(Text)

    # Managed-metadata subject terms (list[str]).
    subject_tags: Mapped[list | None] = mapped_column(JSONB)

    # Time span the content covers.
    year_start: Mapped[int | None] = mapped_column(Integer)
    year_end: Mapped[int | None] = mapped_column(Integer)

    # Geographic granularity levels present (list[str]): "locality",
    # "municipality", "district", "subdistrict", "national".
    geo_levels: Mapped[list | None] = mapped_column(JSONB)

    # Linked downloadable files (list[dict]):
    #   {label, href, ext, size, last_modified}
    # Sized via HEAD — bytes are never fetched.
    file_links: Mapped[list | None] = mapped_column(JSONB)
    # Distinct file extensions on the page (list[str]) — denormalised for fast
    # "has an xlsx" filtering without unnesting file_links.
    file_types: Mapped[list | None] = mapped_column(JSONB)

    # Extracted plain text of the rendered page (feeds search_vector).
    full_text: Mapped[str | None] = mapped_column(Text)
    # SHA-256 of the extracted content — lets the worker skip re-writing
    # unchanged pages and detect updates.
    content_hash: Mapped[str | None] = mapped_column(String(64))

    # pending (seeded, not yet crawled) | ok | error
    crawl_status: Mapped[str] = mapped_column(String(16), default="pending")
    crawl_error: Mapped[str | None] = mapped_column(Text)

    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_crawled: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
