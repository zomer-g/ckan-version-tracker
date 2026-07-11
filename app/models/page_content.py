"""Admin-editable text overrides for the static About / Rationale pages.

One row per (page, lang, key) that an admin has edited in the admin UI. The copy
itself normally lives in the frontend i18n bundles (frontend/src/i18n/*.json,
under the "about" / "rationale" namespaces) and is baked in at build time; a row
here overrides a single string at runtime so typos and rewrites go live without
a redeploy. Absence of a row means "use the bundled default".

``page`` is one of ``PAGES`` ("about" / "rationale"); ``lang`` is "he" / "en";
``key`` is the i18n key WITHIN that page namespace (e.g. "what_text" for
``about.what_text``). ``value`` keeps the same inline-tag convention as the
bundle (``<1>``/``<2>``/``<strong>`` for the <Trans> components) so the public
page renders it identically. See app/api/page_content.py.
"""
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class PageContent(Base):
    __tablename__ = "page_content"
    __table_args__ = (
        UniqueConstraint("page", "lang", "key", name="uq_page_content_plk"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Which page ("about" / "rationale").
    page: Mapped[str] = mapped_column(String(40), nullable=False)
    # Language code ("he" / "en").
    lang: Mapped[str] = mapped_column(String(8), nullable=False)
    # i18n key within the page namespace (e.g. "what_text").
    key: Mapped[str] = mapped_column(String(80), nullable=False)

    # The overriding text (same inline-tag convention as the bundle).
    value: Mapped[str] = mapped_column(Text, nullable=False)

    # Email of the admin who last edited this string (audit trail).
    updated_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
