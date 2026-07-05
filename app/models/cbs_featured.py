"""Admin-curated "featured" CBS pages.

One row per page an admin has pinned. The CBS page (``GET /api/cbs/featured``)
shows these pinned pages as quick-access cards at the top of the search view —
but ONLY on the default, unsearched view: once a query/filter is applied the
pins are hidden and the user sees plain search results (a pinned page that also
matches the search simply appears among those results).

``url`` references ``cbs_index.url`` (the natural key there). We deliberately do
NOT add a hard FK: a pinned URL that later drops out of the index just yields no
card (the featured endpoint inner-joins against cbs_index), which is the desired
graceful degradation. ``sort_order`` fixes the left-to-right order of the cards;
new pins go to the end. See app/api/cbs.py and app/models/cbs_index.py.
"""
from datetime import datetime, timezone

from sqlalchemy import DateTime, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CbsFeatured(Base):
    __tablename__ = "cbs_featured"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # The pinned page — matches cbs_index.url. Unique: a page is pinned once.
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)

    # Display order of the cards (ascending). New pins take max+1.
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
