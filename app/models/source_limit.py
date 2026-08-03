"""Per-source cap on how many workers may scrape one upstream at once.

A row here is an admin decision about ONE source (see app/services/source_load.py
for what a source is and why the cap exists). No row means uncapped, which is
the state every source starts in — the table is empty until someone deliberately
throttles something, so the default behaviour of the fleet is unchanged.
"""
from datetime import datetime, timezone

from sqlalchemy import CheckConstraint, DateTime, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class SourceLimit(Base):
    __tablename__ = "source_limits"

    # The derived source key (source_load.source_key) — a scraper prefix like
    # "munidata"/"jda", or a source_type like "govmap"/"ckan". Not a foreign
    # key: sources are derived from dataset columns, not stored as entities.
    source_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    # 0 is meaningful and deliberate: it stops new claims for this source
    # entirely, which is what you want the moment an upstream starts erroring
    # or asks us to back off. It does NOT abort tasks already running.
    max_workers: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    updated_by: Mapped[str | None] = mapped_column(String(255))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("max_workers >= 0", name="ck_source_limits_non_negative"),
    )
