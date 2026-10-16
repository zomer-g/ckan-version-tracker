"""One Parquet file of closed append rows on R2 (docs/history-phase2-plan.md).

Written by app/services/history_export.py after the file it describes was read
back from R2 and its ``content_hash`` matched the SQL side. A file is never
rewritten: a month that gains closed rows later gets a new ``part``.

``deleted_from_sql`` is False after an export (the default, no window): the rows
are in BOTH places. True means the rows exist ONLY in R2.
"""
import uuid
from datetime import date, datetime, timezone

from sqlalchemy import (BigInteger, Boolean, Date, DateTime, ForeignKey, Integer,
                        Text, UniqueConstraint)
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class HistoryPart(Base):
    __tablename__ = "history_parts"
    __table_args__ = (UniqueConstraint("table_name", "closed_month", "part"),)

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # RESTRICT: deleting a dataset must not silently drop the only record of
    # rows that may exist nowhere but R2.
    dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="RESTRICT"), nullable=False)
    table_name: Mapped[str] = mapped_column(Text, nullable=False)
    closed_month: Mapped[date] = mapped_column(Date, nullable=False)  # first day of the month
    part: Mapped[int] = mapped_column(Integer, nullable=False)
    r2_key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    rows: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_min: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    first_seen_max: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_min: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_seen_max: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    deleted_from_sql: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
