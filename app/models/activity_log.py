"""Append-only activity log for the dataset/scrape lifecycle.

One row per discrete event so the admin can see the full history of a
tracked dataset: the moment it was requested, approved or rejected,
queued for the worker, picked up (scrape started), and finished (scrape
completed or failed — with the error message on the failing rows).

Unlike ScrapeTask (a single mutable current-state row per task), this is
an immutable event stream: rows are only ever inserted, never updated, so
the timeline survives task reuse and dataset deletion. ``dataset_title``
is denormalized (snapshot at write time) and the FK is ``SET NULL`` on
delete, so a deleted dataset's history stays readable.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, String, Text, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ActivityLog(Base):
    __tablename__ = "activity_log"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # Nullable + SET NULL: keep the log line even after the dataset is deleted.
    tracked_dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="SET NULL"), nullable=True, index=True
    )
    # Snapshot of the dataset title at write time (survives deletion / renames).
    dataset_title: Mapped[str | None] = mapped_column(String(1000))
    source_type: Mapped[str | None] = mapped_column(String(20))  # ckan | scraper | govmap
    # requested | approved | rejected | queued | started | completed | failed
    event: Mapped[str] = mapped_column(String(40), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="info")  # ok | error | info
    message: Mapped[str | None] = mapped_column(String(500))
    # Full error message / extra context for failed or rejected steps.
    detail: Mapped[str | None] = mapped_column(Text)
    # Who/what triggered it: an admin email, "system", or "worker".
    actor: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )

    __table_args__ = (
        # The log view orders newest-first, optionally scoped to one dataset.
        Index("ix_activity_log_ds_created", "tracked_dataset_id", "created_at"),
    )
