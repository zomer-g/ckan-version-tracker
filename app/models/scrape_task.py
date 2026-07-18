"""Scrape task queue for external worker integration."""
import uuid
from datetime import datetime, timezone
from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class ScrapeTask(Base):
    __tablename__ = "scrape_tasks"

    # At most one ACTIVE (pending/running) task per dataset. The app-side check
    # in poll_job._create_scrape_task ("is there already a task?") is a
    # check-then-act race: two concurrent polls both read "none" and both
    # INSERT, producing duplicate scrapes of the same dataset. The in-process
    # single-flight guard on poll_dataset covers the single-dyno case; this
    # partial unique index is the DB-level backstop for the cross-process case
    # (a losing INSERT hits a unique violation, handled as a skip).
    __table_args__ = (
        Index(
            "uq_scrape_tasks_active_per_dataset",
            "tracked_dataset_id",
            unique=True,
            postgresql_where=text("status IN ('pending','running')"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tracked_dataset_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tracked_datasets.id", ondelete="CASCADE"), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="pending")  # pending/running/completed/failed
    phase: Mapped[str | None] = mapped_column(String(50))
    progress: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str | None] = mapped_column(String(500))
    error: Mapped[str | None] = mapped_column(Text)
    # Which worker machine is running (or ran) this task.
    #   worker_ip — client IP the worker polled from (Cloudflare/Render
    #     forwarding headers). Can't tell two workers apart behind a shared NAT.
    #   worker_id — explicit identity the worker sends in X-Worker-Id
    #     (``hostname#short`` or the OVER_WORKER_ID override), which DOES
    #     distinguish co-located workers. Preferred for display; IP kept as a
    #     fallback for older workers that don't send the header.
    # Both are set on assignment and refreshed on every progress report.
    worker_ip: Mapped[str | None] = mapped_column(String(64))
    worker_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    # updated_at is bumped on every progress report — used to detect crashed
    # workers (no heartbeat for >10 min means the worker died).
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
