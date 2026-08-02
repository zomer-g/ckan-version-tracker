"""Scrape task queue for external worker integration."""
import uuid
from datetime import datetime, timezone
from sqlalchemy import DateTime, ForeignKey, Index, Integer, SmallInteger, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

# Priority bands. The worker claims the highest priority pending task, oldest
# first within a band (app/api/worker.py). One queue, two kinds of work: a bulk
# backfill can be dumped in wholesale without ever delaying a routine poll,
# because a band-0 task is only claimed when nothing above it is pending.
#
# NOTE what priority does NOT do: a task that is already RUNNING cannot be
# preempted. A heavy GovMap layer holds its worker for up to ~75 min, so when
# every live worker happens to be mid-backfill a routine poll still waits for
# one of them to finish. Priority governs the queue, not the fleet.
#
# PROMOTED is the band the admin queue panel writes by hand (the "⬆ לראש התור"
# button), and nothing else writes it — that is what makes it a reliable "next
# out": no scheduled job can ever land alongside it. It sits above MANUAL so a
# hand-picked row also beats a "דגום" click someone made an hour ago. Inside
# the band the ordinary rule still applies, oldest-queued first: promote three
# rows and all three go before everything else, among themselves in the order
# they entered the queue.
PRIORITY_PROMOTED = 300    # admin picked this row OUT of the queue: strictly next
PRIORITY_MANUAL = 200      # admin "דגום" — a human is watching, jump the queue
PRIORITY_ROUTINE = 100     # scheduled polls: the normal cadence of the system
PRIORITY_COVERAGE = 10     # GovMap coverage rollout's routine quarterly refresh
PRIORITY_BACKFILL = 0      # one-shot whole-catalog re-scrapes; strictly last


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
    # Claim order: higher first, then oldest. See the PRIORITY_* bands above.
    priority: Mapped[int] = mapped_column(
        SmallInteger, default=PRIORITY_ROUTINE, server_default=text("100"), nullable=False
    )
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
    # Per-RUN overrides, merged OVER the dataset's scraper_config in the /poll
    # response (migration 047). This is what makes one dataset samplable several
    # ways — "only what's new", "only the files at status X", "just this one" —
    # without forking it into several datasets or mutating its stored config.
    # None means the routine full poll, which is what every task was before.
    params: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
