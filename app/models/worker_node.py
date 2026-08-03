"""One machine in the scraping fleet, as last seen polling for work.

Until now a worker existed only as a string stamped on the tasks it had run
(``scrape_tasks.worker_id`` / ``worker_ip``), which cannot answer either
question an operator actually has: which machines are alive right now — an idle
worker has stamped nothing — and how do I take ONE of them out of rotation to
update its code, without killing the scrape it is in the middle of.

A row here is written by the poll endpoint, so the table is the fleet as it
reports itself. ``paused`` is the only field an admin sets.
"""
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class WorkerNode(Base):
    __tablename__ = "workers"

    # Stable identity for one machine. Prefers the self-reported X-Worker-Id
    # ("hostname#short", or the OVER_WORKER_ID override), which distinguishes
    # workers sharing a NAT; older workers that send no id fall back to
    # "ip:<addr>" so they are still listed and still pausable.
    worker_key: Mapped[str] = mapped_column(String(80), primary_key=True)
    worker_id: Mapped[str | None] = mapped_column(String(64))
    worker_ip: Mapped[str | None] = mapped_column(String(64))
    # WHICH CODE the machine last reported running, and its own verdict on
    # whether that is current with origin. This is the pair an operator reads
    # to decide a worker needs updating in the first place.
    worker_version: Mapped[str | None] = mapped_column(String(64))
    worker_upstream: Mapped[str | None] = mapped_column(String(16))
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    # Drain. A paused worker is handed no new task; the task it already holds
    # runs to completion, because /poll is only asked when the worker wants its
    # NEXT task. It keeps polling (and keeps refreshing last_seen_at), so the
    # panel can still show it as alive and idle — which is exactly the state you
    # wait for before restarting it.
    paused: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    paused_by: Mapped[str | None] = mapped_column(String(255))
