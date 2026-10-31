"""One pass of the system health checks (app/services/health_checks.py).

A run is written once when it starts (status ``running``) and once when it
ends, with every check's verdict in ``results``. The checks of one run are
read together and never queried one by one, so they are a JSON list on the run
rather than a second table: the admin log reads a page of runs, then one run.

``tier`` is what was asked for: ``daily`` (the light pass), ``weekly`` (every
check), or ``manual`` (a hand-picked set from the admin tab). ``trigger`` is
who asked: ``scheduler`` or an admin's email.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class HealthRun(Base):
    __tablename__ = "health_runs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tier: Mapped[str] = mapped_column(String(16), nullable=False)        # daily | weekly | manual
    trigger: Mapped[str] = mapped_column(String(255), nullable=False)    # scheduler | admin email
    # running | ok | warn | fail | error (the run itself crashed / was cut off)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
        default=lambda: datetime.now(timezone.utc))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    # {"ok": n, "warn": n, "fail": n, "skip": n, "new_problems": [key, …],
    #  "resolved": [key, …], "checks": [key, …]}
    summary: Mapped[dict | None] = mapped_column(JSONB)
    # [{"key", "component", "title", "status", "message", "details",
    #   "duration_ms"}, …] in the order they ran.
    results: Mapped[list | None] = mapped_column(JSONB)

    __table_args__ = (
        Index("ix_health_runs_started", "started_at"),
        Index("ix_health_runs_tier_started", "tier", "started_at"),
    )
