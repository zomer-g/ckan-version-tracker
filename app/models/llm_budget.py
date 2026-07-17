from datetime import date, datetime, timezone

from sqlalchemy import Date, DateTime, Integer
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class LlmDailyUsage(Base):
    """One row per calendar day counting paid-LLM calls made by the public CBS
    natural-language endpoints (/api/cbs/ask + /api/cbs/resolve).

    This is the persistence behind the GLOBAL daily budget (see
    app/services/llm_budget.py + app/config.py). Unlike the per-IP request
    limiter and the per-IP byte budget, this counter is keyed only by the day —
    NOT by client IP — so it caps total LLM spend across all callers and can't
    be reset by rotating X-Forwarded-For or by a restart/deploy. The tally is
    incremented (and the ceiling checked) in a single atomic conditional UPSERT,
    so it is race-free under concurrency.
    """

    __tablename__ = "llm_daily_usage"

    # Calendar day this tally is for (server date, CURRENT_DATE).
    day: Mapped[date] = mapped_column(Date, primary_key=True)
    # LLM calls reserved (successfully served) on this day.
    calls: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
