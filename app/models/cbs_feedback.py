"""A single like/dislike vote on a CBS search.

Populated by POST /api/cbs/feedback (public, rate-limited) from both the website
and the extension; read by the admin feedback report to find the queries that
most need improvement. See app/api/cbs.py + migration 041.
"""
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, SmallInteger, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class CbsFeedback(Base):
    __tablename__ = "cbs_feedback"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(String(12), nullable=False)  # ask | advanced
    vote: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # +1 | -1
    answer_type: Mapped[str | None] = mapped_column(String(24))
    top_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str | None] = mapped_column(String(16))  # web | extension
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
