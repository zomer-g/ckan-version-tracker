"""Admin-editable analysis of a government decision, and its publish gate.

One row per analysed decision (``key`` = the decision number, e.g. "1933"). The
content itself ships as a bundled default in app/data/decision_1933.py; a row
here holds the admin's edited version of the WHOLE document in ``doc`` and
overrides the default wholesale. ``doc`` may be NULL, which means "no edits yet
— serve the bundled default".

``published`` is the visibility gate and it is deliberately separate from the
content: a draft can be edited for weeks while the public endpoint keeps
returning 404 and the page stays out of the navigation. Absence of a row means
unpublished, so a fresh deploy never exposes work in progress.

See app/api/decision_analysis.py for the document schema and validation.
"""
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DecisionAnalysis(Base):
    __tablename__ = "decision_analysis"

    # Decision number — "1933". Also the URL segment (/rationale/1933).
    key: Mapped[str] = mapped_column(String(40), primary_key=True)

    # Public visibility. False (or no row at all) hides the page from everyone
    # except a logged-in admin, who sees it with a draft banner.
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # The whole edited document. NULL = serve the bundled default from
    # app/data/decision_1933.py.
    doc: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Email of the admin who last saved (audit trail).
    updated_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
