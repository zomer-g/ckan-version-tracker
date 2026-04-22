import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # CKAN slug from data.gov.il (e.g. "ministry-of-health"). Unique.
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True, index=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    image_url: Mapped[str | None] = mapped_column(String(1000))
    data_gov_il_id: Mapped[str | None] = mapped_column(String(255))
    # gov.il landing-page metadata — populated via sync-gov-il.
    # Null on rows that exist only on data.gov.il.
    gov_il_url_name: Mapped[str | None] = mapped_column(String(255), index=True)
    gov_il_logo_url: Mapped[str | None] = mapped_column(String(1000))
    external_website: Mapped[str | None] = mapped_column(String(1000))
    org_type: Mapped[int | None] = mapped_column()  # gov.il orgType (1=office)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
