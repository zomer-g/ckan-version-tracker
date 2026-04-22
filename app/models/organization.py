import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import JSONB
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
    # gov.il internal office UUIDs — used to match tracked gov.il scraper
    # datasets (whose source_url has ?officeId=<uuid>) to their org.
    gov_il_office_ids: Mapped[list | None] = mapped_column(JSONB)
    # Parent org (for sub-units under a ministry). Top-level ministries
    # have parent_id = NULL.
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
