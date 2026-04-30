import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class TrackedDataset(Base):
    __tablename__ = "tracked_datasets"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    ckan_id: Mapped[str] = mapped_column(String(255), nullable=False)
    ckan_name: Mapped[str] = mapped_column(String(255), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(255))
    title: Mapped[str] = mapped_column(String(1000), nullable=False)
    organization: Mapped[str | None] = mapped_column(String(255))
    organization_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organizations.id", ondelete="SET NULL"), nullable=True, index=True
    )
    odata_dataset_id: Mapped[str | None] = mapped_column(String(255))
    poll_interval: Mapped[int] = mapped_column(Integer, default=3600)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    status: Mapped[str] = mapped_column(String(20), default="active")
    source_type: Mapped[str] = mapped_column(String(20), default="ckan")  # "ckan" | "scraper"
    source_url: Mapped[str | None] = mapped_column(String(1000))
    scraper_config: Mapped[dict | None] = mapped_column(JSONB)
    storage_mode: Mapped[str] = mapped_column(String(20), default="full_snapshot")  # "full_snapshot" | "append_only"
    appendonly_resource_id: Mapped[str | None] = mapped_column(String(255))
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_modified: Mapped[str | None] = mapped_column(String(50))
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    versions = relationship("VersionIndex", back_populates="tracked_dataset", cascade="all, delete-orphan")
    tags = relationship(
        "Tag",
        secondary="dataset_tags",
        back_populates="datasets",
        lazy="selectin",
        order_by="Tag.name",
    )
