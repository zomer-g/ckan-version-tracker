import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class VersionIndex(Base):
    __tablename__ = "version_index"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tracked_dataset_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="CASCADE"), nullable=False
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    metadata_modified: Mapped[str] = mapped_column(String(50), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    odata_metadata_resource_id: Mapped[str | None] = mapped_column(String(255))
    change_summary: Mapped[dict | None] = mapped_column(JSONB)
    resource_mappings: Mapped[dict | None] = mapped_column(JSONB)

    tracked_dataset = relationship("TrackedDataset", back_populates="versions")
