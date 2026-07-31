import uuid
from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text, text
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
    source_type: Mapped[str] = mapped_column(String(20), default="ckan")  # "ckan" | "scraper" | "govmap"
    source_url: Mapped[str | None] = mapped_column(String(1000))
    scraper_config: Mapped[dict | None] = mapped_column(JSONB)
    storage_mode: Mapped[str] = mapped_column(String(20), default="full_snapshot")  # "full_snapshot" | "append_only"
    appendonly_resource_id: Mapped[str | None] = mapped_column(String(255))
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_modified: Mapped[str | None] = mapped_column(String(50))
    last_error: Mapped[str | None] = mapped_column(Text)
    # When the SOURCE was confirmed gone — the publisher retired the page/layer
    # this dataset tracks. NULL means present (or never checked). Set only on a
    # verdict the scraper reached with certainty (for GovMap: the catalog was
    # fetched successfully AND the layer id is absent from it — a catalog
    # timeout, or a layer that IS listed, produces a different, transient
    # error), and cleared the moment a version lands again. Holds the FIRST
    # detection, so the badge can say how long it has been gone.
    #
    # Deliberately NOT a `status` value: a removed source is the case where the
    # archive matters MOST, so these datasets must stay listed and readable.
    # `status='duplicate'` hides a row; this one is meant to be seen.
    source_gone_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Subset of source resource IDs to mirror. NULL = legacy "track all".
    resource_ids: Mapped[list[str] | None] = mapped_column(JSONB)
    # [{id,name,format}, …] resources that exist at the source but aren't
    # in resource_ids. Populated by the poll job, dismissed by the admin.
    new_resources_at_source: Mapped[list[dict] | None] = mapped_column(JSONB)
    # Probe baselines used by conditional_archiver to decide whether to
    # short-circuit the full download. See migration 017 for shape.
    resource_probes: Mapped[dict | None] = mapped_column(JSONB)
    # Content field-flags: boolean metadata describing what KINDS of columns the
    # tracked table has, e.g. {"has_locality": true}. Dataset metadata (like
    # title/source), NOT rows and NOT tags. Computed + merged additively by
    # app/services/field_flags.py (migration 043).
    # No "::jsonb" cast on the default: Postgres coerces the literal to jsonb
    # for a jsonb column anyway, and the cast is a syntax error in every other
    # dialect — it broke create_all() on the SQLite DBs the tests build.
    field_flags: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'"), default=dict
    )
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
