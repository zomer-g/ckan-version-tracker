from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class SourceRegistry(Base):
    """A scraper source the GOVSCRAPER worker declared, not one OVER hardcodes.

    Rows arrive via POST /api/worker/sources/sync. The manifest shape is
    validated by app.services.source_registry.SourceManifest before it lands
    here, so readers can trust the keys.
    """

    __tablename__ = "source_registry"

    # Manifest id = scraper_config["kind"] = the "<id>-scraper-" ckan_id prefix.
    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    manifest: Mapped[dict] = mapped_column(JSONB, nullable=False)
    manifest_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Kill switch — stops URL classification; tracked datasets keep polling.
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    worker_version: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
