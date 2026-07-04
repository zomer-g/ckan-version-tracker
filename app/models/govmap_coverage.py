"""GovMap layer coverage inventory + rollout state.

Full-coverage plan for GovMap: the catalog of vector layers (859 as of
2026-07 — all public, no credentials) is scraped on a throttled schedule (two a
day, morning + evening) so coverage builds up gradually without overloading
GovMap or the single self-hosted worker. One row per layer. ``last_triggered_at``
drives the "next layer to scrape" pick — never-triggered first (NULLs), then the
stalest — so the rollout walks the whole catalog before repeating. Datasets are
created lazily (only when a layer is first picked), so we don't spawn 859
TrackedDatasets up front. See app/services/govmap_coverage.py.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class GovmapCoverage(Base):
    __tablename__ = "govmap_coverage"

    # GovMap's numeric layer id (the `?lay=<id>` value; string to match the API).
    layer_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    caption: Mapped[str | None] = mapped_column(String(500))
    layer_kind: Mapped[int | None] = mapped_column(Integer)  # 0=point 1=line 2=polygon
    complexity: Mapped[int | None] = mapped_column(Integer)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    # Filled in lazily the first time this layer is scraped.
    tracked_dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="SET NULL"), nullable=True
    )
    # When the coverage scheduler last kicked off a scrape for this layer. Drives
    # the round-robin pick (NULLs first, then oldest).
    last_triggered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
