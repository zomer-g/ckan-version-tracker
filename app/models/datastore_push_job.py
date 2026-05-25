"""Durable job queue for datastore ingest pushes.

Why this exists:
    The upload-csv endpoint used to schedule the datastore push via
    FastAPI's BackgroundTasks. Those run AFTER the response is sent
    but BEFORE the worker process recycles — and on Render starter
    dynos a recycle (deploy, OOM, idle scale-down) is frequent
    enough that long pushes (32K rows × ~7KB on the hesdermutne
    dataset, taking minutes) would routinely die mid-flight. The
    user-visible symptom was an ODATA resource advertising "X rows"
    with a Download button that returned 404 because datastore_active
    never flipped to true.

    Moving to a DB-backed queue makes the work durable: a job row
    survives every process restart, an APScheduler periodic task
    picks it back up, and a heartbeat-based recovery flips abandoned
    "running" jobs back to "pending" so they don't get stuck.

States:
    pending — created, waiting for the runner to pick it up
    running — the runner has claimed it; heartbeat = updated_at
    success — datastore push completed (rows_pushed = total_rows)
    failed  — push errored too many times; admin can /retry to flip
              back to pending. error column holds the last exception.

Recovery model:
    csv_path is on /tmp which is ephemeral on Render. When the runner
    picks up a job whose csv_path no longer exists, it falls back to
    downloading the same resource's file from ODATA — which works
    because the upload-csv endpoint always uploads the (gzipped) CSV
    BEFORE enqueueing the push job. csv_is_gzipped_in_source tells
    the runner whether to gunzip the recovered bytes before parsing.
"""
import uuid
from datetime import datetime, timezone
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base


class DatastorePushJob(Base):
    __tablename__ = "datastore_push_jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # Optional tracking for the admin UI; FK with SET NULL so deleting
    # a dataset doesn't cascade-kill its push history.
    tracked_dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="SET NULL"),
        nullable=True,
    )
    # CKAN/ODATA UUID of the resource we're populating. Doubles as
    # the recovery source: /resource/{resource_id}/download yields
    # the (possibly gzipped) CSV file we already uploaded.
    resource_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # Local /tmp path of the parsed CSV. May be gone when the runner
    # picks the job up; runner falls back to ODATA download in that
    # case.
    csv_path: Mapped[str] = mapped_column(String(512), nullable=False)
    # True iff the file at /resource/{resource_id}/download is gzip-
    # compressed (we hit the file-too-large branch and uploaded a
    # .csv.gz). Drives the recovery decompress step.
    csv_is_gzipped_in_source: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    # Datastore schema. JSON-encoded list of {id, type} dicts —
    # standard CKAN field descriptors.
    fields_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rows_pushed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    total_rows: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # Heartbeat. Bumped by the runner every batch. Stale > 15min
    # while status=running → cleanup_stuck_push_jobs flips it back
    # to pending.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
