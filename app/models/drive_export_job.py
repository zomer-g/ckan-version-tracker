"""Durable job queue for exporting a version's files to Google Drive.

Why this exists:
    A scraper version can hold hundreds of files (e.g. "החלטות שמאי" has
    258 ZIP parts + a CSV index in a single version). The frontend's
    "הורד הכל" button fires one browser download per file — and every
    browser hard-blocks bulk auto-downloads after ~10, so the files are
    effectively un-downloadable that way.

    This queue lets an admin push the whole set straight from the file
    store (R2 / ODATA) into a Google Drive folder server-side: the bytes
    stream R2 → dyno → Drive and never touch the admin's machine. It is
    modelled on the datastore-push queue next door — a DB-backed row that
    survives Render dyno recycles, drained by an APScheduler tick, with a
    heartbeat-based stuck-job rescue.

States:
    pending — created, waiting for the runner to pick it up
    running — runner claimed it; heartbeat = updated_at; completed_files
              advances per file uploaded
    success — every file uploaded to Drive (completed_files == total_files)
    failed  — errored past MAX_ATTEMPTS; error column holds the last cause

Resume model:
    ``completed_files`` is the count of files already in Drive. The file
    list is enumerated deterministically from the version's
    resource_mappings, so a retry (manual or stuck-job rescue) skips the
    first ``completed_files`` entries — no duplicate uploads.
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class DriveExportJob(Base):
    __tablename__ = "drive_export_jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    # The version whose files we're exporting. SET NULL so deleting the
    # version doesn't cascade-kill its export history.
    version_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("version_index.id", ondelete="SET NULL"), nullable=True
    )
    # For the admin UI / display only.
    tracked_dataset_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("tracked_datasets.id", ondelete="SET NULL"), nullable=True
    )
    # Admin who triggered it — whose google_refresh_token the runner uses.
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    # Google Drive destination folder id (extracted from the pasted URL).
    folder_id: Mapped[str] = mapped_column(String(128), nullable=False)
    # Original pasted URL / folder name, for display.
    folder_label: Mapped[str | None] = mapped_column(String(512), nullable=True)

    status: Mapped[str] = mapped_column(
        String(20), default="pending", nullable=False, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # total_files / completed_files count SOURCE files (ZIP parts + CSV) — the
    # 0..N coarse progress bar. The ZIPs are unpacked, so the real unit of work
    # is documents:
    #   documents_uploaded — running count of extracted documents pushed to
    #                        Drive (the headline number; also the fine resume
    #                        cursor within the current archive).
    #   archive_base       — documents_uploaded as of the last fully-finished
    #                        source file. (documents_uploaded - archive_base) =
    #                        how many members of the current archive are already
    #                        done, so a resume skips exactly them — no dupes,
    #                        no re-download of finished archives.
    total_files: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    completed_files: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    documents_uploaded: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    archive_base: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    # Human-readable name of the file currently uploading (UI progress).
    current_file: Mapped[str | None] = mapped_column(String(512), nullable=True)
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
    # Heartbeat. Bumped per file; stale > STUCK_HEARTBEAT_MINUTES while
    # status=running ⇒ cleanup resets it to pending.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
