"""Admin endpoints for exporting a version's files to Google Drive.

Flow:
  1. GET  /api/drive/status                       — is Drive connected?
  2. POST /api/versions/{version_id}/export-to-drive  — validate folder + enqueue
  3. GET  /api/drive/exports/{job_id}             — poll progress

The actual transfer runs in app/worker/drive_export_runner.py (a durable,
APScheduler-drained queue). This module only validates the request up front
(Drive connected, folder reachable, files present) so the admin gets
immediate feedback, then drops a job row.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.drive_export_job import DriveExportJob
from app.models.user import User
from app.models.version_index import VersionIndex
from app.services import drive_client
from app.services import storage_client as storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["drive"])


class ExportRequest(BaseModel):
    folder_url: str


class ExportJobResponse(BaseModel):
    id: str
    status: str
    total_files: int
    completed_files: int
    current_file: str | None = None
    error: str | None = None

    model_config = {"from_attributes": True}


def _job_response(job: DriveExportJob) -> ExportJobResponse:
    return ExportJobResponse(
        id=str(job.id),
        status=job.status,
        total_files=job.total_files,
        completed_files=job.completed_files,
        current_file=job.current_file,
        error=job.error,
    )


@router.get("/drive/status")
async def drive_status(admin: User = Depends(get_admin_user)):
    """Whether the current admin has connected Google Drive."""
    return {"connected": bool(admin.google_refresh_token)}


@router.post("/versions/{version_id}/export-to-drive", response_model=ExportJobResponse)
async def export_version_to_drive(
    version_id: str,
    body: ExportRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_admin_user),
):
    vid = parse_uuid(version_id, "version_id")
    version = (
        await db.execute(select(VersionIndex).where(VersionIndex.id == vid))
    ).scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    if not admin.google_refresh_token:
        raise HTTPException(status_code=409, detail="Google Drive is not connected")

    folder_id = drive_client.extract_folder_id(body.folder_url)
    if not folder_id:
        raise HTTPException(status_code=400, detail="Could not read a Drive folder link")

    files = storage.enumerate_files(version.resource_mappings)
    if not files:
        raise HTTPException(status_code=400, detail="This version has no files to export")

    # Fail fast: confirm the token works and the folder is writable BEFORE
    # enqueueing, so the admin gets an immediate, specific error.
    try:
        access_token = await drive_client.get_access_token(admin.google_refresh_token)
        folder_name = await drive_client.validate_folder(access_token, folder_id)
    except drive_client.DriveError as e:
        raise HTTPException(status_code=400, detail=str(e))

    job = DriveExportJob(
        version_id=version.id,
        tracked_dataset_id=version.tracked_dataset_id,
        user_id=admin.id,
        folder_id=folder_id,
        folder_label=folder_name,
        total_files=len(files),
        status="pending",
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)
    logger.info(
        "Enqueued Drive export job %s: version %s → folder %s (%d files)",
        job.id, version.id, folder_id, len(files),
    )
    return _job_response(job)


@router.get("/drive/exports/{job_id}", response_model=ExportJobResponse)
async def get_export_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_admin_user),
):
    jid = parse_uuid(job_id, "job_id")
    job = (
        await db.execute(select(DriveExportJob).where(DriveExportJob.id == jid))
    ).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Export job not found")
    return _job_response(job)
