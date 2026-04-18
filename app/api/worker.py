"""Worker API for govil-scraper integration."""
import base64
import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services.odata_client import odata_client

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/worker", tags=["worker"])


def _verify_worker_key(request: Request):
    """Verify the worker API key from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing worker key")
    key = auth[7:].strip()
    if not settings.worker_api_key or key != settings.worker_api_key:
        raise HTTPException(status_code=403, detail="Invalid worker key")


# --- Models ---

class ResourceData(BaseModel):
    name: str
    format: str = "CSV"
    records: list[dict] = []
    fields: list[dict] = []
    row_count: int = 0

class AttachmentData(BaseModel):
    name: str
    url: str
    size: int = 0

class ZipFileData(BaseModel):
    filename: str
    content_base64: str
    size: int = 0

class PushVersionRequest(BaseModel):
    tracked_dataset_id: str
    metadata_modified: str
    resources: list[ResourceData] = []
    attachments: list[AttachmentData] = []
    scrape_metadata: dict = {}
    zip_file: ZipFileData | None = None
    # Alternative to inline zip_file: reference a single ZIP already uploaded via /upload-zip
    zip_resource_id: str | None = None
    # Preferred for large attachment sets: list of pre-uploaded ZIP part resource_ids
    zip_resource_ids: list[str] | None = None
    # For huge record sets that would exceed 100MB JSON limit: worker uploads
    # CSV via /upload-csv first and references its resource_id here per
    # resource name (so we can skip push_csv_to_datastore for that resource).
    csv_resource_ids: dict[str, str] | None = None

class ProgressUpdate(BaseModel):
    phase: str
    current: int = 0
    total: int = 0
    percentage: int = 0
    message: str = ""

class FailureReport(BaseModel):
    error: str
    phase: str = ""


# --- Endpoints ---

@router.get("/poll")
@limiter.limit("60/minute")
async def poll_for_task(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Worker polls for the next available scrape task.

    Before returning a new task, auto-fails any 'running' task whose worker
    appears to have died: either no heartbeat (updated_at) for >10 minutes,
    or running for >120 minutes total even if still reporting progress (big
    enough slack for real long scrapes while still catching truly stuck ones).
    """
    _verify_worker_key(request)

    # Auto-reset stuck "running" tasks. Two triggers:
    # 1. no progress update in the last 10 minutes → worker crashed mid-task
    # 2. running for more than 2 hours total → unusually long, likely zombie
    from datetime import timedelta
    from sqlalchemy import or_
    now = datetime.now(timezone.utc)
    heartbeat_cutoff = now - timedelta(minutes=10)
    hard_cutoff = now - timedelta(hours=2)
    stuck_result = await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.status == "running",
            or_(
                ScrapeTask.updated_at < heartbeat_cutoff,
                ScrapeTask.created_at < hard_cutoff,
            ),
        )
    )
    cleaned = 0
    for stuck_task in stuck_result.scalars().all():
        stuck_task.status = "failed"
        stuck_task.phase = "timeout"
        age_min = int((now - stuck_task.created_at).total_seconds() / 60) if stuck_task.created_at else 0
        hb_min = int((now - stuck_task.updated_at).total_seconds() / 60) if stuck_task.updated_at else age_min
        stuck_task.error = (
            f"Task auto-reset: no heartbeat for {hb_min} min "
            f"(task age {age_min} min) — worker likely crashed"
        )
        stuck_task.completed_at = now
        logger.warning("Auto-reset stuck task %s (age=%dmin, no heartbeat for %dmin)",
                       stuck_task.id, age_min, hb_min)
        cleaned += 1
    if cleaned:
        await db.commit()

    result = await db.execute(
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "pending")
        .order_by(ScrapeTask.created_at.asc())
        .limit(1)
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=204)  # No tasks

    task, ds = row
    task.status = "running"
    task.phase = "assigned"
    task.message = "Assigned to worker"
    await db.commit()

    return {
        "task_id": str(task.id),
        "tracked_dataset_id": str(ds.id),
        "source_url": ds.source_url,
        "scraper_config": ds.scraper_config or {"download_files": False},
        "callback_url": "/api/worker/push-version",
    }


@router.post("/push-version")
@limiter.limit("30/minute")
async def push_version(
    request: Request,
    body: PushVersionRequest,
    db: AsyncSession = Depends(get_db),
):
    """Worker pushes scraped data as a new version."""
    _verify_worker_key(request)

    # Find the tracked dataset
    try:
        ds_id = uuid.UUID(body.tracked_dataset_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_id)
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Get next version number
    latest_result = await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc())
        .limit(1)
    )
    latest = latest_result.scalar_one_or_none()
    next_version = (latest.version_number + 1) if latest else 1

    # Skip if same metadata_modified
    if latest and latest.metadata_modified == body.metadata_modified:
        return {"message": "No change detected", "version_number": latest.version_number}

    # Push tabular resources to odata.org.il
    resource_mappings: dict[str, Any] = {}
    odata_resource_ids = []

    if ds.odata_dataset_id:
        from app.services.snapshot_service import _timestamp
        ts = _timestamp()

        csv_resource_ids = body.csv_resource_ids or {}

        for res in body.resources:
            # Prefer pre-uploaded CSV file (used when records JSON would exceed
            # Cloudflare's 100MB limit). Worker uploaded the CSV via
            # /api/worker/upload-csv and passes the resource_id by resource name.
            pre_uploaded = csv_resource_ids.get(res.name)
            if pre_uploaded:
                resource_mappings[res.name] = pre_uploaded
                odata_resource_ids.append(pre_uploaded)
                logger.info("Using pre-uploaded CSV for %s → resource %s (%d rows)",
                            res.name, pre_uploaded, res.row_count)
                continue

            if res.records and res.fields:
                try:
                    odata_result = await odata_client.push_csv_to_datastore(
                        dataset_id=ds.odata_dataset_id,
                        version_number=next_version,
                        resource_name=res.name,
                        fields=res.fields,
                        records=res.records,
                        resource_format=res.format,
                        timestamp=ts,
                    )
                    rid = odata_result["id"]
                    resource_mappings[res.name] = rid
                    odata_resource_ids.append(rid)
                    logger.info("Pushed %d records for %s to odata (resource %s)", len(res.records), res.name, rid)
                except Exception as e:
                    logger.error("Failed to push resource %s to odata: %s", res.name, e)

    # ZIP attachment handling: prefer pre-uploaded zip_resource_ids (list of
    # multipart parts), fall back to single zip_resource_id, then inline base64.
    if body.zip_resource_ids:
        for rid in body.zip_resource_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_zip_parts"] = list(body.zip_resource_ids)
        logger.info("Using %d pre-uploaded ZIP part(s)", len(body.zip_resource_ids))
    elif body.zip_resource_id:
        # Single ZIP was already uploaded via /api/worker/upload-zip
        odata_resource_ids.append(body.zip_resource_id)
        resource_mappings["_zip"] = body.zip_resource_id
        logger.info("Using pre-uploaded ZIP resource %s", body.zip_resource_id)
    elif body.zip_file and ds.odata_dataset_id:
        try:
            zip_bytes = base64.b64decode(body.zip_file.content_base64)
            from app.services.snapshot_service import _timestamp
            ts_zip = _timestamp()
            zip_result = await odata_client.upload_resource(
                dataset_id=ds.odata_dataset_id,
                file_content=zip_bytes,
                filename=body.zip_file.filename,
                name=f"{ts_zip} v{next_version} - קבצים מצורפים",
                description=f"Version {next_version}: {len(body.attachments)} attached files",
                resource_format="ZIP",
            )
            zip_resource_id = zip_result["id"]
            odata_resource_ids.append(zip_resource_id)
            resource_mappings["_zip"] = zip_resource_id
            logger.info("Uploaded ZIP (%d KB) to odata (resource %s)",
                        len(zip_bytes) // 1024, zip_resource_id)
        except Exception as e:
            logger.error("Failed to upload ZIP to odata: %s", e)

    # Compute hash for change detection
    hash_data = json.dumps({
        "resources": [{"name": r.name, "row_count": r.row_count} for r in body.resources],
        "attachments": [{"name": a.name, "url": a.url} for a in body.attachments],
    }, sort_keys=True)
    content_hash = hashlib.sha256(hash_data.encode()).hexdigest()

    resource_mappings["_hashes"] = {"scraper": content_hash}
    resource_mappings["_resource_ids"] = []

    # Create version
    total_rows = sum(r.row_count for r in body.resources)
    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=body.metadata_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "scraper",
            "total_rows": total_rows,
            "total_attachments": len(body.attachments),
            "resources": [{"name": r.name, "format": r.format, "rows": r.row_count} for r in body.resources],
            "scrape_metadata": body.scrape_metadata,
            "resources_added": odata_resource_ids,
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings=resource_mappings,
    )
    db.add(version)

    # Update dataset
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = body.metadata_modified
    await db.commit()

    # Mark any running task as completed
    task_result = await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.tracked_dataset_id == ds.id,
            ScrapeTask.status == "running",
        )
    )
    task = task_result.scalar_one_or_none()
    if task:
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.progress = 100
        task.phase = "complete"
        await db.commit()

    logger.info("Scraper version %d created for %s (%d rows)", next_version, ds.title, total_rows)

    return {
        "version_id": str(version.id),
        "version_number": next_version,
        "odata_resource_ids": odata_resource_ids,
        "message": f"Version {next_version} created with {total_rows} records",
    }


@router.post("/upload-zip/{tracked_dataset_id}")
@limiter.limit("30/minute")
async def upload_zip(
    request: Request,
    tracked_dataset_id: str,
    file: UploadFile = File(...),
    version_number: int = Form(...),
    attachment_count: int = Form(0),
    part: int | None = Form(None),
    total_parts: int | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Worker uploads a ZIP file as multipart. Returns the odata resource_id
    that can then be referenced in /push-version via zip_resource_id(s).

    For large attachment sets, the worker splits the payload into ≤80MB parts
    (to fit under Cloudflare's 100MB edge limit) and calls this endpoint once
    per part with `part` and `total_parts` set. Each part becomes its own
    resource on the odata mirror.
    """
    _verify_worker_key(request)

    try:
        ds_id = uuid.UUID(tracked_dataset_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_id)
    )
    ds = result.scalar_one_or_none()
    if not ds or not ds.odata_dataset_id:
        raise HTTPException(status_code=404, detail="Dataset not found or no odata mirror")

    zip_bytes = await file.read()
    from app.services.snapshot_service import _timestamp
    ts_zip = _timestamp()

    # Build resource name/description, including part info when split
    if part is not None and total_parts is not None and total_parts > 1:
        resource_name = f"{ts_zip} v{version_number} - קבצים מצורפים (חלק {part}/{total_parts})"
        description = f"Version {version_number}: attached files part {part}/{total_parts} ({attachment_count} total)"
    else:
        resource_name = f"{ts_zip} v{version_number} - קבצים מצורפים"
        description = f"Version {version_number}: {attachment_count} attached files"

    try:
        zip_result = await odata_client.upload_resource(
            dataset_id=ds.odata_dataset_id,
            file_content=zip_bytes,
            filename=file.filename or f"v{version_number}_attachments.zip",
            name=resource_name,
            description=description,
            resource_format="ZIP",
        )
        logger.info("Uploaded ZIP %s (%d KB) → resource %s",
                    f"part {part}/{total_parts}" if total_parts else "(single)",
                    len(zip_bytes) // 1024, zip_result["id"])
        return {"resource_id": zip_result["id"], "size": len(zip_bytes)}
    except Exception as e:
        logger.exception("Failed to upload ZIP")
        raise HTTPException(status_code=502, detail=f"ZIP upload failed: {e}")


@router.post("/upload-csv/{tracked_dataset_id}")
@limiter.limit("30/minute")
async def upload_csv(
    request: Request,
    tracked_dataset_id: str,
    file: UploadFile = File(...),
    version_number: int = Form(...),
    resource_name: str = Form("נתוני הסורק"),
    row_count: int = Form(0),
    db: AsyncSession = Depends(get_db),
):
    """Worker uploads a CSV file as multipart. Returns the odata resource_id
    that can then be referenced in /push-version via csv_resource_ids.

    Used by workers when the records JSON would exceed the 100MB Cloudflare
    limit on the push-version POST. Skips the datastore push (so no
    interactive preview on odata.org.il), but the CSV is downloadable.
    """
    _verify_worker_key(request)

    try:
        ds_id = uuid.UUID(tracked_dataset_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_id)
    )
    ds = result.scalar_one_or_none()
    if not ds or not ds.odata_dataset_id:
        raise HTTPException(status_code=404, detail="Dataset not found or no odata mirror")

    csv_bytes = await file.read()
    from app.services.snapshot_service import _timestamp
    ts = _timestamp()
    safe_name = (resource_name or "data").replace("/", "_").replace("\\", "_")

    try:
        csv_result = await odata_client.upload_resource(
            dataset_id=ds.odata_dataset_id,
            file_content=csv_bytes,
            filename=file.filename or f"v{version_number}_{safe_name}.csv",
            name=f"{ts} v{version_number} - {safe_name}",
            description=f"Version {version_number} ({ts}): {resource_name} ({row_count} rows)",
            resource_format="CSV",
        )
        logger.info("Uploaded CSV (%d KB, %d rows) → resource %s",
                    len(csv_bytes) // 1024, row_count, csv_result["id"])
        return {"resource_id": csv_result["id"], "size": len(csv_bytes)}
    except Exception as e:
        logger.exception("Failed to upload CSV")
        raise HTTPException(status_code=502, detail=f"CSV upload failed: {e}")


@router.post("/progress/{task_id}")
@limiter.limit("120/minute")
async def update_progress(
    request: Request,
    task_id: str,
    body: ProgressUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Worker reports progress on a running task."""
    _verify_worker_key(request)

    try:
        tid = uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid task ID")

    result = await db.execute(select(ScrapeTask).where(ScrapeTask.id == tid))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.phase = body.phase
    task.progress = body.percentage
    task.message = body.message
    await db.commit()

    return {"status": "ok"}


@router.post("/fail/{task_id}")
@limiter.limit("30/minute")
async def report_failure(
    request: Request,
    task_id: str,
    body: FailureReport,
    db: AsyncSession = Depends(get_db),
):
    """Worker reports a task failure."""
    _verify_worker_key(request)

    try:
        tid = uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid task ID")

    result = await db.execute(select(ScrapeTask).where(ScrapeTask.id == tid))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.status = "failed"
    task.phase = body.phase
    task.error = body.error
    task.completed_at = datetime.now(timezone.utc)
    await db.commit()

    logger.warning("Scrape task %s failed: %s", task_id, body.error)
    return {"status": "failed"}
