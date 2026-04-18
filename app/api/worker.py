"""Worker API for govil-scraper integration."""
import base64
import hashlib
import httpx
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, Request, UploadFile
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
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    version_number: int = Form(...),
    resource_name: str = Form("נתוני הסורק"),
    row_count: int = Form(0),
    compression: str | None = Form(None),
    fields_json: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Worker uploads a CSV file as multipart. Returns the odata resource_id
    that can then be referenced in /push-version via csv_resource_ids.

    Used by workers when the records JSON would exceed the 100MB Cloudflare
    limit on the push-version POST.

    `compression="gzip"` indicates the uploaded bytes are gzip-compressed.
    The server decompresses on receipt so the resource is stored as a plain
    `.csv` (downloadable + Excel-friendly) rather than `.csv.gz`. We also
    parse the CSV and push it into the datastore so the dataset page shows
    a queryable/filterable table — same UX as small datasets that go through
    the inline JSON push-version path.

    `fields_json` is a JSON-encoded list of {id, type} dicts describing the
    CSV columns (used for datastore schema). If absent, columns are inferred
    from the CSV header row with type=text.
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

    is_gzip = (compression or "").lower() == "gzip"

    # ---- Stream the upload to a temp file on disk (no bytes held in memory) ----
    # Hesdermutne's 166MB plain CSV + 32k parsed dicts previously pushed the
    # Render starter dyno to ~400MB RSS and OOM-crashed. Using temp files
    # keeps peak memory under ~30MB regardless of dataset size.
    import os
    import shutil
    import tempfile
    import uuid as _uuid

    tmp_dir = "/tmp/upload_csv"
    os.makedirs(tmp_dir, exist_ok=True)
    upload_id = _uuid.uuid4().hex[:8]
    gz_path = os.path.join(tmp_dir, f"{upload_id}.in.gz") if is_gzip else None
    csv_path = os.path.join(tmp_dir, f"{upload_id}.csv")

    # Stream uploaded bytes to disk in 256KB chunks
    try:
        target_path = gz_path or csv_path
        with open(target_path, "wb") as out:
            while True:
                chunk = await file.read(256 * 1024)
                if not chunk:
                    break
                out.write(chunk)
    except Exception as e:
        logger.exception("Failed writing upload to temp file")
        _cleanup_paths(gz_path, csv_path)
        raise HTTPException(status_code=500, detail=f"upload write failed: {e}")

    # Decompress gzip → plain CSV on disk, 64KB at a time
    if is_gzip:
        try:
            import gzip as _gzip
            with _gzip.open(gz_path, "rb") as g_in, open(csv_path, "wb") as c_out:
                shutil.copyfileobj(g_in, c_out, length=64 * 1024)
            gz_size = os.path.getsize(gz_path)
            csv_size = os.path.getsize(csv_path)
            logger.info(
                "Decompressed gzip CSV on disk: %d KB → %d KB (%.1fx)",
                gz_size // 1024, csv_size // 1024,
                csv_size / max(gz_size, 1),
            )
            os.remove(gz_path)
            gz_path = None
        except Exception as e:
            logger.exception("Failed to decompress gzip CSV")
            _cleanup_paths(gz_path, csv_path)
            raise HTTPException(status_code=400, detail=f"Bad gzip data: {e}")

    # ---- Read only the header to build fields, not the full CSV ----
    import csv as _csv
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = _csv.reader(fh)
            header = next(reader, []) or []
    except Exception as e:
        logger.exception("Failed to read CSV header")
        _cleanup_paths(gz_path, csv_path)
        raise HTTPException(status_code=400, detail=f"Bad CSV data: {e}")

    # Resolve fields: prefer worker-supplied, fall back to header inference
    fields: list[dict] = []
    if fields_json:
        try:
            parsed = json.loads(fields_json)
            if isinstance(parsed, list):
                fields = parsed
        except Exception:
            logger.warning("Bad fields_json — falling back to header inference")
    if not fields:
        fields = [{"id": col, "type": "text"} for col in header]

    # ---- Step 1: Create the resource on odata ----
    # CKAN itself has a ~100MB limit on uploaded files (resource_create returns
    # 413 Payload Too Large above that), so for huge CSVs we skip the file
    # upload and create an empty resource — the datastore table still holds
    # the data and users can download it via CKAN's built-in datastore dump
    # endpoint (/datastore/dump/<resource_id>).
    from app.services.snapshot_service import _timestamp
    ts = _timestamp()
    safe_name = (resource_name or "data").replace("/", "_").replace("\\", "_")
    filename = f"{ts}_v{version_number}_{safe_name}.csv"
    csv_size = os.path.getsize(csv_path)

    # Threshold: stay comfortably below odata's 100MB limit. 90MB plain CSV
    # leaves margin for multipart overhead. Above that → datastore-only.
    FILE_UPLOAD_LIMIT = 90 * 1024 * 1024
    upload_file = csv_size <= FILE_UPLOAD_LIMIT

    if upload_file:
        try:
            csv_resource = await odata_client.upload_resource(
                dataset_id=ds.odata_dataset_id,
                filename=filename,
                file_path=csv_path,  # streamed from disk
                name=f"{ts} v{version_number} - {safe_name}",
                description=f"Version {version_number} ({ts}): {resource_name} ({row_count} rows)",
                resource_format="CSV",
            )
            resource_id = csv_resource["id"]
            upload_mode = "file+datastore"
            logger.info(
                "Uploaded CSV file (%d KB, ~%d rows) → resource %s — datastore stream queued",
                csv_size // 1024, row_count, resource_id,
            )
        except httpx.HTTPStatusError as e:
            # 413 = CKAN's file-size limit. Fall back to datastore-only rather
            # than failing — data is still accessible via the queryable table.
            if e.response.status_code == 413:
                logger.warning(
                    "ODATA rejected CSV (413 — %d KB exceeds CKAN limit). "
                    "Falling back to datastore-only resource.",
                    csv_size // 1024,
                )
                upload_file = False
            else:
                logger.exception("Failed to upload CSV file (non-413)")
                _cleanup_paths(gz_path, csv_path)
                raise HTTPException(status_code=502, detail=f"CSV upload failed: {e}")
        except Exception as e:
            logger.exception("Failed to upload CSV file")
            _cleanup_paths(gz_path, csv_path)
            raise HTTPException(status_code=502, detail=f"CSV upload failed: {e}")

    if not upload_file:
        # Too big for CKAN file upload, or upload returned 413 above.
        # Create a resource pointing at CKAN's built-in datastore dump
        # endpoint — that way the UI Download button produces a CSV
        # streamed from the datastore on the fly, with no file stored
        # separately on the server.
        try:
            csv_resource = await odata_client.create_resource(
                dataset_id=ds.odata_dataset_id,
                name=f"{ts} v{version_number} - {safe_name}",
                description=(
                    f"Version {version_number} ({ts}): {resource_name} "
                    f"({row_count} rows). File too large for direct upload "
                    f"({csv_size // 1024 // 1024}MB) — data is served from "
                    f"the queryable datastore table; the Download button "
                    f"streams a CSV generated on demand."
                ),
                resource_format="CSV",
            )
            resource_id = csv_resource["id"]
            upload_mode = "datastore-only"
            logger.info(
                "Created empty resource (file %d MB > 90MB limit) → %s — "
                "datastore stream queued",
                csv_size // 1024 // 1024, resource_id,
            )
            # Now that we have the resource_id, patch the URL so Download
            # streams from the datastore dump endpoint. We can't pass this
            # in resource_create (needs the id), hence the follow-up call.
            try:
                dump_url = (
                    f"{settings.odata_url.rstrip('/')}"
                    f"/datastore/dump/{resource_id}"
                )
                await odata_client.update_resource_url(resource_id, dump_url)
                logger.info("Set download URL for %s → %s", resource_id, dump_url)
            except Exception as e:
                logger.warning(
                    "Could not patch download URL for %s: %s — "
                    "users can still access data via the datastore API",
                    resource_id, e,
                )
        except Exception as e:
            logger.exception("Failed to create empty resource")
            _cleanup_paths(gz_path, csv_path)
            raise HTTPException(status_code=502, detail=f"resource_create failed: {e}")

    # ---- Step 2: Stream rows from disk into datastore in the background ----
    # The background task also deletes the temp CSV when done so we don't
    # leak temp files across requests. Batch size is taken from the
    # function's default (2500) — do NOT pass 5000 here, that override is
    # exactly what used to cap the datastore at one batch worth of rows.
    if fields:
        background_tasks.add_task(
            odata_client.push_records_to_datastore_from_file,
            resource_id, fields, csv_path, True,
        )
        datastore_status = "queued"
    else:
        datastore_status = "skipped (no fields detected)"
        logger.warning("No fields available for datastore push (resource %s)", resource_id)
        _cleanup_paths(None, csv_path)

    return {
        "resource_id": resource_id,
        "size": csv_size,
        "rows": row_count,
        "compression": compression or "none",
        "datastore": datastore_status,
        "upload_mode": upload_mode,
    }


def _cleanup_paths(*paths: str | None) -> None:
    """Best-effort removal of temp files used by /upload-csv."""
    import os
    for p in paths:
        if not p:
            continue
        try:
            os.remove(p)
        except OSError:
            pass


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
