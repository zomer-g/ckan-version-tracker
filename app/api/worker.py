"""Worker API for govil-scraper integration."""
import asyncio
import base64
import csv as _csv
import hashlib
import hmac
import httpx
import json
import logging
import os as _os
import tempfile as _tempfile
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.scrape_task import ScrapeTask
from app.models.source_registry import SourceRegistry
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services import source_registry
from app.services.odata_client import odata_client
from app.services import storage_client as storage
from app.services.storage_client import storage_client
from app.services.version_detector import compute_new_rows
from app.services.worker_version import (
    get_required_worker_sha,
    get_required_engine_hash,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/worker", tags=["worker"])

# Raise the csv field cap (default 131072) so _neon_stream_load_r2 can parse
# rows with very large text cells — e.g. gov-decisions bodies (>175K chars).
# Global to the csv module; 10**8 stays under Windows' C-long ceiling.
_csv.field_size_limit(10**8)


def _verify_worker_key(request: Request):
    """Verify the worker API key from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing worker key")
    key = auth[7:].strip()
    # Constant-time compare: `!=` on a static secret leaks length/prefix via
    # timing. Fail closed when no key is configured (empty secret never matches,
    # and we never even reach compare_digest for it).
    if not settings.worker_api_key or not hmac.compare_digest(key, settings.worker_api_key):
        raise HTTPException(status_code=403, detail="Invalid worker key")


# Per-dataset storage routing lives in storage_client (shared with the CKAN
# poll path: snapshot_service / poll_job). These thin aliases preserve the
# worker-local call sites unchanged.
_dataset_storage = storage.dataset_storage_target
_use_r2 = storage.dataset_uses_r2


def _poll_scraper_config(ds) -> dict:
    """Build the scraper_config sent to the worker in the /poll response.

    Starts from the dataset's stored config and fills defaults the worker
    relies on:

    * ``download_files`` — preserve the historical fallback (catalog-only).
    * ``max_missing_fraction`` — the worker's completeness gate fails a scrape
      (and retries it forever) when more than this fraction of attachment
      downloads are missing. gov.il document collections routinely have
      ~10-20% genuinely-dead or IAP-blocked source links, and the worker's
      downloader already exhausts ~5 retries per file (parallel → sequential
      → 2 straggler rounds) before counting one missing — so the old 0.10
      default just retried the same dead links on every poll and the version
      never published. Default to a tolerance that publishes despite scattered
      dead links while still catching genuine mass failure (CF storm / outage,
      which loses far more than this). A dataset may pin its own value.
    """
    cfg = dict(ds.scraper_config or {})
    cfg.setdefault("download_files", False)
    cfg.setdefault("max_missing_fraction", 0.25)
    return cfg


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
    # GeoJSON resources already uploaded via /upload-geojson — referenced here
    # so push-version can link them into the version index without re-uploading.
    geojson_resource_ids: list[str] | None = None
    # GeoPackage resources (heavy GovMap layers publish a GPKG INSTEAD of
    # CSV+GeoJSON — uploaded via the direct-R2 multipart path). Absence of a
    # _geojson mapping is what (deliberately) hides the site's map preview.
    gpkg_resource_ids: list[str] | None = None
    # GeoParquet resources (heavy layers' analytics artifact, WGS84 — shipped
    # alongside the GPKG). Same direct-R2 upload path.
    parquet_resource_ids: list[str] | None = None
    # For huge record sets that would exceed 100MB JSON limit: worker uploads
    # CSV via /upload-csv first and references its resource_id here per
    # resource name (so we can skip push_csv_to_datastore for that resource).
    csv_resource_ids: dict[str, str] | None = None
    # For archive mode: patch fields to merge into ds.scraper_config (used to
    # persist incremental checkpoint back to the server after each run).
    scraper_config_patch: dict | None = None
    # Archive mode with 0 new items: mark the task completed without creating a
    # new version (avoids uploading the full CSV when nothing changed).
    skip_version: bool = False

class ProgressUpdate(BaseModel):
    phase: str
    current: int = 0
    total: int = 0
    percentage: int = 0
    message: str = ""

class FailureReport(BaseModel):
    error: str
    phase: str = ""


class SourceSyncBody(BaseModel):
    manifests: list[dict]
    worker_version: str | None = None


# --- Endpoints ---


@router.post("/sources/sync")
@limiter.limit("30/minute")
async def sync_source_manifests(
    request: Request,
    body: SourceSyncBody,
    db: AsyncSession = Depends(get_db),
):
    """Register the declarative source manifests this worker can run.

    The worker calls this at startup (and therefore after every self-update
    re-exec), so a source added in the GOVSCRAPER repo becomes trackable on
    over.org.il without an OVER deploy. See app/services/source_registry.py.

    Idempotent: a manifest whose hash is unchanged is left alone. Manifests
    absent from the payload are NEVER deleted or disabled — a worker still on
    an older branch would otherwise wipe sources a newer worker registered.
    Removing a source is a deliberate admin action.
    """
    _verify_worker_key(request)

    upserted: list[str] = []
    unchanged: list[str] = []
    rejected: list[dict] = []

    if len(body.manifests) > 200:
        raise HTTPException(status_code=400, detail="too many manifests in one sync")

    for raw in body.manifests:
        try:
            manifest = source_registry.validate_manifest(raw)
        except Exception as e:
            rejected.append({"id": (raw or {}).get("id"), "error": str(e)})
            continue

        digest = source_registry.manifest_hash(raw)
        existing = (
            await db.execute(
                select(SourceRegistry).where(SourceRegistry.id == manifest.id)
            )
        ).scalar_one_or_none()

        if existing and existing.manifest_hash == digest:
            unchanged.append(manifest.id)
            continue

        if existing:
            existing.manifest = raw
            existing.manifest_hash = digest
            existing.worker_version = body.worker_version
            existing.updated_at = datetime.now(timezone.utc)
        else:
            db.add(
                SourceRegistry(
                    id=manifest.id,
                    manifest=raw,
                    manifest_hash=digest,
                    worker_version=body.worker_version,
                )
            )
        upserted.append(manifest.id)

    if upserted:
        await db.commit()
    source_registry.invalidate_cache()
    await source_registry.load_enabled(db, force=True)

    if rejected:
        logger.warning("Source manifest sync rejected %d manifest(s): %s",
                       len(rejected), rejected)
    if upserted:
        logger.info("Source manifest sync upserted: %s", ", ".join(upserted))

    return {"upserted": upserted, "unchanged": unchanged, "rejected": rejected}

@router.get("/poll")
@limiter.limit("60/minute")
async def poll_for_task(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Worker polls for the next available scrape task.

    Before returning a new task, auto-fails any 'running' task whose worker
    has stopped heartbeating (no progress update in the last 10 minutes).
    A worker that's still posting progress is alive by definition, so long
    healthy scrapes (e.g. tens of thousands of attachments behind a slow
    upstream) are not killed by an arbitrary task-age cap.

    Also gates dispatch on worker freshness, to keep a stale worker from
    producing opaque errors that newer code would surface clearly. The worker
    decides that itself — it compares HEAD to origin/<branch> and sends the
    verdict in X-Worker-Upstream — because the worker repo is private and this
    server has no token to look upstream up. Only an explicit "behind" is
    refused; anything else dispatches.

    Setting worker_required_version re-enables the older pinned-SHA gate as an
    emergency override (see config.py).
    """
    _verify_worker_key(request)

    # Worker-version gate. We do this before the auto-reset/dispatch logic
    # so an outdated worker doesn't even trigger the bookkeeping side
    # effects of a poll. The pending task stays in the queue until a
    # matching worker shows up — never burned on a worker we know is
    # stale.
    #
    # Freshness is SELF-REPORTED. The worker compares its HEAD against
    # origin/<branch> — which it already fetches in order to self-update — and
    # sends the verdict in X-Worker-Upstream. Only an explicit "behind" is
    # refused; "current", "unknown" and a worker too old to send the header all
    # dispatch normally.
    #
    # The server can't check this itself: the worker repo is private and there
    # is no GitHub token here, so the commits API answers 404. It used to be
    # papered over with a SHA hardcoded in config.py that had to be bumped by
    # hand on every worker deploy — which is precisely how the whole fleet
    # ended up refused behind a pin nobody had touched.
    worker_version = (request.headers.get("x-worker-version") or "").strip()
    worker_engine_hash = (request.headers.get("x-worker-engine-hash") or "").strip().lower()
    worker_upstream = (request.headers.get("x-worker-upstream") or "").strip().lower()
    # Resolved only when an override is actually configured. Calling it
    # unconditionally would hit the GitHub commits API on every poll for a
    # private repo — a guaranteed 404 that burns the unauthenticated rate
    # limit (60/hour) at roughly the polling rate.
    required_version = (
        await get_required_worker_sha()
        if settings.worker_version_check_enabled and settings.worker_required_version
        else None
    )

    if settings.worker_version_check_enabled and not required_version:
        if worker_upstream == "behind":
            logger.warning(
                "Refusing to dispatch task: worker %s reports it is behind "
                "origin/%s (self-update pending)",
                worker_version[:12] or "(none)", settings.worker_branch,
            )
            return {
                "outdated": True,
                "worker_version": worker_version or "(none)",
                "expected_version": "",
                "worker_engine_hash": worker_engine_hash or "(none)",
                "expected_engine_hash": "",
                "message": (
                    f"Worker reports it is behind origin/{settings.worker_branch}. "
                    "It self-updates between tasks; no action needed unless this "
                    "persists."
                ),
            }

    # The pinned-SHA gate below runs ONLY while an emergency override is set in
    # worker_required_version (empty by default) — e.g. to freeze the fleet on
    # a known-good commit while a bad one is reverted.
    if settings.worker_version_check_enabled and required_version:
        required_engine_hash = await get_required_engine_hash()
        # Two-axis identity check. Either failed axis refuses dispatch.
        # The git-SHA axis is the cheap "did the operator pull?" check;
        # the engine-hash axis is the "did the operator restart after
        # pulling?" check (and also defends against WORKER_VERSION env
        # spoofing). Both must match.
        sha_match = bool(worker_version) and worker_version == required_version
        # The engine-hash axis ALWAYS fails open when undetermined: it can
        # only be sourced from GitHub (no env pin), so failing it closed on
        # a GitHub blip would block the correct worker. The pinned
        # required_version is the real gate; engine-hash is a bonus check
        # that only ever tightens, never blocks on its own.
        engine_match = (
            required_engine_hash is None
            or (bool(worker_engine_hash) and worker_engine_hash == required_engine_hash)
        )

        # Refresh-on-mismatch covers the "cache warmed seconds before
        # push reached upstream" case — only one re-fetch per axis,
        # rate-limited globally inside worker_version.py.
        if bool(worker_version) and not sha_match:
            required_version = (
                await get_required_worker_sha(refresh=True) or required_version
            )
            sha_match = worker_version == required_version
        if required_engine_hash is not None and bool(worker_engine_hash) and not engine_match:
            required_engine_hash = (
                await get_required_engine_hash(refresh=True) or required_engine_hash
            )
            engine_match = worker_engine_hash == required_engine_hash

        if not (sha_match and engine_match):
            reasons = []
            if not sha_match:
                reasons.append(
                    f"git SHA mismatch (worker={worker_version or '<missing>'}, "
                    f"expected={(required_version or '?')[:12]})"
                )
            if not engine_match:
                reasons.append(
                    f"engine hash mismatch (worker={(worker_engine_hash or '<missing>')[:12]}, "
                    f"expected={(required_engine_hash or '?')[:12]} — "
                    f"either workers running old code in memory or "
                    f"restart not done after pull)"
                )
            logger.warning("Refusing to dispatch task: %s", "; ".join(reasons))
            return {
                "outdated": True,
                "worker_version": worker_version or "(none)",
                "worker_engine_hash": worker_engine_hash or "(none)",
                "expected_version": required_version or "(unknown)",
                "expected_engine_hash": required_engine_hash or "(unknown)",
                "message": (
                    "Worker doesn't match upstream. " + "; ".join(reasons) +
                    ". git pull && restart this worker to receive tasks."
                ),
            }

    from datetime import timedelta
    now = datetime.now(timezone.utc)
    heartbeat_cutoff = now - timedelta(minutes=10)
    stuck_result = await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.status == "running",
            ScrapeTask.updated_at < heartbeat_cutoff,
        )
        # Multiple workers poll concurrently; skip rows another poll is
        # already auto-failing (avoids double writes / duplicate log lines).
        .with_for_update(skip_locked=True)
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

    # Walk pending tasks oldest-first and skip any whose dataset is now
    # collected locally (raw CollectorsWebApi URLs — see
    # poll_job._is_datacollector_api). Those tasks were enqueued by
    # _create_scrape_task before the local-collection code shipped; the
    # external scraper would just return "HTML instead of JSON" and clog
    # the recent-failures panel. Cancelling them on the assign path is
    # safer than a one-shot migration and self-heals if any new ones
    # sneak in (e.g. a stale code path or manual SQL insert).
    from app.worker.poll_job import _is_datacollector_api
    row = None
    cancelled_locals = 0
    while True:
        result = await db.execute(
            select(ScrapeTask, TrackedDataset)
            .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
            .where(ScrapeTask.status == "pending")
            .order_by(ScrapeTask.created_at.asc())
            .limit(1)
            # CRITICAL with multiple workers: the claim must be atomic.
            # Without a row lock, two workers polling in the same instant
            # both SELECT the same pending task, both flip it to 'running',
            # and BOTH receive the same task_id — the dataset gets scraped
            # twice concurrently (interleaved heartbeats, duplicate
            # push-version). FOR UPDATE SKIP LOCKED makes each concurrent
            # poll claim a DIFFERENT pending row (or none).
            .with_for_update(of=ScrapeTask, skip_locked=True)
        )
        candidate = result.first()
        if not candidate:
            break
        cand_task, cand_ds = candidate
        if cand_ds.source_type == "scraper" and _is_datacollector_api(cand_ds):
            await db.delete(cand_task)
            await db.commit()
            cancelled_locals += 1
            continue
        row = candidate
        break
    if cancelled_locals:
        logger.info(
            "Skipped %d pending scrape task(s) for datacollector_api datasets",
            cancelled_locals,
        )
    if not row:
        # Proper empty-body response. Do NOT raise HTTPException(204) —
        # FastAPI's exception handler builds a JSON `{"detail":...}` body,
        # but HTTP 204 must have Content-Length: 0 and no body. Starlette
        # then raises RuntimeError("Response content longer than Content-Length")
        # on every call. The worker sees the status line first so functionally
        # it still works, but each error keeps a full traceback object in
        # memory. With ~720 polls/hour/worker this quietly accumulated enough
        # RAM pressure to OOM-kill the dyno mid background-datastore-push.
        return Response(status_code=204)

    task, ds = row
    task.status = "running"
    task.phase = "assigned"
    # Attribute the assignment to a specific worker machine (real client IP,
    # derived through the Cloudflare/Render proxy chain — see app/client_ip.py).
    # With several workers on several machines this is what lets the admin queue
    # show WHICH machine holds each task; the worker sends no identity beyond
    # its version headers. Persisted in a dedicated column (refreshed on every
    # progress report) so it survives the message being overwritten mid-run.
    from app.client_ip import get_client_ip
    worker_ip = get_client_ip(request)
    worker_id = (request.headers.get("x-worker-id") or "").strip()[:64]
    if worker_ip and worker_ip != "unknown":
        task.worker_ip = worker_ip
    if worker_id:
        task.worker_id = worker_id
    # Prefer the explicit machine id (distinguishes workers behind a shared IP);
    # fall back to the IP for older workers that don't send X-Worker-Id.
    who = worker_id or (worker_ip if worker_ip and worker_ip != "unknown" else "")
    task.message = f"Assigned to worker {who}" if who else "Assigned to worker"
    await db.commit()

    from app.services.activity_log import log_event
    await log_event(
        event="started", dataset=ds, status="info", actor="worker",
        message="גירוד התחיל (המשימה נמסרה ל-worker)",
    )

    # Previous version's row count — lets the worker (a) fail FAST on heavy
    # layers when its high-fidelity engine is unavailable and (b) skip the
    # GB-scale uploads for a partial that this server's shrink guard would
    # reject anyway. Same extraction the shrink guard itself uses.
    prev_total_rows = 0
    latest_v = (await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc())
        .limit(1)
    )).scalar_one_or_none()
    if latest_v is not None:
        try:
            prev_total_rows = int((latest_v.change_summary or {}).get("total_rows") or 0)
        except (ValueError, TypeError):
            prev_total_rows = 0

    return {
        "task_id": str(task.id),
        "tracked_dataset_id": str(ds.id),
        "source_url": ds.source_url,
        "scraper_config": _poll_scraper_config(ds),
        "callback_url": "/api/worker/push-version",
        "prev_total_rows": prev_total_rows,
        # How big the worker may make each attachment ZIP part. R2 datasets get
        # a much larger limit (no CKAN/edge upload cap), so 1.5GB of files →
        # ~2 parts instead of ~19. Worker falls back to its own default if absent.
        "max_zip_part_bytes": (
            settings.zip_part_bytes_r2 if _use_r2(ds)
            else settings.zip_part_bytes_odata
        ),
    }


# Keep references to fire-and-forget NEON-load tasks so the event loop
# doesn't garbage-collect them mid-run.
_NEON_BG_TASKS: set = set()


async def _neon_stream_load_r2(table: str, r2_key: str) -> None:
    """Stream a version's R2 CSV into the dataset's NEON table in batches.

    Used for the >50MB out-of-band CSV path (e.g. registries Cosmetics,
    ~60k rows / ~58MB CSV). Parsing the whole file into rows at once
    (parse_csv → 60k dicts) OOMs / times out the 512MB OVER dyno inside
    the push-version request — which surfaced as a 502. Instead: download
    the object to a temp file, free the bytes, then csv.DictReader-stream
    it in fixed batches, inserting each batch and dropping it. Memory stays
    bounded to one batch. append_store stores everything as text and dedups
    on row_hash (ON CONFLICT DO NOTHING), so a partial run interrupted by a
    dyno recycle is safely resumed by the next poll. Best-effort throughout.

    Runs off the request path (scheduled via asyncio.create_task) so
    push-version returns immediately and the version is created regardless.
    """
    from app.services import append_store
    tmp = None
    BATCH = 5000
    try:
        fd, tmp = _tempfile.mkstemp(suffix=".csv", prefix="neon-load-")
        _os.close(fd)
        # Stream straight to disk (boto3 managed transfer, constant memory).
        # get_object_bytes() would materialise the WHOLE object in RAM first —
        # survivable for the ~58MB case this was written for, fatal on the
        # multi-GB index CSVs (the largest in the corpus is 3.58 GB) on a 512MB
        # dyno.
        if not await storage_client.download_to_file(r2_key, tmp):
            return
        cols: list[str] = []
        batch: list[dict] = []
        ensured = False
        total = 0
        with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
            reader = _csv.reader(fh)
            try:
                header = next(reader)
            except StopIteration:
                return
            # Clip + dedup on BYTE length: Postgres truncates identifiers at 63
            # bytes, so two long Hebrew headers sharing a prefix would otherwise
            # collapse into one column and fail the CREATE TABLE.
            safe = append_store.safe_column_names(header)
            # Blank-headed columns stay dropped (they were before this change);
            # materialising them as col_N would try to insert into a column that
            # existing tables don't have.
            keep = [i for i, raw in enumerate(header)
                    if (raw or "").strip() and safe[i] != "_id"]
            cols = [safe[i] for i in keep]
            if not cols:
                return
            for row in reader:
                batch.append({safe[i]: (row[i] if i < len(row) else "") or ""
                              for i in keep})
                if len(batch) >= BATCH:
                    if not ensured:
                        await append_store.ensure_table(table, cols, key_col=None, keyless=True)
                        ensured = True
                    total += await append_store.append_rows(
                        table, cols, batch, key_col=None, keyless=True,
                    )
                    batch = []
        if batch:
            if not ensured:
                await append_store.ensure_table(table, cols, key_col=None, keyless=True)
            total += await append_store.append_rows(
                table, cols, batch, key_col=None, keyless=True,
            )
        logger.info("NEON stream-load: +%d rows into %s from %s", total, table, r2_key)
    except Exception as e:
        logger.warning("NEON stream-load failed for %s (non-fatal): %s", table, e)
    finally:
        if tmp:
            try:
                _os.remove(tmp)
            except OSError:
                pass


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

    # Refuse a push for a dataset with no running task. push_version otherwise
    # creates the version off whatever the worker sends, no matter the state of
    # the task it came from — so a worker whose task was cancelled or reassigned
    # (e.g. an operator killed a wedged run, or a stale process kept churning
    # after a heartbeat timeout re-queued the work) can still land a stale or
    # junk version, and for an archive source a bad checkpoint with it. There
    # is at most one active task per dataset (migration 023), so "a running
    # task exists" is the clean precondition that a legitimate in-flight push
    # always satisfies.
    running_task = (await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.tracked_dataset_id == ds.id,
            ScrapeTask.status == "running",
        )
    )).scalar_one_or_none()
    if running_task is None:
        logger.warning(
            "Rejecting push-version for %s: no running task (cancelled or "
            "reassigned). Worker %s.",
            ds.id, request.headers.get("x-worker-id", "?"),
        )
        raise HTTPException(
            status_code=409,
            detail="No running task for this dataset — the task was cancelled "
                   "or reassigned; this push is stale and was rejected.",
        )

    # GovMap layers carry a placeholder title ("GovMap layer 200541") at
    # creation time because we don't fetch the catalog from the request path.
    # The scraper resolves the real Hebrew caption from govmap's catalog and
    # sends it as scrape_metadata.dataset_title_he. Promote it once, but only
    # while the title is still the default — preserves any manual override.
    if ds.source_type == "govmap" and body.scrape_metadata:
        from app.api.govmap import build_govmap_title
        new_title = (body.scrape_metadata.get("dataset_title_he") or "").strip()
        layer_id = (ds.scraper_config or {}).get("layer_id")
        if new_title and layer_id and ds.title == build_govmap_title(layer_id):
            ds.title = new_title
            if ds.odata_dataset_id:
                try:
                    await odata_client.package_patch(
                        ds.odata_dataset_id, title=f"[Versions] {new_title}"
                    )
                except Exception as e:
                    logger.warning("Failed to patch odata title for %s: %s", ds.id, e)

    # Archive mode: no new items — update checkpoint and mark task done without
    # creating a version (avoids re-uploading the full CSV when nothing changed).
    if body.skip_version:
        if body.scraper_config_patch:
            current = dict(ds.scraper_config or {})
            current.update(body.scraper_config_patch)
            ds.scraper_config = current
        ds.last_polled_at = datetime.now(timezone.utc)
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
            task.message = "No new items — archive up to date"
        await db.commit()
        from app.services.activity_log import log_event
        await log_event(
            event="completed", dataset=ds, status="ok", actor="worker",
            message="גירוד הסתיים — אין פריטים חדשים (הארכיון מעודכן)",
        )
        return {"message": "No new items — task marked done, checkpoint updated"}

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

    # ---- Shrink guard (data-integrity, hard-fail policy) ----
    # An upstream blip (gov.il returning a partial/empty result set, a
    # download that mostly failed) must NEVER overwrite a good version with
    # a drastically smaller one. The scraper already hard-fails on
    # incomplete scrapes, but this is the last line of defence on the OVER
    # side: if the incoming version has far fewer rows than the previous
    # good one, reject it (409 → the worker marks the task failed and
    # retries; the prior version stays the latest). Genuine large
    # shrinks (a source really did purge records) can be allowed by
    # setting scraper_config.allow_shrink = true on the dataset.
    new_total_rows = sum(r.row_count for r in body.resources)
    sc = ds.scraper_config or {}
    if (
        latest is not None
        and not sc.get("allow_shrink")
        and new_total_rows >= 0  # always true; keeps the guard explicit
    ):
        prev_total = 0
        try:
            prev_total = int((latest.change_summary or {}).get("total_rows") or 0)
        except (ValueError, TypeError):
            prev_total = 0
        # Only guard when the previous version actually had data, and the
        # new one collapsed below the threshold (default 50%).
        min_fraction = float(sc.get("min_shrink_fraction", 0.5))
        if prev_total > 0 and new_total_rows < prev_total * min_fraction:
            msg = (
                f"Rejected version: {new_total_rows} rows is far below the "
                f"previous good version's {prev_total} (< {min_fraction:.0%}). "
                f"Likely a partial/failed upstream scrape — keeping v"
                f"{latest.version_number}. Set scraper_config.allow_shrink "
                f"to override if the source genuinely shrank."
            )
            logger.warning("Shrink guard for %s: %s", ds.id, msg)
            # Mark the running task failed so the worker surfaces it and
            # retries on the next poll (no version is created).
            task_result = await db.execute(
                select(ScrapeTask).where(
                    ScrapeTask.tracked_dataset_id == ds.id,
                    ScrapeTask.status == "running",
                )
            )
            task = task_result.scalar_one_or_none()
            if task:
                task.status = "failed"
                task.phase = "shrink_guard"
                task.error = msg
                task.completed_at = datetime.now(timezone.utc)
                await db.commit()
            from app.services.activity_log import log_event
            await log_event(
                event="failed", dataset=ds, status="error", actor="system",
                message="הגרסה נדחתה (shrink guard — ירידה חדה במספר השורות)",
                detail=msg,
            )
            raise HTTPException(status_code=409, detail={
                "error": "shrink_guard",
                "message": msg,
            })

    # Push tabular resources to odata.org.il
    resource_mappings: dict[str, Any] = {}
    odata_resource_ids = []
    push_errors: list[str] = []

    is_append = (ds.storage_mode == "append_only")
    append_key = (ds.scraper_config or {}).get("append_key") if is_append else None
    seen_keys: list[str] = []
    rows_added_total = 0

    if is_append and latest is not None:
        seen_keys = list((latest.resource_mappings or {}).get("_appendonly_seen", []) or [])

    if ds.odata_dataset_id or _use_r2(ds):
        from app.services.snapshot_service import _timestamp
        from app.services.csv_parser import (
            batch_records, records_to_csv_bytes, parse_csv,
        )
        from app.services import append_store
        ts = _timestamp()

        csv_resource_ids = body.csv_resource_ids or {}

        # NEON dual-write for tabular scraper sources (e.g.
        # registries.health.gov.il): when the dataset opted into
        # ``archive_neon``, load each resource's rows into its per-dataset
        # NEON table so they're SQL-queryable — independent of the CKAN
        # datastore-streaming path (which only CKAN sources have). We load
        # from the stored CSV bytes (parse_csv) so the row_hash dedup is
        # identical whether the rows arrived inline or as an out-of-band
        # >50MB CSV. Best-effort: a NEON failure must never fail the version.
        _archive_neon = bool(
            append_store.is_configured()
            and (ds.scraper_config or {}).get("archive_neon")
        )

        async def _neon_load_from_csv(res_name: str, csv_bytes: bytes | None) -> None:
            if not (_archive_neon and csv_bytes):
                return
            try:
                n_fields, n_records = parse_csv(csv_bytes)
                raw_ids = [f["id"] for f in n_fields if f.get("id")]
                # Same 63-BYTE identifier clip Postgres applies server-side, so
                # colliding long Hebrew headers get disambiguated here instead of
                # failing the CREATE TABLE (see append_store.safe_column_names).
                safe_ids = append_store.safe_column_names(raw_ids)
                renamed = {r: s for r, s in zip(raw_ids, safe_ids) if r != s}
                cols = [s for r, s in zip(raw_ids, safe_ids) if r != "_id"]
                if not (cols and n_records):
                    return
                if renamed:
                    n_records = [
                        {renamed.get(k, k): v for k, v in rec.items()}
                        for rec in n_records
                    ]
                table = append_store.table_name(ds)
                await append_store.ensure_table(table, cols, key_col=None, keyless=True)
                n = await append_store.append_rows(
                    table, cols, n_records, key_col=None, keyless=True,
                )
                logger.info(
                    "NEON archive: +%d new rows into %s for %s", n, table, res_name,
                )
            except Exception as e:
                logger.warning(
                    "NEON archive failed for %s (non-fatal): %s", res_name, e,
                )

        for res in body.resources:
            # Pre-uploaded CSV files bypass record-level handling. Append mode
            # can't dedupe a file we never parsed, so we treat pre-uploaded
            # CSVs as a full snapshot for this resource even if the dataset
            # is in append mode (rare edge case: scraper would only do this
            # for >100MB JSON payloads, where append-only with diffing isn't
            # the intended path anyway).
            pre_uploaded = csv_resource_ids.get(res.name)
            if pre_uploaded:
                resource_mappings[res.name] = pre_uploaded
                odata_resource_ids.append(pre_uploaded)
                logger.info("Using pre-uploaded CSV for %s → resource %s (%d rows)",
                            res.name, pre_uploaded, res.row_count)
                # >50MB path: the worker uploaded the CSV out-of-band and sent
                # empty records. For NEON dual-write, stream the CSV back from
                # R2 into NEON in the BACKGROUND (batched, memory-bounded) —
                # doing it synchronously here OOMs/times-out the 512MB dyno for
                # a ~60k-row file and 502s the whole push. Firing it off-request
                # lets the version be created immediately; the load is
                # idempotent (row_hash ON CONFLICT), so it's safe if a recycle
                # interrupts it — the next poll resumes it.
                if _archive_neon and storage.is_storage_value(pre_uploaded):
                    _table = append_store.table_name(ds)
                    _t = asyncio.create_task(
                        _neon_stream_load_r2(_table, pre_uploaded)
                    )
                    _NEON_BG_TASKS.add(_t)
                    _t.add_done_callback(_NEON_BG_TASKS.discard)
                # Worker called /upload-csv with version_number=1 (it can't
                # know next_version yet — same constraint as the ZIP path).
                # Now that we do, rewrite the resource's 'vN' marker so the
                # dataset page doesn't show every CSV version stuck at v1.
                # ODATA-only display rename; R2 keys carry no editable name.
                if not storage.is_storage_value(pre_uploaded):
                    try:
                        await odata_client.update_resource_version_number(
                            pre_uploaded, next_version,
                        )
                    except Exception as e:
                        logger.warning(
                            "Failed to rename pre-uploaded CSV %s to v%d: %s",
                            pre_uploaded, next_version, e,
                        )
                continue

            if not (res.records and res.fields):
                continue

            # R2 backend: object stores have no datastore, so tabular records
            # are written as a downloadable CSV object.
            if _use_r2(ds):
                # --- append-only: maintain ONE growing cumulative CSV object,
                # mirroring ODATA's shared appendonly_resource_id. Each version
                # reads the current cumulative, appends the new rows, and
                # re-uploads to a STABLE key (overwrite). Every version points
                # at that same r2:<key>; the per-version changelog lives in
                # change_summary.rows_added.
                if is_append:
                    new_rows, seen_keys = compute_new_rows(
                        seen_keys, res.records, append_key
                    )
                    rows_added_total += len(new_rows)
                    existing = ds.appendonly_resource_id  # r2:<key> or None/odata
                    if not new_rows and storage.is_storage_value(existing):
                        # Nothing new — reuse the existing cumulative object.
                        resource_mappings[res.name] = existing
                        odata_resource_ids.append(existing)
                        logger.info("Append(R2): 0 new rows for %s — reuse %s",
                                    res.name, existing)
                        continue
                    try:
                        cumulative: list[dict] = []
                        if storage.is_storage_value(existing):
                            prev_bytes = await storage_client.get_object_bytes(existing)
                            if prev_bytes:
                                _f, cumulative = parse_csv(prev_bytes)
                        cumulative = list(cumulative) + new_rows
                        csv_bytes = records_to_csv_bytes(res.fields, cumulative)
                        # Stable key: reuse the existing object's key, or mint a
                        # fixed one under the dataset's appendonly/ prefix.
                        if storage.is_storage_value(existing):
                            key = storage.key_of(existing)
                        else:
                            key = (
                                f"datasets/{ds.id}/appendonly/"
                                f"{storage._safe_filename(res.name)}.csv"
                            )
                        await storage_client.upload_object(
                            key, file_content=csv_bytes,
                            content_type="text/csv; charset=utf-8",
                        )
                        marked = storage.mark(key)
                        ds.appendonly_resource_id = marked
                        resource_mappings[res.name] = marked
                        odata_resource_ids.append(marked)
                        logger.info(
                            "Append(R2): +%d rows → %d cumulative for %s (%s)",
                            len(new_rows), len(cumulative), res.name, key,
                        )
                    except Exception as e:
                        logger.error("Failed R2 append for %s: %s", res.name, e)
                        push_errors.append(f"r2 append {res.name}: {e}")
                    continue

                # --- full_snapshot: each version is its own immutable object.
                try:
                    csv_bytes = records_to_csv_bytes(res.fields, res.records)
                    key = storage.build_key(
                        str(ds.id), next_version, f"{res.name}.csv"
                    )
                    await storage_client.upload_object(
                        key, file_content=csv_bytes,
                        content_type="text/csv; charset=utf-8",
                    )
                    marked = storage.mark(key)
                    resource_mappings[res.name] = marked
                    odata_resource_ids.append(marked)
                    logger.info("Pushed %d rows for %s to R2 (%s)",
                                len(res.records), res.name, key)
                    # Dual-write the same rows to NEON when opted in.
                    await _neon_load_from_csv(res.name, csv_bytes)
                except Exception as e:
                    logger.error("Failed to push resource %s to R2: %s", res.name, e)
                    push_errors.append(f"r2 {res.name}: {e}")
                continue

            if is_append:
                new_rows, seen_keys = compute_new_rows(seen_keys, res.records, append_key)
                rows_added_total += len(new_rows)
                if not new_rows and ds.appendonly_resource_id:
                    # Nothing new this round — point the version at the existing
                    # shared resource and move on.
                    resource_mappings[res.name] = ds.appendonly_resource_id
                    odata_resource_ids.append(ds.appendonly_resource_id)
                    logger.info("Append: 0 new rows for %s (resource %s)",
                                res.name, ds.appendonly_resource_id)
                    continue

                try:
                    if not ds.appendonly_resource_id:
                        # First version in append mode — create the shared
                        # resource the same way snapshot mode would.
                        odata_result = await odata_client.push_csv_to_datastore(
                            dataset_id=ds.odata_dataset_id,
                            version_number=next_version,
                            resource_name=res.name,
                            fields=res.fields,
                            records=new_rows,
                            resource_format=res.format,
                            timestamp=ts,
                        )
                        ds.appendonly_resource_id = odata_result["id"]
                        rid = odata_result["id"]
                        logger.info("Append: created shared resource %s with %d rows for %s",
                                    rid, len(new_rows), res.name)
                    else:
                        # Subsequent version — insert new rows into the same
                        # resource. Reuse the batched-with-retry helper so we
                        # always send force=True and get the same retry/backoff
                        # behavior as the snapshot path.
                        rid = ds.appendonly_resource_id
                        batches = batch_records(new_rows)
                        for i, batch in enumerate(batches, start=1):
                            await odata_client._push_batch_with_retry(
                                resource_id=rid,
                                fields=res.fields,
                                records_batch=batch,
                                create=False,
                                batch_num=i,
                                is_last=(i == len(batches)),
                            )
                        logger.info("Append: inserted %d new rows into %s for %s",
                                    len(new_rows), rid, res.name)

                    resource_mappings[res.name] = rid
                    odata_resource_ids.append(rid)
                except Exception as e:
                    logger.error("Failed to append resource %s to odata: %s", res.name, e)
                    push_errors.append(f"append {res.name}: {e}")
                continue

            # full_snapshot path (unchanged)
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
                push_errors.append(f"push {res.name}: {e}")

    # GeoJSON resources (already uploaded as separate CKAN resources by the
    # scraper via /upload-geojson) — link them into this version.
    # Worker uploads with version_number=1 hardcoded because it can't
    # know next_version yet — patch each resource's "vN" marker to the
    # version we're about to commit, mirroring the ZIP/CSV paths.
    if body.geojson_resource_ids:
        for rid in body.geojson_resource_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_geojson"] = list(body.geojson_resource_ids)
        logger.info("Linked %d pre-uploaded GeoJSON resource(s)", len(body.geojson_resource_ids))
        for rid in body.geojson_resource_ids:
            if storage.is_storage_value(rid):
                continue  # R2 key — no ODATA resource to rename
            try:
                await odata_client.update_resource_version_number(rid, next_version)
            except Exception as e:
                logger.warning(
                    "Failed to rename pre-uploaded GeoJSON %s to v%d: %s",
                    rid, next_version, e,
                )

    # GeoPackage resources (heavy GovMap layers: GPKG only, uploaded straight
    # to R2 via /upload-r2) — link them into this version. R2-marked keys
    # carry no ODATA resource to rename, so no vN patching is needed.
    if body.gpkg_resource_ids:
        for rid in body.gpkg_resource_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_gpkg"] = list(body.gpkg_resource_ids)
        logger.info("Linked %d pre-uploaded GPKG resource(s)",
                    len(body.gpkg_resource_ids))
    if body.parquet_resource_ids:
        for rid in body.parquet_resource_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_parquet"] = list(body.parquet_resource_ids)
        logger.info("Linked %d pre-uploaded GeoParquet resource(s)",
                    len(body.parquet_resource_ids))

    # ZIP attachment handling: prefer pre-uploaded zip_resource_ids (list of
    # multipart parts), fall back to single zip_resource_id, then inline base64.
    if body.zip_resource_ids:
        for rid in body.zip_resource_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_zip_parts"] = list(body.zip_resource_ids)
        logger.info("Using %d pre-uploaded ZIP part(s)", len(body.zip_resource_ids))
        # Worker uploads with version_number=1 hardcoded (it can't know
        # next_version yet). Now that we do, rewrite each resource's
        # 'v1' marker to match the version we're about to commit.
        for rid in body.zip_resource_ids:
            if storage.is_storage_value(rid):
                continue  # R2 key — no ODATA resource to rename
            try:
                await odata_client.update_resource_version_number(rid, next_version)
            except Exception as e:
                logger.warning("Failed to rename pre-uploaded ZIP %s to v%d: %s",
                               rid, next_version, e)
    elif body.zip_resource_id:
        # Single ZIP was already uploaded via /api/worker/upload-zip
        odata_resource_ids.append(body.zip_resource_id)
        resource_mappings["_zip"] = body.zip_resource_id
        logger.info("Using pre-uploaded ZIP resource %s", body.zip_resource_id)
        if not storage.is_storage_value(body.zip_resource_id):
            try:
                await odata_client.update_resource_version_number(
                    body.zip_resource_id, next_version,
                )
            except Exception as e:
                logger.warning("Failed to rename pre-uploaded ZIP %s to v%d: %s",
                               body.zip_resource_id, next_version, e)
    elif body.zip_file and (ds.odata_dataset_id or _use_r2(ds)):
        try:
            zip_bytes = base64.b64decode(body.zip_file.content_base64)
            from app.services.snapshot_service import _timestamp
            ts_zip = _timestamp()
            if _use_r2(ds):
                # R2: store the ZIP object directly; record the marked key.
                key = storage.build_key(
                    str(ds.id), next_version,
                    body.zip_file.filename or f"v{next_version}_attachments.zip",
                )
                await storage_client.upload_object(
                    key, file_content=zip_bytes, content_type="application/zip",
                )
                zip_resource_id = storage.mark(key)
                logger.info("Uploaded ZIP (%d KB) to R2 (%s)",
                            len(zip_bytes) // 1024, key)
            else:
                zip_result = await odata_client.upload_resource(
                    dataset_id=ds.odata_dataset_id,
                    file_content=zip_bytes,
                    filename=body.zip_file.filename,
                    name=f"{ts_zip} v{next_version} - קבצים מצורפים",
                    description=f"Version {next_version}: {len(body.attachments)} attached files",
                    resource_format="ZIP",
                )
                zip_resource_id = zip_result["id"]
                logger.info("Uploaded ZIP (%d KB) to odata (resource %s)",
                            len(zip_bytes) // 1024, zip_resource_id)
            odata_resource_ids.append(zip_resource_id)
            resource_mappings["_zip"] = zip_resource_id
        except Exception as e:
            logger.error("Failed to upload ZIP: %s", e)
            push_errors.append(f"zip upload: {e}")

    # Compute hash for change detection
    hash_data = json.dumps({
        "resources": [{"name": r.name, "row_count": r.row_count} for r in body.resources],
        "attachments": [{"name": a.name, "url": a.url} for a in body.attachments],
    }, sort_keys=True)
    content_hash = hashlib.sha256(hash_data.encode()).hexdigest()

    resource_mappings["_hashes"] = {"scraper": content_hash}
    resource_mappings["_resource_ids"] = []
    if is_append:
        resource_mappings["_appendonly_seen"] = seen_keys

    # Empty-version guard: if the worker sent payload (records or ZIP) but
    # nothing actually landed on odata, don't pretend a version exists.
    # Surface the reason on the dataset and mark the task as failed so the
    # admin can see what happened.
    expected = (
        len([r for r in body.resources if r.records or (body.csv_resource_ids or {}).get(r.name)])
        + (1 if (body.zip_file or body.zip_resource_id or body.zip_resource_ids) else 0)
        + (1 if body.geojson_resource_ids else 0)
    )
    successes = sum(1 for k in resource_mappings if not k.startswith("_"))
    if expected > 0 and successes == 0:
        msg = "; ".join(push_errors)[:2000] or "all scraper pushes failed (no detail)"
        ds.last_error = msg
        ds.last_polled_at = datetime.now(timezone.utc)
        task_result = await db.execute(
            select(ScrapeTask).where(
                ScrapeTask.tracked_dataset_id == ds.id,
                ScrapeTask.status == "running",
            )
        )
        task = task_result.scalar_one_or_none()
        if task:
            task.status = "failed"
            task.completed_at = datetime.now(timezone.utc)
            task.phase = "push_failed"
            task.error = msg
        await db.commit()
        from app.services.activity_log import log_event
        await log_event(
            event="failed", dataset=ds, status="error", actor="system",
            message="העלאת הגרסה נכשלה (אף משאב לא נשמר)",
            detail=msg,
        )
        logger.error("Aborting scraper version for %s — 0/%d resources succeeded: %s",
                     ds.title, expected, msg)
        raise HTTPException(status_code=502, detail={"error": "all_pushes_failed", "message": msg})

    # Create version
    total_rows = sum(r.row_count for r in body.resources)
    if is_append:
        change_summary = {
            "type": "append",
            "rows_added": rows_added_total,
            "rows_total": len(seen_keys),
            "key": append_key or "_hash",
            "total_attachments": len(body.attachments),
            "resources": [{"name": r.name, "format": r.format, "rows": r.row_count} for r in body.resources],
            "scrape_metadata": body.scrape_metadata,
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        }
    else:
        change_summary = {
            "type": "scraper",
            "total_rows": total_rows,
            "total_attachments": len(body.attachments),
            "resources": [{"name": r.name, "format": r.format, "rows": r.row_count} for r in body.resources],
            "scrape_metadata": body.scrape_metadata,
            "resources_added": odata_resource_ids,
            "resources_removed": [],
            "resources_modified": [],
        }
    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=body.metadata_modified,
        odata_metadata_resource_id=None,
        change_summary=change_summary,
        resource_mappings=resource_mappings,
    )
    db.add(version)
    if push_errors:
        change_summary["errors"] = push_errors

    # Update dataset
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = body.metadata_modified
    ds.last_error = "; ".join(push_errors)[:2000] if push_errors else None
    await db.commit()

    # Refresh the ODATA package description so it carries current links back
    # to the source and to this dataset's over.org.il view. Best-effort — a
    # failure here shouldn't break version creation, it only affects the
    # description text visible on the ODATA dataset page.
    if ds.odata_dataset_id:
        try:
            source_url_for_notes = (
                ds.source_url if ds.source_type == "scraper"
                else f"{settings.data_gov_il_url}/dataset/{ds.ckan_name}"
            )
            tracker_url = f"{settings.app_base_url.rstrip('/')}/versions/{ds.id}"
            notes = odata_client.build_notes(
                source_type=ds.source_type,
                source_url=source_url_for_notes,
                tracker_url=tracker_url,
            )
            await odata_client.package_patch(ds.odata_dataset_id, notes=notes)
        except Exception as e:
            logger.warning("Failed to refresh ODATA notes for %s: %s", ds.id, e)

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

    from app.services.activity_log import log_event
    await log_event(
        event="completed", dataset=ds, status="ok", actor="worker",
        message=f"גירוד הסתיים — גרסה {next_version} נוצרה",
    )

    # Persist checkpoint patch back to scraper_config (archive mode).
    # Done after task commit so a failure here doesn't block version creation.
    if body.scraper_config_patch:
        try:
            current = dict(ds.scraper_config or {})
            current.update(body.scraper_config_patch)
            ds.scraper_config = current
            await db.commit()
        except Exception as e:
            logger.warning("Failed to save scraper_config_patch for %s: %s", ds.id, e)

    logger.info("Scraper version %d created for %s (%d rows)", next_version, ds.title, total_rows)

    return {
        "version_id": str(version.id),
        "version_number": next_version,
        "odata_resource_ids": odata_resource_ids,
        "message": f"Version {next_version} created with {total_rows} records",
    }


# ── Direct-to-R2 presigned multipart uploads ──────────────────────────────
# Multi-GB scraper outputs (GovMap heavy layers: 3.6GB CSV / 3.9GB GeoJSON)
# can't be POSTed through this server: over.org.il is fronted by Cloudflare
# and the giant request destabilises the dyno — observed as 502s on
# /upload-csv that also starve every task's progress reports for >10 min,
# tripping the stuck-task watchdog. These endpoints only ORCHESTRATE an S3
# multipart upload against R2; the worker PUTs each part directly to the
# presigned R2 URL, so the file bytes never touch this server. The completed
# object is referenced exactly like an /upload-csv | /upload-geojson R2
# result: an "r2:<key>" marker passed to push-version.


class R2StartBody(BaseModel):
    filename: str
    content_type: str | None = None
    version_number: int = 1  # placeholder, like the other pre-upload paths


class R2PartUrlBody(BaseModel):
    key: str
    upload_id: str
    part_number: int


class R2CompleteBody(BaseModel):
    key: str
    upload_id: str
    parts: list[dict]           # [{"part_number": n, "etag": "..."}]
    row_count: int = 0
    compression: str | None = None


class R2AbortBody(BaseModel):
    key: str
    upload_id: str


def _require_r2_key(key: str) -> None:
    """Presign/complete only object keys our own build_key produces —
    a worker-key holder shouldn't be able to write arbitrary bucket paths."""
    if not key.startswith("datasets/") or ".." in key:
        raise HTTPException(status_code=400, detail="Bad object key")


@router.post("/upload-r2/start/{tracked_dataset_id}")
@limiter.limit("60/minute")
async def r2_upload_start(
    request: Request,
    tracked_dataset_id: str,
    body: R2StartBody,
    db: AsyncSession = Depends(get_db),
):
    _verify_worker_key(request)
    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uuid.UUID(tracked_dataset_id))
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if not storage_client.is_configured():
        raise HTTPException(status_code=400, detail="R2 storage not configured")
    key = storage.build_key(str(ds.id), body.version_number, body.filename)
    upload_id = await storage_client.create_multipart(key, body.content_type)
    logger.info("R2 multipart started for %s: %s", ds.title, key)
    return {"key": key, "upload_id": upload_id,
            "part_size": 100 * 1024 * 1024}


@router.post("/upload-r2/part-url")
@limiter.limit("600/minute")
async def r2_upload_part_url(request: Request, body: R2PartUrlBody):
    _verify_worker_key(request)
    _require_r2_key(body.key)
    if not 1 <= body.part_number <= 10_000:
        raise HTTPException(status_code=400, detail="part_number out of range")
    url = await storage_client.presign_part(body.key, body.upload_id,
                                            body.part_number)
    return {"url": url}


@router.post("/upload-r2/complete")
@limiter.limit("60/minute")
async def r2_upload_complete(request: Request, body: R2CompleteBody):
    _verify_worker_key(request)
    _require_r2_key(body.key)
    if not body.parts:
        raise HTTPException(status_code=400, detail="No parts")
    parts = sorted(
        ({"PartNumber": int(p["part_number"]), "ETag": str(p["etag"])}
         for p in body.parts),
        key=lambda p: p["PartNumber"],
    )
    try:
        await storage_client.complete_multipart(body.key, body.upload_id, parts)
    except Exception as e:
        logger.exception("R2 multipart complete failed for %s", body.key)
        raise HTTPException(status_code=502, detail=f"complete failed: {e}")
    size = await storage_client.object_size(body.key)
    return {
        "resource_id": storage.mark(body.key),
        "size": size or 0,
        "rows": body.row_count,
        "compression": body.compression or "none",
        "datastore": "skipped (r2 direct — file only, no queryable table)",
        "upload_mode": "r2-direct",
    }


@router.post("/upload-r2/abort")
@limiter.limit("60/minute")
async def r2_upload_abort(request: Request, body: R2AbortBody):
    _verify_worker_key(request)
    _require_r2_key(body.key)
    await storage_client.abort_multipart(body.key, body.upload_id)
    return {"status": "aborted"}


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
    if not ds or not (ds.odata_dataset_id or _use_r2(ds)):
        raise HTTPException(status_code=404, detail="Dataset not found or no storage backend")

    from app.services.snapshot_service import _timestamp
    ts_zip = _timestamp()

    # R2 backend: store the ZIP directly in the object store and return a
    # marked key (r2:<key>) as the "resource_id". The worker passes it back
    # in push-version exactly like an ODATA resource_id; the marker lets the
    # version/download/delete paths route it to R2.
    #
    # The upload is STREAMED to a temp file and handed to boto3's managed
    # multipart transfer (file_path) — constant memory regardless of size, so
    # the 1GB R2 parts don't OOM the dyno (the ODATA path below stays ≤80MB and
    # can afford the in-memory read).
    if _use_r2(ds):
        import os as _os
        import tempfile as _tempfile
        part_label = (
            f"_part{part}of{total_parts}"
            if part is not None and total_parts is not None and total_parts > 1
            else ""
        )
        key = storage.build_key(
            str(ds.id), version_number,
            (file.filename or f"v{version_number}_attachments{part_label}.zip"),
        )
        tmp_dir = "/tmp/upload_zip"
        _os.makedirs(tmp_dir, exist_ok=True)
        tmp_path = _os.path.join(tmp_dir, uuid.uuid4().hex + ".zip")
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    size += len(chunk)
            await storage_client.upload_object(
                key, file_path=tmp_path, content_type="application/zip",
            )
            logger.info("Uploaded ZIP %s (%d KB) → R2 %s (streamed)",
                        f"part {part}/{total_parts}" if total_parts else "(single)",
                        size // 1024, key)
            return {"resource_id": storage.mark(key), "size": size}
        except Exception as e:
            logger.exception("Failed to upload ZIP to R2")
            raise HTTPException(status_code=502, detail=f"ZIP upload failed: {e}")
        finally:
            try:
                _os.remove(tmp_path)
            except OSError:
                pass

    # ODATA path: parts are ≤80MB here, so the in-memory read is fine.
    zip_bytes = await file.read()

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


class DeleteResourcesBody(BaseModel):
    resource_ids: list[str]


@router.post("/delete-resources/{tracked_dataset_id}")
@limiter.limit("30/minute")
async def delete_resources(
    request: Request,
    tracked_dataset_id: str,
    body: DeleteResourcesBody,
    db: AsyncSession = Depends(get_db),
):
    """Worker rollback for a failed publish.

    ZIP/CSV/GeoJSON resources are uploaded to ODATA (via /upload-zip etc.)
    BEFORE /push-version commits the VersionIndex. If a task dies after those
    uploads but before push-version (the common failure mode on huge datasets
    that time out / get auto-reset mid-run), those resources are left orphaned —
    no version references them — and every failed retry leaks another full set.
    The worker calls this on its failure path to delete what it just uploaded.

    Safety: a resource referenced by ANY committed version of this dataset is
    never deleted, so a stale or duplicated rollback call can never destroy live
    data — it can only remove genuinely-orphaned resources.
    """
    _verify_worker_key(request)

    try:
        ds_id = uuid.UUID(tracked_dataset_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid dataset ID")

    # Every resource_id referenced by any version of this dataset is off-limits.
    from app.api.versions import _extract_resource_ids
    result = await db.execute(
        select(VersionIndex).where(VersionIndex.tracked_dataset_id == ds_id)
    )
    referenced: set[str] = set()
    for v in result.scalars().all():
        for rid in _extract_resource_ids(v.resource_mappings):
            referenced.add(rid)
        if v.odata_metadata_resource_id:
            referenced.add(v.odata_metadata_resource_id)

    deleted, skipped, failed = 0, 0, 0
    for rid in body.resource_ids:
        if not rid:
            continue
        if rid in referenced:
            skipped += 1  # belongs to a real version — never touch
            continue
        try:
            await odata_client.resource_delete(rid)
            deleted += 1
        except Exception as e:
            failed += 1
            logger.warning("rollback resource_delete(%s) failed: %s", rid, e)

    logger.info(
        "Worker rollback for dataset %s: %d deleted, %d kept (referenced), %d failed",
        tracked_dataset_id, deleted, skipped, failed,
    )
    return {"deleted": deleted, "skipped_referenced": skipped, "failed": failed}


@router.post("/upload-geojson/{tracked_dataset_id}")
@limiter.limit("30/minute")
async def upload_geojson(
    request: Request,
    tracked_dataset_id: str,
    file: UploadFile = File(...),
    version_number: int = Form(...),
    resource_name: str | None = Form(None),
    compression: str | None = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Worker uploads a single .geojson file (a FeatureCollection in WGS84) as
    a standalone CKAN resource with format=GeoJSON. Returns the odata
    resource_id, which the worker references in /push-version via
    `geojson_resource_ids`. Used for GovMap layers so the geometry shows up
    on the dataset page as a separate, downloadable GeoJSON resource rather
    than being buried inside an attachments ZIP.

    When the worker sends ``compression=gzip`` (the default since 2026-05;
    see govil-scraper over_worker.upload_geojson), the request body is
    already gzip-compressed and we forward the bytes to odata as-is.
    GeoJSON compresses ~5×, which is what lets a 200 MB layer fit under
    odata's ~100 MB CKAN resource_create limit — without this we'd get
    HTTP 413 and the version would land with a CSV but no map data.
    The frontend (GovmapView) decompresses on fetch via DecompressionStream.
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
    if not ds or not (ds.odata_dataset_id or _use_r2(ds)):
        raise HTTPException(status_code=404, detail="Dataset not found or no storage backend")

    body_bytes = await file.read()
    from app.services.snapshot_service import _timestamp
    ts = _timestamp()

    is_gzip = (compression or "").lower() == "gzip" or (
        file.filename or "").lower().endswith(".gz")
    # Preserve the .gz suffix on the filename when uploading gzipped so
    # browsers / tooling pick up the right content semantics. Strip it
    # for the human-readable resource label.
    raw_filename = file.filename or (
        f"v{version_number}.geojson.gz" if is_gzip else f"v{version_number}.geojson"
    )
    label_base = raw_filename
    if label_base.lower().endswith(".gz"):
        label_base = label_base[:-3]
    if label_base.lower().endswith(".geojson"):
        label_base = label_base[:-8]
    label = (resource_name or "").strip() or label_base or "GeoJSON"

    # R2 backend: store the GeoJSON object directly. Keeps the .gz suffix on
    # the key when gzipped so GovmapView's fetch + DecompressionStream still
    # works; we deliberately do NOT set ContentEncoding (that would make the
    # CDN auto-inflate and break the client-side decompression contract).
    if _use_r2(ds):
        key = storage.build_key(str(ds.id), version_number, raw_filename)
        try:
            await storage_client.upload_object(
                key, file_content=body_bytes,
                content_type="application/gzip" if is_gzip else "application/geo+json",
            )
            logger.info("Uploaded GeoJSON %s (%d KB%s) → R2 %s",
                        raw_filename, len(body_bytes) // 1024,
                        ", gzipped" if is_gzip else "", key)
            return {"resource_id": storage.mark(key), "size": len(body_bytes)}
        except Exception as e:
            logger.exception("Failed to upload GeoJSON to R2")
            raise HTTPException(status_code=502, detail=f"GeoJSON upload failed: {e}")

    try:
        result_resource = await odata_client.upload_resource(
            dataset_id=ds.odata_dataset_id,
            file_content=body_bytes,
            filename=raw_filename,
            name=f"{ts} v{version_number} - {label}",
            description=(
                f"Version {version_number}: {label} (GeoJSON"
                + (" — gzipped)" if is_gzip else ")")
            ),
            resource_format="GeoJSON",
        )
        logger.info(
            "Uploaded GeoJSON %s (%d KB%s) → resource %s",
            raw_filename, len(body_bytes) // 1024,
            ", gzipped" if is_gzip else "",
            result_resource["id"],
        )
        return {"resource_id": result_resource["id"], "size": len(body_bytes)}
    except Exception as e:
        logger.exception("Failed to upload GeoJSON")
        raise HTTPException(status_code=502, detail=f"GeoJSON upload failed: {e}")


@router.post("/upload-csv/{tracked_dataset_id}")
@limiter.limit("30/minute")
async def upload_csv(
    request: Request,
    tracked_dataset_id: str,
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
    if not ds or not (ds.odata_dataset_id or _use_r2(ds)):
        raise HTTPException(status_code=404, detail="Dataset not found or no storage backend")

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

    # ---- R2 backend: store the CSV as a downloadable object and return ----
    # Object stores have no datastore, so the CSV is served as a file (direct
    # download from R2's public domain) rather than a queryable table. This is
    # the file-only side of full decoupling; row-level querying would need a
    # separate layer (Datasette/DuckDB) and is intentionally out of scope.
    if _use_r2(ds):
        from app.services.snapshot_service import _timestamp
        ts = _timestamp()
        safe_name = (resource_name or "data").replace("/", "_").replace("\\", "_")
        csv_size = os.path.getsize(csv_path)
        key = storage.build_key(str(ds.id), version_number, f"{safe_name}.csv")
        try:
            await storage_client.upload_object(
                key, file_path=csv_path, content_type="text/csv; charset=utf-8",
            )
        except Exception as e:
            logger.exception("Failed to upload CSV to R2")
            _cleanup_paths(gz_path, csv_path)
            raise HTTPException(status_code=502, detail=f"CSV upload failed: {e}")
        _cleanup_paths(gz_path, csv_path)
        logger.info("Uploaded CSV (%d KB, ~%d rows) → R2 %s",
                    csv_size // 1024, row_count, key)
        return {
            "resource_id": storage.mark(key),
            "size": csv_size,
            "rows": row_count,
            "compression": compression or "none",
            "datastore": "skipped (r2 backend — file only, no queryable table)",
            "upload_mode": "r2",
        }

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
        # The plain CSV blew past CKAN's file-upload limit. Before
        # falling back to a datastore-only resource (which leaves
        # users with a dead Download button if the BackgroundTask
        # datastore push fails or is killed by a dyno restart) try
        # one more thing: gzip the CSV and upload the compressed
        # blob. CSVs with repetitive content (HTML descriptions,
        # repeated codes, etc.) typically gzip 5-10x — a 239 MB
        # plain CSV becomes 25-50 MB gzipped, comfortably under the
        # 90 MB ceiling. The user gets an honest downloadable file
        # they can decompress locally; the datastore push still
        # happens in the background but is no longer the only path
        # to the data.
        gzipped_path = csv_path + ".gz"
        gz_size = 0
        try:
            import gzip as _gzip
            with open(csv_path, "rb") as plain, _gzip.open(
                gzipped_path, "wb", compresslevel=6
            ) as gz_out:
                shutil.copyfileobj(plain, gz_out, length=256 * 1024)
            gz_size = os.path.getsize(gzipped_path)
            logger.info(
                "Pre-upload gzip: %d MB CSV → %d MB .csv.gz (%.1fx)",
                csv_size // 1024 // 1024,
                gz_size // 1024 // 1024,
                csv_size / max(gz_size, 1),
            )
        except Exception as e:
            logger.warning(
                "Could not gzip CSV (%s) — falling through to datastore-only path",
                e,
            )
            try:
                os.remove(gzipped_path)
            except OSError:
                pass
            gz_size = csv_size + 1  # force the "doesn't fit" branch below

        if gz_size <= FILE_UPLOAD_LIMIT:
            try:
                gz_filename = filename + ".gz"
                csv_resource = await odata_client.upload_resource(
                    dataset_id=ds.odata_dataset_id,
                    filename=gz_filename,
                    file_path=gzipped_path,
                    name=f"{ts} v{version_number} - {safe_name}",
                    description=(
                        f"Version {version_number} ({ts}): {resource_name} "
                        f"({row_count} rows). The plain CSV ({csv_size // 1024 // 1024} MB) "
                        f"exceeded ODATA's direct-upload limit, so it is "
                        f"served as gzip-compressed CSV "
                        f"({gz_size // 1024 // 1024} MB). Decompress with "
                        f"`gunzip` (Linux/macOS) or 7-Zip (Windows) after "
                        f"download. The same data is also queryable via the "
                        f"datastore API once the background ingest completes."
                    ),
                    resource_format="CSV",
                )
                resource_id = csv_resource["id"]
                upload_file = True  # so we don't enter the datastore-only branch
                upload_mode = "file-gz+datastore"
                logger.info(
                    "Uploaded gzipped CSV (%d MB → %d MB) → resource %s",
                    csv_size // 1024 // 1024,
                    gz_size // 1024 // 1024,
                    resource_id,
                )
            except Exception as e:
                logger.warning(
                    "Gzipped CSV upload failed (%s) — falling back to datastore-only",
                    e,
                )
            finally:
                try:
                    os.remove(gzipped_path)
                except OSError:
                    pass
        else:
            # Gzip didn't get us under the limit (extremely compressible
            # data would have, so this branch is rare). Drop the gz on
            # disk before continuing.
            try:
                os.remove(gzipped_path)
            except OSError:
                pass

    if not upload_file:
        # Even gzip didn't fit. Last-resort: create a resource pointing
        # at CKAN's built-in datastore dump endpoint. The Download
        # button works ONLY if the background datastore push below
        # succeeds — if the worker is recycled mid-push (Render restart,
        # OOM, idle scale-down), the resource will be left orphaned
        # with no downloadable content. We can't avoid that without a
        # durable job queue, so this branch is a known fragile path.
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

    # ---- Step 2: Enqueue a durable push job for the datastore ingest ----
    # Previously this used FastAPI's BackgroundTasks, which silently
    # dies on Render dyno recycles mid-push. The new path persists a
    # row in datastore_push_jobs; the runner in
    # app/worker/datastore_push_runner.py drains pending jobs every
    # 30s and survives restarts (the row stays "pending" until a
    # worker actually picks it up).
    if fields:
        from app.worker.datastore_push_runner import enqueue as _enqueue_push
        # csv_is_gzipped_in_source tells the runner's /tmp-recovery
        # path whether to gunzip the bytes it pulls back from ODATA.
        # Only "file-gz+datastore" puts a gzipped file in the
        # downloadable resource; "file+datastore" (small CSV) puts a
        # plain one, and "datastore-only" (worst case) has no
        # recoverable file at all (recovery will fail and the job
        # will be marked failed, which is the correct surface for
        # the admin UI).
        gzipped_in_source = upload_mode == "file-gz+datastore"
        await _enqueue_push(
            db=db,
            tracked_dataset_id=ds.id,
            resource_id=resource_id,
            csv_path=csv_path,
            csv_is_gzipped_in_source=gzipped_in_source,
            fields=fields,
            total_rows=row_count or None,
        )
        await db.commit()
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

    # Cap to the column widths (phase String(50), message String(500)) so an
    # over-length progress report can't fail the commit (StringDataRightTruncation)
    # and strand the task mid-run.
    task.phase = (body.phase or "")[:50]
    task.progress = body.percentage
    task.message = (body.message or "")[:500]
    # Keep the running machine's identity current — the worker posting progress
    # IS the machine doing the work, and this backfills tasks assigned before
    # these fields existed.
    from app.client_ip import get_client_ip
    worker_ip = get_client_ip(request)
    if worker_ip and worker_ip != "unknown":
        task.worker_ip = worker_ip[:64]
    worker_id = (request.headers.get("x-worker-id") or "").strip()[:64]
    if worker_id:
        task.worker_id = worker_id
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

    ds_row = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == task.tracked_dataset_id)
    )).scalar_one_or_none()
    from app.services.activity_log import log_event
    await log_event(
        event="failed", dataset=ds_row,
        dataset_id=str(task.tracked_dataset_id),
        status="error", actor="worker",
        message=f"גירוד נכשל בשלב «{body.phase}»",
        detail=body.error,
    )

    logger.warning("Scrape task %s failed: %s", task_id, body.error)
    return {"status": "failed"}


class CompleteLocalReport(BaseModel):
    message: str = ""
    file_count: int = 0
    record_count: int = 0


@router.post("/complete-local/{task_id}")
@limiter.limit("30/minute")
async def complete_local(
    request: Request,
    task_id: str,
    body: CompleteLocalReport,
    db: AsyncSession = Depends(get_db),
):
    """Mark a task done in 'local_only' mode: the worker scraped + downloaded the
    files to its own machine and deliberately skipped the ODATA upload + version
    (per the dataset's upload_mode). No version is created — this is a clean
    terminal state so the task isn't left 'running' (auto-reset) or flagged as a
    failure. The informational message records the local path + counts."""
    _verify_worker_key(request)

    try:
        tid = uuid.UUID(task_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid task ID")

    result = await db.execute(select(ScrapeTask).where(ScrapeTask.id == tid))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    task.status = "completed"
    task.phase = "completed_local"
    task.progress = 100
    task.message = (body.message or "הורדה מקומית בלבד (ללא העלאה ל-ODATA)")[:500]
    task.error = None
    task.completed_at = datetime.now(timezone.utc)
    await db.commit()

    ds_row = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == task.tracked_dataset_id)
    )).scalar_one_or_none()
    from app.services.activity_log import log_event
    await log_event(
        event="completed", dataset=ds_row, dataset_id=str(task.tracked_dataset_id),
        status="ok", actor="worker",
        message=f"גירוד מקומי הסתיים ({body.file_count} קבצים, ללא העלאה ל-OVER)",
    )

    logger.info("Scrape task %s completed locally (no upload): %s files, %s records — %s",
                task_id, body.file_count, body.record_count, task.message)
    return {"status": "completed_local"}
