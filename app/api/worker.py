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
import re
import tempfile as _tempfile
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, Response, UploadFile
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.models.scrape_task import ScrapeTask
from app.models.source_registry import SourceRegistry
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services import (append_store, blocked_resources, sampling_runs,
                          source_registry)
from app.services.archive_state import ROW_ARCHIVE_KEYS
from app.services.source_load import saturated_sources, source_filter
from app.services.worker_fleet import touch_worker
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


def _require_file_backend(ds, kind: str) -> None:
    """Refuse a FILE upload for a dataset that has nowhere to put one — saying
    which of the two reasons it is.

    A NEON-only dataset (storage plan ``neon``) archives its rows in the append
    DB and writes no file snapshot, so ``odata_dataset_id`` is null BY DESIGN
    and R2 is not its backend. The upload endpoints used to fold that into the
    not-found check — ``404 "Dataset not found or no storage backend"`` for a
    dataset that plainly exists — and the worker, which retries only on 5xx,
    reported the instant 4xx to the operator as a probable server OOM or a
    deploy in progress. Two conditions deserve two answers: the caller now gets
    409 with the actual reason, and "not found" means not found.

    Note that /upload-csv does NOT call this: rows are exactly what a NEON-only
    dataset CAN take (see the neon branch there).
    """
    if ds.odata_dataset_id or _use_r2(ds):
        return
    if not storage.dataset_stores_files(ds):
        raise HTTPException(status_code=409, detail=(
            f"NEON-only dataset has no file store for {kind} — its archive is "
            f"the queryable row table, not files. Switch the storage plan to "
            f"r2 / r2+neon if this version needs to keep files."
        ))
    raise HTTPException(status_code=409, detail=(
        f"Dataset has no storage backend for {kind}: no ODATA mirror, and R2 "
        f"is not configured for it."
    ))


def _poll_scraper_config(ds, task=None) -> dict:
    """Build the scraper_config sent to the worker in the /poll response.

    Starts from the dataset's stored config, merges the task's per-RUN
    ``params`` over it (migration 047), and fills defaults the worker
    relies on.

    The run params are merged LAST and win: they are how one dataset is sampled
    several ways — the whole register, only what is new, only the files at a
    given status, a single file — without forking the dataset or mutating its
    stored config. A task with no params is the routine full poll, byte for byte
    what it was before this existed, and a scraper that reads none of the keys
    is unaffected by their presence.

    Defaults filled here:

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
    params = getattr(task, "params", None) or {}
    if isinstance(params, dict):
        cfg.update(params)
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
    # The GovMap layer's documentation bundle (OGC SLD symbology + the field
    # dictionary that maps its machine names to Hebrew aliases). Its own key so
    # it stops masquerading as "קבצים מצורפים": it documents the layer, it is
    # not a payload the source published. Older workers have no such field and
    # ship it through `zip_resource_ids` — `_split_doc_bundles` recovers those.
    symbology_resource_ids: list[str] | None = None
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
    # Batched archive: this is one of several versions the SAME task will push
    # (a size-capped file corpus, one version per batch). When true, create the
    # version but leave the task RUNNING — otherwise push_version marks it
    # completed after batch 1, and every later batch's push then trips the
    # no-running-task guard and is rejected. The worker sends false on the last
    # batch, which completes the task as usual.
    more_batches: bool = False
    # Set by the worker on the FINAL push of a multi-batch run: the item-key
    # column to consolidate on. When present (and the run produced >1 version),
    # push_version schedules a background merge of the per-batch versions into
    # ONE — reusing consolidate_dataset_versions — so a big bootstrap ends as a
    # single version + one deduped NEON table without a manual admin step.
    consolidate_dedup_key: str | None = None
    # This run sampled a SUBSET of the corpus on purpose (a single file, the
    # files at one status, only what is new — see scrape_tasks.params). The
    # version is real history, but it is not a measurement of the whole corpus:
    # the shrink guard must not compare it against a full pass, and no later
    # full pass may be compared against it. Recorded on the version so both the
    # guard and the UI can tell the two kinds apart.
    partial_run: bool = False
    # Which run mode produced it — free text, for the version's change_summary
    # and the activity log ("only what's new", "status=נדונה בוועדת המשנה", …).
    run_mode: str | None = None
    # And WHICH named group it was aimed at, when it was aimed at one. A group
    # run reaches the engine as run_mode="status" (that is what the scraper
    # calls reading a named list), so without this a version says "partial, by
    # status" with no status and no group — and nothing on the page tells a
    # reader whether its 4,690 rows are the publication clocks or the year's
    # movers. See app/services/sampling_runs.py.
    run_group: str | None = None

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

# Arbitrary but fixed: the advisory-lock namespace for "someone is claiming a
# scrape task right now". Any other advisory lock in this codebase must pick a
# different number.
_CLAIM_LOCK_KEY = 8_310_557


async def _acquire_claim_lock(db: AsyncSession) -> bool:
    """Take the fleet-wide claim lock for this transaction. True if we got it.

    Postgres-only by nature; the SQLite test DB has no advisory locks and no
    concurrency to protect against, so it always succeeds there.
    """
    if db.bind.dialect.name != "postgresql":
        return True
    return bool(await db.scalar(
        text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": _CLAIM_LOCK_KEY}
    ))


def next_pending_task_q(exclude_sources: Iterable[str] = ()):
    """The claim order: highest priority band first, oldest-first within a band.

    Factored out of ``poll_for_task`` so the ordering can be tested directly
    and so the admin queue panel can render the queue in the same order the
    worker will actually drain it — a panel sorted differently from the claim
    would show the wrong task as "next".

    This ordering is what lets one queue carry both routine polls and a
    whole-catalog GovMap backfill: hundreds of band-0 tasks can sit in the
    queue without pushing back a single routine poll, because a band-0 task is
    only ever claimed when nothing above it is pending.

    ``exclude_sources`` drops every task belonging to a source that is already
    at its worker cap (app/services/source_load.py). Skipping it here — rather
    than claiming and refusing — is what keeps a cap from ever interrupting a
    scrape: a capped source's tasks simply stay pending, and the next-best task
    from another source goes out instead. A source with no cap is never in this
    set, so an unconfigured system builds the same query it always did.
    """
    q = (
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "pending")
    )
    for key in exclude_sources:
        q = q.where(~source_filter(key))
    return q.order_by(ScrapeTask.priority.desc(), ScrapeTask.created_at.asc()).limit(1)


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

    # Record the poll, and honour a drain if this machine is paused.
    #
    # This is the whole per-worker pause: /poll is only ever asked when a worker
    # wants its NEXT task, so answering 204 stops it taking more work while the
    # task it already holds runs to completion. Nothing is killed — which is the
    # point, since a heavy GovMap layer can be an hour in.
    #
    # A paused worker keeps polling and keeps refreshing last_seen_at, so the
    # panel can show it as alive-and-idle: the state to wait for before
    # restarting it to pick up new code.
    from app.client_ip import get_client_ip
    node = await touch_worker(
        db,
        worker_id=request.headers.get("x-worker-id"),
        worker_ip=get_client_ip(request),
        worker_version=worker_version,
        worker_upstream=worker_upstream,
    )
    if node is not None and node.paused:
        logger.info("Worker %s is paused by an admin — dispatching nothing", node.worker_key)
        return Response(status_code=204)
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
        # An interrupted run, not a failed scrape — see PHASE_INTERRUPTED.
        from app.models.scrape_task import INTERRUPTED_MESSAGE, PHASE_INTERRUPTED
        stuck_task.status = "failed"
        stuck_task.phase = PHASE_INTERRUPTED
        age_min = int((now - stuck_task.created_at).total_seconds() / 60) if stuck_task.created_at else 0
        hb_min = int((now - stuck_task.updated_at).total_seconds() / 60) if stuck_task.updated_at else age_min
        stuck_task.error = INTERRUPTED_MESSAGE.format(hb=hb_min, age=age_min)
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
        # Serialize the CLAIM (not the poll) across workers. Without this the
        # per-source cap is only advisory: two workers reading "1 of 2 running"
        # in the same instant would both claim, and the source would run 3 deep.
        # SKIP LOCKED protects one ROW from a double claim; nothing protected a
        # COUNT taken before the claim.
        #
        # try_ rather than plain advisory_xact_lock: a poll must never block on
        # another poll. Losing the race means "not this second" — the worker is
        # back in a second anyway, and 204 is a response it already handles.
        # Re-taken each iteration because a datacollector deletion below commits,
        # and an xact lock ends with its transaction.
        if not await _acquire_claim_lock(db):
            return Response(status_code=204)
        # Sources already at their worker cap are excluded from the query rather
        # than claimed-then-refused, so a cap never interrupts work in flight.
        blocked = await saturated_sources(db)
        result = await db.execute(
            next_pending_task_q(blocked.keys())
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
    worker_ip = get_client_ip(request)
    worker_id = (request.headers.get("x-worker-id") or "").strip()[:64]
    if worker_ip and worker_ip != "unknown":
        task.worker_ip = worker_ip
    if worker_id:
        task.worker_id = worker_id
    # Prefer the explicit machine id (distinguishes workers behind a shared IP);
    # fall back to the IP for older workers that don't send X-Worker-Id.
    who = worker_id or (worker_ip if worker_ip and worker_ip != "unknown" else "")
    # WHICH CODE the machine is running, next to which machine it is. Freshness
    # is self-reported (the worker compares HEAD to origin itself) and only an
    # explicit "behind" is refused, so a worker running an old commit while
    # reporting "current" — or reporting nothing — is dispatched normally and
    # produces versions in an old shape. That is invisible from the queue unless
    # the SHA is shown: two machines on different SHAs is the tell, and it is
    # what a fleet-wide "did everyone pull?" check reads.
    stamp = worker_code_stamp(worker_version, worker_upstream)
    task.message = (f"Assigned to worker {who} {stamp}" if who
                    else f"Assigned to worker {stamp}")
    await db.commit()

    from app.services.activity_log import log_event
    await log_event(
        event="started", dataset=ds, status="info", actor="worker",
        message="גירוד התחיל (המשימה נמסרה ל-worker)",
        # In the log as well as on the task: the log is the copy still there
        # after the task row is gone, when a version turns out to have been
        # produced by the wrong commit.
        detail=f"worker={who or 'unknown'} code={stamp}",
    )

    # Previous version's row count — lets the worker (a) fail FAST on heavy
    # layers when its high-fidelity engine is unavailable and (b) skip the
    # GB-scale uploads for a partial that this server's shrink guard would
    # reject anyway. Same extraction the shrink guard itself uses.
    prev_total_rows = 0
    latest_v = await _shrink_baseline_version(db, ds.id)
    if latest_v is not None:
        try:
            prev_total_rows = int((latest_v.change_summary or {}).get("total_rows") or 0)
        except (ValueError, TypeError):
            prev_total_rows = 0

    return {
        "task_id": str(task.id),
        "tracked_dataset_id": str(ds.id),
        "source_url": ds.source_url,
        "scraper_config": _poll_scraper_config(ds, task),
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


@router.get("/dataset/{dataset_id}/keys")
@limiter.limit("120/minute")
async def dataset_item_keys(
    dataset_id: str,
    request: Request,
    status: str | None = None,
    group: str | None = None,
    limit: int = 5000,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    """The item keys OVER already holds for a dataset — the target list of a
    re-sampling run.

    A run launched as "only the files at status X" (see
    app/services/sampling_runs.py) carries the SELECTOR, not the list: a status
    can hold tens of thousands of items and a task row is no place for them. The
    worker pulls the list here, paged, and re-samples exactly those items.

    Read as ITEMS, not rows: the archive keeps every sample of every item, so
    the status filter is applied to each item's LATEST sample. Asking for
    "נדונה בוועדת המשנה" returns the files that are there now — not every file
    that ever passed through it.

    Worker-authenticated (this is the queue's own API), and read-only.
    """
    _verify_worker_key(request)
    try:
        uid = uuid.UUID(dataset_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="dataset_id must be a UUID")
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    spec = sampling_runs.sampling_spec(ds)
    if not spec:
        raise HTTPException(status_code=409,
                            detail="Dataset declares no sampling spec")
    table = await sampling_runs.resolve_table(ds, db)
    if not table:
        raise HTTPException(status_code=409, detail="Dataset has no archive table yet")
    # A named GROUP is the source's own selector — "the publication clocks",
    # "everything that moved in the past year" — resolved here rather than being
    # spelled out in the task, so the run and this endpoint cannot disagree
    # about what the group means. The activity window inside it resolves to an
    # absolute date on every call, which is why it is computed and not stored.
    filters: dict = {}
    if group:
        try:
            filters = sampling_runs.group_filters(ds, group)
        except sampling_runs.SamplingError as e:
            raise HTTPException(status_code=400, detail=str(e))
    else:
        filters = {"value_col": spec.get("status_column") if status else None,
                   "value": status}

    keys, total = await append_store.latest_item_keys(
        table,
        key_col=spec["item_key"],
        order_col=spec.get("sample_column"),
        limit=limit,
        offset=offset,
        **filters,
    )
    return {
        "dataset_id": str(ds.id),
        "column": spec["item_key"],
        "status": status,
        "group": group,
        "keys": keys,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


async def _shrink_baseline_version(db: AsyncSession, dataset_id: uuid.UUID):
    """The newest version that is a FULL pass, for the shrink guard to measure
    against — or None.

    A partial run (``params.run_mode`` other than the whole corpus: one file, the
    files at one status, only what is new) publishes a version holding just what
    it sampled. Those versions are real history, but they are not a measurement
    of the corpus, so using one as the baseline would defeat the guard for every
    later run: after a single-file sample the baseline would read 1 row and any
    collapse would pass. Skip them; if a dataset has only partial versions there
    is nothing meaningful to compare against and the guard stays out of the way.
    """
    rows = (await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == dataset_id)
        .order_by(VersionIndex.version_number.desc())
        .limit(25)
    )).scalars().all()
    for v in rows:
        if not (v.change_summary or {}).get("partial_run"):
            return v
    return None


def worker_code_stamp(version: str | None, upstream: str | None = None) -> str:
    """``[3f9c1d2]`` — which commit the machine reporting this is running.

    Rendered on EVERY task card, not just at assignment: the whole value is
    seeing a mixed-SHA fleet at a glance, and a stamp present on one card and
    absent on the others reads as "no information" rather than "same code".
    The worker sends X-Worker-Version as a session-level header, so every
    request it makes carries it — including the progress reports that overwrite
    the assignment message.

    The upstream verdict is appended only when it is NOT "current": an assigned
    task's worker cannot be "behind" (dispatch refuses that), so printing the
    normal case would be noise on every row, while "unknown" is worth seeing.
    """
    sha = (version or "").strip()[:7]
    if not sha:
        return "[no version]"
    verdict = (upstream or "").strip().lower()
    return f"[{sha}]" if verdict in ("", "current") else f"[{sha}/{verdict}]"


def neon_per_resource(scraper_config: dict | None, tabular_names: list[str]) -> bool:
    """Whether this dataset's NEON rows go into one table PER RESOURCE.

    Pure, so the rule is testable on its own. It RATCHETS: a dataset that has
    ever published several tabular resources keeps per-resource tables forever,
    recorded as ``scraper_config.neon_tables_per_resource``. Deciding per
    version instead would send a partial scrape that happened to return one
    resource back into the single merged table, quietly re-creating the mixed
    grains the split exists to prevent."""
    return bool(
        (scraper_config or {}).get("neon_tables_per_resource")
        or len(tabular_names) > 1
    )


async def _record_short_load(table: str, ds_id, expected: int,
                             res_name: str = "", force: bool = False) -> None:
    """Write a warning onto the dataset when a table holds fewer rows than the
    version that filled it promised.

    Shared by every loader, because the first version of this check was added to
    two of the three and the one it missed is the one that failed: the parcel
    layer's version reported 1,097,775 rows over a table holding 150,000, and
    the dataset page said nothing, because its loader was the third.

    One-directional and so free of false alarms — an append table accumulates
    across versions and samples, so it may hold far MORE than any single
    version's count and can never legitimately hold less.
    """
    if ds_id is None or expected <= 0:
        return
    try:
        held = await append_store.table_count_estimate(table)
    except Exception as e:  # noqa: BLE001 — a check must not become the failure
        logger.warning("NEON landed-check failed for %s: %s", table, e)
        return
    if not force and (held < 0 or held >= expected * 0.95):
        return
    note = (f"⚠ {res_name or table}: {held:,} שורות בטבלה מתוך {expected:,} "
            f"שנקלטו בגרסה — הטעינה ל-NEON חלקית")
    try:
        from app.database import async_session
        from app.models.tracked_dataset import TrackedDataset as _TD
        async with async_session() as _db:
            row = (await _db.execute(
                select(_TD).where(_TD.id == ds_id))).scalar_one_or_none()
            if row is not None:
                row.import_warning = note
                row.import_warning_at = datetime.now(timezone.utc)
                await _db.commit()
        logger.error("NEON short load: %s holds ~%d of %d rows", table, held, expected)
    except Exception as e:  # noqa: BLE001
        logger.warning("could not record the short-load warning: %s", e)


async def _sample_column_for(ds_id) -> str | None:
    """The source's own "when was this sampled" column, or None.

    Read here rather than threaded from the caller because both NEON loaders
    already carry ``ds_id`` and nothing else about the dataset — and the answer
    lives in the manifest, which ``sampling_spec`` resolves even for a dataset
    created before it declared one. Never raises: a source that cannot be looked
    up simply keeps the old DO NOTHING behaviour."""
    if ds_id is None:
        return None
    try:
        from app.database import async_session
        from app.models.tracked_dataset import TrackedDataset as _TD
        from app.services import sampling_runs
        async with async_session() as _db:
            ds = (await _db.execute(
                select(_TD).where(_TD.id == ds_id))).scalar_one_or_none()
        if ds is None:
            return None
        return (sampling_runs.sampling_spec(ds) or {}).get("sample_column")
    except Exception as e:  # noqa: BLE001 — an optimisation must not break a load
        logger.debug("sample-column lookup failed for %s: %s", ds_id, e)
        return None


def _open_maybe_gzip(path: str):
    """Open a CSV that may or may not be gzipped, as text, without reading it.

    Which one it is depends on how the bytes got here, and both routes are
    legitimate. /upload-csv receives gzip and decompresses on the way to
    storage, so its object is plain. A CSV too large for that endpoint goes
    straight from the worker to R2 and stays compressed — which is not just
    acceptable but necessary: the national parcel layer is 2.8 GB plain and
    894 MB gzipped, and this dyno's /tmp is capped at 2 GB. Downloading the
    plain form to load it would exceed the disk before it exceeded anything
    else ("Size of temporary storage volume /tmp exceeded the limit of 2GB",
    2026-08-09).

    Sniffed by magic number rather than suffix: the key an object is stored
    under is not a promise about its bytes.
    """
    with open(path, "rb") as probe:
        magic = probe.read(2)
    if magic == b"\x1f\x8b":
        import gzip as _gzip
        return _gzip.open(path, "rt", encoding="utf-8-sig", newline="")
    return open(path, "r", encoding="utf-8-sig", newline="")


async def _neon_stream_load_file(
    table: str, path: str, *, delete_after: bool = False,
    stamp_col: str | None = None,
) -> int:
    """Stream a CSV file on local disk into ``table``, batch by batch. Returns
    the number of rows appended.

    Parsing a whole file into rows at once (parse_csv → N dicts) OOMs / times
    out the 512MB OVER dyno; csv-streaming it in fixed batches, inserting each
    and dropping it, keeps memory bounded to one batch no matter the size —
    which is what makes the multi-GB corpora (mavat entities: ~2.78M rows /
    777MB plain CSV) survivable at all. append_store stores everything as text
    and dedups on row_hash (ON CONFLICT DO NOTHING), so a run interrupted
    halfway is safely re-run.

    Raises on failure; the callers below decide how loud that is.
    """
    from app.services import append_store
    # Sized so ONE batch is ONE insert, rather than a round number that happens
    # to straddle the parameter ceiling.
    #
    # append_rows re-splits anything over _MAX_PARAMS, so a fixed 5000 rows over
    # a 23-column table came out as 2,730 + 2,270 — two differently-shaped INSERT
    # statements, on every batch, each with tens of thousands of placeholders.
    # asyncpg prepares and caches by statement text, so that is two large
    # prepared statements per batch shape rather than one reused, and the peak
    # is the bigger of the two rather than the size actually chosen.
    #
    # Deriving it from the column count instead makes every insert identical and
    # the memory per insert a property of the table rather than of an arbitrary
    # constant — which matters most exactly where it went wrong: 1,097,775 parcel
    # rows carrying polygon WKT, on a 512MB dyno.
    #
    # Set after the header is read, since the column count is what sizes it.
    BATCH = 5000
    try:
        cols: list[str] = []
        batch: list[dict] = []
        ensured = False
        total = 0
        with _open_maybe_gzip(path) as fh:
            reader = _csv.reader(fh)
            try:
                header = next(reader)
            except StopIteration:
                return 0
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
                return 0
            BATCH = min(BATCH, append_store.chunk_size_for(len(cols), True))
            for row in reader:
                batch.append({safe[i]: (row[i] if i < len(row) else "") or ""
                              for i in keep})
                if len(batch) >= BATCH:
                    if not ensured:
                        await append_store.ensure_table(table, cols, key_col=None, keyless=True)
                        ensured = True
                    total += await append_store.append_rows(
                        table, cols, batch, key_col=None, keyless=True,
                        stamp_col=stamp_col,
                    )
                    batch = []
        if batch:
            if not ensured:
                await append_store.ensure_table(table, cols, key_col=None, keyless=True)
            total += await append_store.append_rows(
                table, cols, batch, key_col=None, keyless=True,
                stamp_col=stamp_col,
            )
        # Once the whole file is down, not once per batch — see
        # append_store.fill_geometry. `cols` is set as soon as the header is
        # read, so a file that turned out to be header-only skips this too.
        #
        # NOT gated on rows being new. A table loaded while the geometry switch
        # was off keeps every row and dedups them on re-load, so `total` comes
        # back 0 and a run conditioned on it could never repair what it already
        # holds — which is exactly the state append_subgushallshape_aa29e909 was
        # left in: 11,578 rows of WKT and no geom. Re-polling is the repair.
        # The cost of the no-op case is one indexed pass over rows whose geom is
        # already set.
        if cols:
            await append_store.fill_geometry(table, cols)
        return total
    finally:
        if delete_after:
            try:
                _os.remove(path)
            except OSError:
                pass


async def _neon_stream_load_r2(table: str, r2_key: str,
                               ds_id=None, expected: int = 0) -> None:
    """Stream a version's R2 CSV into the dataset's NEON table.

    Used for the >50MB out-of-band CSV path (e.g. registries Cosmetics,
    ~60k rows / ~58MB CSV) of a dataset that ALSO keeps files. Download the
    object to a temp file, free the bytes, then stream it in (see
    _neon_stream_load_file). Best-effort: this is the queryable MIRROR of an
    archive whose authority is the R2 object, so a failure is a warning and the
    next poll re-loads it.

    Runs off the request path (scheduled via asyncio.create_task) so
    push-version returns immediately and the version is created regardless.
    """
    tmp = None
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
        total = await _neon_stream_load_file(
            table, tmp, stamp_col=await _sample_column_for(ds_id))
        logger.info("NEON stream-load: +%d rows into %s from %s", total, table, r2_key)
    except Exception as e:
        logger.warning("NEON stream-load failed for %s (non-fatal): %s", table, e)
    finally:
        if tmp:
            try:
                _os.remove(tmp)
            except OSError:
                pass
    # The third loader, and the one the short-load check first shipped without.
    # It is also the one most likely to stop early: it runs as a background task
    # AFTER the response, so a dyno recycle kills it mid-stream with no
    # exception to catch and nothing written anywhere. The national parcel layer
    # landed 150,000 of 1,097,775 rows that way on 2026-08-09 — killed by an OOM
    # eighty seconds in — under a version reporting the full count and a
    # dataset page reporting nothing at all.
    await _record_short_load(table, ds_id, expected, res_name=r2_key)


# A NEON-only dataset's out-of-band CSV, handed from /upload-csv to
# /push-version. It is NOT a storage reference — there is no file store to put
# it in — just the temp path the uploaded bytes were streamed to, which
# push-version then streams into the append table and deletes. It never reaches
# resource_mappings: a NEON-only version's content is its append_table key.
_NEON_CSV_PREFIX = "neon-csv:"


def _neon_csv_ref(path: str) -> str:
    return f"{_NEON_CSV_PREFIX}{path}"


def _is_neon_csv_ref(value) -> bool:
    return isinstance(value, str) and value.startswith(_NEON_CSV_PREFIX)


def _neon_csv_path(value: str) -> str:
    return value[len(_NEON_CSV_PREFIX):]


async def _neon_only_load_csv(table: str, path: str, res_name: str,
                              ds_id=None, expected: int = 0) -> None:
    """Load a NEON-only dataset's uploaded CSV into its append table.

    Unlike the R2 mirror above, this IS the archive: nothing else holds these
    rows. So a failure is an error, not a warning — the version exists and its
    table is empty or partial, and only the next poll (which re-scrapes and
    re-uploads; the append is idempotent on row_hash) will fill it.

    "The next poll will fill it" is the reasoning that let this stay quiet, and
    on a monthly or quarterly corpus it means the gap stands for months. Worse,
    nothing said the gap was there: the version keeps reporting the count the
    scrape produced. So the row count is checked against what the version
    promised and a shortfall is written to the dataset, where the page shows it.
    """
    total = 0
    failed = None
    try:
        total = await _neon_stream_load_file(
            table, path, delete_after=True,
            stamp_col=await _sample_column_for(ds_id))
        logger.info("NEON-only archive: +%d rows into %s for %s",
                    total, table, res_name)
    except Exception as e:  # noqa: BLE001 — recorded below, never re-raised
        failed = e
        logger.error(
            "NEON-only archive FAILED for %s → %s: %s. The version exists but "
            "its rows are missing or partial; the next poll re-loads them.",
            res_name, table, e,
        )
    # One implementation, shared with the other two loaders — the first version
    # of this check was written per-loader and the one it was not written into
    # is the one that lost 947,775 rows.
    #
    # `force` covers the case a count alone cannot see: a table already holding
    # an earlier version's rows can pass the comparison while THIS load put
    # nothing in it.
    await _record_short_load(table, ds_id, expected, res_name=res_name,
                             force=failed is not None)


async def _run_consolidate_bg(ds_id: uuid.UUID, dedup_key: str) -> None:
    """Merge a finished multi-batch run's per-batch versions into ONE, then
    rebuild NEON deduped. Best-effort: opens its own session, and if it dies
    (e.g. dyno recycle) the versions simply stay un-merged and the admin
    consolidate endpoint remains the fallback."""
    from app.database import async_session
    from app.services.r2_backfill import (
        consolidate_dataset_versions, seed_neon_from_versions,
    )
    try:
        async with async_session() as db:
            s = await consolidate_dataset_versions(
                db, ds_id, dedup_key=dedup_key, apply=True)
            logger.info("Auto-consolidate %s: %s→1 (zip_parts=%s, csv_rows=%s)",
                        ds_id, s.get("old_version_count"), s.get("zip_parts"),
                        s.get("csv_rows"))
        if s.get("committed"):
            async with async_session() as db:
                await seed_neon_from_versions(db, ds_id, apply=True, reset=True)
                logger.info("Auto-consolidate %s: NEON reseeded from the merged version", ds_id)
    except Exception:
        logger.exception("Auto-consolidate failed for %s (admin endpoint remains available)", ds_id)


# A GovMap documentation bundle is named "<layer>_symbology.zip" (SLD + icons +
# field dictionary) or "<layer>_fields.zip" (dictionary only) — see govscraper
# field_dictionary.documentation_zip. The Hebrew part of the name is stripped by
# storage.build_key's _safe_filename, so the object key tail is what survives.
_DOC_BUNDLE_RE = re.compile(r"(?:^|_)(?:symbology|fields)\.zip$", re.IGNORECASE)


def _is_doc_bundle(value: str) -> bool:
    """Is this pre-uploaded ZIP the layer's documentation bundle rather than
    source attachments? Judged by filename, which only an R2 key carries — an
    ODATA resource_id is an opaque UUID here, so those stay in the ZIP channel
    (govmap versions are all R2-backed, so nothing real is missed)."""
    if not isinstance(value, str) or not storage.is_storage_value(value):
        return False
    return bool(_DOC_BUNDLE_RE.search(storage.key_of(value).rsplit("/", 1)[-1]))


# Mapping keys that hold real, downloadable files despite the underscore. A
# heavy GovMap layer publishes its features as _gpkg/_parquet INSTEAD of a named
# CSV resource, so "did anything land?" cannot be answered by counting
# human-named keys alone. `_symbology` is deliberately absent: a version
# carrying the layer's SLD and no data is exactly the empty version the guard
# below exists to catch.
_FILE_AGGREGATE_KEYS = ("_geojson", "_gpkg", "_parquet", "_zip", "_zip_parts")

# What counts as content having landed. The row archive is not a file, but for a
# NEON-only dataset it is the ONLY thing a version holds — there is no named CSV
# mapping to count, because there is no file store to put one in. Without this
# the guard below reads a fully-loaded 36,784-row version as "nothing landed".
# (``append_table`` was already counted, by accident of not starting with an
# underscore; ``_append_tables`` — the multi-resource shape — was not.)
_LANDED_KEYS = _FILE_AGGREGATE_KEYS + tuple(sorted(ROW_ARCHIVE_KEYS))


# The scraper's verdict that a source is GONE, not merely unreachable. For
# GovMap it is reached only after the catalog was fetched successfully and the
# layer id was absent from it; a catalog timeout, or a layer that IS listed,
# raises a different, explicitly transient error (see the govmap legacy_engine).
# So matching this string records a finding the scraper already made with
# certainty — it does not infer one from a bare failure.
# Keep in sync with migration 049.
_SOURCE_GONE_MARKER = "is not in the catalog and returned 0"


def _is_source_gone_error(error: str | None) -> bool:
    return bool(error) and _SOURCE_GONE_MARKER in error


def _import_warning_for(new_engine: str | None, previous_engines: list[str],
                        declared: str | None) -> str | None:
    """Why a reader should distrust the version just pushed, or None.

    ONLY the worker's own verdict (`scrape_metadata.quality_warning`) counts.
    OVER cannot see geometry — it stores files it never parses — and an attempt
    to infer degradation from the engine name was measured wrong in both
    directions on 1.8.2026:

      * גני ילדים fell from spatial-analysis to quadtree and was flagged, while
        both versions hold the identical 20,465 POINTs. A kindergarten layer IS
        points; the engine changed and the data did not.
      * קווי גובה 50 ס"מ, the case that motivated the whole feature, went
        unflagged — it has only ever used quadtree, so there was no "downgrade"
        to spot, even though its 93,436 contour LINES had just become points.

    A warning that fires on healthy data and stays silent on erased data is
    worse than none, so the inference is gone. `new_engine`/`previous_engines`
    are kept in the signature: the scraper's geometry gate is what will supply
    real verdicts here, and a future rule may want the history.
    """
    if declared and str(declared).strip():
        return str(declared).strip()[:2000]
    return None


def _landed_resource_count(resource_mappings: dict) -> int:
    """How many resources actually made it into this version — the counterpart
    to the push's `expected`. Counts every source-named resource plus the
    underscore aggregates that hold content (files, or the NEON row archive)."""
    return sum(
        1 for k, v in (resource_mappings or {}).items()
        if v and (not k.startswith("_") or k in _LANDED_KEYS)
    )


# How big a pre-uploaded CSV may be before the append path stops reading it
# back to merge it. Parsing happens on the request path of a 512MB dyno, so
# this is a memory bound, not a policy one; every append corpus that arrives
# pre-uploaded today is orders of magnitude below it.
APPEND_MERGE_MAX_BYTES = 32 * 1024 * 1024


def _split_doc_bundles(
    zip_ids: list[str], declared: list[str] | None,
) -> tuple[list[str], list[str]]:
    """Partition the pre-uploaded ZIPs into (attachments, documentation).

    Workers that know about `symbology_resource_ids` declare the bundle
    outright; older ones (and the pinned worker at the time of writing) push it
    through `zip_resource_ids`, where it lands in `_zip_parts` and is presented
    to the reader as generic attachments. Recognizing it here means the fix
    holds for both, and no worker deploy has to precede this one.
    """
    docs = list(declared or [])
    keep: list[str] = []
    for rid in zip_ids:
        (docs if _is_doc_bundle(rid) else keep).append(rid)
    return keep, docs


async def _append_seed_from_snapshot(ds, latest) -> dict[str, list[dict]]:
    """Rows a full_snapshot dataset already archived, keyed by resource name.

    Flipping a tracked dataset to ``append_only`` starts its cumulative CSV
    from nothing: the first append push would carry only what the source
    still lists, so everything the source had already dropped would vanish
    from the latest version — the exact loss the switch is meant to prevent
    (jeden.co.il cleared 37 of its 39 tenders off the page). Reading the
    previous version's object instead makes the conversion carry its own
    history forward.

    R2 only: an ODATA-backed dataset's snapshot resources are per-version
    datastore tables, not one file to read back, so there is nothing safe to
    seed from and the caller leaves the seen-set empty. Best-effort — a
    resource that cannot be read back is skipped, and the push proceeds with
    what the source gave it.

    One rough edge, bounded to the conversion push itself: with no
    ``append_key`` the row identity is a hash of the whole row, and a row read
    back from CSV carries every column (blank where the scraper simply had no
    such key). So a SPARSE row that the source still lists can hash
    differently from its archived twin and land in the cumulative a second
    time — at most once per still-listed row, never on later polls, since
    from then on both sides are the scraper's own shape.
    """
    from app.services.csv_parser import parse_csv

    out: dict[str, list[dict]] = {}
    for name, value in (latest.resource_mappings or {}).items():
        if name.startswith("_") or not storage.is_storage_value(value):
            continue
        if not str(storage.key_of(value) or "").lower().endswith(".csv"):
            continue
        try:
            prev_bytes = await storage_client.get_object_bytes(value)
        except Exception as e:  # noqa: BLE001
            logger.warning("Append seed: cannot read %s for %s: %s", value, ds.id, e)
            continue
        if not prev_bytes:
            continue
        _fields, rows = parse_csv(prev_bytes)
        if rows:
            out[name] = rows
            logger.info("Append seed: %d row(s) carried forward for %s/%s",
                        len(rows), ds.id, name)
    return out


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
        # An append_only dataset's archive is cumulative — the push carries
        # what the source lists RIGHT NOW and the merge only ever adds to the
        # stored rows, so a smaller batch is the normal shape, not a collapse.
        # (The guard is already inert once a dataset has an append version:
        # those record `rows_total`, not `total_rows`. Saying so explicitly is
        # what lets a converted dataset publish its first append version at
        # all, measured against its last snapshot.)
        and ds.storage_mode != "append_only"
        # A deliberately partial run is not a measurement of the corpus, so
        # there is nothing for the guard to measure: one file sampled against a
        # 90k-file register is a 99.99% "shrink" and would be rejected every
        # single time. The version records that it was partial, and no later
        # full pass is measured against it (_shrink_baseline_version).
        and not body.partial_run
    ):
        prev_total = 0
        try:
            baseline = await _shrink_baseline_version(db, ds.id)
            prev_total = int(((baseline.change_summary if baseline else None)
                              or {}).get("total_rows") or 0)
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
    # Which NEON table each tabular resource's rows went into, in resource
    # order. Empty unless this dataset archives to NEON (see _record_neon_table).
    _neon_layout: list[dict] = []
    odata_resource_ids = []
    push_errors: list[str] = []

    is_append = (ds.storage_mode == "append_only")
    append_key = (ds.scraper_config or {}).get("append_key") if is_append else None
    seen_keys: list[str] = []
    rows_added_total = 0

    # Rows carried over from a full_snapshot past, by resource name — empty
    # unless this push is the first one after a switch to append_only.
    append_seed: dict[str, list[dict]] = {}

    if is_append and latest is not None:
        seen_keys = list((latest.resource_mappings or {}).get("_appendonly_seen", []) or [])
        # Keyed on the cumulative object's absence, NOT on an empty seen-set:
        # a dataset can carry a seen-set from a version that never wrote a
        # cumulative file, and reading that as "already converted" would
        # publish an empty archive. Re-seeding identities is idempotent.
        if (_use_r2(ds)
                and not storage.is_storage_value(ds.appendonly_resource_id)):
            append_seed = await _append_seed_from_snapshot(ds, latest)
            for _rows in append_seed.values():
                _, seen_keys = compute_new_rows(seen_keys, _rows, append_key)

    # A NEON-only dataset (storage plan 'neon') has no file backend at all — no
    # ODATA mirror, not R2 — and its rows ARE the archive, so it has to enter
    # this block too. It used not to: every tabular resource fell straight
    # through to the empty-version guard below, which is why no NEON-only
    # scraper dataset could publish a version, at any size.
    _stores_files = storage.dataset_stores_files(ds)
    if ds.odata_dataset_id or _use_r2(ds) or not _stores_files:
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
        # ``dataset_archives_neon`` — not the raw ``archive_neon`` flag — so the
        # NEON-ONLY plan (storage_backend='neon', which sets no such flag) is
        # covered by the same predicate the CKAN poll path and every reader use.
        _archive_neon = bool(
            append_store.is_configured() and storage.dataset_archives_neon(ds)
        )
        if not _stores_files and not _archive_neon:
            # Nothing else can hold this dataset's data. Say so here rather than
            # letting the empty-version guard report "all pushes failed (no
            # detail)".
            push_errors.append(
                "NEON-only dataset, but the append DB is unavailable "
                "(APPEND_DATABASE_URL) — there is nowhere to put the rows"
            )

        # ONE NEON TABLE PER TABULAR RESOURCE, once a version carries more than
        # one. A scraper version's resources are separate CSVs on R2 precisely
        # because they are separate things — ykpubdata publishes a row per
        # building file AND a row per document, different grains with different
        # columns. Loading them all into ``table_name(ds)`` produced one table
        # holding the union of both column sets, where neither grain is
        # queryable: measured on prod as 199 rows and 65 columns across three
        # resources.
        #
        # The layout is a property of the DATASET, not of the version, and it
        # only ever ratchets: once a dataset has published several resources it
        # keeps per-resource tables even if a later (e.g. partial) scrape
        # returns one, which would otherwise dump that resource back into the
        # merged table. Datasets that have only ever published a single resource
        # — 52 of the 53 scraper NEON datasets on prod — keep the historical
        # single-table name, so nothing they have accumulated moves.
        #
        # A dataset that CROSSES from one resource to several leaves its old
        # merged table behind: the rows are still on R2, and
        # ``POST /api/admin/datasets/{id}/seed-neon?apply=true&reset=true``
        # replays every version's snapshots into the new per-resource tables
        # with their historical first_seen (see r2_backfill).
        _tabular_names = [
            r.name for r in body.resources
            if (r.records and r.fields) or csv_resource_ids.get(r.name)
        ]
        _neon_multi = neon_per_resource(ds.scraper_config, _tabular_names)
        if _archive_neon and _neon_multi and not (
            ds.scraper_config or {}
        ).get("neon_tables_per_resource"):
            ds.scraper_config = {
                **(ds.scraper_config or {}), "neon_tables_per_resource": True,
            }
            logger.warning(
                "NEON archive: %s now publishes %d tabular resources — switching "
                "to one table per resource. Its previously merged table (%s) is "
                "left in place; run seed-neon?reset=true to rebuild history.",
                ds.ckan_name, len(_tabular_names), append_store.table_name(ds),
            )

        def _neon_table_for(res_name: str) -> str:
            return (
                append_store.table_name_for_scraper_resource(ds, res_name)
                if _neon_multi else append_store.table_name(ds)
            )

        # _neon_layout (declared above, so the mapping-writing code below can
        # see it) records where this version's rows went, in resource order —
        # so every reader (the public /api/append endpoints, the /data catalog,
        # MCP) resolves the same tables. Without it they fall back to the
        # deterministic single-table name and see one table.
        def _record_neon_table(res_name: str) -> str:
            table = _neon_table_for(res_name)
            if not any(e["resource"] == res_name for e in _neon_layout):
                _neon_layout.append({"resource": res_name, "table": table})
            return table

        # Rows that were scraped, published in the version's count, and are not
        # in the table anyone can query. Collected here and turned into the
        # dataset's import_warning after the version commits.
        _neon_short: list[str] = []

        async def _check_neon_landed(table: str, res_name: str, parsed: int) -> None:
            """Did the rows this version promises actually reach the table?

            They can silently not. The load is best-effort by design — a failure
            is logged and the version is published anyway, on the reasoning that
            the next poll refills it — and nothing compares the two numbers
            afterwards. גושים shape sat for a day with a version reporting 18,689
            rows over a table holding 11,578, its published GeoJSON containing
            18,689 features and not one duplicate among them: a third of the
            national block layer missing, with every surface in the product
            reporting success. It surfaced only because a spatial join came up
            two thirds short and someone counted.

            The comparison is one-directional and therefore free of false
            alarms: an APPEND table accumulates across versions and samples, so
            it can legitimately hold far MORE than this version's count and can
            never legitimately hold less. Anything under is missing rows.
            """
            if parsed <= 0:
                return
            try:
                # The ESTIMATE, not count(*): this runs while push-version's
                # request is open, and an exact count of the parcel layer's 1.1M
                # rows is the kind of long synchronous step that gets the task
                # reclaimed mid-push — the failure this whole area keeps having.
                total = await append_store.table_count_estimate(table)
            except Exception as e:  # noqa: BLE001 — a check must not fail a push
                logger.warning("NEON landed-check failed for %s: %s", table, e)
                return
            if total < 0:
                return  # never analysed — no answer is not the same as none
            # A margin, because the estimate is a few percent out and a false
            # "rows are missing" on a healthy dataset would teach everyone to
            # ignore the warning. A third of a layer missing clears this easily.
            if total < parsed * 0.95:
                msg = (f"{res_name}: {total:,} שורות בטבלה מתוך {parsed:,} "
                       f"שנקלטו בגרסה — הטעינה ל-NEON חלקית")
                _neon_short.append(msg)
                logger.error(
                    "NEON short load: %s has %d rows, version carried %d (%s)",
                    table, total, parsed, res_name,
                )

        async def _neon_load_from_csv(res_name: str, csv_bytes: bytes | None) -> bool:
            """Load one resource's rows from its CSV bytes. Returns whether they
            landed — which the NEON-only path needs (there it is the version's
            whole content), while the dual-write callers ignore it and stay
            best-effort."""
            if not (_archive_neon and csv_bytes):
                return False
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
                    return False
                if renamed:
                    n_records = [
                        {renamed.get(k, k): v for k, v in rec.items()}
                        for rec in n_records
                    ]
                table = _neon_table_for(res_name)
                await append_store.ensure_table(table, cols, key_col=None, keyless=True)
                n = await append_store.append_rows(
                    table, cols, n_records, key_col=None, keyless=True,
                )
                # Stamped only once the rows are actually in: a table key on a
                # version whose load threw would point every reader at a table
                # that holds nothing of this version.
                _record_neon_table(res_name)
                await _check_neon_landed(table, res_name, len(n_records))
                # Same step the streaming loaders run, for the same reason: a
                # table that holds geometry gets a PostGIS `geom`. It was only
                # wired to _neon_stream_load_file, which is reached exclusively
                # by the >50MB out-of-band path — so a spatial corpus whose rows
                # fit inline (every CBS GIS layer but two) landed its
                # geometry_wkt in NEON and never got a geometry column at all,
                # with nothing in the logs to say so. Swallows its own failures.
                await append_store.fill_geometry(table, cols)
                logger.info(
                    "NEON archive: +%d new rows into %s for %s", n, table, res_name,
                )
                return True
            except Exception as e:
                logger.warning(
                    "NEON archive failed for %s (non-fatal): %s", res_name, e,
                )
                return False

        for res in body.resources:
            # Pre-uploaded CSV files bypass record-level handling. Append mode
            # can't dedupe a file we never parsed, so we treat pre-uploaded
            # CSVs as a full snapshot for this resource even if the dataset
            # is in append mode (rare edge case: scraper would only do this
            # for >100MB JSON payloads, where append-only with diffing isn't
            # the intended path anyway).
            pre_uploaded = csv_resource_ids.get(res.name)
            if _is_neon_csv_ref(pre_uploaded):
                # NEON-only, out-of-band CSV: /upload-csv had no file store to
                # put it in, so it left the bytes on disk and handed back a
                # reference. Stream them into the append table OFF the request
                # path — 36,784 rows for the mavat register, millions for its
                # landuse/entities corpora, which no request should hold open.
                # No file mapping is written (there is no file); the version's
                # content is the append_table key stamped below.
                if not _archive_neon:
                    push_errors.append(f"neon {res.name}: append DB unavailable")
                    continue
                _table = _record_neon_table(res.name)
                _t = asyncio.create_task(
                    _neon_only_load_csv(
                        _table, _neon_csv_path(pre_uploaded), res.name,
                        # What the version is about to promise. The load runs
                        # after this response, so the check has to carry the
                        # number with it rather than read it back off a version
                        # that may not be committed yet.
                        ds_id=ds.id, expected=int(res.row_count or 0),
                    )
                )
                _NEON_BG_TASKS.add(_t)
                _t.add_done_callback(_NEON_BG_TASKS.discard)
                logger.info(
                    "NEON-only: queued the row load for %s (%d rows) → %s",
                    res.name, res.row_count, _table,
                )
                continue

            # ...unless the dataset is append_only, where mapping the file in
            # whole makes the version a snapshot of whatever the source lists
            # right now — the exact loss append mode exists to prevent. Read
            # the object back so the record path below can dedupe it against
            # the archive. Bounded by size: the >100MB payloads this branch was
            # written for are not append-shaped and stay mapped as-is.
            if (pre_uploaded and is_append and not res.records
                    and storage.is_storage_value(pre_uploaded)):
                size = await storage_client.object_size(pre_uploaded)
                if size is not None and size <= APPEND_MERGE_MAX_BYTES:
                    raw = await storage_client.get_object_bytes(pre_uploaded)
                    if raw:
                        _pf, _pr = parse_csv(raw)
                        res.records = _pr
                        res.fields = res.fields or _pf
                        pre_uploaded = None
                        logger.info(
                            "Append: read back the pre-uploaded CSV for %s "
                            "(%d rows, %s bytes) so it can be merged",
                            res.name, len(_pr), size,
                        )
                else:
                    logger.warning(
                        "Append: pre-uploaded CSV for %s is %s bytes — too big "
                        "to merge on the request path, stored as a snapshot",
                        res.name, size,
                    )

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
                    _table = _record_neon_table(res.name)
                    _t = asyncio.create_task(
                        _neon_stream_load_r2(
                            _table, pre_uploaded,
                            # Carried in, not read back: this runs after the
                            # response, off a version that may not be committed.
                            ds_id=ds.id, expected=int(res.row_count or 0),
                        )
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

            # NEON-only, rows inline (under the worker's out-of-band threshold):
            # the same archive, with no file to write alongside it.
            #
            # Note this runs ahead of the append-only branch below, so a
            # NEON-only dataset does NOT maintain `_appendonly_seen`: the append
            # store already dedups on row_hash, and a seen-set of millions of
            # hashes in jsonb is the exact shape that OOMed the poll path.
            if not _stores_files:
                if not _archive_neon:
                    push_errors.append(f"neon {res.name}: append DB unavailable")
                    continue
                try:
                    csv_bytes = records_to_csv_bytes(res.fields, res.records)
                except Exception as e:
                    logger.error("Failed to serialise %s for NEON: %s", res.name, e)
                    push_errors.append(f"neon {res.name}: {e}")
                    continue
                if not await _neon_load_from_csv(res.name, csv_bytes):
                    push_errors.append(f"neon {res.name}: row load failed")
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
                        else:
                            # First push after a switch to append_only — the
                            # snapshot history IS the cumulative's starting
                            # point (see _append_seed_from_snapshot).
                            cumulative = list(append_seed.get(res.name) or [])
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

    # A layer's documentation bundle travels on the same channel as attachments;
    # give it its own mapping key so the UI and the API can name it for what it
    # is, and so an attachment-less govmap version stops reporting attachments.
    zip_ids, doc_ids = _split_doc_bundles(
        list(body.zip_resource_ids or []), body.symbology_resource_ids,
    )
    zip_single = body.zip_resource_id
    if zip_single and _is_doc_bundle(zip_single):
        doc_ids.append(zip_single)
        zip_single = None
    if doc_ids:
        for rid in doc_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_symbology"] = doc_ids
        logger.info("Linked %d documentation bundle(s) (symbology + fields)",
                    len(doc_ids))

    # ZIP attachment handling: prefer pre-uploaded zip_resource_ids (list of
    # multipart parts), fall back to single zip_resource_id, then inline base64.
    if zip_ids:
        for rid in zip_ids:
            odata_resource_ids.append(rid)
        resource_mappings["_zip_parts"] = list(zip_ids)
        logger.info("Using %d pre-uploaded ZIP part(s)", len(zip_ids))
        # Worker uploads with version_number=1 hardcoded (it can't know
        # next_version yet). Now that we do, rewrite each resource's
        # 'v1' marker to match the version we're about to commit.
        for rid in zip_ids:
            if storage.is_storage_value(rid):
                continue  # R2 key — no ODATA resource to rename
            try:
                await odata_client.update_resource_version_number(rid, next_version)
            except Exception as e:
                logger.warning("Failed to rename pre-uploaded ZIP %s to v%d: %s",
                               rid, next_version, e)
    elif zip_single:
        # Single ZIP was already uploaded via /api/worker/upload-zip
        odata_resource_ids.append(zip_single)
        resource_mappings["_zip"] = zip_single
        logger.info("Using pre-uploaded ZIP resource %s", zip_single)
        if not storage.is_storage_value(zip_single):
            try:
                await odata_client.update_resource_version_number(
                    zip_single, next_version,
                )
            except Exception as e:
                logger.warning("Failed to rename pre-uploaded ZIP %s to v%d: %s",
                               zip_single, next_version, e)
    elif body.zip_file and not _stores_files:
        # A NEON-only dataset has nowhere to put attachments. Say it, rather
        # than dropping them silently — for a version that carried ONLY a ZIP
        # this is the difference between a named reason and the guard's
        # "all pushes failed (no detail)".
        push_errors.append(
            "attachments dropped: a NEON-only dataset stores rows, not files"
        )
        logger.warning(
            "Dropped a %d-file ZIP for %s — NEON-only plan has no file store",
            len(body.attachments), ds.ckan_name,
        )
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
    # The NEON row-archive markers, in the two shapes every reader already
    # understands (see append_store.tables_from_mappings). A jsonb ARRAY is used
    # for the multi shape because jsonb does not preserve object key order, and
    # the order here IS the order of the resources as scraped.
    if _neon_layout:
        if len(_neon_layout) > 1:
            resource_mappings["_append_tables"] = _neon_layout
        else:
            resource_mappings["append_table"] = _neon_layout[0]["table"]
    if is_append:
        resource_mappings["_appendonly_seen"] = seen_keys

    # Empty-version guard: if the worker sent payload (records or ZIP) but
    # nothing actually landed on odata, don't pretend a version exists.
    # Surface the reason on the dataset and mark the task as failed so the
    # admin can see what happened.
    #
    # `successes` must count the underscore aggregates that hold files, not just
    # source-named keys — see _landed_resource_count. A GPKG-only layer has no
    # named key at all, so once the documentation bundle started arriving on
    # every govmap push (making `expected` non-zero), this guard read a complete
    # 241,586-feature scrape of layer 335 as "nothing landed", answered the
    # worker 502, and discarded 2.2 hours of scraping.
    expected = (
        len([r for r in body.resources if r.records or (body.csv_resource_ids or {}).get(r.name)])
        + (1 if (body.zip_file or body.zip_resource_id or body.zip_resource_ids) else 0)
        + (1 if body.geojson_resource_ids else 0)
    )
    successes = _landed_resource_count(resource_mappings)
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
    if body.partial_run:
        # Both flags live in change_summary because that is what the shrink
        # guard, the versions API and the dataset page already read. Absent on
        # a full pass rather than false, so nothing about existing versions
        # changes meaning.
        change_summary["partial_run"] = True
        if body.run_mode:
            change_summary["run_mode"] = body.run_mode
        if body.run_group:
            change_summary["run_group"] = body.run_group
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

    # A worker just delivered files this server is refused. Record WHICH ones,
    # so the standing "waiting to be fetched" notice stops being said over a
    # dataset that is now archived. The resources stay blocked — data.gov.il
    # goes on refusing us and the next poll will detect them again, correctly —
    # they simply stop being missing. Only what the run actually delivered is
    # stamped, so a partial rescue reads as partial.
    _delivered = [
        r.get("resource_id") or r.get("id")
        for r in ((body.scrape_metadata or {}).get("blocked_files", {})
                  .get("resources") or [])
        if r.get("status") in ("features", "raw_only")
    ]
    if _delivered:
        blocked_resources.mark_fetched(
            ds, [r for r in _delivered if r],
            modified=body.metadata_modified, version=next_version)
    # A version landed, so the source is demonstrably there — clear any
    # "removed at the publisher" mark. Layer ids get renumbered and pages come
    # back; the badge must not outlive the outage that produced it.
    ds.source_gone_at = None

    # Is there a reason to distrust what just landed? Evaluated per push and
    # CLEARED when there isn't, so a warning never outlives the version that
    # earned it.
    _prev_engines = [
        (v.change_summary or {}).get("scrape_metadata", {}).get("engine")
        for v in (await db.execute(
            select(VersionIndex)
            .where(VersionIndex.tracked_dataset_id == ds.id)
            .order_by(VersionIndex.version_number.desc())
            .limit(12)
        )).scalars().all()
        if v.version_number != next_version
    ]
    _warn = _import_warning_for(
        (body.scrape_metadata or {}).get("engine"),
        [e for e in _prev_engines if e],
        (body.scrape_metadata or {}).get("quality_warning"),
    )
    # A short NEON load outranks an engine-change note: one says the data may be
    # shaped differently, the other says some of it is not there. Prepended
    # rather than replacing, so a version that earned both still says both.
    if _neon_short:
        _warn = "⚠ " + " · ".join(_neon_short) + (f" · {_warn}" if _warn else "")
    if _warn != ds.import_warning:
        ds.import_warning = _warn
        ds.import_warning_at = datetime.now(timezone.utc) if _warn else None
        if _warn:
            logger.warning("Dataset %s (%s) flagged: %s", ds.id, ds.title, _warn[:120])
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

    # Mark the running task completed — UNLESS more batches of this same run
    # are coming, in which case keep it running so their pushes still pass the
    # no-running-task guard. Its phase is nudged so the queue shows progress.
    task_result = await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.tracked_dataset_id == ds.id,
            ScrapeTask.status == "running",
        )
    )
    task = task_result.scalar_one_or_none()
    if task:
        if body.more_batches:
            task.phase = "batch_committed"
            task.message = "מנה נשמרה — ממשיך לאסוף"
        else:
            task.status = "completed"
            task.completed_at = datetime.now(timezone.utc)
            task.progress = 100
            task.phase = "complete"
        await db.commit()

    from app.services.activity_log import log_event
    await log_event(
        event="completed", dataset=ds, status="ok", actor="worker",
        message=(
            f"מנה נשמרה — גרסה {next_version} נוצרה, ממשיך לאסוף"
            if body.more_batches
            else f"גירוד הסתיים — גרסה {next_version} נוצרה"
        ),
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

    # Final push of a multi-batch run + a dedup key → auto-merge the per-batch
    # versions into one, so a big bootstrap ends as a single version without a
    # manual admin step. Only when this run actually produced >1 version
    # (next_version > 1), so a first-ever single-batch run is left alone.
    if (not body.more_batches) and body.consolidate_dedup_key and next_version > 1:
        import asyncio as _asyncio
        _asyncio.create_task(
            _run_consolidate_bg(ds.id, body.consolidate_dedup_key.strip())
        )
        logger.info("Scheduled auto-consolidation for %s (dedup_key=%s)",
                    ds.id, body.consolidate_dedup_key)

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
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    _require_file_backend(ds, "a ZIP attachment")

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
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    # Geometry genuinely needs a file store — a GeoJSON is not rows — so a
    # NEON-only dataset is refused here, but told why.
    _require_file_backend(ds, "a GeoJSON file")

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

    A NEON-only dataset has no file store to upload to and no CKAN datastore to
    stream into — its archive IS the append table — so it takes a third path:
    the CSV is held on disk and returned as a ``neon-csv:<path>`` reference that
    push-version streams into that table. See the branch below.
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
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    # NEON-only datasets ARE accepted here: rows are exactly what they archive,
    # and this endpoint's whole job for them is to take those rows in. Only a
    # dataset that stores FILES yet has nowhere to put them is refused.
    if storage.dataset_stores_files(ds):
        _require_file_backend(ds, "a CSV file")
    elif not append_store.is_configured():
        raise HTTPException(status_code=409, detail=(
            "NEON-only dataset, but the append DB is unavailable "
            "(APPEND_DATABASE_URL) — there is nowhere to put the rows."
        ))

    is_gzip = (compression or "").lower() == "gzip"

    # ---- Stream the upload to a temp file on disk (no bytes held in memory) ----
    # Hesdermutne's 166MB plain CSV + 32k parsed dicts previously pushed the
    # Render starter dyno to ~400MB RSS and OOM-crashed. Using temp files
    # keeps peak memory under ~30MB regardless of dataset size.
    import os
    import shutil
    import tempfile
    import uuid as _uuid

    tmp_dir = _UPLOAD_TMP_DIR
    os.makedirs(tmp_dir, exist_ok=True)
    _sweep_stale_uploads(tmp_dir)
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

    # ---- NEON-only: no file store, so the CSV stays on disk for push-version -
    # There is no object to upload and no CKAN datastore to stream into: the
    # rows are the archive. The load itself is deferred to push-version because
    # only IT knows the full set of tabular resources this version carries, and
    # that set decides whether the rows go into the dataset's merged table or
    # its per-resource one (see neon_per_resource). Returning a path rather than
    # loading here also keeps a multi-GB corpus off the request clock.
    if not storage.dataset_stores_files(ds):
        csv_size = os.path.getsize(csv_path)
        _cleanup_paths(gz_path, None)  # the plain CSV is deliberately kept
        logger.info(
            "Held CSV (%d KB, ~%d rows) on disk for %s — NEON-only dataset, "
            "rows load at push-version",
            csv_size // 1024, row_count, ds.ckan_name,
        )
        return {
            "resource_id": _neon_csv_ref(csv_path),
            "size": csv_size,
            "rows": row_count,
            "compression": compression or "none",
            "datastore": "deferred — rows stream into NEON at push-version",
            "upload_mode": "neon",
        }

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


# Where /upload-csv stages the uploaded bytes. Mirrored by the datastore push
# runner (its TMP_DIR), which recovers a job's CSV from the same place.
_UPLOAD_TMP_DIR = "/tmp/upload_csv"

# How long an upload temp file may sit unclaimed before it is swept.
_UPLOAD_TMP_MAX_AGE_S = 6 * 3600


def _sweep_stale_uploads(tmp_dir: str) -> None:
    """Drop /upload-csv temp files nobody came back for.

    Both deferred paths leave a file here on purpose — the datastore push runner
    reads it on its next tick, and a NEON-only upload waits for push-version to
    stream it in. If that second call never arrives (the worker died mid-run)
    the file would sit until the dyno recycles, and these are not small files:
    the mavat entities corpus is a 777MB plain CSV. Six hours is far longer than
    any legitimate upload→claim gap, so nothing in flight is ever swept.
    """
    import os
    import time
    cutoff = time.time() - _UPLOAD_TMP_MAX_AGE_S
    try:
        names = os.listdir(tmp_dir)
    except OSError:
        return
    for name in names:
        path = os.path.join(tmp_dir, name)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                os.remove(path)
                logger.info("Swept stale upload temp file %s", path)
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
    # Re-stamp the worker's commit onto every report. Without this the SHA is
    # visible only until the first progress report overwrites the assignment
    # message, so a running fleet shows it on whichever cards happen not to have
    # reported yet — the one reading of the queue that is worse than not showing
    # it at all. The header is a session default on the worker's HTTP client, so
    # it rides along on progress reports for free; the upstream verdict does not
    # (it is sent per-poll), hence the SHA alone here.
    stamp = worker_code_stamp(request.headers.get("x-worker-version"))
    task.message = f"{(body.message or '')[:500 - len(stamp) - 1]} {stamp}".strip()
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
    # Stamp the heartbeat EXPLICITLY. updated_at is what the stuck-task sweeper
    # reads, and it is an ``onupdate`` — which fires only when an UPDATE is
    # actually emitted. The worker's heartbeat thread posts an IDENTICAL payload
    # every 30s (same phase, same percentage, same message), so between two
    # progress messages SQLAlchemy sees an empty change set, emits nothing, and
    # the timestamp freezes while the worker is demonstrably alive and calling.
    #
    # That is invisible while a scrape reports often. It becomes fatal on a long
    # one: the Jerusalem register's plan axis takes ~5 minutes per chunk, so the
    # message stood still past the 10-minute cutoff and a 13-hour run was about
    # to be auto-failed as a crashed worker. The heartbeat exists precisely to
    # prevent that, and it was being discarded by the ORM.
    task.updated_at = datetime.now(timezone.utc)
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

    # A source the publisher has REMOVED (not one that merely failed): record it
    # on the dataset so the site can say so, instead of the archive just looking
    # stale. Keep the FIRST detection — re-running a dead layer must not keep
    # resetting "gone since" to today.
    if ds_row is not None and _is_source_gone_error(body.error) and ds_row.source_gone_at is None:
        ds_row.source_gone_at = datetime.now(timezone.utc)
        await db.commit()
        logger.info("Dataset %s (%s) marked source_gone", ds_row.id, ds_row.title)

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


# ── נדל"ן לעם: address geocoding batches ─────────────────────────────────────
# Two endpoints, same worker Bearer as /poll. The worker fetches a batch, asks
# GovMap one address at a time at ~2/s, and posts back what it got.
#
# The batch is re-selected from `point IS NULL` on EVERY call and never reserved:
# the worker deliberately keeps no checkpoint, so an aborted batch is recovered
# purely by the next selection seeing those addresses again. A reservation would
# be a second copy of that state, free to disagree with the first.

@router.get("/geocode/batch/{task_id}")
async def geocode_batch(
    request: Request,
    task_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """Addresses for this batch: ``{"addresses": [{address_key, query}, ...]}``.

    The worker accepts either this shape or a bare array; the wrapped form is
    used so the count and the batch size can travel with it."""
    _verify_worker_key(request)
    from app.services import geocode_queue

    task = await db.get(ScrapeTask, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    size = int((task.params or {}).get("batch_size") or geocode_queue.BATCH_SIZE)
    rows = await geocode_queue.batch_for_task(size)
    logger.info("geocode: handing task %s a batch of %d", task_id, len(rows))
    return {"task_id": str(task_id), "batch_size": size,
            "count": len(rows), "addresses": rows}


@router.post("/geocode/results")
async def geocode_results(
    request: Request,
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """Record one batch's outcome and close its task.

    The three outcomes are NOT interchangeable:
      * ``results``   → recorded with the point.
      * ``not_found`` (== ``misses``) → recorded as a miss so the address stops
        being offered; without this, `point IS NULL` would re-offer it forever.
      * ``failed``    → recorded NOWHERE, so it returns in the next batch. It
        means "we could not ask", not "there is nothing there".
    Anything in neither list was never attempted, and likewise returns.
    """
    _verify_worker_key(request)
    from app.services import geocode_queue

    summary = await geocode_queue.record_results(body)

    task_id = body.get("task_id")
    if task_id:
        try:
            task = await db.get(ScrapeTask, uuid.UUID(str(task_id)))
        except (ValueError, AttributeError):
            task = None
        if task is not None:
            aborted = bool(body.get("aborted"))
            task.status = "completed"
            task.phase = "aborted" if aborted else "done"
            task.progress = 100
            task.completed_at = datetime.now(timezone.utc)
            task.message = (
                f"גיאוקודינג: {summary['recorded_hits']} נמצאו, "
                f"{summary['recorded_not_found']} לא קיימות, "
                f"{summary['requeued_failed']} חוזרות לתור"
                + (f" — נעצר: {body.get('abort_reason')}" if aborted else "")
            )[:500]
            await db.commit()
    return {"status": "recorded", **summary}


# ── יומן לעם (Ocal) diary import via the residential worker ────────────────────
# odata.org.il's file downloads 403 Render's datacenter IP, so the Render backend
# can discover diaries + parse/import their bytes but cannot DOWNLOAD them. The
# GOVSCRAPER worker (residential IP) closes the gap: it asks for candidates, fetches
# each file, and POSTs the bytes back here to import — the same client-upload
# pattern as /odata/import-file, but automated on the always-on worker fleet
# instead of a browser, so it runs even when the operator's own machine is off.

@router.get("/ocal-candidates")
async def ocal_worker_candidates(request: Request, limit: int = 25,
                                 _: None = Depends(_verify_worker_key)):
    """New diary resources for a residential worker to fetch + upload. Throttled
    to ~every 6h across the fleet: returns [] unless the newest diary source is
    >5h old, so one worker per window does the batch (the unique index on
    resource_id makes an overlapping fetch harmless). Cheap when throttled — one
    fast query, no odata discovery."""
    from app.services import ocal_db, ocal_import
    if not ocal_db.is_configured():
        return {"candidates": [], "reason": "ocal_not_configured"}
    # Stamp the poll (best-effort) so "is the residential worker actually
    # reaching us?" is answerable from SQL even when the throttle returns [] —
    # a successful stamp means an authenticated worker called in this minute.
    # The same round-trip reads the throttle window (hours) so it is tunable from
    # the admin without a redeploy — set worker_throttle_hours=0 to force the
    # fleet to import immediately (e.g. to verify the path end-to-end).
    throttle_h = 5.0
    try:
        row = await ocal_db.fetchrow(
            "UPDATE automation_settings SET worker_last_poll = now(), "
            "worker_poll_count = COALESCE(worker_poll_count, 0) + 1 "
            "RETURNING worker_throttle_hours")
        if row and row.get("worker_throttle_hours") is not None:
            throttle_h = float(row["worker_throttle_hours"])
    except Exception:  # noqa: BLE001 — a telemetry write must never fail the poll
        pass
    last = await ocal_db.fetchval("SELECT max(created_at) FROM diary_sources")
    if last is not None and throttle_h > 0:
        gap = await ocal_db.fetchval("SELECT EXTRACT(epoch FROM now() - $1)", last)
        if gap is not None and gap < throttle_h * 3600:
            return {"candidates": [], "reason": "throttled",
                    "next_in_s": int(throttle_h * 3600 - gap)}
    cands = await ocal_import.discover_candidates(limit=max(1, min(limit, 50)))
    return {"candidates": [
        {"resource_id": c["resource_id"], "url": c.get("url"), "format": c.get("format")}
        for c in cands if c.get("url")]}


@router.post("/ocal-import")
async def ocal_worker_import(request: Request,
                             resource_id: str = Form(...),
                             file: UploadFile = File(...),
                             fmt: str = Form(""),
                             refresh: bool = Form(False),
                             _: None = Depends(_verify_worker_key)):
    """Import a diary from the bytes a residential worker downloaded from odata.
    Applies the SAME auto-gate as the scheduler (title+date mapped, conf/min_rows)
    so non-diaries land in diary_exceptions. ``refresh=true`` (set by the worker on
    the last file of its batch) refreshes mv_entity_counts once, not per file."""
    from app.services import ocal_import
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    try:
        res = await ocal_import.import_diary_bytes(resource_id, data, fmt, refresh_matview=refresh)
        logger.info("worker ocal-import ok: %s -> %s events", resource_id, res.get("events_upserted"))
        return {"ok": True, "imported": True,
                **{k: res.get(k) for k in ("events_upserted", "source_id", "map_method", "confidence")}}
    except ocal_import.SkipImport as e:
        return {"ok": True, "imported": False, "skipped": True, "reason": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("worker ocal-import failed for %s", resource_id)
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


# ── ניגוד עניינים לעם (OCOI) document pipeline via the residential worker ─────
# Same split as the ocal diary path above, for the same reason: odata.org.il
# 403s Render's datacenter IP, so OVER discovers candidates (metadata is
# allowed) and the worker downloads + converts + extracts on a residential IP
# with real RAM and the poppler/tesseract binaries OVER cannot install. OCOI
# already shipped this contract as /api/v1/push/documents + tools/local_processor;
# this is that contract on OVER's worker auth and storage.

@router.get("/ocoi-candidates")
async def ocoi_worker_candidates(request: Request, limit: int = 25,
                                 _: None = Depends(_verify_worker_key)):
    """Conflict-of-interest PDFs on CKAN that we have not imported yet.

    Throttled across the fleet the same way the diary endpoint is: returns []
    unless the newest document is >5h old, so one worker per window does the
    batch. Cheap when throttled — one fast query, no CKAN round trip.
    """
    from app.services import ocoi_db, ocoi_ingest
    if not ocoi_db.is_configured():
        return {"candidates": [], "reason": "ocoi_not_configured"}
    last = await ocoi_db.fetchval("SELECT max(created_at) FROM documents")
    if last is not None:
        gap = await ocoi_db.fetchval("SELECT EXTRACT(epoch FROM now() - $1)", last)
        if gap is not None and gap < 5 * 3600:
            return {"candidates": [], "reason": "throttled",
                    "next_in_s": int(5 * 3600 - gap)}
    cands = await ocoi_ingest.discover_candidates(limit=max(1, min(limit, 50)))
    return {"candidates": cands}


@router.post("/ocoi-push")
async def ocoi_worker_push(request: Request,
                           payload: str = Form(...),
                           file: UploadFile | None = File(None),
                           _: None = Depends(_verify_worker_key)):
    """Store one worker-processed declaration.

    ``payload`` is the JSON metadata + markdown + extraction (OCOI's
    PushDocumentItem shape, minus pdf_base64); ``file`` carries the raw bytes.
    Multipart rather than base64-in-JSON because a 40MB PDF becomes ~53MB of
    base64 and the worker has no reason to pay that on every push.
    """
    from app.services import ocoi_ingest
    try:
        item = json.loads(payload)
    except (TypeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"payload is not JSON: {e}")
    if not isinstance(item, dict):
        raise HTTPException(status_code=400, detail="payload must be a JSON object")

    data = await file.read() if file is not None else None
    try:
        res = await ocoi_ingest.push_document(item, data)
        logger.info("worker ocoi-push ok: %s -> doc %s (%s relationships)",
                    item.get("file_url"), res.get("document_id"), res.get("relationships"))
        return {"ok": True, "created": True, **res}
    except ocoi_ingest.SkipDocument as e:
        return {"ok": True, "created": False, "skipped": True, "reason": str(e)}
    except Exception as e:  # noqa: BLE001
        logger.exception("worker ocoi-push failed for %s", item.get("file_url"))
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


@router.post("/ocoi-check-duplicates")
async def ocoi_worker_check_duplicates(request: Request,
                                       body: dict,
                                       _: None = Depends(_verify_worker_key)):
    """Which of these URLs do we already hold? Lets the worker skip downloading
    a file it would only be told to discard — the download is the expensive part
    of its cycle, not the push."""
    from app.services import ocoi_db
    urls = [u for u in (body or {}).get("urls") or [] if isinstance(u, str)]
    if not urls:
        return {"existing_urls": []}
    rows = await ocoi_db.fetch(
        "SELECT file_url FROM documents WHERE file_url = ANY($1::text[]) "
        "UNION SELECT file_url FROM ignored_resources WHERE file_url = ANY($1::text[])",
        urls)
    return {"existing_urls": [r["file_url"] for r in rows]}


@router.get("/ocoi-config")
async def ocoi_worker_config(request: Request,
                             _: None = Depends(_verify_worker_key)):
    """Runtime config for the OCOI pipeline — currently the extraction prompt.

    OCOI kept this in a JSON file on an ephemeral disk, so every admin edit was
    silently reverted by the next deploy and the pipeline always ran the
    hardcoded default. Serving it from the DB is what makes editing it mean
    anything. An empty value tells the worker to use its own built-in prompt.
    """
    from app.services import ocoi_db
    if not ocoi_db.is_configured():
        return {"extraction_prompt": "", "reason": "ocoi_not_configured"}
    val = await ocoi_db.fetchval(
        "SELECT value FROM site_content WHERE key = 'extraction_prompt'")
    return {"extraction_prompt": val or ""}
