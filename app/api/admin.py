"""Admin endpoints for approving/rejecting dataset tracking requests."""

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid, sanitize_ckan_name
from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.organization import Organization
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.rate_limit import limiter
from app.api.datasets import (
    apply_storage_target,
    dataset_is_neon_eligible,
    storage_target_of,
)
from app.services.odata_client import odata_client
from app.services import storage_client as storage_lib
from app.services.storage_client import storage_client
from app.services.r2_backfill import (
    backfill_dataset_to_r2,
    repair_dataset_r2,
    rebuild_dataset_versions,
    seed_neon_from_versions,
)
from app.worker.scheduler import add_poll_job, scheduler

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])


class ApproveRequest(BaseModel):
    poll_interval: int | None = None
    title: str | None = None
    organization_id: str | None = None  # "" or null to leave as-is; UUID to assign
    # Optional override for the user's submitted resource selection.
    # null = keep what's already on the dataset row.
    resource_ids: list[str] | None = None
    # Storage destination chosen at approval: "odata" | "r2" | "local".
    # Honored for scraper/govmap datasets; ckan datasets are forced to odata.
    # null → default ("r2" for scraper/govmap).
    storage_target: str | None = None


class PendingRequest(BaseModel):
    id: str
    ckan_id: str
    ckan_name: str
    title: str
    organization: str | None
    organization_id: str | None = None
    organization_title: str | None = None
    poll_interval: int
    status: str
    created_at: str
    requester_email: str
    requester_name: str
    source_type: str = "ckan"
    source_url: str | None = None
    storage_mode: str = "full_snapshot"
    # Suggested unified storage plan for the approval UI, derived from the
    # request's scraper_config (falls back to the global STORAGE_BACKEND
    # default, R2). The admin can override it on approve.
    storage_target: str = "r2"
    # Whether the NEON (tabular-rows) plans are offered for this source (CKAN
    # only). The approval UI greys NEON out when false.
    neon_eligible: bool = True
    resource_ids: list[str] | None = None  # what the requester chose
    resource_id: str | None = None  # legacy single-resource selection


@router.get("/pending", response_model=list[PendingRequest])
@limiter.limit("30/minute")
async def list_pending(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """List all pending dataset tracking requests."""
    result = await db.execute(
        select(TrackedDataset, User, Organization)
        .outerjoin(User, TrackedDataset.created_by == User.id)
        .outerjoin(Organization, TrackedDataset.organization_id == Organization.id)
        .where(TrackedDataset.status == "pending")
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = result.all()
    return [
        PendingRequest(
            id=str(ds.id),
            ckan_id=ds.ckan_id,
            ckan_name=ds.ckan_name,
            title=ds.title,
            organization=ds.organization,
            organization_id=str(ds.organization_id) if ds.organization_id else None,
            organization_title=org.title if org else None,
            poll_interval=ds.poll_interval,
            status=ds.status,
            created_at=ds.created_at.isoformat(),
            requester_email=requester.email if requester else "אנונימי",
            requester_name=requester.display_name if requester else "אנונימי",
            source_type=ds.source_type or "ckan",
            source_url=ds.source_url,
            storage_mode=ds.storage_mode or "full_snapshot",
            storage_target=storage_target_of(ds.scraper_config),
            neon_eligible=dataset_is_neon_eligible(ds),
            resource_ids=ds.resource_ids,
            resource_id=ds.resource_id,
        )
        for ds, requester, org in rows
    ]


@router.post("/approve/{dataset_id}")
@limiter.limit("30/minute")
async def approve_request(
    request: Request,
    dataset_id: str,
    background_tasks: BackgroundTasks,
    body: ApproveRequest | None = None,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Approve a pending dataset tracking request."""
    uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if ds.status != "pending":
        raise HTTPException(status_code=400, detail="Dataset is not pending")

    # Override poll interval if admin specified one
    if body and body.poll_interval is not None:
        ds.poll_interval = max(body.poll_interval, settings.min_poll_interval)

    # Override resource selection if admin specified one
    if body and body.resource_ids is not None:
        cleaned = [rid.strip() for rid in body.resource_ids if isinstance(rid, str) and rid.strip()]
        # Remove dupes preserving order.
        seen: set[str] = set()
        deduped = [rid for rid in cleaned if not (rid in seen or seen.add(rid))]
        if not deduped:
            raise HTTPException(
                status_code=400,
                detail="resource_ids must contain at least one resource id",
            )
        ds.resource_ids = deduped

    # Override title if admin specified one (applied BEFORE mirror creation
    # so the mirror gets the new title from the start)
    if body and body.title is not None and body.title.strip():
        ds.title = body.title.strip()

    # Assign organization if admin specified one
    if body and body.organization_id:
        org_uid = parse_uuid(body.organization_id, "organization_id")
        org_row = (await db.execute(
            select(Organization).where(Organization.id == org_uid)
        )).scalar_one_or_none()
        if not org_row:
            raise HTTPException(status_code=404, detail="Organization not found")
        ds.organization_id = org_row.id
        ds.organization = org_row.name

    # Resolve + pin the unified storage plan. The admin's explicit choice wins
    # for every source type; with no choice we default scraper/govmap to R2 and
    # CKAN to its derived target (the global STORAGE_BACKEND default, R2). NEON
    # plans are valid only for CKAN tabular sources.
    if body and body.storage_target:
        target = body.storage_target
    elif ds.source_type in ("scraper", "govmap"):
        target = "r2"
    else:
        target = storage_target_of(ds.scraper_config)
    if "neon" in target and not dataset_is_neon_eligible(ds):
        raise HTTPException(
            status_code=400,
            detail=(
                "NEON archiving is only available for CKAN (data.gov.il) "
                "tabular datasets; this source archives files/catalog data."
            ),
        )
    ds.scraper_config = apply_storage_target(ds.scraper_config, target)
    # The file-snapshot destination drives whether an ODATA mirror is needed.
    file_target = "odata" if target.startswith("odata") else (
        "local" if target == "local" else ("neon" if target == "neon" else "r2")
    )

    # Update status to active
    ds.status = "active"

    # Create odata mirror dataset only when this dataset stores files on ODATA.
    # R2 / local / neon datasets need no CKAN mirror (files go to R2, stay on
    # the worker, or there's no file snapshot at all for neon-only).
    if file_target == "odata" and not ds.odata_dataset_id and settings.odata_api_key:
        if ds.source_type == "scraper":
            mirror_name = f"gov-versions-scraper-{sanitize_ckan_name(ds.ckan_name)}"
            extras = [
                {"key": "source_type", "value": "scraper"},
                {"key": "source_url", "value": ds.source_url or ""},
                {"key": "auto_managed", "value": "true"},
            ]
        else:
            mirror_name = f"gov-versions-{sanitize_ckan_name(ds.ckan_name)}"
            if ds.resource_id:
                mirror_name = f"{mirror_name}-{ds.resource_id[:8]}"
            extras = [
                {"key": "source_ckan_id", "value": ds.ckan_id},
                {"key": "source_url", "value": f"{settings.data_gov_il_url}/dataset/{ds.ckan_name}"},
                {"key": "auto_managed", "value": "true"},
            ]
        # Build notes with explicit links back to the source page on gov.il /
        # data.gov.il AND to this dataset's version-history view on over.org.il.
        # Users reading the ODATA page should be able to jump both ways.
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
        try:
            mirror = await odata_client.create_dataset(
                name=mirror_name,
                title=f"[Versions] {ds.title}",
                owner_org=settings.odata_owner_org,
                notes=notes,
                extras=extras,
            )
            ds.odata_dataset_id = mirror["id"]
        except Exception as e1:
            logger.warning("Mirror create failed on approve: %s", e1)
            try:
                mirror = await odata_client.package_show(mirror_name)
                ds.odata_dataset_id = mirror["id"]
            except Exception as e2:
                logger.error("Mirror find also failed on approve: %s", e2)

    ds.updated_at = datetime.now(timezone.utc)
    await db.commit()

    # Add poll job to scheduler
    add_poll_job(str(ds.id), ds.poll_interval)

    # Auto-trigger first poll immediately after approval
    from app.worker.poll_job import poll_dataset
    background_tasks.add_task(poll_dataset, str(ds.id))

    from app.services.activity_log import log_event
    await log_event(
        event="approved", dataset=ds, status="ok", actor=user.email,
        message=f"הבקשה אושרה (אחסון: {target}) — נשלחה לגירוד",
    )

    return {"message": "Dataset approved", "dataset_id": str(ds.id)}


@router.post("/reject/{dataset_id}")
@limiter.limit("30/minute")
async def reject_request(
    request: Request,
    dataset_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Reject a pending dataset tracking request."""
    uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if ds.status != "pending":
        raise HTTPException(status_code=400, detail="Dataset is not pending")

    ds.status = "rejected"
    ds.updated_at = datetime.now(timezone.utc)
    await db.commit()

    from app.services.activity_log import log_event
    await log_event(
        event="rejected", dataset=ds, status="error", actor=user.email,
        message="הבקשה נדחתה",
    )

    return {"message": "Dataset rejected", "dataset_id": str(ds.id)}


@router.post("/backfill/{dataset_id}")
@limiter.limit("5/minute")
async def backfill_versions(
    request: Request,
    dataset_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Backfill version_index entries from existing odata.org.il resources.
    Reads the mirror dataset on odata.org.il and creates a version entry
    for each resource that has a date in its name.
    """
    import re
    from app.models.version_index import VersionIndex

    uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )
    ds = result.scalar_one_or_none()
    if not ds or not ds.odata_dataset_id:
        raise HTTPException(status_code=404, detail="Dataset not found or no odata mirror")

    # Fetch all resources from the odata mirror dataset
    try:
        pkg = await odata_client.package_show(ds.odata_dataset_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch odata dataset: {e}")

    resources = pkg.get("resources", [])
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")

    # Get existing version numbers
    existing = await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number)
    )
    existing_versions = existing.scalars().all()
    existing_dates = {v.metadata_modified for v in existing_versions}
    max_version = max((v.version_number for v in existing_versions), default=0)

    created = 0
    for r in sorted(resources, key=lambda x: x.get("name", "")):
        name = r.get("name", "")
        fmt = (r.get("format") or "").upper()

        # Skip metadata JSONs
        if fmt == "JSON" or "metadata" in name.lower():
            continue

        # Extract date from name
        match = date_pattern.search(name)
        if not match:
            continue

        date_str = match.group(1)
        if date_str in existing_dates:
            continue

        max_version += 1
        try:
            detected_at = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        version = VersionIndex(
            tracked_dataset_id=ds.id,
            version_number=max_version,
            metadata_modified=date_str,
            detected_at=detected_at,
            odata_metadata_resource_id=None,
            change_summary={
                "resources_added": [r["id"]],
                "resources_removed": [],
                "resources_modified": [],
                "total_resources": 1,
                "note": f"Backfilled from odata.org.il resource: {name}",
            },
            resource_mappings={
                "backfilled": r["id"],
                "_hashes": {},
                "_resource_ids": [],
            },
        )
        db.add(version)
        existing_dates.add(date_str)
        created += 1

    await db.commit()
    return {"message": f"Backfilled {created} versions", "dataset_id": str(ds.id)}


async def _run_migrate_r2_bg(ds_uuid: uuid.UUID, activate: bool, who: str) -> None:
    """Background runner for the ODATA→R2 migration. Opens its own DB session
    (the request's session is closed once the 202 response is sent) and runs
    the full download+upload+repoint, which can exceed the HTTP timeout."""
    from app.database import async_session
    async with async_session() as db:
        try:
            s = await backfill_dataset_to_r2(
                db, ds_uuid, apply=True, activate=activate,
            )
            logger.info(
                "R2 migrate for %s by %s: migrated=%s repointed=%s failed=%s "
                "activated=%s committed=%s",
                ds_uuid, who, s.get("migrated"), s.get("repointed_values"),
                len(s.get("failed") or []), s.get("activated"), s.get("committed"),
            )
        except Exception:
            logger.exception("R2 migrate background task failed for %s", ds_uuid)


@router.post("/datasets/{dataset_id}/migrate-r2")
@limiter.limit("5/minute")
async def migrate_dataset_to_r2(
    request: Request,
    dataset_id: str,
    background_tasks: BackgroundTasks,
    apply: bool = False,
    activate: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Migrate a dataset's file history from the ODATA mirror onto R2.

    See ``app.services.r2_backfill`` for mechanics. Dates/metadata already live
    in Postgres and are untouched; ODATA originals are kept as a backup.

    - ``apply=false`` (default): DRY-RUN. Returns the full plan inline (every
      unique ODATA resource → its target R2 key) without uploading or writing.
    - ``apply=true``: runs in the BACKGROUND (the download+upload of every
      version's files can exceed the HTTP request timeout) and returns 202.
      Verify completion via the versions API — mappings flip to ``r2:`` once
      done — or the server logs.
    - ``activate=true`` (with apply): also sets ``storage_backend=r2`` and
      ``is_active=true`` so future polls archive straight to R2.
    """
    uid = parse_uuid(dataset_id, "dataset_id")
    if not storage_client.is_configured():
        raise HTTPException(status_code=503, detail="R2 storage is not configured")

    if not apply:
        s = await backfill_dataset_to_r2(db, uid, apply=False, activate=False)
        if s.get("error"):
            raise HTTPException(status_code=404, detail=s["error"])
        return s

    # apply=true → validate the dataset exists, then hand off to the background.
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    background_tasks.add_task(_run_migrate_r2_bg, uid, activate, user.email)
    logger.info("R2 migrate started for %s (activate=%s) by %s",
                uid, activate, user.email)
    return {
        "status": "started",
        "dataset_id": str(uid),
        "activate": activate,
        "message": (
            "Migration running in the background. Re-check the versions API; "
            "resource_mappings flip to r2:<key> as it completes."
        ),
    }


@router.post("/register-append-datasets")
@limiter.limit("5/minute")
async def register_append_datasets_endpoint(
    request: Request,
    background_tasks: BackgroundTasks,
    poll: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Idempotently register the APPEND-set datasets (flights + vehicle
    registry) as tracked append_only sources. Same server-side-via-admin
    pattern as the R2 backfill, so prod can run it without shell access.

    For each newly-created dataset we schedule its recurring poll job
    immediately (don't wait for the next deploy's init_scheduler). With
    ``poll=true`` we also kick an initial seeding poll in the background
    (the vehicle seed streams ~4.1M rows — it runs detached, not inline).
    Re-running is safe: already-tracked datasets are skipped.
    """
    from app.services.append_registrar import register_append_datasets

    results = await register_append_datasets(db)

    # Schedule recurring polls for the rows that were just created (or already
    # exist but might be missing a job after a cold start). add_poll_job
    # anchors to last_polled_at=None → fires on the next due tick.
    for r in results:
        if r.get("status") in ("created", "skipped") and r.get("id"):
            try:
                ds = (await db.execute(
                    select(TrackedDataset).where(TrackedDataset.id == parse_uuid(r["id"], "id"))
                )).scalar_one_or_none()
                if ds:
                    add_poll_job(str(ds.id), ds.poll_interval, last_polled_at=ds.last_polled_at)
                    if poll:
                        from app.worker.poll_job import poll_dataset
                        background_tasks.add_task(poll_dataset, str(ds.id))
            except Exception:
                logger.exception("register-append: scheduling failed for %s", r.get("ckan_id"))

    logger.info("register-append-datasets by %s: %s", user.email, results)
    return {"results": results, "polled": poll}


@router.post("/datasets/{dataset_id}/repair-r2")
@limiter.limit("5/minute")
async def repair_dataset_r2_endpoint(
    request: Request,
    dataset_id: str,
    apply: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Post-migration repair for an R2-backed dataset (see
    ``app.services.r2_backfill.repair_dataset_r2``):

    1. Recover any version still pointing at a 404-deleted ODATA resource by
       relinking it to an existing R2 object with identical content (matched on
       the per-resource sha256 in ``_hashes``) — recovers historical bytes with
       zero new upload.
    2. Capture a friendly ``_names`` map (mapping key → clean Hebrew resource
       name) from ODATA while it's still readable, so the UI can label files by
       name instead of opaque source UUIDs.

    Idempotent. ``apply=false`` (default) reports what it would do. Runs inline
    (only resource_show round-trips + a DB write — no file transfers).
    """
    uid = parse_uuid(dataset_id, "dataset_id")
    if not storage_client.is_configured():
        raise HTTPException(status_code=503, detail="R2 storage is not configured")
    s = await repair_dataset_r2(db, uid, apply=apply)
    if s.get("error"):
        raise HTTPException(status_code=404, detail=s["error"])
    logger.info("R2 repair for %s by %s: recovered=%s named=%s unrecoverable=%s apply=%s",
                uid, user.email, s.get("recovered"), s.get("named"),
                len(s.get("unrecoverable") or []), apply)
    return s


@router.post("/datasets/{dataset_id}/rebuild-versions")
@limiter.limit("5/minute")
async def rebuild_versions_endpoint(
    request: Request,
    dataset_id: str,
    apply: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Rebuild a dataset's version history into a clean, deduplicated,
    date-numbered series (see ``app.services.r2_backfill.rebuild_dataset_versions``).

    DESTRUCTIVE on apply: deletes and recreates the dataset's VersionIndex rows
    (R2 bytes untouched) and retracks only the live resource going forward.
    ``apply=false`` (default) returns the proposed timeline without changing
    anything — always review it first.
    """
    uid = parse_uuid(dataset_id, "dataset_id")
    s = await rebuild_dataset_versions(db, uid, apply=apply)
    if s.get("error") and not s.get("timeline"):
        raise HTTPException(status_code=400, detail=s["error"])
    logger.info("Rebuild versions for %s by %s: %s→%s versions, apply=%s",
                uid, user.email, s.get("old_version_count"),
                s.get("new_version_count"), apply)
    return s


async def _run_seed_neon_bg(ds_uuid: uuid.UUID, who: str, reset: bool) -> None:
    """Background runner for the NEON seed (replaying ~25 snapshots + NEON
    inserts can exceed the HTTP timeout). Opens its own DB session."""
    from app.database import async_session
    async with async_session() as db:
        try:
            s = await seed_neon_from_versions(db, ds_uuid, apply=True, reset=reset)
            logger.info(
                "Seed NEON for %s by %s: inserted=%s table_total=%s skipped=%s archive_neon=%s",
                ds_uuid, who, s.get("rows_inserted"), s.get("table_total"),
                len(s.get("skipped") or []), s.get("archive_neon_enabled"),
            )
        except Exception:
            logger.exception("Seed NEON background task failed for %s", ds_uuid)


@router.post("/datasets/{dataset_id}/seed-neon")
@limiter.limit("3/minute")
async def seed_neon_endpoint(
    request: Request,
    dataset_id: str,
    background_tasks: BackgroundTasks,
    apply: bool = False,
    reset: bool = False,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Replay a dataset's per-version R2 snapshots into its NEON append table with
    historical first_seen, and enable the r2+neon dual-write going forward
    (see ``app.services.r2_backfill.seed_neon_from_versions``).

    Idempotent (ON CONFLICT DO NOTHING). ``apply=false`` (default) returns the
    per-version plan inline without writing. ``apply=true`` runs in the
    BACKGROUND (replaying snapshots + NEON inserts can exceed the HTTP timeout)
    and returns 'started'; verify via GET /api/append/{id}/schema (total grows
    as it runs). ``reset=true`` drops the table first (clean re-seed)."""
    uid = parse_uuid(dataset_id, "dataset_id")
    from app.services import append_store as _as
    if not _as.is_configured():
        raise HTTPException(status_code=409, detail="NEON append DB is not configured")
    if not apply:
        s = await seed_neon_from_versions(db, uid, apply=False)
        if s.get("error"):
            raise HTTPException(status_code=400, detail=s["error"])
        return s
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="dataset not found")
    if request.query_params.get("sync") in ("1", "true", "yes"):
        # Diagnostic: run inline and return the full summary (incl. per-version
        # skip reasons). Use only when the background run isn't inserting, since
        # a healthy seed can exceed the HTTP timeout.
        try:
            return await seed_neon_from_versions(db, uid, apply=True, reset=reset)
        except Exception as e:  # noqa: BLE001 — surface the real error for diagnosis
            import traceback
            logger.exception("seed_neon sync crashed for %s", uid)
            return {"error": f"{type(e).__name__}: {e}", "trace": traceback.format_exc()[-2500:]}
    background_tasks.add_task(_run_seed_neon_bg, uid, user.email, reset)
    logger.info("Seed NEON started for %s by %s", uid, user.email)
    return {"status": "started", "dataset_id": str(uid),
            "message": "Seeding NEON in the background; check /api/append/{id}/schema."}


@router.get("/scrape-tasks")
@limiter.limit("60/minute")
async def list_scrape_tasks(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Return current scrape queue: running tasks, pending tasks, and recent failures.

    Used by the admin Queue panel to show scrape progress and history.
    """
    from datetime import timedelta
    from app.models.scrape_task import ScrapeTask

    # All running tasks (typically 0-1 — only one worker active)
    running_result = await db.execute(
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "running")
        .order_by(ScrapeTask.created_at.asc())
    )
    running = [
        {
            "task_id": str(t.id),
            "dataset_id": str(ds.id),
            "dataset_title": ds.title,
            "phase": t.phase,
            "progress": t.progress,
            "message": t.message,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
        for t, ds in running_result.all()
    ]

    # Pending tasks (FIFO)
    pending_result = await db.execute(
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "pending")
        .order_by(ScrapeTask.created_at.asc())
    )
    pending = [
        {
            "task_id": str(t.id),
            "dataset_id": str(ds.id),
            "dataset_title": ds.title,
            "created_at": t.created_at.isoformat() if t.created_at else None,
        }
        for t, ds in pending_result.all()
    ]

    # Failed tasks in the last 24 hours (max 20)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    failed_result = await db.execute(
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(
            ScrapeTask.status == "failed",
            ScrapeTask.completed_at >= cutoff,
        )
        .order_by(ScrapeTask.completed_at.desc())
        .limit(20)
    )
    failed = [
        {
            "task_id": str(t.id),
            "dataset_id": str(ds.id),
            "dataset_title": ds.title,
            "phase": t.phase,
            "error": t.error,
            "completed_at": t.completed_at.isoformat() if t.completed_at else None,
        }
        for t, ds in failed_result.all()
    ]

    return {"running": running, "pending": pending, "failed": failed}


@router.delete("/scrape-tasks/{task_id}")
@limiter.limit("30/minute")
async def cancel_scrape_task(
    request: Request,
    task_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Cancel or dismiss a scrape task.

    - Pending → deleted from queue
    - Running → marked as failed (so a new task can be queued on next poll)
    - Failed → deleted (admin clears it from the recent-failures panel)
    - Completed → 400 (nothing to do)
    """
    from app.models.scrape_task import ScrapeTask

    tid = parse_uuid(task_id, "task_id")
    result = await db.execute(select(ScrapeTask).where(ScrapeTask.id == tid))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")

    if task.status == "pending":
        await db.delete(task)
        await db.commit()
        return {"status": "deleted", "was": "pending"}
    elif task.status == "running":
        task.status = "failed"
        task.phase = "cancelled"
        task.error = f"Cancelled by admin ({user.email})"
        task.completed_at = datetime.now(timezone.utc)
        await db.commit()
        return {"status": "failed", "was": "running"}
    elif task.status == "failed":
        await db.delete(task)
        await db.commit()
        return {"status": "deleted", "was": "failed"}
    else:
        raise HTTPException(status_code=400, detail=f"Cannot cancel task with status '{task.status}'")


@router.get("/datasets/{dataset_id}/scrape-tasks")
@limiter.limit("30/minute")
async def all_scrape_tasks_for_dataset(
    request: Request,
    dataset_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """All ScrapeTask rows for one dataset, every status, no time filter.

    Diagnostic for the symptom "I clicked 'דגום' and the response is OK but
    nothing shows in the queue panel" — the queue panel filters by
    status='pending'/'running'/failed-in-last-24h with an INNER JOIN to
    TrackedDataset; a row with a weird status, a NULL tracked_dataset_id,
    or just one created hours ago and short-circuiting _create_scrape_task
    will be invisible there but blocks new task creation. This endpoint
    cuts past all of that and shows the raw truth.
    """
    from app.models.scrape_task import ScrapeTask

    ds_uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(ScrapeTask)
        .where(ScrapeTask.tracked_dataset_id == ds_uid)
        .order_by(ScrapeTask.created_at.desc())
    )
    tasks = result.scalars().all()
    return {
        "dataset_id": dataset_id,
        "total": len(tasks),
        "tasks": [
            {
                "task_id": str(t.id),
                "status": t.status,
                "phase": t.phase,
                "message": t.message,
                "error": t.error,
                "progress": t.progress,
                "created_at": t.created_at.isoformat() if t.created_at else None,
                "updated_at": t.updated_at.isoformat() if t.updated_at else None,
                "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            }
            for t in tasks
        ],
    }


# ---------------------------------------------------------------------------
# GovMap full-coverage rollout (throttled — 2 layers/day, morning + evening)
# ---------------------------------------------------------------------------

@router.post("/govmap-coverage/populate")
@limiter.limit("6/minute")
async def govmap_coverage_populate(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Fetch GovMap's layer catalog and upsert the coverage inventory. Safe to
    re-run (idempotent). The twice-daily scheduler then walks this inventory."""
    from app.services.govmap_coverage import populate_from_catalog
    return await populate_from_catalog(db)


@router.get("/govmap-coverage/status")
@limiter.limit("60/minute")
async def govmap_coverage_status_ep(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Rollout progress: total layers, how many have ever been triggered, etc."""
    from app.services.govmap_coverage import coverage_status
    return await coverage_status(db)


@router.post("/govmap-coverage/scrape-next")
@limiter.limit("6/minute")
async def govmap_coverage_scrape_next(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Manually run one coverage tick now (same logic as the scheduled job:
    skips if the worker is busy, else scrapes the next layer). For testing /
    kicking off the rollout without waiting for the next 08:00/20:00 tick."""
    from app.services.govmap_coverage import scrape_next_layer
    return await scrape_next_layer()


_dataset_sizes_cache: dict = {"at": 0.0, "payload": None, "refreshing": False}
# Dataset sizes change slowly (only on new versions/uploads) — no need to
# re-fan-out every minute. A longer TTL means the background refresh (still
# a ~dozens-of-package_show burst against a shared 512MB dyno) fires far
# less often.
_DATASET_SIZES_TTL = 600.0  # seconds


async def _compute_dataset_sizes(db: AsyncSession) -> dict:
    """Do the actual odata fan-out + size aggregation. Pulled out of the
    route so it can run in a detached background task on a stale-cache hit
    (see dataset_sizes) without holding up the request that triggered it."""
    import asyncio

    from app.models.version_index import VersionIndex

    # All active datasets we'll size up
    ds_result = await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.is_active.is_(True),
            TrackedDataset.status == "active",
        )
    )
    datasets = ds_result.scalars().all()

    # Per-dataset version rows (single query, group in Python)
    v_result = await db.execute(
        select(VersionIndex).order_by(VersionIndex.version_number.asc())
    )
    versions_by_ds: dict[str, list[VersionIndex]] = {}
    for v in v_result.scalars().all():
        versions_by_ds.setdefault(str(v.tracked_dataset_id), []).append(v)

    # Capped low (not 10) — this fan-out shares a 512MB dyno with scheduled
    # poll jobs; too much concurrency here was a contributor to OOM crashes.
    sem = asyncio.Semaphore(4)

    async def _fetch_resource_sizes(ds: TrackedDataset) -> dict[str, int]:
        sizes: dict[str, int] = {}
        if ds.odata_dataset_id:
            async with sem:
                try:
                    pkg = await odata_client.package_show(ds.odata_dataset_id)
                    sizes.update({
                        r["id"]: int(r.get("size") or 0)
                        for r in (pkg.get("resources") or [])
                        if r.get("id")
                    })
                except Exception as e:
                    logger.warning("package_show failed for %s: %s", ds.ckan_name, e)
        # R2 backend: size objects via HEAD. Keyed by the FULL mapping value
        # ("r2:<key>") so the per-version sum below works unchanged. Fail-open
        # (missing → 0). Bounded: a version has a handful of objects.
        if storage_client.is_configured():
            r2_values: set[str] = set()
            for v in versions_by_ds.get(str(ds.id), []):
                for val in (v.resource_mappings or {}).values():
                    if storage_lib.is_storage_value(val):
                        r2_values.add(val)
                    elif isinstance(val, list):
                        r2_values.update(x for x in val if storage_lib.is_storage_value(x))
            async with sem:
                for val in r2_values:
                    try:
                        sizes[val] = await storage_client.object_size(val) or 0
                    except Exception:
                        sizes[val] = 0
        return sizes

    rid_size_lists = await asyncio.gather(
        *[_fetch_resource_sizes(ds) for ds in datasets],
        return_exceptions=False,
    )

    out_datasets = []
    for ds, rid_to_size in zip(datasets, rid_size_lists):
        ds_total = sum(rid_to_size.values())
        ds_versions: list[dict] = []
        latest_type: str | None = None
        latest_v_num = -1
        for v in versions_by_ds.get(str(ds.id), []):
            mappings = v.resource_mappings or {}
            seen: set[str] = set()
            v_total = 0
            for val in mappings.values():
                if isinstance(val, str):
                    if val and val not in seen:
                        seen.add(val)
                        v_total += rid_to_size.get(val, 0)
                elif isinstance(val, list):
                    for rid in val:
                        if isinstance(rid, str) and rid and rid not in seen:
                            seen.add(rid)
                            v_total += rid_to_size.get(rid, 0)
            v_type = (v.change_summary or {}).get("type") if isinstance(v.change_summary, dict) else None
            ds_versions.append({
                "version_id": str(v.id),
                "version_number": v.version_number,
                "total_bytes": v_total,
                "type": v_type,
            })
            if v.version_number > latest_v_num:
                latest_v_num = v.version_number
                latest_type = v_type
        # Suggest the delta-archive flow when a dataset is being stored
        # only as metadata-stubs (the >50k-row path) AND the operator
        # hasn't already opted in via storage_mode/append_key. This is
        # the visible signal that "this dataset deserves real archiving".
        suggest_delta = (
            latest_type == "large_dataset"
            and (ds.storage_mode != "append_only"
                 or not ((ds.scraper_config or {}).get("append_key")))
        )
        out_datasets.append({
            "dataset_id": str(ds.id),
            "title": ds.title,
            "total_bytes": ds_total,
            "version_count": len(ds_versions),
            "versions": ds_versions,
            "latest_version_type": latest_type,
            "suggest_delta_archive": suggest_delta,
        })

    return {"datasets": out_datasets}


async def _refresh_dataset_sizes_cache(db: AsyncSession) -> None:
    """Background refresh: recompute and update the cache, without an HTTP
    request waiting on it. Guarded by "refreshing" so a burst of page loads
    while the cache is stale triggers exactly one fan-out, not one per
    request."""
    import time

    try:
        payload = await _compute_dataset_sizes(db)
        _dataset_sizes_cache["payload"] = payload
        _dataset_sizes_cache["at"] = time.time()
    except Exception:
        logger.exception("Background dataset-sizes refresh failed")
    finally:
        _dataset_sizes_cache["refreshing"] = False
        await db.close()


@router.get("/dataset-sizes")
@limiter.limit("30/minute")
async def dataset_sizes(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Resource sizes per active dataset, plus per-version breakdown.

    Used by the admin UI's Datasets tab to surface "this dataset's mirror
    is now 4.2 GB across 7 versions" at a glance, and by the VersionsPage
    (admin only) to show "this version: 312 MB".

    Implementation: one CKAN package_show per dataset on the odata mirror
    (the resource list it returns includes a 'size' field in bytes), then
    sum sizes both globally and per-version via VersionIndex
    .resource_mappings. Concurrency capped at 10 to avoid hammering the
    odata API.

    Stale-while-revalidate: a fresh cache hit (< 60s) returns instantly.
    A stale hit ALSO returns instantly (the last known payload) and kicks
    off a background refresh — this used to fan out ~one package_show per
    active dataset synchronously on every admin page load once the cache
    aged out, which stalled the request (and the shared connection pool)
    for as long as odata.org.il took to answer all of them. Only a genuine
    cold start (no payload at all yet) blocks on the fan-out.

    Failure mode: if package_show fails for a dataset (network blip,
    mirror gone), we report total_bytes=0 for it rather than 500-ing
    the whole endpoint — partial data is more useful than none.
    """
    import asyncio
    import time

    from app.database import async_session

    now = time.time()
    cached = _dataset_sizes_cache
    is_stale = (now - cached["at"]) >= _DATASET_SIZES_TTL

    if cached["payload"] is not None:
        if is_stale and not cached["refreshing"]:
            cached["refreshing"] = True
            asyncio.create_task(_refresh_dataset_sizes_cache(async_session()))
        return cached["payload"]

    payload = await _compute_dataset_sizes(db)
    _dataset_sizes_cache["payload"] = payload
    _dataset_sizes_cache["at"] = now
    return payload


@router.get("/scheduled-jobs")
@limiter.limit("60/minute")
async def list_scheduled_jobs(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Return the live in-memory APScheduler job state joined with dataset
    metadata. Used by the admin UI's "תזמון משימות" section to confirm
    which datasets are actually scheduled, when their next poll fires,
    and how long ago they were last polled.

    APScheduler is the source of truth for `next_run_at` (its trigger
    state is what actually decides when poll_dataset fires). The DB is
    the source of truth for `last_polled_at` and the dataset's
    configured interval. We join the two so the admin sees, at a
    glance, every active dataset and any drift between its configured
    cadence and its actual schedule.
    """
    # Pull DB rows for active datasets
    db_result = await db.execute(
        select(TrackedDataset)
        .where(
            TrackedDataset.is_active.is_(True),
            TrackedDataset.status == "active",
        )
        .order_by(TrackedDataset.title.asc())
    )
    datasets = db_result.scalars().all()
    by_id: dict[str, TrackedDataset] = {str(d.id): d for d in datasets}

    # Pull APScheduler's view of pending fires
    job_next: dict[str, datetime] = {}
    if scheduler.running:
        for job in scheduler.get_jobs():
            jid = (job.id or "")
            if jid.startswith("poll_"):
                ds_id = jid[len("poll_"):]
                if job.next_run_time:
                    job_next[ds_id] = job.next_run_time

    now = datetime.now(timezone.utc)
    rows = []
    for ds_id, ds in by_id.items():
        next_run = job_next.get(ds_id)
        seconds_until = (
            int((next_run - now).total_seconds()) if next_run else None
        )
        rows.append({
            "dataset_id": ds_id,
            "title": ds.title,
            "source_type": ds.source_type,
            "poll_interval": ds.poll_interval,
            "last_polled_at": ds.last_polled_at.isoformat() if ds.last_polled_at else None,
            "next_run_at": next_run.isoformat() if next_run else None,
            "seconds_until_next_run": seconds_until,
            "scheduled": ds_id in job_next,
        })

    # Datasets with a job but no DB row would be orphans — surface them
    # so the operator can clean them up.
    orphans = [
        {"job_id": f"poll_{ds_id}", "next_run_at": dt.isoformat()}
        for ds_id, dt in job_next.items() if ds_id not in by_id
    ]

    rows.sort(key=lambda r: (
        r["seconds_until_next_run"] if r["seconds_until_next_run"] is not None else 10**12
    ))
    return {
        "scheduler_running": scheduler.running,
        "now": now.isoformat(),
        "jobs": rows,
        "orphan_jobs": orphans,
    }


# ---------------------------------------------------------------------------
# Datastore-push queue admin
# ---------------------------------------------------------------------------
#
# The durable queue that replaced FastAPI's BackgroundTasks for the
# datastore ingest step (see app/worker/datastore_push_runner.py).
# Two endpoints:
#   GET  /api/admin/datastore-jobs            — list recent jobs
#   POST /api/admin/datastore-jobs/{id}/retry — flip failed → pending

class DatastorePushJobOut(BaseModel):
    id: str
    tracked_dataset_id: str | None
    tracked_dataset_title: str | None
    resource_id: str
    csv_path: str
    csv_is_gzipped_in_source: bool
    status: str
    attempts: int
    rows_pushed: int
    total_rows: int | None
    error: str | None
    created_at: str
    started_at: str | None
    completed_at: str | None
    updated_at: str


@router.get("/datastore-jobs", response_model=list[DatastorePushJobOut])
@limiter.limit("60/minute")
async def list_datastore_jobs(
    request: Request,
    status: str | None = None,
    limit: int = 100,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Recent datastore push jobs, newest first.

    Optional ``?status=pending|running|success|failed`` filter for the
    "show me only the broken ones" view.
    """
    from app.models.datastore_push_job import DatastorePushJob

    q = select(DatastorePushJob).order_by(DatastorePushJob.created_at.desc())
    if status:
        q = q.where(DatastorePushJob.status == status)
    q = q.limit(min(limit, 500))
    rows = (await db.execute(q)).scalars().all()

    # Resolve dataset titles in one extra query so the admin UI can
    # show "which dataset is this push for" without N+1 round-trips.
    ds_ids = {r.tracked_dataset_id for r in rows if r.tracked_dataset_id}
    ds_titles: dict = {}
    if ds_ids:
        ds_rows = (
            await db.execute(
                select(TrackedDataset.id, TrackedDataset.title).where(
                    TrackedDataset.id.in_(ds_ids)
                )
            )
        ).all()
        ds_titles = {str(r.id): r.title for r in ds_rows}

    return [
        DatastorePushJobOut(
            id=str(r.id),
            tracked_dataset_id=str(r.tracked_dataset_id) if r.tracked_dataset_id else None,
            tracked_dataset_title=ds_titles.get(str(r.tracked_dataset_id)) if r.tracked_dataset_id else None,
            resource_id=r.resource_id,
            csv_path=r.csv_path,
            csv_is_gzipped_in_source=r.csv_is_gzipped_in_source,
            status=r.status,
            attempts=r.attempts,
            rows_pushed=r.rows_pushed,
            total_rows=r.total_rows,
            error=r.error,
            created_at=r.created_at.isoformat() if r.created_at else "",
            started_at=r.started_at.isoformat() if r.started_at else None,
            completed_at=r.completed_at.isoformat() if r.completed_at else None,
            updated_at=r.updated_at.isoformat() if r.updated_at else "",
        )
        for r in rows
    ]


@router.post("/datastore-jobs/{job_id}/retry")
@limiter.limit("30/minute")
async def retry_datastore_job(
    request: Request,
    job_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Flip a failed job back to ``pending`` so the runner re-attempts.

    Resets ``attempts`` to 0 — otherwise a job that already burned
    through MAX_ATTEMPTS would be skipped by the runner. Keeps the
    previous error in the column for audit (cleared once the new run
    succeeds; overwritten if it fails again).
    """
    from app.models.datastore_push_job import DatastorePushJob

    try:
        jid = uuid.UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid job id")
    job = (
        await db.execute(
            select(DatastorePushJob).where(DatastorePushJob.id == jid)
        )
    ).scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="job not found")
    if job.status not in ("failed", "success"):
        # Already pending or running — no-op.
        return {"status": job.status, "id": str(job.id)}
    job.status = "pending"
    job.attempts = 0
    job.started_at = None
    job.completed_at = None
    job.updated_at = datetime.now(timezone.utc)
    await db.commit()
    logger.info("Admin %s retried datastore push job %s", user.email, job.id)
    return {"status": "pending", "id": str(job.id)}


# ---------------------------------------------------------------------------
# OVER full-version coverage audit (requirement: every dataset must hold at
# least one full version archived on OVER, not just a request with no data).
# ---------------------------------------------------------------------------

class CoverageDataset(BaseModel):
    id: str
    title: str
    source_type: str
    storage_target: str
    version_count: int
    reason: str  # why it's listed (no_version | local_only)


class CoverageReport(BaseModel):
    total_active: int
    covered: int
    missing: list[CoverageDataset]       # active, on-OVER, but 0 versions
    local_only: list[CoverageDataset]    # intentionally not on OVER (local plan)


async def _coverage_scan(db: AsyncSession) -> CoverageReport:
    """Classify every active dataset by whether it has ≥1 version on OVER.

    'On OVER' = any storage plan except 'local' (local keeps files on the
    worker only). A dataset with 0 versions and a non-local plan is a real
    gap. Local-plan datasets are reported separately (off-OVER by design),
    not as gaps.
    """
    from sqlalchemy import func
    from app.models.version_index import VersionIndex

    rows = (await db.execute(
        select(TrackedDataset, func.count(VersionIndex.id))
        .outerjoin(VersionIndex, VersionIndex.tracked_dataset_id == TrackedDataset.id)
        .where(TrackedDataset.is_active.is_(True), TrackedDataset.status == "active")
        .group_by(TrackedDataset.id)
    )).all()

    missing: list[CoverageDataset] = []
    local_only: list[CoverageDataset] = []
    covered = 0
    for ds, vcount in rows:
        plan = storage_target_of(ds.scraper_config)
        brief = CoverageDataset(
            id=str(ds.id),
            title=ds.title,
            source_type=ds.source_type or "ckan",
            storage_target=plan,
            version_count=int(vcount or 0),
            reason="",
        )
        if plan == "local":
            brief.reason = "local_only"
            local_only.append(brief)
        elif (vcount or 0) == 0:
            brief.reason = "no_version"
            missing.append(brief)
        else:
            covered += 1
    return CoverageReport(
        total_active=len(rows),
        covered=covered,
        missing=missing,
        local_only=local_only,
    )


@router.get("/over-coverage", response_model=CoverageReport)
@limiter.limit("30/minute")
async def over_coverage(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Report which active datasets still lack a full version archived on OVER."""
    return await _coverage_scan(db)


@router.post("/over-coverage/fix", response_model=CoverageReport)
@limiter.limit("10/minute")
async def over_coverage_fix(
    request: Request,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Force a full snapshot for every active on-OVER dataset that has 0
    versions. Mirrors the per-dataset force_repoll + poll: nulls last_modified
    (so the unchanged-metadata guard is bypassed) and enqueues a poll, which
    creates a scrape task for scraper/govmap or runs the CKAN snapshot inline.
    Returns the coverage report AS OF BEFORE triggering (so the caller sees
    exactly what was kicked off)."""
    report = await _coverage_scan(db)
    if not report.missing:
        return report

    from app.worker.poll_job import poll_dataset

    ids = [m.id for m in report.missing]
    for did in ids:
        ds = (await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == uuid.UUID(did))
        )).scalar_one_or_none()
        if ds:
            ds.last_modified = None   # force re-snapshot (bypass no-change guard)
            ds.last_error = None
    await db.commit()
    for did in ids:
        background_tasks.add_task(poll_dataset, did)
    logger.info(
        "Admin %s triggered full-snapshot for %d uncovered datasets: %s",
        user.email, len(ids), ", ".join(i[:8] for i in ids),
    )
    return report


# ---------------------------------------------------------------------------
# Activity log — append-only event stream of the dataset/scrape lifecycle
# (requested / approved / rejected / queued / started / completed / failed),
# with the error message on failed or rejected steps. See
# app/models/activity_log.py and app/services/activity_log.py.
# ---------------------------------------------------------------------------

class ActivityLogEntry(BaseModel):
    id: str
    tracked_dataset_id: str | None
    dataset_title: str | None
    source_type: str | None
    event: str
    status: str
    message: str | None
    detail: str | None
    actor: str | None
    created_at: str


class ActivityLogPage(BaseModel):
    entries: list[ActivityLogEntry]
    total: int
    limit: int
    offset: int


@router.get("/activity-log", response_model=ActivityLogPage)
@limiter.limit("60/minute")
async def activity_log(
    request: Request,
    dataset_id: str | None = None,
    event: str | None = None,
    status: str | None = None,
    q: str | None = None,
    limit: int = 100,
    offset: int = 0,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Newest-first page of activity-log events. Optional filters: a specific
    dataset, an event type, a status (ok/error/info), or a free-text match on
    the dataset title / message / detail."""
    from sqlalchemy import func, or_
    from app.models.activity_log import ActivityLog

    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))

    conds = []
    if dataset_id:
        try:
            conds.append(ActivityLog.tracked_dataset_id == uuid.UUID(dataset_id))
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid dataset_id")
    if event:
        conds.append(ActivityLog.event == event)
    if status:
        conds.append(ActivityLog.status == status)
    if q and q.strip():
        like = f"%{q.strip()}%"
        conds.append(or_(
            ActivityLog.dataset_title.ilike(like),
            ActivityLog.message.ilike(like),
            ActivityLog.detail.ilike(like),
        ))

    base = select(ActivityLog)
    for c in conds:
        base = base.where(c)

    total = (await db.execute(
        select(func.count()).select_from(base.subquery())
    )).scalar() or 0

    rows = (await db.execute(
        base.order_by(ActivityLog.created_at.desc()).limit(limit).offset(offset)
    )).scalars().all()

    return ActivityLogPage(
        entries=[
            ActivityLogEntry(
                id=str(e.id),
                tracked_dataset_id=str(e.tracked_dataset_id) if e.tracked_dataset_id else None,
                dataset_title=e.dataset_title,
                source_type=e.source_type,
                event=e.event,
                status=e.status,
                message=e.message,
                detail=e.detail,
                actor=e.actor,
                created_at=e.created_at.isoformat() if e.created_at else "",
            )
            for e in rows
        ],
        total=int(total),
        limit=limit,
        offset=offset,
    )


@router.post("/backfill-ckan-uuids")
@limiter.limit("2/minute")
async def backfill_ckan_uuids(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """One-time: populate each CKAN dataset's data.gov.il dataset UUID
    (scraper_config.ckan_dataset_uuid) by calling package_show, so the
    /api/v1/datasets ?ckan_id= bridge resolves a data.gov.il UUID. Idempotent —
    skips datasets that already have it. New datasets fill in on their next poll."""
    from app.services.ckan_client import ckan_client

    rows = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.source_type == "ckan")
    )).scalars().all()
    updated = skipped = 0
    failed: list[dict] = []
    for ds in rows:
        sc = ds.scraper_config or {}
        if sc.get("ckan_dataset_uuid"):
            skipped += 1
            continue
        try:
            pkg = await ckan_client.package_show(ds.ckan_id)
            gov = pkg.get("id")
            if gov:
                ds.scraper_config = {**sc, "ckan_dataset_uuid": gov}
                updated += 1
            else:
                failed.append({"id": str(ds.id), "ckan_id": ds.ckan_id, "error": "no id in package"})
        except Exception as e:  # noqa: BLE001
            failed.append({"id": str(ds.id), "ckan_id": ds.ckan_id, "error": str(e)[:150]})
    await db.commit()
    logger.info("Admin %s backfilled %d ckan_dataset_uuid (%d skipped, %d failed)",
                user.email, updated, skipped, len(failed))
    return {"total_ckan": len(rows), "updated": updated, "skipped": skipped, "failed": failed}
