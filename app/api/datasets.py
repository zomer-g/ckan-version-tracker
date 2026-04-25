import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid, sanitize_ckan_name, scraper_url_slug
from app.auth.dependencies import get_admin_user, get_current_user
from app.database import get_db
from app.models.organization import Organization
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.rate_limit import limiter
from app.services.ckan_client import ckan_client
from app.services.odata_client import odata_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


class TrackRequest(BaseModel):
    ckan_id: str | None = None
    source_type: str = "ckan"  # "ckan" | "scraper"
    source_url: str | None = None
    title: str | None = None
    scraper_config: dict | None = None
    poll_interval: int = 604800
    preferred_interval: int | None = None
    resource_id: str | None = None


class UpdateRequest(BaseModel):
    poll_interval: int | None = None
    is_active: bool | None = None
    title: str | None = None
    organization_id: str | None = None  # "" or null to clear; UUID to assign


class TagBrief(BaseModel):
    id: str
    name: str

    model_config = {"from_attributes": True}


class DatasetResponse(BaseModel):
    id: str
    ckan_id: str
    ckan_name: str
    title: str
    organization: str | None
    organization_id: str | None = None
    organization_title: str | None = None
    odata_dataset_id: str | None
    poll_interval: int
    is_active: bool
    status: str = "active"
    last_polled_at: str | None
    last_modified: str | None
    version_count: int = 0
    requester_name: str | None = None
    requester_email: str | None = None
    resource_id: str | None = None
    resource_name: str | None = None
    requester_notes: str = ""
    source_url: str = ""
    source_type: str = "ckan"
    tags: list[TagBrief] = []

    model_config = {"from_attributes": True}


def _build_source_url(ds: TrackedDataset) -> str:
    """Compute the source URL for a tracked dataset."""
    if ds.source_type == "scraper" and ds.source_url:
        return ds.source_url
    org = ds.organization or ""
    name = ds.ckan_name or ""
    base = f"https://data.gov.il/he/datasets/{org}/{name}"
    if ds.resource_id:
        base = f"{base}/{ds.resource_id}"
    return base


@router.get("", response_model=list[DatasetResponse])
async def list_tracked(
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint — lists all active/pending tracked datasets."""
    from app.models.user import User as UserModel
    from app.models.version_index import VersionIndex
    from sqlalchemy import func
    from sqlalchemy.orm import selectinload

    result = await db.execute(
        select(TrackedDataset, UserModel, Organization)
        .options(selectinload(TrackedDataset.tags))
        .outerjoin(UserModel, TrackedDataset.created_by == UserModel.id)
        .outerjoin(Organization, TrackedDataset.organization_id == Organization.id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = result.unique().all()

    # Get version counts for all datasets in one query
    count_result = await db.execute(
        select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
        .group_by(VersionIndex.tracked_dataset_id)
    )
    version_counts = dict(count_result.all())
    # Build response — no external API calls here (performance critical)
    response_list = []
    for ds, requester, org in rows:
        response_list.append(
            DatasetResponse(
                id=str(ds.id),
                ckan_id=ds.ckan_id,
                ckan_name=ds.ckan_name,
                title=ds.title,
                organization=ds.organization,
                organization_id=str(ds.organization_id) if ds.organization_id else None,
                organization_title=org.title if org else None,
                odata_dataset_id=ds.odata_dataset_id,
                poll_interval=ds.poll_interval,
                is_active=ds.is_active,
                status=ds.status,
                last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
                last_modified=ds.last_modified,
                requester_name=requester.display_name if requester else None,
                requester_email=requester.email if requester else None,
                resource_id=ds.resource_id,
                resource_name=None,  # resource name is already in the title
                source_url=_build_source_url(ds),
                source_type=ds.source_type or "ckan",
                version_count=version_counts.get(ds.id, 0),
                tags=[TagBrief(id=str(t.id), name=t.name) for t in ds.tags],
            )
        )
    return response_list


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def track_dataset(
    body: TrackRequest,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    raw_interval = body.preferred_interval if body.preferred_interval is not None else body.poll_interval
    interval = max(raw_interval, settings.min_poll_interval)

    # Determine status based on admin privilege
    dataset_status = "active" if user.is_admin else "pending"

    # ---- Scraper-type dataset ----
    if body.source_type == "scraper":
        if not body.source_url:
            raise HTTPException(status_code=400, detail="source_url is required for scraper datasets")
        if not body.title:
            raise HTTPException(status_code=400, detail="title is required for scraper datasets")

        # Parse collector name from URL for ckan_id/ckan_name
        from app.api.govil import _parse_govil_url
        page_type, collector_name = _parse_govil_url(body.source_url)
        if not collector_name:
            raise HTTPException(status_code=400, detail="Invalid gov.il collector URL")

        # Build a unique slug that includes a hash of the full source URL,
        # so two URLs with the same collector path (e.g. /collectors/policies
        # with different officeId query params) don't collide on the same mirror.
        unique_slug = scraper_url_slug(collector_name, body.source_url)
        ckan_id = f"govil-scraper-{unique_slug}"
        ckan_name = unique_slug

        # Duplicate check by source_url
        existing = await db.execute(
            select(TrackedDataset).where(TrackedDataset.source_url == body.source_url)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Dataset already tracked")

        mirror_name = f"gov-versions-scraper-{unique_slug}"

        # Create mirror on odata.org.il for active datasets
        odata_dataset_id = None
        if dataset_status == "active" and settings.odata_api_key:
            try:
                mirror = await odata_client.create_dataset(
                    name=mirror_name,
                    title=f"[Versions] {body.title}",
                    owner_org=settings.odata_owner_org,
                    notes=odata_client.NOTES_SCRAPER,
                    extras=[
                        {"key": "source_type", "value": "scraper"},
                        {"key": "source_url", "value": body.source_url},
                        {"key": "auto_managed", "value": "true"},
                    ],
                )
                odata_dataset_id = mirror["id"]
            except Exception as e1:
                logger.warning("Mirror create failed: %s", e1)
                try:
                    mirror = await odata_client.package_show(mirror_name)
                    odata_dataset_id = mirror["id"]
                except Exception as e2:
                    logger.error("Mirror find also failed: %s", e2)

        ds = TrackedDataset(
            ckan_id=ckan_id,
            ckan_name=ckan_name,
            title=body.title,
            organization="gov.il",
            source_type="scraper",
            source_url=body.source_url,
            scraper_config=body.scraper_config or {"download_files": False},
            odata_dataset_id=odata_dataset_id,
            poll_interval=interval,
            status=dataset_status,
            created_by=user.id,
            last_modified=None,
        )
        db.add(ds)
        await db.commit()
        await db.refresh(ds)

        if dataset_status == "active":
            from app.worker.poll_job import poll_dataset
            background_tasks.add_task(poll_dataset, str(ds.id))

        return DatasetResponse(
            id=str(ds.id),
            ckan_id=ds.ckan_id,
            ckan_name=ds.ckan_name,
            title=ds.title,
            organization=ds.organization,
            odata_dataset_id=ds.odata_dataset_id,
            poll_interval=ds.poll_interval,
            is_active=ds.is_active,
            status=ds.status,
            last_polled_at=None,
            last_modified=ds.last_modified,
            source_url=ds.source_url or "",
            source_type=ds.source_type,
        )

    # ---- CKAN-type dataset (original flow) ----
    if not body.ckan_id:
        raise HTTPException(status_code=400, detail="ckan_id is required for CKAN datasets")

    # Check for duplicate (ckan_id + resource_id combination)
    dup_query = select(TrackedDataset).where(TrackedDataset.ckan_id == body.ckan_id)
    if body.resource_id:
        dup_query = dup_query.where(TrackedDataset.resource_id == body.resource_id)
    else:
        dup_query = dup_query.where(TrackedDataset.resource_id.is_(None))
    existing = await db.execute(dup_query)
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Dataset already tracked")

    try:
        pkg = await ckan_client.package_show(body.ckan_id)
    except Exception:
        logger.exception("Failed to fetch dataset %s from data.gov.il", body.ckan_id)
        raise HTTPException(status_code=404, detail="Dataset not found on data.gov.il")

    org_name = pkg.get("organization", {}).get("name", "") if pkg.get("organization") else ""

    # Link to local Organization row if it exists (best-effort)
    org_id = None
    if org_name:
        org_row = (await db.execute(
            select(Organization).where(Organization.name == org_name)
        )).scalar_one_or_none()
        if org_row:
            org_id = org_row.id

    # Resolve resource name if tracking a specific resource
    resource_name = None
    if body.resource_id:
        for r in pkg.get("resources", []):
            if r["id"] == body.resource_id:
                resource_name = r.get("name") or r.get("description") or body.resource_id
                break
        if not resource_name:
            raise HTTPException(status_code=404, detail="Resource not found in dataset")

    # Build title: append resource name if tracking a specific resource
    dataset_title = pkg.get("title", pkg["name"])
    if resource_name:
        dataset_title = f"{dataset_title} — {resource_name}"

    mirror_name = f"gov-versions-{sanitize_ckan_name(pkg['name'])}"
    if body.resource_id:
        mirror_name = f"{mirror_name}-{body.resource_id[:8]}"

    # Create mirror dataset on odata.org.il only for active (admin-approved) datasets
    odata_dataset_id = None
    if dataset_status == "active" and settings.odata_api_key:
        try:
            mirror = await odata_client.create_dataset(
                name=mirror_name,
                title=f"[Versions] {dataset_title}",
                owner_org=settings.odata_owner_org,
                extras=[
                    {"key": "source_ckan_id", "value": body.ckan_id},
                    {"key": "source_url", "value": f"{settings.data_gov_il_url}/dataset/{pkg['name']}"},
                    {"key": "auto_managed", "value": "true"},
                ],
            )
            odata_dataset_id = mirror["id"]
        except Exception as e1:
            logger.warning("Mirror create failed: %s", e1)
            try:
                mirror = await odata_client.package_show(mirror_name)
                odata_dataset_id = mirror["id"]
            except Exception as e2:
                logger.error("Mirror find also failed: %s", e2)
    elif dataset_status == "pending":
        logger.info("Dataset %s pending admin approval — skipping odata mirror", body.ckan_id)
    else:
        logger.info("ODATA_API_KEY not set — tracking without odata.org.il mirror")

    ds = TrackedDataset(
        ckan_id=body.ckan_id,
        ckan_name=pkg["name"],
        resource_id=body.resource_id,
        title=dataset_title,
        organization=org_name,
        organization_id=org_id,
        odata_dataset_id=odata_dataset_id,
        poll_interval=interval,
        status=dataset_status,
        created_by=user.id,
        last_modified=None,  # None so first poll always creates version 1
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)

    # Auto-trigger first poll for admin-approved datasets
    if dataset_status == "active":
        from app.worker.poll_job import poll_dataset
        background_tasks.add_task(poll_dataset, str(ds.id))

    return DatasetResponse(
        id=str(ds.id),
        ckan_id=ds.ckan_id,
        ckan_name=ds.ckan_name,
        title=ds.title,
        organization=ds.organization,
        odata_dataset_id=ds.odata_dataset_id,
        poll_interval=ds.poll_interval,
        is_active=ds.is_active,
        status=ds.status,
        last_polled_at=None,
        last_modified=ds.last_modified,
        resource_id=ds.resource_id,
        resource_name=resource_name,
        source_type=ds.source_type,
    )


@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_tracked(
    dataset_id: str,
    body: UpdateRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    from sqlalchemy.orm import selectinload

    uid = parse_uuid(dataset_id, "dataset_id")
    query = (
        select(TrackedDataset)
        .options(selectinload(TrackedDataset.tags))
        .where(TrackedDataset.id == uid)
    )
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if body.poll_interval is not None:
        ds.poll_interval = max(body.poll_interval, settings.min_poll_interval)
    if body.is_active is not None:
        ds.is_active = body.is_active

    if body.organization_id is not None:
        if body.organization_id == "":
            ds.organization_id = None
        else:
            org_uid = parse_uuid(body.organization_id, "organization_id")
            org_row = (await db.execute(
                select(Organization).where(Organization.id == org_uid)
            )).scalar_one_or_none()
            if not org_row:
                raise HTTPException(status_code=404, detail="Organization not found")
            ds.organization_id = org_row.id
            # Also update the legacy display string so it stays in sync
            ds.organization = org_row.name

    title_changed = False
    if body.title is not None and body.title.strip() and body.title.strip() != ds.title:
        ds.title = body.title.strip()
        title_changed = True

    await db.commit()
    await db.refresh(ds)

    # Propagate title change to odata mirror (best-effort, don't fail the request)
    if title_changed and ds.odata_dataset_id:
        try:
            await odata_client.package_patch(
                ds.odata_dataset_id,
                title=f"[Versions] {ds.title}",
            )
            logger.info("Updated odata mirror title for %s", ds.id)
        except Exception as e:
            logger.warning("Failed to update odata mirror title: %s", e)

    org_title = None
    if ds.organization_id:
        org_row = (await db.execute(
            select(Organization).where(Organization.id == ds.organization_id)
        )).scalar_one_or_none()
        if org_row:
            org_title = org_row.title

    return DatasetResponse(
        id=str(ds.id),
        ckan_id=ds.ckan_id,
        ckan_name=ds.ckan_name,
        title=ds.title,
        organization=ds.organization,
        organization_id=str(ds.organization_id) if ds.organization_id else None,
        organization_title=org_title,
        odata_dataset_id=ds.odata_dataset_id,
        poll_interval=ds.poll_interval,
        is_active=ds.is_active,
        status=ds.status,
        last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
        last_modified=ds.last_modified,
        resource_id=ds.resource_id,
        source_url=_build_source_url(ds),
        source_type=ds.source_type or "ckan",
        tags=[TagBrief(id=str(t.id), name=t.name) for t in ds.tags],
    )


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def untrack_dataset(
    dataset_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Remove a tracked dataset AND its ODATA mirror package.

    Order:
      1. Call package_delete (+ dataset_purge) on ODATA if we have a mirror.
         Best-effort — ODATA errors are logged but don't block the local row
         deletion; that way a user can always clean up broken state by
         deleting on this side even if ODATA is down.
      2. Remove scrape jobs from the APScheduler so no stale poll runs.
      3. Delete the TrackedDataset row (cascades to VersionIndex via FK).
    """
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if ds.odata_dataset_id:
        try:
            await odata_client.package_delete(ds.odata_dataset_id, purge=True)
            logger.info("Deleted ODATA package %s for tracked dataset %s",
                        ds.odata_dataset_id, uid)
        except Exception as e:
            logger.warning(
                "ODATA package_delete failed for %s (tracked %s): %s — "
                "continuing with local delete anyway",
                ds.odata_dataset_id, uid, e,
            )

    # Remove any running poll job so we don't keep polling a deleted dataset
    try:
        from app.worker.scheduler import remove_poll_job
        remove_poll_job(str(uid))
    except Exception as e:
        logger.warning("remove_poll_job(%s) failed: %s", uid, e)

    await db.delete(ds)
    await db.commit()
    logger.info("Tracked dataset %s deleted by %s", uid, user.email)


@router.post("/{dataset_id}/poll")
async def trigger_poll(
    dataset_id: str,
    background_tasks: BackgroundTasks,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    from app.worker.poll_job import poll_dataset

    background_tasks.add_task(poll_dataset, str(ds.id))
    return {"message": "Poll triggered", "dataset_id": str(ds.id)}


# ---------------------------------------------------------------------------
# Public endpoints (no auth required)
# ---------------------------------------------------------------------------

class TrackingRequest(BaseModel):
    ckan_id: str | None = None
    source_type: str = "ckan"  # "ckan" | "scraper"
    source_url: str | None = None
    title: str | None = None
    resource_id: str | None = None
    preferred_interval: int = 604800
    requester_name: str = ""
    requester_notes: str = ""
    requester_contact: str = ""


@router.post("/requests", status_code=status.HTTP_201_CREATED)
@limiter.limit("10/hour")
async def submit_tracking_request(
    request: Request,
    body: TrackingRequest,
    db: AsyncSession = Depends(get_db),
):
    """Anonymous endpoint -- anyone can request tracking without login."""

    interval = max(body.preferred_interval, settings.min_poll_interval)

    # ---- Scraper-type request ----
    if body.source_type == "scraper":
        if not body.source_url:
            raise HTTPException(status_code=400, detail="source_url is required for scraper datasets")
        if not body.title:
            raise HTTPException(status_code=400, detail="title is required for scraper datasets")

        from app.api.govil import _parse_govil_url
        page_type, collector_name = _parse_govil_url(body.source_url)
        if not collector_name:
            raise HTTPException(status_code=400, detail="Invalid gov.il collector URL")

        # Duplicate check by source_url
        existing = await db.execute(
            select(TrackedDataset).where(TrackedDataset.source_url == body.source_url)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Already tracked or requested")

        unique_slug = scraper_url_slug(collector_name, body.source_url)
        ds = TrackedDataset(
            ckan_id=f"govil-scraper-{unique_slug}",
            ckan_name=unique_slug,
            title=body.title,
            organization="gov.il",
            source_type="scraper",
            source_url=body.source_url,
            scraper_config={"download_files": False},
            poll_interval=interval,
            status="pending",
            created_by=None,
            last_modified=None,
        )
        db.add(ds)
        await db.commit()
        return {"message": "Request submitted", "status": "pending"}

    # ---- CKAN-type request (original flow) ----
    if not body.ckan_id:
        raise HTTPException(status_code=400, detail="ckan_id is required for CKAN datasets")

    # Check not already tracked
    query = select(TrackedDataset).where(TrackedDataset.ckan_id == body.ckan_id)
    if body.resource_id:
        query = query.where(TrackedDataset.resource_id == body.resource_id)
    existing = await db.execute(query)
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Already tracked or requested")

    # Fetch dataset info from data.gov.il
    try:
        pkg = await ckan_client.package_show(body.ckan_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Find resource name if resource_id provided
    resource_name = ""
    if body.resource_id:
        for r in pkg.get("resources", []):
            if r["id"] == body.resource_id:
                resource_name = r.get("name", "")
                break

    org_name = pkg.get("organization", {}).get("name", "") if pkg.get("organization") else ""
    title = pkg.get("title", pkg["name"])
    if resource_name:
        title = f"{title} — {resource_name}"

    # Link to local Organization row if it exists (best-effort)
    org_id = None
    if org_name:
        org_row = (await db.execute(
            select(Organization).where(Organization.name == org_name)
        )).scalar_one_or_none()
        if org_row:
            org_id = org_row.id

    ds = TrackedDataset(
        ckan_id=body.ckan_id,
        ckan_name=pkg["name"],
        resource_id=body.resource_id,
        title=title,
        organization=org_name,
        organization_id=org_id,
        poll_interval=interval,
        status="pending",
        created_by=None,
        last_modified=None,
    )
    db.add(ds)
    await db.commit()

    return {"message": "Request submitted", "status": "pending"}


@router.get("/public/{dataset_id}")
async def get_tracked_public(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Public endpoint -- get a single active tracked dataset."""
    from sqlalchemy.orm import selectinload

    uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(TrackedDataset)
        .options(selectinload(TrackedDataset.tags))
        .where(
            TrackedDataset.id == uid,
            TrackedDataset.status == "active",
        )
    )
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return DatasetResponse(
        id=str(ds.id),
        ckan_id=ds.ckan_id,
        ckan_name=ds.ckan_name,
        title=ds.title,
        organization=ds.organization,
        odata_dataset_id=ds.odata_dataset_id,
        poll_interval=ds.poll_interval,
        is_active=ds.is_active,
        status=ds.status,
        last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
        last_modified=ds.last_modified,
        resource_id=ds.resource_id,
        source_url=_build_source_url(ds),
        source_type=ds.source_type or "ckan",
        tags=[TagBrief(id=str(t.id), name=t.name) for t in ds.tags],
    )
