import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid, sanitize_ckan_name
from app.auth.dependencies import get_admin_user, get_current_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.rate_limit import limiter
from app.services.ckan_client import ckan_client
from app.services.odata_client import odata_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


class TrackRequest(BaseModel):
    ckan_id: str
    poll_interval: int = 604800
    preferred_interval: int | None = None
    resource_id: str | None = None


class UpdateRequest(BaseModel):
    poll_interval: int | None = None
    is_active: bool | None = None


class DatasetResponse(BaseModel):
    id: str
    ckan_id: str
    ckan_name: str
    title: str
    organization: str | None
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

    model_config = {"from_attributes": True}


def _build_source_url(ds: TrackedDataset) -> str:
    """Compute the data.gov.il source URL for a tracked dataset."""
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
    result = await db.execute(
        select(TrackedDataset, UserModel)
        .outerjoin(UserModel, TrackedDataset.created_by == UserModel.id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = result.all()
    # Resolve resource names for datasets that track a specific resource
    response_list = []
    for ds, requester in rows:
        resource_name = None
        if ds.resource_id:
            try:
                pkg = await ckan_client.package_show(ds.ckan_id)
                for r in pkg.get("resources", []):
                    if r["id"] == ds.resource_id:
                        resource_name = r.get("name") or r.get("description") or r["id"]
                        break
            except Exception:
                resource_name = ds.resource_id
        response_list.append(
            DatasetResponse(
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
                requester_name=requester.display_name if requester else None,
                requester_email=requester.email if requester else None,
                resource_id=ds.resource_id,
                resource_name=resource_name,
                source_url=_build_source_url(ds),
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
    # Check for duplicate (ckan_id + resource_id combination)
    dup_query = select(TrackedDataset).where(TrackedDataset.ckan_id == body.ckan_id)
    if body.resource_id:
        dup_query = dup_query.where(TrackedDataset.resource_id == body.resource_id)
    else:
        dup_query = dup_query.where(TrackedDataset.resource_id.is_(None))
    existing = await db.execute(dup_query)
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Dataset already tracked")

    raw_interval = body.preferred_interval if body.preferred_interval is not None else body.poll_interval
    interval = max(raw_interval, settings.min_poll_interval)

    try:
        pkg = await ckan_client.package_show(body.ckan_id)
    except Exception:
        logger.exception("Failed to fetch dataset %s from data.gov.il", body.ckan_id)
        raise HTTPException(status_code=404, detail="Dataset not found on data.gov.il")

    org_name = pkg.get("organization", {}).get("name", "") if pkg.get("organization") else ""

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

    # Determine status based on admin privilege
    dataset_status = "active" if user.is_admin else "pending"

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
    )


@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_tracked(
    dataset_id: str,
    body: UpdateRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if body.poll_interval is not None:
        ds.poll_interval = max(body.poll_interval, settings.min_poll_interval)
    if body.is_active is not None:
        ds.is_active = body.is_active

    await db.commit()
    await db.refresh(ds)
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
    )


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def untrack_dataset(
    dataset_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    await db.delete(ds)
    await db.commit()


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
    ckan_id: str
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

    interval = max(body.preferred_interval, settings.min_poll_interval)

    ds = TrackedDataset(
        ckan_id=body.ckan_id,
        ckan_name=pkg["name"],
        resource_id=body.resource_id,
        title=title,
        organization=org_name,
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
    uid = parse_uuid(dataset_id, "dataset_id")
    result = await db.execute(
        select(TrackedDataset).where(
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
    )
