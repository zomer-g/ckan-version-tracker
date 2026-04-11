import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid, sanitize_ckan_name
from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.services.ckan_client import ckan_client
from app.services.odata_client import odata_client
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/datasets", tags=["datasets"])


class TrackRequest(BaseModel):
    ckan_id: str
    poll_interval: int = 3600


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

    model_config = {"from_attributes": True}


@router.get("", response_model=list[DatasetResponse])
async def list_tracked(
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Show ALL active datasets to everyone (not just creator's)
    from app.models.user import User as UserModel
    result = await db.execute(
        select(TrackedDataset, UserModel)
        .join(UserModel, TrackedDataset.created_by == UserModel.id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = result.all()
    return [
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
            requester_name=requester.display_name,
            requester_email=requester.email,
        )
        for ds, requester in rows
    ]


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def track_dataset(
    body: TrackRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    existing = await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_id == body.ckan_id)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=400, detail="Dataset already tracked")

    interval = max(body.poll_interval, settings.min_poll_interval)

    try:
        pkg = await ckan_client.package_show(body.ckan_id)
    except Exception:
        logger.exception("Failed to fetch dataset %s from data.gov.il", body.ckan_id)
        raise HTTPException(status_code=404, detail="Dataset not found on data.gov.il")

    org_name = pkg.get("organization", {}).get("name", "") if pkg.get("organization") else ""
    mirror_name = f"gov-versions-{sanitize_ckan_name(pkg['name'])}"

    # Determine status based on admin privilege
    dataset_status = "active" if user.is_admin else "pending"

    # Create mirror dataset on odata.org.il only for active (admin-approved) datasets
    odata_dataset_id = None
    if dataset_status == "active" and settings.odata_api_key:
        try:
            mirror = await odata_client.create_dataset(
                name=mirror_name,
                title=f"[Versions] {pkg.get('title', pkg['name'])}",
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
        title=pkg.get("title", pkg["name"]),
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
    )


@router.patch("/{dataset_id}", response_model=DatasetResponse)
async def update_tracked(
    dataset_id: str,
    body: UpdateRequest,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    if not user.is_admin:
        query = query.where(TrackedDataset.created_by == user.id)
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
    )


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def untrack_dataset(
    dataset_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    if not user.is_admin:
        query = query.where(TrackedDataset.created_by == user.id)
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
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    query = select(TrackedDataset).where(TrackedDataset.id == uid)
    if not user.is_admin:
        query = query.where(TrackedDataset.created_by == user.id)
    result = await db.execute(query)
    ds = result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    from app.worker.poll_job import poll_dataset

    background_tasks.add_task(poll_dataset, str(ds.id))
    return {"message": "Poll triggered", "dataset_id": str(ds.id)}
