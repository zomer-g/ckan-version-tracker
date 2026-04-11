"""Admin endpoints for approving/rejecting dataset tracking requests."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid, sanitize_ckan_name
from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.rate_limit import limiter
from app.services.odata_client import odata_client
from app.worker.scheduler import add_poll_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin"])


class ApproveRequest(BaseModel):
    poll_interval: int | None = None


class PendingRequest(BaseModel):
    id: str
    ckan_id: str
    ckan_name: str
    title: str
    organization: str | None
    poll_interval: int
    status: str
    created_at: str
    requester_email: str
    requester_name: str


@router.get("/pending", response_model=list[PendingRequest])
@limiter.limit("30/minute")
async def list_pending(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """List all pending dataset tracking requests."""
    result = await db.execute(
        select(TrackedDataset, User)
        .join(User, TrackedDataset.created_by == User.id)
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
            poll_interval=ds.poll_interval,
            status=ds.status,
            created_at=ds.created_at.isoformat(),
            requester_email=requester.email,
            requester_name=requester.display_name,
        )
        for ds, requester in rows
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

    # Update status to active
    ds.status = "active"

    # Create odata mirror dataset if not already created
    if not ds.odata_dataset_id and settings.odata_api_key:
        mirror_name = f"gov-versions-{sanitize_ckan_name(ds.ckan_name)}"
        if ds.resource_id:
            mirror_name = f"{mirror_name}-{ds.resource_id[:8]}"
        try:
            mirror = await odata_client.create_dataset(
                name=mirror_name,
                title=f"[Versions] {ds.title}",
                owner_org=settings.odata_owner_org,
                extras=[
                    {"key": "source_ckan_id", "value": ds.ckan_id},
                    {"key": "source_url", "value": f"{settings.data_gov_il_url}/dataset/{ds.ckan_name}"},
                    {"key": "auto_managed", "value": "true"},
                ],
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
