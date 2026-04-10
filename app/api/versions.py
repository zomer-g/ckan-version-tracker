import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_current_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.models.version_index import VersionIndex
from app.services.diff_service import compute_metadata_diff
from app.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["versions"])


class VersionResponse(BaseModel):
    id: str
    version_number: int
    metadata_modified: str
    detected_at: str
    odata_metadata_resource_id: str | None = None
    change_summary: dict | None
    resource_mappings: dict | None

    model_config = {"from_attributes": True}


@router.get("/datasets/{dataset_id}/versions", response_model=list[VersionResponse])
async def list_versions(
    dataset_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    ds_result = await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.id == uid,
            TrackedDataset.created_by == user.id,
        )
    )
    if not ds_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Dataset not found")

    result = await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == uid)
        .order_by(VersionIndex.version_number.desc())
    )
    versions = result.scalars().all()
    return [
        VersionResponse(
            id=str(v.id),
            version_number=v.version_number,
            metadata_modified=v.metadata_modified,
            detected_at=v.detected_at.isoformat(),
            odata_metadata_resource_id=v.odata_metadata_resource_id,
            change_summary=v.change_summary,
            resource_mappings=v.resource_mappings,
        )
        for v in versions
    ]


@router.get("/versions/{version_id}", response_model=VersionResponse)
async def get_version(
    version_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    vid = parse_uuid(version_id, "version_id")
    result = await db.execute(
        select(VersionIndex).where(VersionIndex.id == vid)
    )
    version = result.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    ds_result = await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.id == version.tracked_dataset_id,
            TrackedDataset.created_by == user.id,
        )
    )
    if not ds_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Version not found")

    return VersionResponse(
        id=str(version.id),
        version_number=version.version_number,
        metadata_modified=version.metadata_modified,
        detected_at=version.detected_at.isoformat(),
        odata_metadata_resource_id=version.odata_metadata_resource_id,
        change_summary=version.change_summary,
        resource_mappings=version.resource_mappings,
    )


@router.get("/versions/{version_id}/download/{resource_id}")
async def download_resource(
    version_id: str,
    resource_id: str,
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    vid = parse_uuid(version_id, "version_id")
    result = await db.execute(
        select(VersionIndex).where(VersionIndex.id == vid)
    )
    version = result.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    mappings = version.resource_mappings or {}
    odata_resource_id = mappings.get(resource_id)
    if not odata_resource_id:
        if resource_id == "metadata":
            odata_resource_id = version.odata_metadata_resource_id
        if not odata_resource_id:
            raise HTTPException(status_code=404, detail="Resource not found in this version")

    download_url = f"{settings.odata_url}/dataset/{version.tracked_dataset_id}/resource/{odata_resource_id}/download"
    return RedirectResponse(url=download_url)


@router.get("/diff")
async def diff_versions(
    from_version: str = Query(..., alias="from"),
    to_version: str = Query(..., alias="to"),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    from_id = parse_uuid(from_version, "from")
    to_id = parse_uuid(to_version, "to")

    v1_result = await db.execute(
        select(VersionIndex).where(VersionIndex.id == from_id)
    )
    v1 = v1_result.scalar_one_or_none()

    v2_result = await db.execute(
        select(VersionIndex).where(VersionIndex.id == to_id)
    )
    v2 = v2_result.scalar_one_or_none()

    if not v1 or not v2:
        raise HTTPException(status_code=404, detail="Version not found")

    if v1.tracked_dataset_id != v2.tracked_dataset_id:
        raise HTTPException(status_code=400, detail="Versions must belong to the same dataset")

    ds_result = await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.id == v1.tracked_dataset_id,
            TrackedDataset.created_by == user.id,
        )
    )
    if not ds_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Dataset not found")

    try:
        from app.services.snapshot_service import fetch_metadata_from_odata
        meta1 = await fetch_metadata_from_odata(v1.odata_metadata_resource_id)
        meta2 = await fetch_metadata_from_odata(v2.odata_metadata_resource_id)
    except Exception:
        logger.exception("Failed to fetch metadata snapshots for diff")
        raise HTTPException(status_code=502, detail="Failed to fetch metadata for comparison")

    diff = compute_metadata_diff(meta1, meta2)
    return {
        "from_version": from_version,
        "to_version": to_version,
        "from_number": v1.version_number,
        "to_number": v2.version_number,
        "diff": diff,
    }
