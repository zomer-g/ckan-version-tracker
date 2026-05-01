"""Tags API — cross-organization categorization of datasets.

Tags are admin-managed (create/delete/assign) but read-public (anyone can see
the tag list and the datasets under each tag).
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.organization import Organization
from app.models.tag import Tag, dataset_tags
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.models.version_index import VersionIndex
from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tags", tags=["tags"])
admin_router = APIRouter(prefix="/api/admin", tags=["admin-tags"])


class TagBrief(BaseModel):
    id: str
    name: str

    model_config = {"from_attributes": True}


class TagWithCount(TagBrief):
    description: str | None = None
    dataset_count: int = 0


class TagDatasetMini(BaseModel):
    id: str
    title: str
    ckan_name: str
    organization: str | None = None
    organization_id: str | None = None
    organization_title: str | None = None
    source_type: str = "ckan"
    version_count: int = 0
    last_polled_at: str | None = None
    tags: list[TagBrief] = []


class TagDetailResponse(TagWithCount):
    datasets: list[TagDatasetMini] = []


class CreateTagRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=2000)


class SetDatasetTagsRequest(BaseModel):
    tag_ids: list[str]


def _tag_brief(t: Tag) -> TagBrief:
    return TagBrief(id=str(t.id), name=t.name)


# ---------------------------------------------------------------------------
# Public reads
# ---------------------------------------------------------------------------


@router.get("", response_model=list[TagWithCount])
async def list_tags(db: AsyncSession = Depends(get_db)):
    """List every tag with the count of datasets it's assigned to."""
    count_subq = (
        select(
            dataset_tags.c.tag_id.label("tag_id"),
            func.count(dataset_tags.c.dataset_id).label("cnt"),
        )
        .join(TrackedDataset, TrackedDataset.id == dataset_tags.c.dataset_id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .group_by(dataset_tags.c.tag_id)
        .subquery()
    )
    result = await db.execute(
        select(Tag, count_subq.c.cnt)
        .outerjoin(count_subq, Tag.id == count_subq.c.tag_id)
        .order_by(Tag.name.asc())
    )
    return [
        TagWithCount(
            id=str(t.id),
            name=t.name,
            description=t.description,
            dataset_count=cnt or 0,
        )
        for t, cnt in result.all()
    ]


@router.get("/{tag_id}", response_model=TagDetailResponse)
async def get_tag(tag_id: str, db: AsyncSession = Depends(get_db)):
    uid = parse_uuid(tag_id, "tag_id")
    tag = (await db.execute(select(Tag).where(Tag.id == uid))).scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    ds_result = await db.execute(
        select(TrackedDataset, Organization)
        .options(selectinload(TrackedDataset.tags))
        .join(dataset_tags, dataset_tags.c.dataset_id == TrackedDataset.id)
        .outerjoin(Organization, TrackedDataset.organization_id == Organization.id)
        .where(dataset_tags.c.tag_id == uid)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .order_by(TrackedDataset.created_at.desc())
    )
    rows = ds_result.unique().all()

    count_result = await db.execute(
        select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
        .group_by(VersionIndex.tracked_dataset_id)
    )
    version_counts = dict(count_result.all())

    datasets = [
        TagDatasetMini(
            id=str(ds.id),
            title=ds.title,
            ckan_name=ds.ckan_name,
            organization=ds.organization,
            organization_id=str(ds.organization_id) if ds.organization_id else None,
            organization_title=org.title if org else None,
            source_type=ds.source_type or "ckan",
            version_count=version_counts.get(ds.id, 0),
            last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
            tags=[_tag_brief(t) for t in ds.tags if t.id != tag.id],
        )
        for ds, org in rows
    ]

    return TagDetailResponse(
        id=str(tag.id),
        name=tag.name,
        description=tag.description,
        dataset_count=len(datasets),
        datasets=datasets,
    )


# ---------------------------------------------------------------------------
# Admin writes
# ---------------------------------------------------------------------------


async def _find_tag_by_name(db: AsyncSession, name: str) -> Tag | None:
    """Case-insensitive name lookup."""
    result = await db.execute(
        select(Tag).where(func.lower(Tag.name) == name.lower())
    )
    return result.scalar_one_or_none()


@router.post(
    "",
    response_model=TagBrief,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit("60/minute")
async def create_tag(
    request: Request,
    body: CreateTagRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    name = body.name.strip()
    if not name:
        raise HTTPException(status_code=422, detail="Tag name cannot be empty")

    existing = await _find_tag_by_name(db, name)
    if existing:
        # 409 with the existing tag in detail so the UI can adopt it without
        # bothering the admin about the collision.
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Tag already exists",
                "tag": {"id": str(existing.id), "name": existing.name},
            },
        )

    tag = Tag(
        name=name,
        description=(body.description or "").strip() or None,
        created_by=user.id,
    )
    db.add(tag)
    await db.commit()
    await db.refresh(tag)
    return _tag_brief(tag)


@admin_router.delete("/tags/{tag_id}", status_code=status.HTTP_204_NO_CONTENT)
@limiter.limit("60/minute")
async def delete_tag(
    request: Request,
    tag_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(tag_id, "tag_id")
    tag = (await db.execute(select(Tag).where(Tag.id == uid))).scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")
    await db.delete(tag)
    await db.commit()


@admin_router.put("/datasets/{dataset_id}/tags")
@limiter.limit("60/minute")
async def set_dataset_tags(
    request: Request,
    dataset_id: str,
    body: SetDatasetTagsRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Replace the full tag set on a dataset. Returns the updated dataset
    in the same DatasetResponse shape as /api/datasets so the frontend can
    swap it into its list state directly."""
    from app.api.datasets import DatasetResponse, _build_source_url

    ds_uid = parse_uuid(dataset_id, "dataset_id")
    ds = (
        await db.execute(
            select(TrackedDataset)
            .options(selectinload(TrackedDataset.tags))
            .where(TrackedDataset.id == ds_uid)
        )
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Resolve and validate every requested tag id (deduped) up front so we
    # don't half-apply on a bad id.
    seen: set[str] = set()
    tag_uids = []
    for t_id in body.tag_ids:
        if t_id in seen:
            continue
        seen.add(t_id)
        tag_uids.append(parse_uuid(t_id, "tag_id"))

    new_tags: list[Tag] = []
    if tag_uids:
        result = await db.execute(select(Tag).where(Tag.id.in_(tag_uids)))
        new_tags = list(result.scalars().all())
        found_ids = {t.id for t in new_tags}
        missing = [str(u) for u in tag_uids if u not in found_ids]
        if missing:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown tag id(s): {', '.join(missing)}",
            )

    ds.tags = new_tags
    await db.commit()
    await db.refresh(ds)

    # Version count + org title for the response (matches /api/datasets shape).
    version_count = (
        await db.execute(
            select(func.count(VersionIndex.id)).where(
                VersionIndex.tracked_dataset_id == ds.id
            )
        )
    ).scalar() or 0

    org_title = None
    if ds.organization_id:
        org_row = (
            await db.execute(
                select(Organization).where(Organization.id == ds.organization_id)
            )
        ).scalar_one_or_none()
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
        storage_mode=ds.storage_mode or "full_snapshot",
        append_key=(ds.scraper_config or {}).get("append_key"),
        last_error=ds.last_error,
        resource_ids=ds.resource_ids,
        new_resources_at_source=ds.new_resources_at_source,
        version_count=version_count,
        tags=[_tag_brief(t) for t in ds.tags],
    )
