"""Public read-only API v1.

Stable, documented surface for external consumers (separate from the
internal endpoints used by the SPA, which can change without notice).
See `docs/API.md` for the human-readable reference.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.api.utils import parse_uuid
from app.config import settings
from app.database import get_db
from app.models.organization import Organization
from app.models.tag import Tag, dataset_tags
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["public-v1"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TagBrief(BaseModel):
    id: str
    name: str
    url: str  # /api/v1/tags/{id}


class OrganizationBrief(BaseModel):
    id: str
    name: str  # CKAN slug
    title: str
    url: str  # /api/v1/organizations/{id}


class DatasetSummary(BaseModel):
    id: str
    title: str
    source_type: str  # "ckan" | "scraper"
    source_url: str  # data.gov.il page or gov.il scraper URL
    odata_dataset_id: str | None
    odata_url: str | None  # mirror dataset on odata.org.il
    organization: OrganizationBrief | None
    tags: list[TagBrief]
    poll_interval: int
    last_polled_at: str | None
    last_modified: str | None
    status: str
    storage_mode: str  # "full_snapshot" | "append_only"
    version_count: int
    versions_url: str  # /api/v1/datasets/{id}/versions


class VersionResource(BaseModel):
    name: str  # original resource key (or "_zip", "_metadata", ...)
    odata_resource_id: str
    odata_resource_url: str  # ODATA resource page
    download_url: str  # direct download via ODATA


class VersionDetail(BaseModel):
    id: str
    version_number: int
    detected_at: str
    metadata_modified: str | None
    change_summary: dict | None
    odata_metadata_resource_id: str | None
    odata_metadata_url: str | None  # snapshot of CKAN metadata at this version
    resources: list[VersionResource]


class TagWithCount(BaseModel):
    id: str
    name: str
    description: str | None = None
    dataset_count: int
    url: str


class TagDetailResponse(TagWithCount):
    datasets: list[DatasetSummary]


class OrganizationWithCount(BaseModel):
    id: str
    name: str
    title: str
    description: str | None = None
    dataset_count: int
    parent_id: str | None
    url: str


class DatasetListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DatasetSummary]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_request_url(request: Request, path: str) -> str:
    """Absolute URL for a path under the same host as the current request."""
    return str(request.url.replace(path=path, query=""))


def _source_url(ds: TrackedDataset) -> str:
    if ds.source_type == "scraper" and ds.source_url:
        return ds.source_url
    org = ds.organization or ""
    name = ds.ckan_name or ""
    base = f"{settings.data_gov_il_url}/he/datasets/{org}/{name}"
    if ds.resource_id:
        base = f"{base}/{ds.resource_id}"
    return base


def _odata_dataset_url(ds: TrackedDataset) -> str | None:
    if not ds.odata_dataset_id:
        return None
    return f"{settings.odata_url}/dataset/{ds.odata_dataset_id}"


def _odata_resource_url(ds: TrackedDataset, resource_id: str) -> str:
    return f"{settings.odata_url}/dataset/{ds.odata_dataset_id}/resource/{resource_id}"


def _odata_resource_download_url(ds: TrackedDataset, resource_id: str) -> str:
    return (
        f"{settings.odata_url}/dataset/{ds.odata_dataset_id}"
        f"/resource/{resource_id}/download"
    )


def _tag_brief(t: Tag, request: Request) -> TagBrief:
    return TagBrief(
        id=str(t.id),
        name=t.name,
        url=_build_request_url(request, f"/api/v1/tags/{t.id}"),
    )


def _org_brief(org: Organization | None, request: Request) -> OrganizationBrief | None:
    if org is None:
        return None
    return OrganizationBrief(
        id=str(org.id),
        name=org.name,
        title=org.title,
        url=_build_request_url(request, f"/api/v1/organizations/{org.id}"),
    )


def _dataset_summary(
    ds: TrackedDataset,
    org: Organization | None,
    version_count: int,
    request: Request,
) -> DatasetSummary:
    return DatasetSummary(
        id=str(ds.id),
        title=ds.title,
        source_type=ds.source_type or "ckan",
        source_url=_source_url(ds),
        odata_dataset_id=ds.odata_dataset_id,
        odata_url=_odata_dataset_url(ds),
        organization=_org_brief(org, request),
        tags=[_tag_brief(t, request) for t in (ds.tags or [])],
        poll_interval=ds.poll_interval,
        last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
        last_modified=ds.last_modified,
        status=ds.status,
        storage_mode=ds.storage_mode or "full_snapshot",
        version_count=version_count,
        versions_url=_build_request_url(
            request, f"/api/v1/datasets/{ds.id}/versions"
        ),
    )


def _extract_version_resources(
    ds: TrackedDataset, mappings: dict | None
) -> list[VersionResource]:
    """Turn a VersionIndex.resource_mappings dict into a clean list.

    `resource_mappings` mixes named resources with internal bookkeeping
    keys; we surface only the named resources plus `_zip` if present.
    """
    if not mappings:
        return []
    out: list[VersionResource] = []
    seen: set[str] = set()
    for key, value in mappings.items():
        if key == "_hashes":
            continue
        if key == "_resource_ids":
            continue  # already covered by named keys
        if key == "_zip_parts":
            continue
        if isinstance(value, str) and len(value) >= 30:
            if value in seen:
                continue
            seen.add(value)
            out.append(
                VersionResource(
                    name=key,
                    odata_resource_id=value,
                    odata_resource_url=_odata_resource_url(ds, value),
                    download_url=_odata_resource_download_url(ds, value),
                )
            )
    return out


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


@router.get("/datasets", response_model=DatasetListResponse)
async def list_datasets(
    request: Request,
    organization_id: str | None = Query(
        None, description="Filter by organization UUID"
    ),
    tag_id: list[str] | None = Query(
        None,
        description=(
            "Filter by tag UUID. May be repeated; multiple values are "
            "AND-combined (returns only datasets that have ALL given tags)."
        ),
    ),
    tag: list[str] | None = Query(
        None,
        description=(
            "Filter by tag NAME (case-insensitive). May be repeated and is "
            "AND-combined just like tag_id. Unknown names return zero rows."
        ),
    ),
    status: str = Query(
        "active",
        description="active | pending | all (default: active)",
        pattern="^(active|pending|all)$",
    ),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List datasets, optionally filtered by organization and/or tags.

    Multiple `tag_id` (or `tag`) parameters AND together: passing
    `?tag=enforcement&tag=guidelines` returns only datasets tagged with
    BOTH (not either-or).
    """
    # Resolve tag names → ids and merge with explicit tag_ids
    resolved_tag_ids: list = []
    if tag_id:
        for t_id in tag_id:
            resolved_tag_ids.append(parse_uuid(t_id, "tag_id"))
    if tag:
        names_lower = [n.strip().lower() for n in tag if n.strip()]
        if names_lower:
            tag_rows = (
                await db.execute(
                    select(Tag.id, Tag.name).where(
                        func.lower(Tag.name).in_(names_lower)
                    )
                )
            ).all()
            found_lower = {n.lower() for _, n in tag_rows}
            missing = [n for n in names_lower if n not in found_lower]
            if missing:
                # An unknown tag name in an AND-filter is a no-op match by
                # design; return an empty result rather than an error so
                # callers can experiment safely.
                return DatasetListResponse(
                    total=0, limit=limit, offset=offset, items=[]
                )
            for tid, _ in tag_rows:
                resolved_tag_ids.append(tid)

    # Dedupe (a tag named twice or once-by-id-once-by-name shouldn't double-count)
    resolved_tag_ids = list({t for t in resolved_tag_ids})

    base = (
        select(TrackedDataset, Organization)
        .options(selectinload(TrackedDataset.tags))
        .outerjoin(Organization, TrackedDataset.organization_id == Organization.id)
    )

    if status == "active":
        base = base.where(TrackedDataset.status == "active")
    elif status == "pending":
        base = base.where(TrackedDataset.status == "pending")
    # "all" → no status filter

    if organization_id:
        oid = parse_uuid(organization_id, "organization_id")
        base = base.where(TrackedDataset.organization_id == oid)

    if resolved_tag_ids:
        n = len(resolved_tag_ids)
        subq = (
            select(dataset_tags.c.dataset_id)
            .where(dataset_tags.c.tag_id.in_(resolved_tag_ids))
            .group_by(dataset_tags.c.dataset_id)
            .having(func.count(distinct(dataset_tags.c.tag_id)) == n)
        )
        base = base.where(TrackedDataset.id.in_(subq))

    # Total before pagination — same filters, just count distinct datasets
    count_query = base.with_only_columns(func.count(distinct(TrackedDataset.id)))
    total = (await db.execute(count_query)).scalar() or 0

    base = base.order_by(TrackedDataset.created_at.desc()).limit(limit).offset(offset)
    rows = (await db.execute(base)).unique().all()

    if rows:
        ds_ids = [r[0].id for r in rows]
        version_counts = dict(
            (
                await db.execute(
                    select(
                        VersionIndex.tracked_dataset_id,
                        func.count(VersionIndex.id),
                    )
                    .where(VersionIndex.tracked_dataset_id.in_(ds_ids))
                    .group_by(VersionIndex.tracked_dataset_id)
                )
            ).all()
        )
    else:
        version_counts = {}

    items = [
        _dataset_summary(ds, org, version_counts.get(ds.id, 0), request)
        for ds, org in rows
    ]
    return DatasetListResponse(total=total, limit=limit, offset=offset, items=items)


@router.get("/datasets/{dataset_id}", response_model=DatasetSummary)
async def get_dataset(
    dataset_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    row = (
        await db.execute(
            select(TrackedDataset, Organization)
            .options(selectinload(TrackedDataset.tags))
            .outerjoin(
                Organization, TrackedDataset.organization_id == Organization.id
            )
            .where(TrackedDataset.id == uid)
        )
    ).unique().one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail="Dataset not found")
    ds, org = row
    version_count = (
        await db.execute(
            select(func.count(VersionIndex.id)).where(
                VersionIndex.tracked_dataset_id == ds.id
            )
        )
    ).scalar() or 0
    return _dataset_summary(ds, org, version_count, request)


@router.get(
    "/datasets/{dataset_id}/versions",
    response_model=list[VersionDetail],
)
async def list_dataset_versions(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    ds = (
        await db.execute(select(TrackedDataset).where(TrackedDataset.id == uid))
    ).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    rows = (
        await db.execute(
            select(VersionIndex)
            .where(VersionIndex.tracked_dataset_id == uid)
            .order_by(VersionIndex.version_number.desc())
        )
    ).scalars().all()

    return [
        VersionDetail(
            id=str(v.id),
            version_number=v.version_number,
            detected_at=v.detected_at.isoformat(),
            metadata_modified=v.metadata_modified,
            change_summary=v.change_summary,
            odata_metadata_resource_id=v.odata_metadata_resource_id,
            odata_metadata_url=(
                _odata_resource_url(ds, v.odata_metadata_resource_id)
                if v.odata_metadata_resource_id and ds.odata_dataset_id
                else None
            ),
            resources=_extract_version_resources(ds, v.resource_mappings),
        )
        for v in rows
    ]


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


@router.get("/tags", response_model=list[TagWithCount])
async def list_tags(request: Request, db: AsyncSession = Depends(get_db)):
    """Every tag with the count of active+pending datasets carrying it."""
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
    rows = (
        await db.execute(
            select(Tag, count_subq.c.cnt)
            .outerjoin(count_subq, Tag.id == count_subq.c.tag_id)
            .order_by(Tag.name.asc())
        )
    ).all()
    return [
        TagWithCount(
            id=str(t.id),
            name=t.name,
            description=t.description,
            dataset_count=cnt or 0,
            url=_build_request_url(request, f"/api/v1/tags/{t.id}"),
        )
        for t, cnt in rows
    ]


@router.get("/tags/{tag_id}", response_model=TagDetailResponse)
async def get_tag(
    tag_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(tag_id, "tag_id")
    tag = (
        await db.execute(select(Tag).where(Tag.id == uid))
    ).scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag not found")

    rows = (
        await db.execute(
            select(TrackedDataset, Organization)
            .options(selectinload(TrackedDataset.tags))
            .join(dataset_tags, dataset_tags.c.dataset_id == TrackedDataset.id)
            .outerjoin(
                Organization, TrackedDataset.organization_id == Organization.id
            )
            .where(dataset_tags.c.tag_id == uid)
            .where(TrackedDataset.status.in_(["active", "pending"]))
            .order_by(TrackedDataset.created_at.desc())
        )
    ).unique().all()

    if rows:
        ds_ids = [ds.id for ds, _ in rows]
        version_counts = dict(
            (
                await db.execute(
                    select(
                        VersionIndex.tracked_dataset_id,
                        func.count(VersionIndex.id),
                    )
                    .where(VersionIndex.tracked_dataset_id.in_(ds_ids))
                    .group_by(VersionIndex.tracked_dataset_id)
                )
            ).all()
        )
    else:
        version_counts = {}

    datasets = [
        _dataset_summary(ds, org, version_counts.get(ds.id, 0), request)
        for ds, org in rows
    ]

    return TagDetailResponse(
        id=str(tag.id),
        name=tag.name,
        description=tag.description,
        dataset_count=len(datasets),
        url=_build_request_url(request, f"/api/v1/tags/{tag.id}"),
        datasets=datasets,
    )


# ---------------------------------------------------------------------------
# Organizations
# ---------------------------------------------------------------------------


@router.get("/organizations", response_model=list[OrganizationWithCount])
async def list_organizations(request: Request, db: AsyncSession = Depends(get_db)):
    count_subq = (
        select(
            TrackedDataset.organization_id,
            func.count(TrackedDataset.id).label("cnt"),
        )
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .group_by(TrackedDataset.organization_id)
        .subquery()
    )
    rows = (
        await db.execute(
            select(Organization, count_subq.c.cnt)
            .outerjoin(count_subq, Organization.id == count_subq.c.organization_id)
            .order_by(Organization.title.asc())
        )
    ).all()
    return [
        OrganizationWithCount(
            id=str(o.id),
            name=o.name,
            title=o.title,
            description=o.description,
            dataset_count=cnt or 0,
            parent_id=str(o.parent_id) if o.parent_id else None,
            url=_build_request_url(request, f"/api/v1/organizations/{o.id}"),
        )
        for o, cnt in rows
    ]


@router.get("/organizations/{org_id}", response_model=OrganizationWithCount)
async def get_organization(
    org_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(org_id, "org_id")
    org = (
        await db.execute(select(Organization).where(Organization.id == uid))
    ).scalar_one_or_none()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")
    count = (
        await db.execute(
            select(func.count(TrackedDataset.id))
            .where(TrackedDataset.organization_id == uid)
            .where(TrackedDataset.status.in_(["active", "pending"]))
        )
    ).scalar() or 0
    return OrganizationWithCount(
        id=str(org.id),
        name=org.name,
        title=org.title,
        description=org.description,
        dataset_count=count,
        parent_id=str(org.parent_id) if org.parent_id else None,
        url=_build_request_url(request, f"/api/v1/organizations/{org.id}"),
    )
