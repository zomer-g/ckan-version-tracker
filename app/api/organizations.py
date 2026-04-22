"""Organizations API — public browse + admin sync from data.gov.il."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.organization import Organization
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services.ckan_client import ckan_client
from app.services.govil_landing import fetch_offices as fetch_govil_offices

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/organizations", tags=["organizations"])


class OrganizationResponse(BaseModel):
    id: str
    name: str
    title: str
    description: str | None = None
    image_url: str | None = None
    gov_il_url_name: str | None = None
    gov_il_logo_url: str | None = None
    external_website: str | None = None
    dataset_count: int = 0


class DatasetMini(BaseModel):
    id: str
    title: str
    ckan_name: str
    source_type: str
    version_count: int
    last_polled_at: str | None


class OrganizationDetailResponse(BaseModel):
    id: str
    name: str
    title: str
    description: str | None = None
    image_url: str | None = None
    gov_il_url_name: str | None = None
    gov_il_logo_url: str | None = None
    external_website: str | None = None
    dataset_count: int
    datasets: list[DatasetMini]


@router.get("", response_model=list[OrganizationResponse])
async def list_organizations(
    db: AsyncSession = Depends(get_db),
):
    """Public — list all organizations with their active/pending dataset counts."""
    # Count only active/pending datasets per org (skip rejected)
    count_subq = (
        select(
            TrackedDataset.organization_id,
            func.count(TrackedDataset.id).label("cnt"),
        )
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .group_by(TrackedDataset.organization_id)
        .subquery()
    )
    result = await db.execute(
        select(Organization, count_subq.c.cnt)
        .outerjoin(count_subq, Organization.id == count_subq.c.organization_id)
        .order_by(Organization.title.asc())
    )
    return [
        OrganizationResponse(
            id=str(org.id),
            name=org.name,
            title=org.title,
            description=org.description,
            image_url=org.image_url,
            gov_il_url_name=org.gov_il_url_name,
            gov_il_logo_url=org.gov_il_logo_url,
            external_website=org.external_website,
            dataset_count=cnt or 0,
        )
        for org, cnt in result.all()
    ]


@router.get("/{org_id}", response_model=OrganizationDetailResponse)
async def get_organization(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Public — organization details with its datasets.

    Accepts either a UUID or the CKAN slug (name).
    """
    # Try UUID first, fall back to slug
    org = None
    try:
        uid = parse_uuid(org_id, "org_id")
        result = await db.execute(select(Organization).where(Organization.id == uid))
        org = result.scalar_one_or_none()
    except HTTPException:
        pass
    if not org:
        result = await db.execute(select(Organization).where(Organization.name == org_id))
        org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(status_code=404, detail="Organization not found")

    ds_result = await db.execute(
        select(TrackedDataset)
        .where(TrackedDataset.organization_id == org.id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .order_by(TrackedDataset.created_at.desc())
    )
    datasets = ds_result.scalars().all()

    count_result = await db.execute(
        select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
        .group_by(VersionIndex.tracked_dataset_id)
    )
    version_counts = dict(count_result.all())

    return OrganizationDetailResponse(
        id=str(org.id),
        name=org.name,
        title=org.title,
        description=org.description,
        image_url=org.image_url,
        gov_il_url_name=org.gov_il_url_name,
        gov_il_logo_url=org.gov_il_logo_url,
        external_website=org.external_website,
        dataset_count=len(datasets),
        datasets=[
            DatasetMini(
                id=str(ds.id),
                title=ds.title,
                ckan_name=ds.ckan_name,
                source_type=ds.source_type or "ckan",
                version_count=version_counts.get(ds.id, 0),
                last_polled_at=ds.last_polled_at.isoformat() if ds.last_polled_at else None,
            )
            for ds in datasets
        ],
    )


# ---------------------------------------------------------------------------
# Admin endpoints
# ---------------------------------------------------------------------------

admin_router = APIRouter(prefix="/api/admin/organizations", tags=["admin-organizations"])


class SyncResponse(BaseModel):
    created: int
    updated: int
    total: int
    linked_datasets: int


class SyncGovIlResponse(BaseModel):
    created: int
    matched: int
    total: int


@admin_router.post("/sync", response_model=SyncResponse)
@limiter.limit("5/minute")
async def sync_organizations(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Pull organization list from data.gov.il and upsert into our DB.
    Also back-fills organization_id on tracked_datasets by matching
    the legacy `organization` string field to Organization.name.
    """
    try:
        orgs = await ckan_client.organization_list(all_fields=True)
    except Exception as e:
        logger.exception("Failed to fetch organizations from data.gov.il")
        raise HTTPException(status_code=502, detail=f"data.gov.il fetch failed: {e}")

    if not isinstance(orgs, list):
        raise HTTPException(status_code=502, detail="Unexpected response from data.gov.il")

    created = 0
    updated = 0
    for o in orgs:
        name = o.get("name")
        if not name:
            continue
        title = o.get("title") or name
        description = o.get("description") or None
        image_url = o.get("image_display_url") or o.get("image_url") or None
        data_gov_il_id = o.get("id") or None

        existing = await db.execute(select(Organization).where(Organization.name == name))
        org = existing.scalar_one_or_none()
        if org:
            org.title = title
            org.description = description
            org.image_url = image_url
            org.data_gov_il_id = data_gov_il_id
            updated += 1
        else:
            db.add(Organization(
                name=name,
                title=title,
                description=description,
                image_url=image_url,
                data_gov_il_id=data_gov_il_id,
            ))
            created += 1
    await db.commit()

    # Back-fill organization_id on tracked_datasets by matching legacy string
    all_orgs_result = await db.execute(select(Organization))
    name_to_id = {o.name: o.id for o in all_orgs_result.scalars().all()}

    unlinked = await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.organization_id.is_(None),
            TrackedDataset.organization.is_not(None),
        )
    )
    linked = 0
    for ds in unlinked.scalars().all():
        oid = name_to_id.get(ds.organization or "")
        if oid:
            ds.organization_id = oid
            linked += 1
    await db.commit()

    return SyncResponse(
        created=created,
        updated=updated,
        total=created + updated,
        linked_datasets=linked,
    )


def _normalize_title(s: str) -> str:
    """Normalize for fuzzy matching — collapse whitespace, strip quotes/punct."""
    if not s:
        return ""
    return (
        s.strip()
        .replace("״", "")
        .replace('"', "")
        .replace("׳", "")
        .replace("'", "")
        .replace("-", " ")
        .replace("  ", " ")
        .lower()
    )


def _normalize_slug(s: str) -> str:
    if not s:
        return ""
    return s.strip().lower().replace("_", "-")


@admin_router.post("/sync-gov-il", response_model=SyncGovIlResponse)
@limiter.limit("5/minute")
async def sync_organizations_gov_il(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Pull ministry/office list from gov.il landing page.

    Matches each gov.il entry to an existing Organization row (populated
    via /sync from data.gov.il) by:
      1. exact normalized title match, then
      2. normalized slug match (_ <-> -, case-insensitive).
    If no match: creates a new Organization. If match found: updates
    gov.il-specific fields on the existing row.
    """
    try:
        offices = await fetch_govil_offices()
    except Exception as e:
        logger.exception("Failed to fetch offices from gov.il")
        raise HTTPException(status_code=502, detail=f"gov.il fetch failed: {e}")

    all_rows = (await db.execute(select(Organization))).scalars().all()
    by_title = {_normalize_title(o.title): o for o in all_rows if o.title}
    by_slug = {_normalize_slug(o.name): o for o in all_rows if o.name}

    created = 0
    matched = 0
    for o in offices:
        existing = (
            by_title.get(_normalize_title(o.title))
            or by_slug.get(_normalize_slug(o.url_name))
        )
        if existing:
            existing.gov_il_url_name = o.url_name
            existing.gov_il_logo_url = o.logo_url
            existing.external_website = o.external_website
            existing.org_type = o.org_type
            matched += 1
        else:
            # New row. Use gov.il urlName as the unique slug (fall back to
            # a disambiguated form if a collision exists, though we already
            # checked above).
            slug = o.url_name
            if slug in by_slug:
                slug = f"gov-il-{slug}"
            new_org = Organization(
                name=slug,
                title=o.title,
                gov_il_url_name=o.url_name,
                gov_il_logo_url=o.logo_url,
                external_website=o.external_website,
                org_type=o.org_type,
            )
            db.add(new_org)
            by_slug[slug] = new_org
            by_title[_normalize_title(o.title)] = new_org
            created += 1

    await db.commit()
    return SyncGovIlResponse(
        created=created,
        matched=matched,
        total=created + matched,
    )
