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

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/organizations", tags=["organizations"])


class OrganizationResponse(BaseModel):
    id: str
    name: str
    title: str
    description: str | None = None
    image_url: str | None = None
    data_gov_il_id: str | None = None
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
    data_gov_il_id: str | None = None
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
            data_gov_il_id=org.data_gov_il_id,
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
        data_gov_il_id=org.data_gov_il_id,
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


class GovIlOfficePayload(BaseModel):
    url_name: str
    title: str
    logo_url: str | None = None
    external_website: str | None = None
    org_type: int | None = None
    offices: list[str] = []  # gov.il internal office UUIDs


class SyncGovIlRequest(BaseModel):
    """Browser-side fetched payload.

    gov.il sits behind a Cloudflare challenge that blocks Render's cloud
    IPs but accepts residential browsers. The admin's browser fetches
    the list and POSTs it here for server-side merging.
    """
    offices: list[GovIlOfficePayload]


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
    body: SyncGovIlRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Merge gov.il ministry/office list into Organization table.

    Accepts the list from the admin's browser (gov.il blocks cloud IPs).
    Matches each entry to an existing Organization row by:
      1. exact normalized title match, then
      2. normalized slug match (_ <-> -, case-insensitive).
    If no match: creates a new Organization. If match found: updates
    gov.il-specific fields on the existing row.
    """
    offices = body.offices
    if not offices:
        raise HTTPException(status_code=400, detail="No offices provided")

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
            existing.gov_il_office_ids = o.offices or None
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
                gov_il_office_ids=o.offices or None,
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


class LinkScrapersResponse(BaseModel):
    linked_by_office_id: int
    linked_by_path: int
    unlinked: int
    total_scraper_datasets: int


@admin_router.post("/link-scrapers", response_model=LinkScrapersResponse)
@limiter.limit("5/minute")
async def link_scraper_datasets_to_organizations(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Back-fill organization_id on scraper-type tracked_datasets.

    Strategy (first match wins):
      1. URL has ?officeId=<uuid> → match against gov_il_office_ids.
      2. URL path starts with /he/departments/<slug>/ → match Organization
         by gov_il_url_name (case-insensitive, _ and - normalized).

    Requires /sync-gov-il to have been run first so gov_il_office_ids
    and gov_il_url_name are populated.
    """
    import re
    from urllib.parse import parse_qs, urlparse

    orgs = (await db.execute(select(Organization))).scalars().all()

    # Build lookup indexes
    by_office_id: dict[str, Organization] = {}
    for o in orgs:
        for uid in (o.gov_il_office_ids or []):
            if uid:
                by_office_id[uid.lower()] = o

    by_slug: dict[str, Organization] = {}
    for o in orgs:
        if o.gov_il_url_name:
            by_slug[_normalize_slug(o.gov_il_url_name)] = o

    dept_path_re = re.compile(r"^/he/departments/([^/?#]+)", re.IGNORECASE)

    scrapers = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.source_type == "scraper")
    )).scalars().all()

    by_office = 0
    by_path = 0
    unlinked = 0
    for ds in scrapers:
        url = ds.source_url or ""
        matched: Organization | None = None
        try:
            parsed = urlparse(url)
        except Exception:
            parsed = None

        # Strategy 1: ?officeId=<uuid>
        if parsed:
            qs = parse_qs(parsed.query)
            office_id = (qs.get("officeId") or qs.get("officeid") or [None])[0]
            if office_id:
                matched = by_office_id.get(office_id.lower())
                if matched:
                    by_office += 1

        # Strategy 2: /he/departments/<slug>/...
        if not matched and parsed:
            m = dept_path_re.match(parsed.path or "")
            if m:
                slug = m.group(1)
                # Skip the "dynamiccollectors" path — it's not a dept slug
                if slug.lower() not in ("dynamiccollectors", "dynamiccollector"):
                    candidate = by_slug.get(_normalize_slug(slug))
                    if candidate:
                        matched = candidate
                        by_path += 1

        if matched:
            ds.organization_id = matched.id
            # Keep the legacy string in sync for display consistency
            ds.organization = matched.name
        else:
            unlinked += 1

    await db.commit()
    return LinkScrapersResponse(
        linked_by_office_id=by_office,
        linked_by_path=by_path,
        unlinked=unlinked,
        total_scraper_datasets=len(scrapers),
    )
