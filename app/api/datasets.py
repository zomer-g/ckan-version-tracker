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
    source_type: str = "ckan"  # "ckan" | "scraper" | "govmap"
    source_url: str | None = None
    title: str | None = None
    scraper_config: dict | None = None
    poll_interval: int = 604800
    preferred_interval: int | None = None
    resource_id: str | None = None
    # Subset of source resources to mirror. Required (>=1) for new CKAN
    # datasets — see _validate_resource_ids. Ignored for scrapers.
    resource_ids: list[str] | None = None
    storage_mode: str = "full_snapshot"  # "full_snapshot" | "append_only"
    append_key: str | None = None  # column name when storage_mode="append_only"
    # Bounds the append seen-set to a sliding window of N versions (opt-in).
    # Needed for high-churn live boards (e.g. flights, polled every 15 min)
    # where an unbounded seen-set would grow without limit. Leave null for
    # slow-cadence append datasets (e.g. the vehicle registry).
    seen_window_versions: int | None = None


class UpdateRequest(BaseModel):
    poll_interval: int | None = None
    is_active: bool | None = None
    title: str | None = None
    organization_id: str | None = None  # "" or null to clear; UUID to assign
    storage_mode: str | None = None  # "full_snapshot" | "append_only"
    append_key: str | None = None  # only meaningful when storage_mode="append_only"
    # Sliding-window bound for the append seen-set; <=0 or null clears it.
    seen_window_versions: int | None = None
    # Content-diff mode for a heavy append_only registry: dedup by full-row hash
    # (capture changes to existing rows, not just new keys) via a COPY-staged
    # set-based diff. First poll migrates the table. null = leave unchanged.
    capture_changes: bool | None = None
    # "full" (scrape→download→upload to ODATA→version) | "local_only"
    # (scrape→download to the worker machine, skip ODATA upload + version).
    upload_mode: str | None = None
    # Per-dataset storage destination: "odata" | "r2" | "local". Supersedes
    # upload_mode when given ("local" maps to upload_mode=local_only).
    storage_target: str | None = None
    # Whether the scraper downloads the actual document files (PDF/Word/…) and
    # archives them as a ZIP, vs catalog-only (index CSV only). Stored in
    # scraper_config; honored by mevaker/idf/health engines.
    download_files: bool | None = None
    # New: replace the tracked-resources set. Empty list ([]) is rejected
    # so an admin can't accidentally orphan a CKAN dataset; pass null to
    # leave unchanged.
    resource_ids: list[str] | None = None
    # Acknowledge the new_resources_at_source alert without adding any.
    dismiss_new_resources: bool | None = None
    # Force the next poll to redo the snapshot: nulls last_modified (so the
    # unchanged-metadata_modified check is bypassed → forced_repoll path) and
    # clears last_error. Used to re-run a dataset whose last poll failed for a
    # now-fixed reason (e.g. an IAP-blocked attachment) without changing the
    # tracked-resource set or deleting version history.
    force_repoll: bool | None = None
    # Admin escape hatch: merge arbitrary keys into scraper_config for
    # per-dataset engine tuning that has no dedicated field yet (e.g.
    # idf_recursion_root for a hub whose children sit under a sibling path).
    # Keys with a null value are removed. Shallow merge; other keys untouched.
    scraper_config_merge: dict | None = None


def _validate_storage_mode(mode: str) -> str:
    if mode not in ("full_snapshot", "append_only"):
        raise HTTPException(status_code=400, detail="storage_mode must be 'full_snapshot' or 'append_only'")
    return mode


def _validate_upload_mode(mode: str) -> str:
    if mode not in ("full", "local_only"):
        raise HTTPException(status_code=400, detail="upload_mode must be 'full' or 'local_only'")
    return mode


# The unified per-dataset storage plan the admin picks. It folds two
# orthogonal backend axes into ONE selector:
#   • file destination — where snapshot files (CSV/PDF/ZIP) land:
#       local (worker keeps them) | odata (CKAN mirror) | r2 (object store)
#   • NEON archive — whether tabular rows are ALSO streamed to the NEON
#       append DB (queryable Postgres). NEON-only ("neon") writes rows and
#       stores NO file snapshot; the "+neon" combos do both.
# Encoded in scraper_config as: upload_mode (local), storage_backend
# (odata/r2/neon), and archive_neon (bool, for the r2+neon / odata+neon combos).
VALID_STORAGE_TARGETS = (
    "local",       # files on the worker machine, nothing on OVER
    "odata",       # files → ODATA CKAN mirror (legacy)
    "r2",          # files → Cloudflare R2 object store
    "neon",        # tabular rows → NEON only, no file snapshot
    "r2+neon",     # files → R2  AND  rows → NEON
    "odata+neon",  # files → ODATA AND rows → NEON (legacy combo)
)


def storage_target_of(scraper_config: dict | None) -> str:
    """Derive the unified storage plan from a dataset's scraper_config:
    one of VALID_STORAGE_TARGETS. Falls back to the global STORAGE_BACKEND
    default for the file destination when no per-dataset choice is pinned."""
    sc = scraper_config or {}
    if sc.get("upload_mode") == "local_only":
        return "local"
    backend = sc.get("storage_backend") or settings.storage_backend
    if backend == "neon":
        return "neon"
    if sc.get("archive_neon"):
        return f"{backend}+neon"
    return backend


def apply_storage_target(scraper_config: dict | None, target: str) -> dict | None:
    """Return an updated scraper_config for the chosen unified storage plan.

    Keeps the worker-facing ``upload_mode`` contract intact (only ever
    'local_only' or absent — the worker reads it to decide local vs upload),
    pins the file destination in ``storage_backend`` (odata/r2/neon, read by
    the OVER server), and sets ``archive_neon`` for the dual-write combos.
    """
    if target not in VALID_STORAGE_TARGETS:
        raise HTTPException(
            status_code=400,
            detail=f"storage_target must be one of {VALID_STORAGE_TARGETS}",
        )
    sc = dict(scraper_config or {})
    # Reset all three storage keys, then set the ones this plan needs.
    sc.pop("upload_mode", None)
    sc.pop("archive_neon", None)
    sc.pop("storage_backend", None)
    if target == "local":
        sc["upload_mode"] = "local_only"
    elif target == "neon":
        sc["storage_backend"] = "neon"
    elif target.endswith("+neon"):
        sc["storage_backend"] = target.split("+", 1)[0]  # "r2" | "odata"
        sc["archive_neon"] = True
    else:  # "odata" | "r2"
        sc["storage_backend"] = target
    return sc or None


def dataset_is_neon_eligible(ds) -> bool:
    """Whether the NEON (tabular-rows) archive is meaningful for this dataset.

    NEON stores queryable tabular rows, which only the CKAN/data.gov.il
    datastore path produces. Scraper/govmap sources archive files
    (PDF/ZIP) or a catalog index, not row-level tabular data, so the NEON
    options are offered for CKAN datasets only — the admin UI greys them
    out otherwise and the API rejects a NEON plan for a non-eligible source.
    """
    return (getattr(ds, "source_type", None) or "ckan") == "ckan"


def _normalize_resource_ids(ids: list[str] | None) -> list[str] | None:
    """De-dupe + strip, reject obviously-bad inputs. None passes through
    so callers can distinguish "no change" from "track all"."""
    if ids is None:
        return None
    cleaned: list[str] = []
    seen: set[str] = set()
    for rid in ids:
        if not isinstance(rid, str):
            raise HTTPException(status_code=400, detail="resource_ids must be a list of strings")
        s = rid.strip()
        if s and s not in seen:
            cleaned.append(s)
            seen.add(s)
    return cleaned


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
    storage_mode: str = "full_snapshot"
    append_key: str | None = None
    upload_mode: str = "full"  # "full" | "local_only"
    # Unified storage plan: local | odata | r2 | neon | r2+neon | odata+neon
    storage_target: str = "odata"
    # Whether the NEON (tabular-rows) options are meaningful for this source
    # (CKAN only). The admin UI greys NEON out when false.
    neon_eligible: bool = True
    # DIFF mode (append_only only): dedup by full-row hash so CHANGES to existing
    # rows are captured, via a COPY-staged set diff + a one-time table migration.
    # Heavy — reserved for rare cases (e.g. the vehicle registry). Default off.
    capture_changes: bool = False
    last_error: str | None = None
    resource_ids: list[str] | None = None
    new_resources_at_source: list[dict] | None = None
    tags: list[TagBrief] = []

    model_config = {"from_attributes": True}


def _build_source_url(ds: TrackedDataset) -> str:
    """Compute the source URL for a tracked dataset."""
    if ds.source_type in ("scraper", "govmap") and ds.source_url:
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
                storage_mode=ds.storage_mode or "full_snapshot",
                append_key=(ds.scraper_config or {}).get("append_key"),
                upload_mode=(ds.scraper_config or {}).get("upload_mode", "full"),
        storage_target=storage_target_of(ds.scraper_config),
        neon_eligible=dataset_is_neon_eligible(ds),
        capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
                last_error=ds.last_error,
                resource_ids=ds.resource_ids,
                new_resources_at_source=ds.new_resources_at_source,
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

    storage_mode = _validate_storage_mode(body.storage_mode)

    # Determine status based on admin privilege
    dataset_status = "active" if user.is_admin else "pending"

    # ---- Scraper-type dataset ----
    if body.source_type == "scraper":
        if not body.source_url:
            raise HTTPException(status_code=400, detail="source_url is required for scraper datasets")
        if not body.title:
            raise HTTPException(status_code=400, detail="title is required for scraper datasets")

        # Parse the URL — try gov.il first, then idf.il, then
        # practitioners.health.gov.il, then avodata.labor.gov.il.
        # The validators don't overlap (different hosts) so order is
        # cosmetic, but gov.il is by far the common case.
        from app.api.govil import _parse_govil_url
        from app.api.idf import _parse_idf_url
        from app.api.health import _parse_health_url
        from app.api.avodata import _parse_avodata_url
        from app.api.mevaker import _parse_mevaker_url
        from app.api.hatzav import _parse_hatzav_url
        page_type, collector_name = _parse_govil_url(body.source_url)
        origin = "gov.il"
        slug_prefix = "govil-scraper"
        mirror_prefix = "gov-versions-scraper"
        if not collector_name:
            page_type, collector_name = _parse_idf_url(body.source_url)
            if collector_name:
                origin = "idf.il"
                slug_prefix = "idf-scraper"
                mirror_prefix = "gov-versions-idf"
        if not collector_name:
            page_type, collector_name = _parse_health_url(body.source_url)
            if collector_name:
                origin = "practitioners.health.gov.il"
                slug_prefix = "health-scraper"
                mirror_prefix = "gov-versions-health"
        if not collector_name:
            page_type, collector_name = _parse_avodata_url(body.source_url)
            if collector_name:
                origin = "avodata.labor.gov.il"
                slug_prefix = "avodata-scraper"
                mirror_prefix = "gov-versions-avodata"
        if not collector_name:
            page_type, collector_name = _parse_mevaker_url(body.source_url)
            if collector_name:
                origin = "mevaker.gov.il"
                slug_prefix = "mevaker-scraper"
                mirror_prefix = "gov-versions-mevaker"
        if not collector_name:
            page_type, collector_name = _parse_hatzav_url(body.source_url)
            if collector_name:
                origin = "geo.mot.gov.il"
                slug_prefix = "hatzav-scraper"
                mirror_prefix = "gov-versions-hatzav"
        if not collector_name:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid scraper URL — must be a gov.il collector, "
                    "idf.il page, practitioners.health.gov.il registry, "
                    "avodata.labor.gov.il scope, mevaker.gov.il reports, "
                    "or geo.mot.gov.il (חצב) portal"
                ),
            )

        # Build a unique slug that includes a hash of the full source URL,
        # so two URLs with the same collector path (e.g. /collectors/policies
        # with different officeId query params) don't collide on the same mirror.
        unique_slug = scraper_url_slug(collector_name, body.source_url)
        ckan_id = f"{slug_prefix}-{unique_slug}"
        ckan_name = unique_slug

        # Duplicate check by source_url
        existing = await db.execute(
            select(TrackedDataset).where(TrackedDataset.source_url == body.source_url)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Dataset already tracked")

        mirror_name = f"{mirror_prefix}-{unique_slug}"

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

        # Start empty (NOT {"download_files": False}) so each per-type branch's
        # `setdefault("download_files", True)` can actually take effect — the
        # old default pre-seeded False, which silently neutered the True
        # defaults for mevaker/idf/health (they scraped catalog-only, no docs).
        # Types with no branch fall back to download_files=False at poll time
        # (worker.py: `ds.scraper_config or {"download_files": False}`).
        sc = dict(body.scraper_config or {})
        if body.append_key:
            sc["append_key"] = body.append_key
        # Raw collector API URLs are collected locally (see poll_job /
        # services.datacollector_client) — the external scraper only
        # understands SPA URLs and was returning HTML for these. The
        # ``kind`` marker tells the poller to take the local path.
        if page_type == "data_collector_api":
            sc["kind"] = "datacollector_api"
        elif page_type and page_type.startswith("idf_"):
            # IDF pages are scraped by the external worker via a
            # Playwright-backed module (govscraper.scrapers.idf). The
            # worker dispatches on scraper_config.kind, so this marker
            # is what makes it pick the right scraper.
            # The page_type startswith check (rather than == "idf_unit")
            # absorbs older values like "idf_prosecution" from any cached
            # parse and keeps the branch alive if we re-split into
            # per-section types later.
            # max_depth / max_docs are section-aware: Orders portal
            # has ~60 category pages each containing many orders, so
            # it needs deeper recursion + a much higher document cap
            # than the flat Prosecution layout.
            from app.api.idf import get_idf_limits
            depth, docs = get_idf_limits(page_type)
            sc["kind"] = "idf"
            sc.setdefault("download_files", True)
            sc.setdefault("max_depth", depth)
            sc.setdefault("max_docs", docs)
        elif page_type and page_type.startswith("health_"):
            # practitioners.health.gov.il is a SPA backed by REST
            # endpoints behind a WAF — scraped by the external worker
            # via Playwright (govscraper.scrapers.health). Per-registry
            # limits live in app.api.health.get_health_limits;
            # registry_id is duplicated into config so the worker
            # doesn't have to re-parse the URL.
            from app.api.health import get_health_limits
            depth, docs = get_health_limits(page_type)
            registry_id = page_type.split(":", 1)[1] if ":" in page_type else ""
            sc["kind"] = "health_practitioners"
            sc.setdefault("download_files", True)
            sc.setdefault("max_depth", depth)
            sc.setdefault("max_docs", docs)
            if registry_id:
                sc.setdefault("registry_id", registry_id)
        elif page_type and page_type.startswith("avodata_"):
            # avodata.labor.gov.il index page — fully server-rendered
            # HTML, scraped via plain httpx + bs4 (no Playwright, no
            # scope filtering). The corpus (occupations | education)
            # is stamped so the worker walks the right sitemap family.
            from app.api.avodata import get_avodata_limits, corpus_of_page_type
            depth, docs = get_avodata_limits(page_type)
            sc["kind"] = "avodata"
            sc.setdefault("corpus", corpus_of_page_type(page_type))
            sc.setdefault("download_files", False)
            sc.setdefault("max_depth", depth)
            sc.setdefault("max_docs", docs)
        elif page_type and page_type.startswith("mevaker_"):
            # mevaker.gov.il — State Comptroller reports from the
            # SharePoint Digital Library's anonymous JSON REST service.
            # Plain httpx + bs4; one row per audit task with PDF + Word
            # downloaded.
            from app.api.mevaker import get_mevaker_limits, type_hebrew_of
            depth, docs = get_mevaker_limits(page_type)
            sc["kind"] = "mevaker"
            sc.setdefault("download_files", True)
            ptype = type_hebrew_of(page_type)
            if ptype:
                sc.setdefault("publication_type", ptype)
            sc.setdefault("max_depth", depth)
            sc.setdefault("max_docs", docs)
        elif page_type and page_type.startswith("hatzav_"):
            # geo.mot.gov.il (חצב) — CATALOG-ONLY. The layer catalog ships
            # as static JS, scraped via plain httpx (no Playwright). One row
            # per layer, with each layer's data.gov.il download URLs as
            # columns. We do NOT mirror the files: data.gov.il bot-blocks
            # automated/bulk downloads (Google IAP), so the headless worker
            # can't fetch them — that's the in-browser GovScraper extension's
            # job. download_files=False; the worker also forces it off.
            from app.api.hatzav import get_hatzav_limits
            depth, docs = get_hatzav_limits(page_type)
            sc["kind"] = "hatzav"
            sc.setdefault("download_files", False)
            sc.setdefault("max_depth", depth)
            sc.setdefault("max_docs", docs)

        ds = TrackedDataset(
            ckan_id=ckan_id,
            ckan_name=ckan_name,
            title=body.title,
            organization=origin,
            source_type="scraper",
            source_url=body.source_url,
            scraper_config=sc,
            storage_mode=storage_mode,
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
            storage_mode=ds.storage_mode or "full_snapshot",
            append_key=(ds.scraper_config or {}).get("append_key"),
            upload_mode=(ds.scraper_config or {}).get("upload_mode", "full"),
        storage_target=storage_target_of(ds.scraper_config),
        neon_eligible=dataset_is_neon_eligible(ds),
        capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
            last_error=ds.last_error,
            resource_ids=ds.resource_ids,
            new_resources_at_source=ds.new_resources_at_source,
        )

    # ---- GovMap layer (single URL — bulk is exposed only on the
    # public /requests endpoint to keep the admin path simple) ----
    if body.source_type == "govmap":
        if not body.source_url:
            raise HTTPException(status_code=400, detail="source_url is required for govmap datasets")

        from app.api.govmap import parse_govmap_url, build_govmap_title
        parsed = parse_govmap_url(body.source_url)
        if not parsed:
            raise HTTPException(status_code=400, detail="Invalid govmap.gov.il layer URL (missing lay=<id>)")

        existing = await db.execute(
            select(TrackedDataset).where(TrackedDataset.source_url == body.source_url)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Dataset already tracked")

        layer_id = parsed.layer_id
        unique_slug = scraper_url_slug(f"govmap-{layer_id}", body.source_url)
        ckan_id = f"govmap-{unique_slug}"
        ckan_name = unique_slug
        title = body.title or build_govmap_title(layer_id)

        mirror_name = f"gov-versions-govmap-{unique_slug}"
        odata_dataset_id = None
        if dataset_status == "active" and settings.odata_api_key:
            try:
                mirror = await odata_client.create_dataset(
                    name=mirror_name,
                    title=f"[Versions] {title}",
                    owner_org=settings.odata_owner_org,
                    extras=[
                        {"key": "source_type", "value": "govmap"},
                        {"key": "source_url", "value": body.source_url},
                        {"key": "govmap_layer_id", "value": layer_id},
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

        sc: dict = dict(body.scraper_config or {})
        sc.update({
            "kind": "govmap",
            "layer_id": layer_id,
            "download_files": False,
        })
        if parsed.center_itm:
            sc["center_itm"] = parsed.center_itm

        ds = TrackedDataset(
            ckan_id=ckan_id,
            ckan_name=ckan_name,
            title=title,
            organization="govmap.gov.il",
            source_type="govmap",
            source_url=body.source_url,
            scraper_config=sc,
            storage_mode=storage_mode,
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
            storage_mode=ds.storage_mode or "full_snapshot",
            append_key=None,
            storage_target=storage_target_of(ds.scraper_config),
            neon_eligible=dataset_is_neon_eligible(ds),
            capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
            last_error=ds.last_error,
            resource_ids=ds.resource_ids,
            new_resources_at_source=ds.new_resources_at_source,
        )

    # ---- CKAN-type dataset (original flow) ----
    if not body.ckan_id:
        raise HTTPException(status_code=400, detail="ckan_id is required for CKAN datasets")

    resource_ids = _normalize_resource_ids(body.resource_ids)
    # Require explicit resource selection for every NEW CKAN dataset.
    # Existing rows with resource_ids=None are grandfathered as "track all";
    # this only applies at creation time. Single-resource tracking via the
    # legacy `resource_id` field continues to work and is implicitly
    # promoted to a 1-element resource_ids below.
    if not resource_ids and not body.resource_id:
        raise HTTPException(
            status_code=400,
            detail="resource_ids must contain at least one resource id",
        )

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

    # Validate every requested resource_id actually lives on the source.
    if resource_ids:
        source_ids = {r["id"] for r in pkg.get("resources", [])}
        bad = [rid for rid in resource_ids if rid not in source_ids]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"resource_ids not found on source: {bad}",
            )
    elif body.resource_id:
        # Promote legacy single-resource into resource_ids so polls go
        # through the multi-resource path uniformly.
        resource_ids = [body.resource_id]

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

    ckan_scraper_config = None
    if body.append_key or (body.seen_window_versions and body.seen_window_versions > 0):
        ckan_scraper_config = {}
        if body.append_key:
            ckan_scraper_config["append_key"] = body.append_key
        if body.seen_window_versions and body.seen_window_versions > 0:
            ckan_scraper_config["seen_window_versions"] = body.seen_window_versions

    ds = TrackedDataset(
        ckan_id=body.ckan_id,
        ckan_name=pkg["name"],
        resource_id=body.resource_id,
        resource_ids=resource_ids,
        title=dataset_title,
        organization=org_name,
        organization_id=org_id,
        odata_dataset_id=odata_dataset_id,
        poll_interval=interval,
        status=dataset_status,
        storage_mode=storage_mode,
        scraper_config=ckan_scraper_config,
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
        storage_mode=ds.storage_mode or "full_snapshot",
        append_key=(ds.scraper_config or {}).get("append_key"),
        upload_mode=(ds.scraper_config or {}).get("upload_mode", "full"),
        storage_target=storage_target_of(ds.scraper_config),
        neon_eligible=dataset_is_neon_eligible(ds),
        capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
        last_error=ds.last_error,
        resource_ids=ds.resource_ids,
        new_resources_at_source=ds.new_resources_at_source,
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
    if body.storage_mode is not None:
        ds.storage_mode = _validate_storage_mode(body.storage_mode)
    if body.resource_ids is not None:
        new_ids = _normalize_resource_ids(body.resource_ids)
        if not new_ids:
            raise HTTPException(
                status_code=400,
                detail="resource_ids must contain at least one resource id",
            )
        old_ids = list(ds.resource_ids or [])
        if sorted(new_ids) != sorted(old_ids):
            # Force the next poll to redo the snapshot rather than skip on
            # the unchanged metadata_modified check. Without this, resizing
            # a resource set never produces a new version (a stale empty
            # or wrong-set version 1 persists forever) — and the admin's
            # mental model is "I changed what's tracked, the mirror should
            # reflect that on the next run."
            ds.last_modified = None
            ds.last_error = None
        ds.resource_ids = new_ids
        # Setting an explicit subset clears the new-resources alert: the
        # admin has just made an active choice about what to track, and
        # any leftover entries here either got included (no longer "new")
        # or were intentionally skipped.
        ds.new_resources_at_source = None
    if body.dismiss_new_resources:
        ds.new_resources_at_source = None
    if body.force_repoll:
        # See UpdateRequest.force_repoll: re-run the snapshot on next poll.
        ds.last_modified = None
        ds.last_error = None
    if body.scraper_config_merge is not None:
        # Admin escape hatch (see UpdateRequest): shallow-merge into
        # scraper_config; a null value drops the key.
        sc = dict(ds.scraper_config or {})
        for k, v in body.scraper_config_merge.items():
            if v is None:
                sc.pop(k, None)
            else:
                sc[k] = v
        ds.scraper_config = sc or None
    if body.append_key is not None:
        sc = dict(ds.scraper_config or {})
        if body.append_key.strip():
            sc["append_key"] = body.append_key.strip()
        else:
            sc.pop("append_key", None)
        ds.scraper_config = sc or None
    if body.seen_window_versions is not None:
        sc = dict(ds.scraper_config or {})
        if body.seen_window_versions > 0:
            sc["seen_window_versions"] = body.seen_window_versions
        else:
            sc.pop("seen_window_versions", None)
        ds.scraper_config = sc or None
    if body.capture_changes is not None:
        sc = dict(ds.scraper_config or {})
        if body.capture_changes:
            sc["capture_changes"] = True
        else:
            sc.pop("capture_changes", None)
        ds.scraper_config = sc or None
    if body.upload_mode is not None:
        # Stored in scraper_config (no migration) — flows straight to the worker
        # via the /poll response, which returns ds.scraper_config verbatim.
        mode = _validate_upload_mode(body.upload_mode)
        sc = dict(ds.scraper_config or {})
        if mode == "local_only":
            sc["upload_mode"] = "local_only"
        else:
            sc.pop("upload_mode", None)  # "full" is the default — keep config clean
        ds.scraper_config = sc or None

    if body.storage_target is not None:
        # Unified storage plan (local/odata/r2/neon/r2+neon/odata+neon).
        # Applied after upload_mode so it wins if both are sent. Stored in
        # scraper_config (no migration). NEON plans require a tabular (CKAN)
        # source — reject them on a file/catalog source so the admin can't
        # pick an archive that would never write a row.
        if "neon" in body.storage_target and not dataset_is_neon_eligible(ds):
            raise HTTPException(
                status_code=400,
                detail=(
                    "NEON archiving is only available for CKAN (data.gov.il) "
                    "datasets with tabular rows; this source archives files/"
                    "catalog data. Choose 'local', 'r2' or 'odata'."
                ),
            )
        ds.scraper_config = apply_storage_target(ds.scraper_config, body.storage_target)

    if body.download_files is not None:
        sc = dict(ds.scraper_config or {})
        sc["download_files"] = body.download_files
        ds.scraper_config = sc or None

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
        storage_mode=ds.storage_mode or "full_snapshot",
        append_key=(ds.scraper_config or {}).get("append_key"),
        upload_mode=(ds.scraper_config or {}).get("upload_mode", "full"),
        storage_target=storage_target_of(ds.scraper_config),
        neon_eligible=dataset_is_neon_eligible(ds),
        capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
        last_error=ds.last_error,
        resource_ids=ds.resource_ids,
        new_resources_at_source=ds.new_resources_at_source,
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

@router.get("/pending-count")
@limiter.limit("120/minute")
async def pending_count(request: Request, db: AsyncSession = Depends(get_db)):
    """Public, lightweight count of pending tracking requests. Powers the
    subtle "you have requests waiting" dot next to the site title — visible to
    everyone so the admin spots a backlog the moment they land on the site,
    without logging in. Exposes only a number (no titles / requesters)."""
    from sqlalchemy import func
    total = (await db.execute(
        select(func.count()).select_from(TrackedDataset).where(
            TrackedDataset.status == "pending"
        )
    )).scalar() or 0
    return {"count": int(total)}


class TrackingRequest(BaseModel):
    ckan_id: str | None = None
    source_type: str = "ckan"  # "ckan" | "scraper" | "govmap"
    source_url: str | None = None
    # GovMap supports bulk submission: one request creates one TrackedDataset
    # per URL. Other source_types ignore this field and use source_url.
    source_urls: list[str] | None = None
    title: str | None = None
    resource_id: str | None = None
    resource_ids: list[str] | None = None
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

        # Try gov.il first, then idf.il, then practitioners.health.gov.il,
        # then avodata.labor.gov.il. Mirrors the admin POST /datasets
        # branch — keep the two in sync.
        from app.api.govil import _parse_govil_url
        from app.api.idf import _parse_idf_url
        from app.api.health import _parse_health_url
        from app.api.avodata import _parse_avodata_url
        from app.api.mevaker import _parse_mevaker_url
        from app.api.hatzav import _parse_hatzav_url
        page_type, collector_name = _parse_govil_url(body.source_url)
        origin = "gov.il"
        slug_prefix = "govil-scraper"
        if not collector_name:
            page_type, collector_name = _parse_idf_url(body.source_url)
            if collector_name:
                origin = "idf.il"
                slug_prefix = "idf-scraper"
        if not collector_name:
            page_type, collector_name = _parse_health_url(body.source_url)
            if collector_name:
                origin = "practitioners.health.gov.il"
                slug_prefix = "health-scraper"
        if not collector_name:
            page_type, collector_name = _parse_avodata_url(body.source_url)
            if collector_name:
                origin = "avodata.labor.gov.il"
                slug_prefix = "avodata-scraper"
        if not collector_name:
            page_type, collector_name = _parse_mevaker_url(body.source_url)
            if collector_name:
                origin = "mevaker.gov.il"
                slug_prefix = "mevaker-scraper"
        if not collector_name:
            page_type, collector_name = _parse_hatzav_url(body.source_url)
            if collector_name:
                origin = "geo.mot.gov.il"
                slug_prefix = "hatzav-scraper"
        if not collector_name:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid scraper URL — must be a gov.il collector, "
                    "idf.il page, practitioners.health.gov.il registry, "
                    "avodata.labor.gov.il scope, mevaker.gov.il reports, "
                    "or geo.mot.gov.il (חצב) portal"
                ),
            )

        # Duplicate check by source_url
        existing = await db.execute(
            select(TrackedDataset).where(TrackedDataset.source_url == body.source_url)
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="Already tracked or requested")

        unique_slug = scraper_url_slug(collector_name, body.source_url)
        sc: dict = {"download_files": False}
        if page_type == "data_collector_api":
            sc["kind"] = "datacollector_api"
        elif page_type and page_type.startswith("idf_"):
            # Mirror of the admin-POST branch — keep in sync. Same
            # forward-compat reason for the startswith check.
            # Section-aware limits — see get_idf_limits for the table.
            from app.api.idf import get_idf_limits
            depth, docs = get_idf_limits(page_type)
            sc["kind"] = "idf"
            sc["download_files"] = True
            sc["max_depth"] = depth
            sc["max_docs"] = docs
        elif page_type and page_type.startswith("health_"):
            # Mirror of the admin-POST branch — keep in sync.
            from app.api.health import get_health_limits
            depth, docs = get_health_limits(page_type)
            registry_id = page_type.split(":", 1)[1] if ":" in page_type else ""
            sc["kind"] = "health_practitioners"
            sc["download_files"] = True
            sc["max_depth"] = depth
            sc["max_docs"] = docs
            if registry_id:
                sc["registry_id"] = registry_id
        elif page_type and page_type.startswith("avodata_"):
            # Mirror of the admin-POST branch — keep in sync.
            from app.api.avodata import get_avodata_limits, corpus_of_page_type
            depth, docs = get_avodata_limits(page_type)
            sc["kind"] = "avodata"
            sc["corpus"] = corpus_of_page_type(page_type)
            sc["download_files"] = False
            sc["max_depth"] = depth
            sc["max_docs"] = docs
        elif page_type and page_type.startswith("mevaker_"):
            # Mirror of the admin-POST branch — keep in sync.
            from app.api.mevaker import get_mevaker_limits, type_hebrew_of
            depth, docs = get_mevaker_limits(page_type)
            sc["kind"] = "mevaker"
            sc["download_files"] = True
            ptype = type_hebrew_of(page_type)
            if ptype:
                sc["publication_type"] = ptype
            sc["max_depth"] = depth
            sc["max_docs"] = docs
        elif page_type and page_type.startswith("hatzav_"):
            # Mirror of the admin-POST branch — keep in sync. Catalog-only:
            # data.gov.il bot-blocks bulk downloads, so no file mirroring.
            from app.api.hatzav import get_hatzav_limits
            depth, docs = get_hatzav_limits(page_type)
            sc["kind"] = "hatzav"
            sc["download_files"] = False
            sc["max_depth"] = depth
            sc["max_docs"] = docs
        ds = TrackedDataset(
            ckan_id=f"{slug_prefix}-{unique_slug}",
            ckan_name=unique_slug,
            title=body.title,
            organization=origin,
            source_type="scraper",
            source_url=body.source_url,
            scraper_config=sc,
            poll_interval=interval,
            status="pending",
            created_by=None,
            last_modified=None,
        )
        db.add(ds)
        await db.commit()
        from app.services.activity_log import log_event
        await log_event(event="requested", dataset=ds, status="info", actor="request",
                        message="התקבלה בקשת גירוד (ממתינה לאישור)")
        return {"message": "Request submitted", "status": "pending"}

    # ---- GovMap-type request (bulk: one TrackedDataset per URL) ----
    if body.source_type == "govmap":
        from app.api.govmap import parse_govmap_url, build_govmap_title

        urls = list(body.source_urls or [])
        if body.source_url and body.source_url not in urls:
            urls.insert(0, body.source_url)
        urls = [u.strip() for u in urls if u and u.strip()]
        if not urls:
            raise HTTPException(
                status_code=400,
                detail="At least one source_url is required for govmap datasets",
            )

        results: list[dict] = []
        any_created = False
        created_govmap: list = []
        for url in urls:
            parsed = parse_govmap_url(url)
            if not parsed:
                results.append({
                    "url": url,
                    "status": "invalid",
                    "error": "Invalid govmap.gov.il URL (missing lay=<id>)",
                })
                continue

            dup = await db.execute(
                select(TrackedDataset).where(TrackedDataset.source_url == url)
            )
            if dup.scalar_one_or_none():
                results.append({"url": url, "status": "duplicate"})
                continue

            layer_id = parsed.layer_id
            unique_slug = scraper_url_slug(f"govmap-{layer_id}", url)
            sc: dict = {
                "kind": "govmap",
                "layer_id": layer_id,
                "download_files": False,
            }
            if parsed.center_itm:
                sc["center_itm"] = parsed.center_itm

            ds = TrackedDataset(
                ckan_id=f"govmap-{unique_slug}",
                ckan_name=unique_slug,
                title=body.title.strip() if body.title and body.title.strip() else build_govmap_title(layer_id),
                organization="govmap.gov.il",
                source_type="govmap",
                source_url=url,
                scraper_config=sc,
                poll_interval=interval,
                status="pending",
                created_by=None,
                last_modified=None,
            )
            db.add(ds)
            any_created = True
            created_govmap.append(ds)
            results.append({
                "url": url,
                "status": "pending",
                "layer_id": layer_id,
            })

        if any_created:
            await db.commit()
            from app.services.activity_log import log_event
            for created in created_govmap:
                await log_event(event="requested", dataset=created, status="info",
                                actor="request",
                                message="התקבלה בקשת גירוד (govmap, ממתינה לאישור)")

        return {
            "message": "Request submitted" if any_created else "No new layers added",
            "status": "pending" if any_created else "noop",
            "results": results,
        }

    # ---- CKAN-type request (original flow) ----
    if not body.ckan_id:
        raise HTTPException(status_code=400, detail="ckan_id is required for CKAN datasets")

    # Resource selection: every new CKAN request must pin at least one
    # resource. Single-resource (resource_id) is auto-promoted to a
    # 1-element resource_ids so downstream code only handles one shape.
    resource_ids = _normalize_resource_ids(body.resource_ids)
    if not resource_ids and not body.resource_id:
        raise HTTPException(
            status_code=400,
            detail="resource_ids must contain at least one resource id",
        )

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

    if resource_ids:
        source_ids = {r["id"] for r in pkg.get("resources", [])}
        bad = [rid for rid in resource_ids if rid not in source_ids]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=f"resource_ids not found on source: {bad}",
            )
    elif body.resource_id:
        resource_ids = [body.resource_id]

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
        resource_ids=resource_ids,
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

    from app.services.activity_log import log_event
    await log_event(event="requested", dataset=ds, status="info", actor="request",
                    message="התקבלה בקשת גירוד (ממתינה לאישור)")

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
        storage_mode=ds.storage_mode or "full_snapshot",
        append_key=(ds.scraper_config or {}).get("append_key"),
        upload_mode=(ds.scraper_config or {}).get("upload_mode", "full"),
        storage_target=storage_target_of(ds.scraper_config),
        neon_eligible=dataset_is_neon_eligible(ds),
        capture_changes=bool((ds.scraper_config or {}).get("capture_changes")),
        last_error=ds.last_error,
        resource_ids=ds.resource_ids,
        new_resources_at_source=ds.new_resources_at_source,
        tags=[TagBrief(id=str(t.id), name=t.name) for t in ds.tags],
    )
