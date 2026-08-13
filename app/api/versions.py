import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.rate_limit import limiter
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.models.version_index import VersionIndex
from app.services.archive_state import ROW_ARCHIVE_KEYS
from app.services.diff_service import compute_metadata_diff
from app.services.odata_client import odata_client
from app.services import storage_client as storage
from app.services.storage_client import storage_client
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
    source: str = "legacy"
    dataset_title: str | None = None
    dataset_source_type: str | None = None

    model_config = {"from_attributes": True}


def _extract_resource_ids(mappings: dict | None) -> list[str]:
    """Pull every ODATA resource_id out of a version's resource_mappings.

    `resource_mappings` mixes real resource_ids (string UUIDs, keyed by
    user-visible resource name) with internal bookkeeping keys like
    `_hashes` (dict), `_resource_ids` (list), `_zip` (string), and
    `_zip_parts` (list of strings). This helper returns only the actual
    ODATA resource_ids, deduplicated, ready for resource_delete.

    R2-marked values (``r2:<key>``) are object-store keys, NOT ODATA
    resource_ids — they are skipped here (see `_extract_storage_keys`).
    """
    if not mappings:
        return []
    ids: set[str] = set()
    for key, value in mappings.items():
        # Skip purely internal state dicts
        if key == "_hashes":
            continue
        # A NEON table name is not a resource id — deleting a version must not
        # ask ODATA to delete a table (see archive_state.ROW_ARCHIVE_KEYS).
        if key in ROW_ARCHIVE_KEYS:
            continue
        if key in ("_resource_ids", "_zip_parts") and isinstance(value, list):
            for v in value:
                if storage.is_storage_value(v):
                    continue
                if isinstance(v, str) and len(v) >= 30:
                    ids.add(v)
            continue
        # Everything else: strings that look like UUIDs get treated as
        # resource_ids (covers named resources AND `_zip`). R2-marked
        # values are excluded.
        if storage.is_storage_value(value):
            continue
        if isinstance(value, str) and len(value) >= 30:
            ids.add(value)
    return list(ids)


def _extract_storage_keys(mappings: dict | None) -> list[str]:
    """Pull every object-store key (bare, marker stripped) out of a version's
    resource_mappings. The mirror of `_extract_resource_ids` for the R2
    backend — used to delete a version's files from the object store.

    Walks the same shapes (named values, `_zip`, and the `_zip_parts`,
    `_geojson`, `_resource_ids` lists), collecting only ``r2:``-marked values.
    """
    if not mappings:
        return []
    keys: set[str] = set()
    for key, value in mappings.items():
        if key == "_hashes":
            continue
        if isinstance(value, list):
            for v in value:
                if storage.is_storage_value(v):
                    keys.add(storage.key_of(v))
            continue
        if storage.is_storage_value(value):
            keys.add(storage.key_of(value))
    return list(keys)


@router.get("/datasets/{dataset_id}/versions", response_model=list[VersionResponse])
@limiter.limit("60/minute")
async def list_versions(
    dataset_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(dataset_id, "dataset_id")
    ds_result = await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )
    ds = ds_result.scalar_one_or_none()
    if not ds:
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
            source=v.source,
            dataset_title=ds.title,
            dataset_source_type=ds.source_type,
        )
        for v in versions
    ]


@router.get("/versions/{version_id}", response_model=VersionResponse)
@limiter.limit("60/minute")
async def get_version(
    version_id: str,
    request: Request,
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
        select(TrackedDataset).where(TrackedDataset.id == version.tracked_dataset_id)
    )
    ds = ds_result.scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Version not found")

    return VersionResponse(
        id=str(version.id),
        version_number=version.version_number,
        metadata_modified=version.metadata_modified,
        detected_at=version.detected_at.isoformat(),
        odata_metadata_resource_id=version.odata_metadata_resource_id,
        change_summary=version.change_summary,
        resource_mappings=version.resource_mappings,
        source=version.source,
        dataset_title=ds.title,
        dataset_source_type=ds.source_type,
    )


@router.delete("/versions/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
@limiter.limit("10/minute")
async def delete_version(
    version_id: str,
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete a version from our DB AND remove its ODATA resources.

    Order:
      1. Pull the resource_ids out of the version's resource_mappings.
      2. Call resource_delete on each — best-effort; ODATA failures are
         logged but don't block the DB row deletion (if the resource is
         already gone on ODATA we still want to clean up our side).
      3. Delete the metadata snapshot resource if present.
      4. Delete the VersionIndex row.
    """
    vid = parse_uuid(version_id, "version_id")
    result = await db.execute(select(VersionIndex).where(VersionIndex.id == vid))
    version = result.scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    # Conditional-source versions are metadata-only — they reuse the
    # previous version's ODATA resource_ids verbatim. Calling
    # resource_delete on them would delete the bytes that earlier
    # versions still depend on. Only the local DB row should go.
    if version.source == "conditional":
        await db.delete(version)
        await db.commit()
        logger.info(
            "Conditional version %s (v%d of dataset %s) deleted by %s — "
            "no ODATA resources removed (shared with earlier version)",
            version_id, version.version_number, version.tracked_dataset_id,
            user.email,
        )
        return

    to_delete = _extract_resource_ids(version.resource_mappings)
    if version.odata_metadata_resource_id:
        to_delete.append(version.odata_metadata_resource_id)

    deleted, failed = 0, 0
    for rid in to_delete:
        try:
            await odata_client.resource_delete(rid)
            deleted += 1
        except Exception as e:
            failed += 1
            logger.warning("resource_delete(%s) failed during version %s cleanup: %s",
                           rid, version_id, e)

    # R2 backend: also delete this version's objects from the object store.
    # Best-effort, mirroring the ODATA branch — a store error is logged but
    # never blocks the DB row deletion.
    r2_deleted, r2_failed = 0, 0
    if storage_client.is_configured():
        for skey in _extract_storage_keys(version.resource_mappings):
            try:
                await storage_client.delete_object(skey)
                r2_deleted += 1
            except Exception as e:
                r2_failed += 1
                logger.warning("delete_object(%s) failed during version %s cleanup: %s",
                               skey, version_id, e)

    await db.delete(version)
    await db.commit()

    logger.info(
        "Version %s (v%d of dataset %s) deleted by %s — %d ODATA + %d R2 resources removed, "
        "%d ODATA + %d R2 failed",
        version_id, version.version_number, version.tracked_dataset_id,
        user.email, deleted, r2_deleted, failed, r2_failed,
    )


SYMBOLOGY_KEY = "_symbology"
MAX_CONVERTIBLE_BUNDLE_BYTES = 32 * 1024 * 1024


def _mapping_value(mappings: dict | None, key: str, index: int = 0) -> str | None:
    """One resource out of a mapping entry, list-valued or not."""
    value = (mappings or {}).get(key)
    if isinstance(value, list):
        valid = [x for x in value if x]
        return valid[index] if index < len(valid) else (valid[0] if valid else None)
    return value or None


async def _resolve_symbology(
    db: AsyncSession, version: VersionIndex,
) -> tuple[str | None, VersionIndex]:
    """This version's symbology bundle — or the newest one the dataset has.

    A GovMap layer's cartography belongs to the LAYER, not to the snapshot: the
    scraper only started attaching the bundle in July 2026, and even now it is
    re-uploaded only when it changes, so most versions carry data with no style
    beside it. Downloading a layer and getting no symbology because it happened
    to be captured one version earlier is not a useful archive, so the lookup
    falls back to the newest version that does carry one. The caller is
    expected to SAY it fell back (the UI labels it) — an older version's files
    must never silently pass as that version's own.
    """
    own = _mapping_value(version.resource_mappings, SYMBOLOGY_KEY)
    if own:
        return own, version
    result = await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == version.tracked_dataset_id)
        .order_by(VersionIndex.version_number.desc())
    )
    for candidate in result.scalars().all():
        value = _mapping_value(candidate.resource_mappings, SYMBOLOGY_KEY)
        if value:
            return value, candidate
    return None, version


async def _read_resource_bytes(value: str, ds: TrackedDataset | None,
                               dataset_id) -> bytes | None:
    """The bytes behind a mapping value, wherever they are stored."""
    if storage.is_storage_value(value):
        return await storage_client.get_object_bytes(value)
    import httpx

    odata_pkg = ds.odata_dataset_id if ds and ds.odata_dataset_id else dataset_id
    url = f"{settings.odata_url}/dataset/{odata_pkg}/resource/{value}/download"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(url)
        if resp.status_code != 200:
            return None
        return resp.content


@router.get("/versions/{version_id}/symbology.lyrx.zip")
@limiter.limit("20/minute")
async def download_symbology_lyrx(
    version_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """The version's symbology as an ArcGIS Pro bundle (``.lyrx`` per style).

    Converted on the fly from the archived SLD bundle rather than stored: the
    conversion is milliseconds on a file of a few hundred KB, and doing it here
    means every version ever archived — including the ~870 layers captured long
    before ArcGIS support existed — has an ArcGIS download without re-scraping
    anything.
    """
    from fastapi.responses import Response
    from urllib.parse import quote

    from app.services.lyrx import LyrxError, convert_bundle

    vid = parse_uuid(version_id, "version_id")
    version = (
        await db.execute(select(VersionIndex).where(VersionIndex.id == vid))
    ).scalar_one_or_none()
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")

    value, source_version = await _resolve_symbology(db, version)
    if not value:
        raise HTTPException(
            status_code=404,
            detail="No symbology bundle is archived for this dataset",
        )
    ds = (
        await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == version.tracked_dataset_id)
        )
    ).scalar_one_or_none()
    raw = await _read_resource_bytes(value, ds, version.tracked_dataset_id)
    if not raw:
        raise HTTPException(status_code=502, detail="Symbology bundle is unreadable")
    # Conversion holds the bundle and its output in memory. Real bundles are
    # tiny (median ~3 KB, largest seen ~2 MB), so this ceiling only exists so a
    # pathological one can never be the thing that OOMs a 512 MB instance —
    # this service has been killed that way before (see the poll memory guards).
    if len(raw) > MAX_CONVERTIBLE_BUNDLE_BYTES:
        raise HTTPException(
            status_code=413,
            detail="Symbology bundle is too large to convert on the fly; "
                   "download the SLD bundle and convert it locally",
        )

    page = f"{settings.app_base_url.rstrip('/')}/versions/{version.tracked_dataset_id}"
    try:
        converted = convert_bundle(raw, source_url=page)
    except LyrxError as e:
        logger.warning("lyrx conversion failed for version %s: %s", version_id, e)
        raise HTTPException(
            status_code=422,
            detail=f"This symbology bundle cannot be converted to ArcGIS: {e}",
        )

    base = (ds.title if ds and ds.title else "symbology").strip()
    ascii_name = "symbology_arcgis.zip"
    utf8_name = quote(f"{base} — ArcGIS.zip".replace("/", "-"))
    logger.info(
        "Served ArcGIS symbology for version %s (bundle from v%d, %d → %d bytes)",
        version_id, source_version.version_number, len(raw), len(converted),
    )
    return Response(
        content=converted,
        media_type="application/zip",
        headers={
            "Content-Disposition":
                f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{utf8_name}',
            # The archived bundle for a given version never changes, so this is
            # safe to keep — and it keeps a re-download off the converter.
            "Cache-Control": "public, max-age=86400",
        },
    )


@router.get("/versions/{version_id}/download/{resource_id}")
@limiter.limit("60/minute")
async def download_resource(
    version_id: str,
    resource_id: str,
    request: Request,
    index: int = Query(
        0, ge=0,
        description="For list-valued resources (e.g. multi-part ZIP "
        "`_zip_parts`, multi-layer `_geojson`): which element to download.",
    ),
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
    # Conditional-source versions reuse the previous version's
    # odata_resource_ids verbatim, so the same lookup works for them.
    mapped = mappings.get(resource_id)
    # List-valued mappings (e.g. `_geojson`, multi-part `_zip_parts`) hold one
    # or more resources. `index` selects which element (default 0); each part
    # is addressable so the UI can offer a link per part. Out-of-range falls
    # back to the first element rather than 404-ing.
    if isinstance(mapped, list):
        valid = [x for x in mapped if x]
        mapped = (valid[index] if index < len(valid) else (valid[0] if valid else None))
    if not mapped:
        if resource_id == "metadata":
            mapped = version.odata_metadata_resource_id
        elif resource_id == SYMBOLOGY_KEY:
            # A layer's style is the layer's, not the snapshot's — serve the
            # newest one the dataset has rather than 404-ing a version that was
            # captured before the bundle existed (see _resolve_symbology).
            mapped, _ = await _resolve_symbology(db, version)
        if not mapped:
            raise HTTPException(status_code=404, detail="Resource not found in this version")

    # R2 backend: redirect straight to the object store's public domain so the
    # file bytes are served by R2, never proxied through this backend.
    if storage.is_storage_value(mapped):
        return RedirectResponse(url=storage_client.public_url(mapped))

    # ODATA-stored resource: redirect to the file on the CKAN mirror. The URL
    # must use the ODATA *package* id (ds.odata_dataset_id), NOT the OVER
    # dataset UUID — CKAN validates the /dataset/<id>/ segment and 404s a
    # mismatch (this silently broke every ODATA download via this endpoint).
    ds = (
        await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == version.tracked_dataset_id)
        )
    ).scalar_one_or_none()
    odata_pkg = (
        ds.odata_dataset_id if ds and ds.odata_dataset_id
        else version.tracked_dataset_id
    )
    download_url = f"{settings.odata_url}/dataset/{odata_pkg}/resource/{mapped}/download"
    return RedirectResponse(url=download_url)


@router.get("/diff")
@limiter.limit("20/minute")  # heavy: fetches two ODATA metadata snapshots + computes a diff
async def diff_versions(
    request: Request,
    from_version: str = Query(..., alias="from"),
    to_version: str = Query(..., alias="to"),
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
        select(TrackedDataset).where(TrackedDataset.id == v1.tracked_dataset_id)
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
