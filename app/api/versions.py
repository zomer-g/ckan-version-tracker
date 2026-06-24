import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.models.version_index import VersionIndex
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
async def list_versions(
    dataset_id: str,
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
async def get_version(
    version_id: str,
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
async def delete_version(
    version_id: str,
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
    if storage_client.is_enabled():
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


@router.get("/versions/{version_id}/download/{resource_id}")
async def download_resource(
    version_id: str,
    resource_id: str,
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
    # List-valued mappings (e.g. `_geojson`, `_zip_parts`) point at one or
    # more resources — redirect to the first one (single-layer/single-part is
    # the common case; multi-part download isn't exposed here).
    if isinstance(mapped, list):
        mapped = next((x for x in mapped if x), None)
    if not mapped:
        if resource_id == "metadata":
            mapped = version.odata_metadata_resource_id
        if not mapped:
            raise HTTPException(status_code=404, detail="Resource not found in this version")

    # R2 backend: redirect straight to the object store's public domain so the
    # file bytes are served by R2, never proxied through this backend.
    if storage.is_storage_value(mapped):
        return RedirectResponse(url=storage_client.public_url(mapped))

    download_url = f"{settings.odata_url}/dataset/{version.tracked_dataset_id}/resource/{mapped}/download"
    return RedirectResponse(url=download_url)


@router.get("/diff")
async def diff_versions(
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
