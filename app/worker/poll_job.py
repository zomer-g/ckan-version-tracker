import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select

from app.database import async_session
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services.ckan_client import ckan_client
from app.services.version_detector import (
    compute_change_summary,
    detect_resource_changes,
    has_metadata_changed,
)
from app.services.snapshot_service import create_version_snapshot

logger = logging.getLogger(__name__)


async def poll_dataset(dataset_id: str) -> None:
    """Poll a single tracked dataset for changes."""
    logger.info("Polling dataset %s", dataset_id)

    async with async_session() as db:
        result = await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == uuid.UUID(dataset_id))
        )
        ds = result.scalar_one_or_none()
        if not ds or not ds.is_active:
            logger.info("Dataset %s not found or inactive, skipping", dataset_id)
            return

        try:
            # Fetch current state from data.gov.il
            pkg = await ckan_client.package_show(ds.ckan_id)
            new_modified = pkg.get("metadata_modified", "")

            # Quick check: has anything changed?
            if not has_metadata_changed(ds.last_modified, new_modified):
                logger.info("Dataset %s unchanged (modified=%s)", ds.ckan_name, new_modified)
                ds.last_polled_at = datetime.now(timezone.utc)
                await db.commit()
                return

            # Get the latest version to compare against
            latest_result = await db.execute(
                select(VersionIndex)
                .where(VersionIndex.tracked_dataset_id == ds.id)
                .order_by(VersionIndex.version_number.desc())
                .limit(1)
            )
            latest_version = latest_result.scalar_one_or_none()
            old_mappings = latest_version.resource_mappings if latest_version else None
            next_version = (latest_version.version_number + 1) if latest_version else 1

            # Detect resource-level changes
            resources = pkg.get("resources", [])
            changed_resources, hash_map = await detect_resource_changes(
                old_mappings, resources
            )

            # If this is version 1 or resources changed, create a new version
            is_first_version = latest_version is None
            if is_first_version or changed_resources:
                logger.info(
                    "Creating version %d for %s (%d resources changed)",
                    next_version, ds.ckan_name, len(changed_resources),
                )

                # For first version, download all resources
                resources_to_upload = changed_resources
                if is_first_version:
                    resources_to_upload = []
                    for r in resources:
                        if not r.get("url"):
                            continue
                        try:
                            content, sha256 = await ckan_client.download_resource(r["url"])
                            resources_to_upload.append({
                                "resource": r,
                                "content": content,
                                "sha256": sha256,
                            })
                            hash_map[r["id"]] = sha256
                        except Exception as e:
                            logger.warning("Failed to download resource %s: %s", r["id"], e)

                # Upload snapshot to odata.org.il
                if ds.odata_dataset_id:
                    meta_resource_id, resource_mappings = await create_version_snapshot(
                        odata_dataset_id=ds.odata_dataset_id,
                        version_number=next_version,
                        metadata=pkg,
                        changed_resources=resources_to_upload,
                        hash_map=hash_map,
                        old_mappings=old_mappings,
                    )
                else:
                    meta_resource_id = None
                    resource_mappings = {"_hashes": hash_map, "_resource_ids": [r["id"] for r in resources]}

                # Compute change summary
                change_summary = compute_change_summary(
                    old_mappings, resources, changed_resources, hash_map
                )

                # Save version index
                version = VersionIndex(
                    tracked_dataset_id=ds.id,
                    version_number=next_version,
                    metadata_modified=new_modified,
                    odata_metadata_resource_id=meta_resource_id,
                    change_summary=change_summary,
                    resource_mappings=resource_mappings,
                )
                db.add(version)
                logger.info("Version %d created for %s", next_version, ds.ckan_name)
            else:
                logger.info(
                    "Metadata changed but no resource content changed for %s",
                    ds.ckan_name,
                )

            # Update tracking state
            ds.last_polled_at = datetime.now(timezone.utc)
            ds.last_modified = new_modified
            await db.commit()

        except Exception:
            logger.exception("Error polling dataset %s", ds.ckan_name)
            ds.last_polled_at = datetime.now(timezone.utc)
            await db.commit()
