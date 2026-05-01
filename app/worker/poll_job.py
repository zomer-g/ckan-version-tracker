import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import func, select

from app.config import settings
from app.database import async_session
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services.ckan_client import ckan_client
from app.services.version_detector import (
    compute_change_summary,
    compute_new_rows,
    detect_resource_changes,
    has_metadata_changed,
)
from app.services.snapshot_service import (
    append_new_rows_to_shared_resource,
    create_version_snapshot,
)

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

        # For scraper-type datasets (gov.il collectors, govmap layers, etc.),
        # create a task instead of polling CKAN. The external GOV SCRAPER
        # worker dispatches to the right per-domain scraper based on the
        # source_url + scraper_config.kind it receives in the poll response.
        if ds.source_type in ("scraper", "govmap"):
            await _create_scrape_task(ds, db)
            return

        if ds.status != "active":
            logger.info("Dataset %s status is '%s', skipping poll", dataset_id, ds.status)
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

            # Skip if a version already exists with this exact metadata_modified.
            # Exception: when ds.last_modified was reset to NULL by the admin
            # (e.g. they changed resource_ids), treat this as a forced re-poll
            # — the existing version was created against a different tracked
            # set and the admin's mental model is "I changed what's tracked,
            # the mirror should reflect that now". Without this exception the
            # forced poll silently no-ops on every run.
            forced_repoll = ds.last_modified is None
            if (
                not forced_repoll
                and latest_version
                and latest_version.metadata_modified == new_modified
            ):
                logger.info("Version already exists for %s with modified=%s, skipping", ds.ckan_name, new_modified)
                ds.last_polled_at = datetime.now(timezone.utc)
                ds.last_modified = new_modified
                await db.commit()
                return

            old_mappings = latest_version.resource_mappings if latest_version else None
            next_version = (latest_version.version_number + 1) if latest_version else 1

            # Detect resource-level changes
            all_source_resources = pkg.get("resources", [])
            resources = list(all_source_resources)

            # Resource selection precedence:
            #   1. resource_ids (new): explicit subset chosen by admin
            #   2. resource_id (legacy): single-resource tracking
            #   3. None: track every resource at the source (legacy default)
            tracked_ids: set[str] | None = None
            if ds.resource_ids:
                tracked_ids = set(ds.resource_ids)
                resources = [r for r in resources if r["id"] in tracked_ids]
                if not resources:
                    logger.warning(
                        "None of the tracked resources %s were found in dataset %s",
                        ds.resource_ids, ds.ckan_name,
                    )
            elif ds.resource_id:
                tracked_ids = {ds.resource_id}
                resources = [r for r in resources if r["id"] == ds.resource_id]
                if not resources:
                    logger.warning(
                        "Tracked resource %s not found in dataset %s",
                        ds.resource_id, ds.ckan_name,
                    )

            # New-resources-at-source detection. If the admin opted into a
            # specific subset (tracked_ids set), surface anything at the
            # source not in that subset so they can choose to add it. For
            # legacy "track all" datasets there's nothing to alert on.
            if tracked_ids is not None:
                new_at_source = [
                    {
                        "id": r["id"],
                        "name": r.get("name") or r["id"],
                        "format": (r.get("format") or "").upper() or None,
                    }
                    for r in all_source_resources
                    if r["id"] not in tracked_ids
                ]
                ds.new_resources_at_source = new_at_source or None

            # Check if this is a large dataset
            resource_to_check = resources[0] if resources else None
            if resource_to_check and ds.resource_id:
                try:
                    ds_info = await ckan_client.datastore_info(ds.resource_id)
                    total_rows = ds_info["total"]
                except Exception:
                    total_rows = 0

                if total_rows >= settings.large_dataset_threshold:
                    # LARGE DATASET PATH: metadata + sample only
                    await _poll_large_dataset(ds, pkg, resource_to_check, ds_info, next_version, old_mappings, db)
                    return

            changed_resources, hash_map = await detect_resource_changes(
                old_mappings, resources
            )

            is_first_version = latest_version is None

            # Append-only path: only handles a single tabular resource. For
            # multi-resource or non-tabular datasets the shape doesn't match
            # row-level append semantics, so we silently fall through to the
            # snapshot path below.
            if ds.storage_mode == "append_only" and (is_first_version or changed_resources):
                appended = await _poll_append_only(
                    ds, pkg, resources, changed_resources, hash_map,
                    old_mappings, next_version, new_modified, latest_version, db,
                )
                if appended:
                    return

            if is_first_version or changed_resources:
                logger.info(
                    "Creating version %d for %s (%d resources changed)",
                    next_version, ds.ckan_name, len(changed_resources),
                )

                errors: list[str] = []

                # For first version, download all resources. Each download
                # streams to a temp file on disk (returned as ``file_path``)
                # instead of accumulating bytes in memory — required for the
                # 512MB Render dyno once a resource grows past ~50MB. The
                # file paths get cleaned up after the snapshot is uploaded.
                resources_to_upload = changed_resources
                if is_first_version:
                    resources_to_upload = []
                    for r in resources:
                        if not r.get("url"):
                            continue
                        try:
                            file_path, sha256, byte_count = await ckan_client.download_resource(
                                r["url"], resource_id=r["id"],
                            )
                            resources_to_upload.append({
                                "resource": r,
                                "file_path": file_path,
                                "byte_count": byte_count,
                                "sha256": sha256,
                            })
                            hash_map[r["id"]] = sha256
                        except Exception as e:
                            logger.warning("Failed to download resource %s: %s", r["id"], e)
                            errors.append(f"download {r.get('name', r['id'][:8])}: {e}")

                # Lazily create mirror dataset if it doesn't exist yet
                if not ds.odata_dataset_id and settings.odata_api_key:
                    from app.services.odata_client import odata_client
                    from app.api.utils import sanitize_ckan_name
                    mirror_name = f"gov-versions-{sanitize_ckan_name(ds.ckan_name)}"
                    try:
                        mirror = await odata_client.create_dataset(
                            name=mirror_name,
                            title=f"[Versions] {ds.title}",
                            owner_org=settings.odata_owner_org,
                        )
                        ds.odata_dataset_id = mirror["id"]
                        logger.info("Lazily created mirror dataset %s", mirror_name)
                    except Exception as e1:
                        logger.warning("Mirror create failed for %s: %s", mirror_name, e1)
                        try:
                            mirror = await odata_client.package_show(mirror_name)
                            ds.odata_dataset_id = mirror["id"]
                        except Exception as e2:
                            logger.error("Mirror find also failed for %s: %s", mirror_name, e2)
                            errors.append(f"mirror create/find: {e2}")

                # Upload snapshot to odata.org.il
                if ds.odata_dataset_id:
                    meta_resource_id, resource_mappings, upload_errors = await create_version_snapshot(
                        odata_dataset_id=ds.odata_dataset_id,
                        version_number=next_version,
                        metadata=pkg,
                        changed_resources=resources_to_upload,
                        hash_map=hash_map,
                        old_mappings=old_mappings,
                    )
                    errors.extend(upload_errors)
                else:
                    meta_resource_id = None
                    resource_mappings = {"_hashes": hash_map, "_resource_ids": [r["id"] for r in resources]}

                # Count actual successful resource mappings (anything that's
                # not a bookkeeping key like _hashes / _resource_ids).
                successes = sum(1 for k in resource_mappings if not k.startswith("_"))
                expected = sum(1 for r in resources if r.get("url"))

                # If we expected to upload something and ended up with zero
                # working resources, do NOT create a misleading empty version.
                # Persist the failure so the user can see it in the UI.
                if expected > 0 and successes == 0:
                    msg = "; ".join(errors)[:2000] or "all resource downloads/uploads failed (no detail)"
                    ds.last_error = msg
                    logger.error(
                        "Aborting version %d for %s — 0/%d resources succeeded: %s",
                        next_version, ds.ckan_name, expected, msg,
                    )
                else:
                    # Compute change summary
                    change_summary = compute_change_summary(
                        old_mappings, resources, changed_resources, hash_map
                    )
                    if errors:
                        change_summary["errors"] = errors

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
                    # Partial success → keep last_error so the user knows
                    # something didn't make it; full success → clear it.
                    ds.last_error = "; ".join(errors)[:2000] if errors else None
                    logger.info(
                        "Version %d created for %s (%d/%d resources)",
                        next_version, ds.ckan_name, successes, expected,
                    )
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


async def _create_scrape_task(ds: TrackedDataset, db) -> None:
    """Create a scrape task for the worker to pick up."""
    from app.models.scrape_task import ScrapeTask

    # Check if there's already a pending/running task
    existing = await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.tracked_dataset_id == ds.id,
            ScrapeTask.status.in_(["pending", "running"]),
        )
    )
    if existing.scalar_one_or_none():
        logger.info("Scrape task already exists for %s, skipping", ds.ckan_name)
        ds.last_polled_at = datetime.now(timezone.utc)
        await db.commit()
        return

    task = ScrapeTask(
        tracked_dataset_id=ds.id,
        status="pending",
        phase="queued",
        message=f"Queued for scraping: {ds.source_url}",
    )
    db.add(task)
    ds.last_polled_at = datetime.now(timezone.utc)
    await db.commit()
    logger.info("Created scrape task for %s (source: %s)", ds.ckan_name, ds.source_url)


async def _poll_append_only(
    ds: TrackedDataset,
    pkg: dict,
    resources: list[dict],
    changed_resources: list[dict],
    hash_map: dict,
    old_mappings: dict | None,
    next_version: int,
    new_modified: str,
    latest_version: VersionIndex | None,
    db,
) -> bool:
    """Append-only CKAN poll: parse the (one) tabular resource and insert
    only rows whose identity wasn't seen in any previous version.

    Returns True if the append path handled this poll (caller should stop),
    False if the dataset doesn't fit the append shape and the caller should
    continue with the regular snapshot path.
    """
    from app.services.snapshot_service import TABULAR_FORMATS
    from app.services.csv_parser import parse_csv

    # Append needs exactly one tabular resource. Most odata.gov.il datasets
    # tracked at the resource level (ds.resource_id set) match this; package-
    # level tracking with multiple files does not.
    if len(resources) != 1:
        logger.info("Append: %s has %d resources, falling back to snapshot",
                    ds.ckan_name, len(resources))
        return False

    resource = resources[0]
    fmt = (resource.get("format", "") or "").lower().strip()
    if fmt not in TABULAR_FORMATS:
        logger.info("Append: %s format %r is not tabular, falling back to snapshot",
                    ds.ckan_name, fmt)
        return False

    # On version 1 we always need to download. On subsequent versions,
    # only if the file hash actually changed (changed_resources is the
    # diff). Either way the bytes live in a temp file we have to clean
    # up after parsing.
    import os as _os
    file_path: str | None = None
    own_file = False
    if changed_resources:
        file_path = changed_resources[0].get("file_path")
    else:
        try:
            file_path, sha, _n = await ckan_client.download_resource(
                resource["url"], resource_id=resource["id"],
            )
            hash_map[resource["id"]] = sha
            own_file = True
        except Exception as e:
            logger.warning("Append: failed to download %s: %s", resource["id"], e)
            return False

    try:
        if not file_path:
            return False
        with open(file_path, "rb") as fh:
            content = fh.read()
        fields, records = parse_csv(content)
    except Exception as e:
        logger.warning("Append: failed to parse CSV for %s: %s", ds.ckan_name, e)
        return False
    finally:
        # We re-downloaded just for this poll; the snapshot path also
        # cleans up its own temp files, so only delete here when we
        # owned this download.
        if own_file and file_path:
            try:
                _os.unlink(file_path)
            except OSError:
                pass

    append_key = (ds.scraper_config or {}).get("append_key")
    seen_keys = list((latest_version.resource_mappings or {}).get("_appendonly_seen", []) or []) if latest_version else []
    new_rows, seen_keys = compute_new_rows(seen_keys, records, append_key)

    # Lazily create mirror dataset (mirrors the snapshot path's behavior)
    if not ds.odata_dataset_id and settings.odata_api_key:
        from app.services.odata_client import odata_client
        from app.api.utils import sanitize_ckan_name
        mirror_name = f"gov-versions-{sanitize_ckan_name(ds.ckan_name)}"
        try:
            mirror = await odata_client.create_dataset(
                name=mirror_name,
                title=f"[Versions] {ds.title}",
                owner_org=settings.odata_owner_org,
            )
            ds.odata_dataset_id = mirror["id"]
        except Exception:
            try:
                from app.services.odata_client import odata_client as _oc
                mirror = await _oc.package_show(mirror_name)
                ds.odata_dataset_id = mirror["id"]
            except Exception as e:
                logger.error("Append: mirror create/find failed for %s: %s", mirror_name, e)
                return False

    rid: str | None = ds.appendonly_resource_id
    rows_inserted = 0
    if ds.odata_dataset_id:
        try:
            rid, rows_inserted = await append_new_rows_to_shared_resource(
                odata_dataset_id=ds.odata_dataset_id,
                appendonly_resource_id=ds.appendonly_resource_id,
                version_number=next_version,
                resource_name=resource.get("name", ds.ckan_name),
                fields=fields,
                new_rows=new_rows,
                resource_format=fmt.upper(),
            )
            if rid and not ds.appendonly_resource_id:
                ds.appendonly_resource_id = rid
        except Exception as e:
            logger.error("Append: failed to push rows for %s: %s", ds.ckan_name, e)
            return False

    resource_mappings = {
        "_hashes": hash_map,
        "_resource_ids": [resource["id"]],
        "_appendonly_seen": seen_keys,
    }
    if rid:
        resource_mappings[resource["id"]] = rid

    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "append",
            "rows_added": rows_inserted,
            "rows_total": len(seen_keys),
            "key": append_key or "_hash",
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings=resource_mappings,
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    await db.commit()
    logger.info("Append version %d created for %s (%d new rows, %d total)",
                next_version, ds.ckan_name, rows_inserted, len(seen_keys))
    return True


async def _poll_large_dataset(
    ds: TrackedDataset,
    pkg: dict,
    resource: dict,
    ds_info: dict,
    next_version: int,
    old_mappings: dict | None,
    db,
):
    """Handle large datasets with metadata-only versioning."""
    total_rows = ds_info["total"]
    fields = ds_info["fields"]

    # Check if record count changed from previous version
    previous_count = None
    if old_mappings:
        prev_summary = old_mappings.get("_large_dataset_info", {})
        previous_count = prev_summary.get("record_count")

    new_modified = pkg.get("metadata_modified", "")

    # Get sample data (first 100 + last 100 rows)
    try:
        head_records, tail_records = await ckan_client.datastore_sample(ds.resource_id)
    except Exception as e:
        logger.warning("Failed to get sample for large dataset %s: %s", ds.ckan_name, e)
        head_records, tail_records = [], []

    # Compute a lightweight hash from record count + field names + sample
    lightweight_data = json.dumps({
        "total": total_rows,
        "fields": [f["id"] for f in fields],
        "head_sample": head_records[:5],  # first 5 for hash
    }, sort_keys=True)
    sha256 = hashlib.sha256(lightweight_data.encode()).hexdigest()

    # Check against previous hash
    old_hash = (old_mappings or {}).get("_hashes", {}).get("lightweight")
    if old_hash == sha256 and previous_count == total_rows:
        logger.info("Large dataset %s unchanged (count=%d, hash=%s)", ds.ckan_name, total_rows, sha256[:8])
        ds.last_polled_at = datetime.now(timezone.utc)
        ds.last_modified = new_modified
        await db.commit()
        return

    logger.info("Large dataset %s changed: %s rows (prev=%s)", ds.ckan_name, total_rows, previous_count)

    # Create lightweight snapshot on odata.org.il
    odata_resource_id = None
    if ds.odata_dataset_id:
        try:
            from app.services.snapshot_service import create_lightweight_snapshot
            odata_resource_id = await create_lightweight_snapshot(
                odata_dataset_id=ds.odata_dataset_id,
                version_number=next_version,
                resource_name=resource.get("name", ds.ckan_name),
                total_rows=total_rows,
                fields=fields,
                head_records=head_records,
                tail_records=tail_records,
            )
        except Exception as e:
            logger.error("Failed to create lightweight snapshot: %s", e)

    delta = total_rows - previous_count if previous_count is not None else total_rows

    # Save version
    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "large_dataset",
            "record_count": total_rows,
            "previous_count": previous_count,
            "delta": delta,
            "fields": [f["id"] for f in fields],
            "sample_rows": len(head_records) + len(tail_records),
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings={
            "_hashes": {"lightweight": sha256},
            "_resource_ids": [resource.get("id", "")],
            "_large_dataset_info": {
                "record_count": total_rows,
                "fields": [f["id"] for f in fields],
            },
            **({"sample": odata_resource_id} if odata_resource_id else {}),
        },
    )
    db.add(version)

    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    await db.commit()

    logger.info("Large dataset version %d created for %s (%d rows, delta=%d)",
                next_version, ds.ckan_name, total_rows, delta)
