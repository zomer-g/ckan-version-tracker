import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from app.config import settings
from app.services.csv_parser import parse_csv
from app.services.odata_client import odata_client

logger = logging.getLogger(__name__)

# Formats that can be parsed as CSV and pushed to datastore
TABULAR_FORMATS = {"csv", "tsv", "txt"}


def _timestamp() -> str:
    """Current timestamp for resource names in Israel local time: 2026-04-10_14-30.

    Users read these in the CKAN UI in Hebrew context and expect Israel
    time, not UTC. Standard library `zoneinfo` (Python 3.9+) handles DST
    automatically (IST in winter, IDT in summer).
    """
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Jerusalem")
    except Exception:
        # Fallback: fixed UTC+3 (Israel winter time) if tzdata unavailable
        from datetime import timedelta
        tz = timezone(timedelta(hours=3))
    return datetime.now(tz).strftime("%Y-%m-%d_%H-%M")


async def create_version_snapshot(
    odata_dataset_id: str,
    version_number: int,
    metadata: dict,
    changed_resources: list[dict],
    hash_map: dict,
    old_mappings: dict | None,
) -> tuple[str, dict, list[str]]:
    """
    Upload a full version snapshot to odata.org.il.
    - Tabular data (CSV) -> pushed to Datastore (queryable via API)
    - Metadata -> uploaded as JSON file
    - Resource names include date for easy identification.
    Returns (metadata_resource_id, resource_mappings, errors). `errors` is
    a list of per-resource upload failure messages — empty when every
    resource was uploaded cleanly. The caller decides whether to persist
    a version when only some/none of the uploads succeeded.
    """
    ts = _timestamp()

    # No separate metadata JSON upload — metadata is stored in version_index JSONB
    metadata_resource_id = None

    resource_mappings: dict[str, Any] = {}
    errors: list[str] = []

    # Carry forward unchanged resources from old mappings
    if old_mappings:
        for rid, odata_rid in old_mappings.items():
            if rid.startswith("_"):
                continue
            if rid not in {cr["resource"]["id"] for cr in changed_resources}:
                resource_mappings[rid] = odata_rid

    # Upload new/changed resources
    for cr in changed_resources:
        resource = cr["resource"]
        rid = resource["id"]
        content: bytes = cr["content"]
        fmt = (resource.get("format", "") or "").lower().strip()

        try:
            if fmt in TABULAR_FORMATS:
                result = await _push_to_datastore(
                    odata_dataset_id, version_number, resource, content, ts,
                )
            else:
                result = await odata_client.upload_resource(
                    dataset_id=odata_dataset_id,
                    file_content=content,
                    filename=f"{ts}_v{version_number}_{resource.get('name', rid)}",
                    name=f"{ts} v{version_number} - {resource.get('name', rid)}",
                    description=f"Resource snapshot (version {version_number}, {ts})",
                    resource_format=fmt.upper(),
                )

            resource_mappings[rid] = result["id"]
        except Exception as e:
            logger.error("Failed to upload resource %s to odata: %s", rid, e)
            errors.append(f"upload {resource.get('name', rid[:8])} ({fmt or '?'}): {e}")

    # Store hashes and resource IDs in mappings for next comparison
    resource_mappings["_hashes"] = hash_map
    resource_mappings["_resource_ids"] = [r["id"] for r in metadata.get("resources", [])]

    return metadata_resource_id, resource_mappings, errors


async def _push_to_datastore(
    odata_dataset_id: str,
    version_number: int,
    resource: dict,
    content: bytes,
    timestamp: str,
) -> dict:
    """Parse CSV content and push rows into CKAN Datastore."""
    name = resource.get("name", resource["id"])
    fmt = (resource.get("format", "") or "CSV").upper()

    logger.info("Parsing CSV for datastore: %s (%d bytes)", name, len(content))
    fields, records = parse_csv(content)

    if not records:
        logger.warning("No records parsed from %s — uploading as file instead", name)
        return await odata_client.upload_resource(
            dataset_id=odata_dataset_id,
            file_content=content,
            filename=f"{timestamp}_v{version_number}_{name}",
            name=f"{timestamp} v{version_number} - {name} (unparseable)",
            description=f"Resource snapshot (version {version_number}, {timestamp})",
            resource_format=fmt,
        )

    logger.info("Pushing %d records (%d fields) to datastore for %s", len(records), len(fields), name)

    return await odata_client.push_csv_to_datastore(
        dataset_id=odata_dataset_id,
        version_number=version_number,
        resource_name=name,
        fields=fields,
        records=records,
        resource_format=fmt,
        timestamp=timestamp,
    )


async def append_new_rows_to_shared_resource(
    *,
    odata_dataset_id: str,
    appendonly_resource_id: str | None,
    version_number: int,
    resource_name: str,
    fields: list[dict],
    new_rows: list[dict],
    resource_format: str = "CSV",
) -> tuple[str | None, int]:
    """Append-only push: insert `new_rows` into a shared odata resource.

    - If `appendonly_resource_id` is None, the shared resource is created
      via the standard push_csv_to_datastore path (defines schema +
      uploads the CSV file). Caller must persist the returned id back to
      the dataset row.
    - Otherwise inserts new rows in batches into the existing resource via
      datastore_upsert(method="insert", force=True).

    Returns (resource_id, rows_inserted). resource_id may be None only when
    new_rows is empty AND there is no shared resource yet (caller skips).
    """
    from app.services.csv_parser import batch_records

    if not new_rows and not appendonly_resource_id:
        return None, 0

    if not appendonly_resource_id:
        result = await odata_client.push_csv_to_datastore(
            dataset_id=odata_dataset_id,
            version_number=version_number,
            resource_name=resource_name,
            fields=fields,
            records=new_rows,
            resource_format=resource_format,
            timestamp=_timestamp(),
        )
        return result["id"], len(new_rows)

    if not new_rows:
        return appendonly_resource_id, 0

    batches = batch_records(new_rows)
    for i, batch in enumerate(batches, start=1):
        await odata_client._push_batch_with_retry(
            resource_id=appendonly_resource_id,
            fields=fields,
            records_batch=batch,
            create=False,
            batch_num=i,
            is_last=(i == len(batches)),
        )
    return appendonly_resource_id, len(new_rows)


async def create_lightweight_snapshot(
    odata_dataset_id: str,
    version_number: int,
    resource_name: str,
    total_rows: int,
    fields: list[dict],
    head_records: list[dict],
    tail_records: list[dict],
) -> str | None:
    """Create a lightweight version snapshot for large datasets.
    Pushes only sample rows (head + tail) to odata.org.il datastore.
    Returns the odata resource_id or None.
    """
    ts = _timestamp()
    all_sample = head_records + tail_records
    if not all_sample:
        return None

    safe_name = resource_name.replace("/", "_").replace("\\", "_")

    try:
        result = await odata_client.push_csv_to_datastore(
            dataset_id=odata_dataset_id,
            version_number=version_number,
            resource_name=f"{safe_name} (sample {len(all_sample)}/{total_rows:,})",
            fields=[{"id": f["id"], "type": f.get("type", "text")} for f in fields],
            records=all_sample,
            resource_format="CSV",
            timestamp=ts,
        )
        return result["id"]
    except Exception as e:
        logger.error("Failed to push lightweight snapshot: %s", e)
        return None


async def fetch_metadata_from_odata(odata_resource_id: str) -> dict:
    """Fetch a metadata snapshot JSON from odata.org.il."""
    if not odata_resource_id:
        return {}

    url = f"{settings.odata_url}/api/3/action/resource_show"
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        resp = await client.get(url, params={"id": odata_resource_id})
        resp.raise_for_status()
        result = resp.json()
        resource = result.get("result", {})

    download_url = resource.get("url")
    if not download_url:
        return {}

    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), follow_redirects=True) as client:
        resp = await client.get(download_url)
        resp.raise_for_status()
        return resp.json()
