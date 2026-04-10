import json
import logging
from typing import Any

import httpx

from app.config import settings
from app.services.csv_parser import parse_csv
from app.services.odata_client import odata_client

logger = logging.getLogger(__name__)

# Formats that can be parsed as CSV and pushed to datastore
TABULAR_FORMATS = {"csv", "tsv", "txt"}


async def create_version_snapshot(
    odata_dataset_id: str,
    version_number: int,
    metadata: dict,
    changed_resources: list[dict],
    hash_map: dict,
    old_mappings: dict | None,
) -> tuple[str, dict]:
    """
    Upload a full version snapshot to odata.org.il.
    - Tabular data (CSV) → pushed to Datastore (queryable via API)
    - Metadata → uploaded as JSON file
    Returns (metadata_resource_id, resource_mappings).
    """
    # Upload metadata snapshot (always as file — not tabular)
    meta_result = await odata_client.upload_metadata_snapshot(
        dataset_id=odata_dataset_id,
        version_number=version_number,
        metadata=metadata,
    )
    metadata_resource_id = meta_result["id"]

    resource_mappings: dict[str, Any] = {}

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
                # Parse CSV and push to Datastore
                result = await _push_to_datastore(
                    odata_dataset_id, version_number, resource, content,
                )
            else:
                # Non-tabular: upload as file (fallback)
                result = await odata_client.upload_resource(
                    dataset_id=odata_dataset_id,
                    file_content=content,
                    filename=f"v{version_number}_{resource.get('name', rid)}",
                    name=f"v{version_number} - {resource.get('name', rid)}",
                    description=f"Resource snapshot (version {version_number})",
                    resource_format=fmt.upper(),
                )

            resource_mappings[rid] = result["id"]
        except Exception as e:
            logger.error("Failed to upload resource %s to odata: %s", rid, e)

    # Store hashes and resource IDs in mappings for next comparison
    resource_mappings["_hashes"] = hash_map
    resource_mappings["_resource_ids"] = [r["id"] for r in metadata.get("resources", [])]

    return metadata_resource_id, resource_mappings


async def _push_to_datastore(
    odata_dataset_id: str,
    version_number: int,
    resource: dict,
    content: bytes,
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
            filename=f"v{version_number}_{name}",
            name=f"v{version_number} - {name}",
            description=f"Resource snapshot (version {version_number}) - empty/unparseable",
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
    )


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
