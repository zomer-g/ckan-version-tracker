import json
import logging
from typing import Any

import httpx

from app.config import settings
from app.services.odata_client import odata_client

logger = logging.getLogger(__name__)


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
    Returns (metadata_resource_id, resource_mappings).
    """
    # Upload metadata snapshot
    meta_result = await odata_client.upload_metadata_snapshot(
        dataset_id=odata_dataset_id,
        version_number=version_number,
        metadata=metadata,
    )
    metadata_resource_id = meta_result["id"]

    # Upload changed resource files
    resource_mappings: dict[str, str] = {}

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
        try:
            result = await odata_client.upload_resource_snapshot(
                dataset_id=odata_dataset_id,
                version_number=version_number,
                resource_name=resource.get("name", rid),
                file_content=cr["content"],
                resource_format=resource.get("format", ""),
            )
            resource_mappings[rid] = result["id"]
        except Exception as e:
            logger.error("Failed to upload resource %s to odata: %s", rid, e)

    # Store hashes and resource IDs in mappings for next comparison
    resource_mappings["_hashes"] = hash_map
    resource_mappings["_resource_ids"] = [r["id"] for r in metadata.get("resources", [])]

    return metadata_resource_id, resource_mappings


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
