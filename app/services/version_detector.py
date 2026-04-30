import hashlib
import json
import logging
from typing import Any

from app.services.ckan_client import ckan_client

logger = logging.getLogger(__name__)

# Fields to ignore when computing metadata diffs
IGNORED_METADATA_FIELDS = {
    "metadata_modified",
    "metadata_created",
    "revision_id",
    "tracking_summary",
}


def has_metadata_changed(old_modified: str | None, new_modified: str) -> bool:
    """Quick check: has the metadata_modified timestamp changed?"""
    if old_modified is None:
        return True
    return old_modified != new_modified


async def detect_resource_changes(
    old_mappings: dict | None, resources: list[dict]
) -> tuple[list[dict], dict[str, str]]:
    """
    Download resources and detect which ones changed.
    Returns (changed_resources, hash_map) where:
      - changed_resources: list of {resource, content, sha256}
      - hash_map: {ckan_resource_id: sha256}
    """
    changed = []
    hash_map = {}
    old_hashes = {}

    # Extract old hashes from mappings if available
    if old_mappings:
        old_hashes = old_mappings.get("_hashes", {})

    for resource in resources:
        rid = resource["id"]
        url = resource.get("url", "")
        if not url:
            continue

        try:
            content, sha256 = await ckan_client.download_resource(url, resource_id=rid)
            hash_map[rid] = sha256

            old_hash = old_hashes.get(rid)
            if old_hash != sha256:
                changed.append({
                    "resource": resource,
                    "content": content,
                    "sha256": sha256,
                })
                logger.info(
                    "Resource changed: %s (old=%s, new=%s)",
                    resource.get("name", rid),
                    old_hash,
                    sha256,
                )
        except Exception as e:
            logger.warning("Failed to download resource %s: %s", rid, e)
            hash_map[rid] = "download_failed"

    return changed, hash_map


def compute_change_summary(
    old_resources: dict | None, new_resources: list[dict], changed_resources: list[dict], hash_map: dict
) -> dict:
    """Compute a structured change summary."""
    old_ids = set((old_resources or {}).get("_resource_ids", []))
    new_ids = {r["id"] for r in new_resources}

    added = new_ids - old_ids
    removed = old_ids - new_ids

    modified = []
    for cr in changed_resources:
        rid = cr["resource"]["id"]
        if rid not in added:
            modified.append({
                "resource_id": rid,
                "name": cr["resource"].get("name", ""),
                "format": cr["resource"].get("format", ""),
            })

    return {
        "resources_added": list(added),
        "resources_removed": list(removed),
        "resources_modified": modified,
        "total_resources": len(new_resources),
    }


def _row_identity(row: dict, key_field: str | None) -> str:
    """Identity used to recognize a row across versions in append mode.

    With key_field: stringified value of that column (None/missing → '').
    Without key_field: SHA-256 of the row's JSON (sorted keys, str-coerced
    values) so semantically-equal rows hash equal regardless of dict order.
    """
    if key_field:
        v = row.get(key_field)
        return "" if v is None else str(v)
    canonical = json.dumps(
        {str(k): ("" if v is None else str(v)) for k, v in row.items()},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_new_rows(
    seen_keys: list[str] | None,
    new_records: list[dict],
    key_field: str | None,
) -> tuple[list[dict], list[str]]:
    """Filter new_records down to ones not in seen_keys.

    Returns (rows_to_insert, updated_seen_keys). Order is preserved.
    Duplicates within new_records are deduplicated against each other too,
    so a single push that contains the same row twice only inserts it once.
    """
    seen: set[str] = set(seen_keys or [])
    out_rows: list[dict] = []
    out_keys: list[str] = list(seen_keys or [])
    for row in new_records:
        ident = _row_identity(row, key_field)
        if ident in seen:
            continue
        seen.add(ident)
        out_rows.append(row)
        out_keys.append(ident)
    return out_rows, out_keys
