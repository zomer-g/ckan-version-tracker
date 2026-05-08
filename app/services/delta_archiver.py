"""Delta archiving for large data.gov.il datasets.

Default for >50k-row datasets is the metadata-only "lightweight snapshot"
path (poll_job._poll_large_dataset): record-count + field-list + 200
sample rows. The full data is never archived because downloading +
parsing a multi-hundred-MB CSV blows the dyno's RAM ceiling.

This module is the opt-in alternative: when an admin sets
storage_mode='append_only' and an append_key, we stream the dataset out
of CKAN's datastore one 32k-row page at a time, dedupe against the
seen-keys set carried forward from the previous version, and append
only the new rows to the same shared odata resource the rest of the
append-only flow uses.

Memory bounds:
  - Each datastore page is loaded then released; we never hold the
    whole dataset in RAM.
  - Pending new-rows are flushed to odata every PENDING_FLUSH_THRESHOLD,
    so even on the seeding poll (where every row is "new") peak memory
    is O(threshold), not O(dataset).
  - The seen-keys set inherently grows with the dataset's logical size,
    but each entry is a short string (license number, etc.) — 10M
    entries × 8-12 bytes is comfortable in JSONB.

Caveats vs full-snapshot semantics:
  - Identity is by `append_key` value, not full-row hash. A row whose
    payload changes but whose key stays the same (e.g. ownership
    transfer on the same license plate) is NOT captured as a new row.
    That's a known trade-off — the user opts into append_only because
    they want growth tracking, not in-place mutation tracking.
  - Deletions aren't surfaced. A key that disappears from the source
    stays in seen_keys forever.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import AsyncGenerator

import httpx

from app.config import settings
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services.ckan_client import DATASTORE_PAGE_SIZE
from app.services.snapshot_service import append_new_rows_to_shared_resource
from app.services.version_detector import compute_new_rows

logger = logging.getLogger(__name__)

# Flush pending-new buffer this often. Bounds peak memory during the
# seeding poll where every row is "new" (~32k pages × 5k flushes ≈
# constant memory regardless of total dataset size).
PENDING_FLUSH_THRESHOLD = 5000

# httpx client timeout per page request. CKAN datastore_search is
# usually <2s per 32k-row page; some are slower under load.
PAGE_TIMEOUT_SECONDS = 60.0


async def _stream_datastore_pages(
    resource_id: str,
    page_size: int = DATASTORE_PAGE_SIZE,
) -> AsyncGenerator[list[dict], None]:
    """Yield successive pages of records from data.gov.il's datastore for
    `resource_id`. Strips the synthetic `_id` column on each row.

    Stops when a page comes back empty (datastore exhausted) or when the
    API returns success=False (treated as a hard error)."""
    base_url = settings.data_gov_il_url.rstrip("/") + "/api/3/action"
    offset = 0
    async with httpx.AsyncClient(timeout=PAGE_TIMEOUT_SECONDS) as client:
        while True:
            resp = await client.get(
                f"{base_url}/datastore_search",
                params={
                    "resource_id": resource_id,
                    "limit": page_size,
                    "offset": offset,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(
                    f"datastore_search failed at offset={offset}: "
                    f"{data.get('error')}"
                )
            records = data.get("result", {}).get("records") or []
            if not records:
                return
            yield [{k: v for k, v in r.items() if k != "_id"} for r in records]
            offset += len(records)


def _fields_for_odata(ds_info_fields: list[dict]) -> list[dict]:
    """Convert CKAN datastore field metadata to the shape
    push_csv_to_datastore / append_new_rows_to_shared_resource expect:
    [{"id": "<col>", "type": "<text|integer|...>"}, ...].

    Skips the synthetic `_id` column. Falls back to "text" when the
    source type is missing or unknown — datastore is permissive about
    text columns."""
    out: list[dict] = []
    for f in ds_info_fields or []:
        col_id = f.get("id")
        if not col_id or col_id == "_id":
            continue
        out.append({"id": col_id, "type": f.get("type") or "text"})
    return out


async def archive_via_datastore_streaming(
    *,
    ds: TrackedDataset,
    resource: dict,
    ds_info: dict,
    next_version: int,
    latest_version: VersionIndex | None,
    new_modified: str,
    db,
) -> bool:
    """Stream-and-append a large CKAN dataset's deltas to its shared odata
    resource. Caller routes here when:
      - the dataset is too large for the snapshot path (>50k rows), AND
      - storage_mode == 'append_only', AND
      - scraper_config.append_key is set.

    Returns True if a version was committed; False if the function
    couldn't proceed (caller should fall through to the legacy
    metadata-only stub).
    """
    append_key = (ds.scraper_config or {}).get("append_key")
    if not append_key:
        logger.info(
            "delta_archiver: %s missing append_key, falling back",
            ds.ckan_name,
        )
        return False

    target_rid = resource.get("id") or ds.resource_id
    if not target_rid:
        logger.info("delta_archiver: %s no resource id", ds.ckan_name)
        return False

    if not ds.odata_dataset_id:
        logger.info(
            "delta_archiver: %s has no odata mirror, can't append",
            ds.ckan_name,
        )
        return False

    fields = _fields_for_odata(ds_info.get("fields") or [])
    if not fields:
        logger.info(
            "delta_archiver: %s no usable fields from datastore_info",
            ds.ckan_name,
        )
        return False

    seen_keys: list[str] = []
    if latest_version and latest_version.resource_mappings:
        seen_keys = list(
            latest_version.resource_mappings.get("_appendonly_seen") or []
        )

    rid: str | None = ds.appendonly_resource_id
    rows_inserted_total = 0
    pages_processed = 0
    pending: list[dict] = []

    async def _flush() -> None:
        """Push pending rows to odata; reuse-or-create the shared resource."""
        nonlocal rid, rows_inserted_total, pending
        if not pending:
            return
        try:
            new_rid, n = await append_new_rows_to_shared_resource(
                odata_dataset_id=ds.odata_dataset_id,
                appendonly_resource_id=rid,
                version_number=next_version,
                resource_name=resource.get("name") or ds.ckan_name,
                fields=fields,
                new_rows=pending,
                resource_format="CSV",
            )
        except Exception as e:
            logger.error(
                "delta_archiver: append failed for %s after %d pages: %s",
                ds.ckan_name, pages_processed, e,
            )
            raise
        if new_rid and not rid:
            rid = new_rid
            ds.appendonly_resource_id = new_rid
        rows_inserted_total += n
        pending = []

    try:
        async for batch in _stream_datastore_pages(target_rid):
            pages_processed += 1
            new_rows_in_batch, seen_keys = compute_new_rows(
                seen_keys, batch, append_key,
            )
            if new_rows_in_batch:
                pending.extend(new_rows_in_batch)
            if len(pending) >= PENDING_FLUSH_THRESHOLD:
                await _flush()

        # Final flush for any leftover rows that didn't reach the threshold.
        await _flush()
    except Exception as e:
        logger.error(
            "delta_archiver: streaming aborted for %s at page %d: %s",
            ds.ckan_name, pages_processed, e,
        )
        return False

    if rid is None:
        # Nothing to write yet AND no pre-existing shared resource — this
        # only happens when the dataset is empty on the first poll. Fall
        # through so the legacy stub still leaves a marker.
        logger.info(
            "delta_archiver: %s yielded zero rows, leaving for stub path",
            ds.ckan_name,
        )
        return False

    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "delta_via_datastore",
            "rows_added": rows_inserted_total,
            "rows_total": len(seen_keys),
            "key": append_key,
            "pages_processed": pages_processed,
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings={
            "_resource_ids": [target_rid],
            "_appendonly_seen": seen_keys,
            target_rid: rid,
        },
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    await db.commit()
    logger.info(
        "delta_archiver: %s version %d committed (%d new rows, %d seen, %d pages)",
        ds.ckan_name, next_version, rows_inserted_total, len(seen_keys),
        pages_processed,
    )
    return True
