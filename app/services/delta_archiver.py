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
from app.services.snapshot_service import (
    append_new_rows_to_shared_resource,
    _iso_timestamp,
)
from app.services.version_detector import (
    compute_new_rows,
    compute_new_rows_windowed,
)

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
    """Stream-and-append a CKAN dataset's deltas to its shared odata resource
    via the datastore API (never the file download — that path is IAP-blocked
    on some sources, e.g. the flights board). Caller routes here when
    storage_mode == 'append_only' and the resource is datastore-active, whether
    the dataset is huge (vehicle registry) or small (flights board).

    Two dedup modes, chosen by scraper_config:
      - keyed (``append_key`` set): identity is that single column's value.
        Used for the vehicle registry (mispar_rechev). Seen-set is an
        unbounded list — fine for slow-cadence, append-only-growth sources.
      - keyless (no ``append_key``): identity is the full-row hash, so every
        distinct row state is captured. Used for the flights board, where rows
        mutate through their lifecycle (scheduled→landed).

    ``seen_window_versions`` (int, opt-in): bounds the seen-set to a sliding
    window of that many versions so a 15-min-cadence board's bookkeeping
    doesn't grow without bound. Required for the keyless flights case.

    Returns True if a version was committed; False if the function
    couldn't proceed (caller should fall through to the legacy
    metadata-only stub).
    """
    cfg = ds.scraper_config or {}
    append_key = cfg.get("append_key")  # None → keyless full-row-hash dedup
    window = cfg.get("seen_window_versions")
    windowed = isinstance(window, int) and window > 0

    target_rid = resource.get("id") or ds.resource_id
    if not target_rid:
        logger.info("delta_archiver: %s no resource id", ds.ckan_name)
        return False

    # Lazily create the odata mirror if it doesn't exist yet (mirrors the
    # snapshot / _poll_append_only paths). Without this, a freshly-registered
    # append_only dataset whose mirror wasn't pre-created would bail here and
    # fall through to the metadata-only stub instead of appending its rows.
    if not ds.odata_dataset_id:
        if not settings.odata_api_key:
            logger.info(
                "delta_archiver: %s has no odata mirror and no api key, can't append",
                ds.ckan_name,
            )
            return False
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
            logger.info("delta_archiver: lazily created mirror %s for %s",
                        mirror_name, ds.ckan_name)
        except Exception as e1:
            logger.warning("delta_archiver: mirror create failed for %s: %s",
                           mirror_name, e1)
            try:
                mirror = await odata_client.package_show(mirror_name)
                ds.odata_dataset_id = mirror["id"]
            except Exception as e2:
                logger.error("delta_archiver: mirror find also failed for %s: %s",
                             mirror_name, e2)
                return False

    fields = _fields_for_odata(ds_info.get("fields") or [])
    if not fields:
        logger.info(
            "delta_archiver: %s no usable fields from datastore_info",
            ds.ckan_name,
        )
        return False

    # Load the carried-forward seen-set. Windowed mode keeps a
    # {identity: last_seen_version} map under a distinct key so it never
    # collides with the legacy flat list (and a dataset can't silently switch
    # shapes mid-life). Non-windowed mode keeps the original list.
    prev_mappings = (latest_version.resource_mappings or {}) if latest_version else {}
    seen_keys: list[str] = list(prev_mappings.get("_appendonly_seen") or [])
    seen_gen: dict[str, int] = dict(prev_mappings.get("_appendonly_seen_gen") or {})

    rid: str | None = ds.appendonly_resource_id
    rows_inserted_total = 0
    pages_processed = 0
    pending: list[dict] = []
    # One timestamp for the whole poll: every row first observed in this run
    # shares the same first_seen value (cleaner than per-flush drift, and the
    # seeding run can flush thousands of times).
    run_ts = _iso_timestamp()

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
                add_first_seen=True,
                first_seen_value=run_ts,
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
            if windowed:
                new_rows_in_batch, seen_gen = compute_new_rows_windowed(
                    seen_gen, batch, append_key, next_version,
                )
            else:
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
        # Surface the real reason — a silent fall-through to the metadata stub
        # otherwise hides that the append never happened (no last_error, just a
        # "large_dataset" version that records counts but accumulates nothing).
        ds.last_error = (
            f"delta append failed (page {pages_processed}): "
            f"{type(e).__name__}: {e}"
        )[:2000]
        try:
            await db.commit()
        except Exception:
            await db.rollback()
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

    # Build the carried-forward seen-set for the next poll. Windowed mode
    # evicts identities not seen within the last `window` versions (their
    # generation has fallen outside the window); list mode carries everything.
    mappings: dict = {"_resource_ids": [target_rid], target_rid: rid}
    if windowed:
        cutoff = next_version - window
        seen_gen = {k: g for k, g in seen_gen.items() if g > cutoff}
        mappings["_appendonly_seen_gen"] = seen_gen
        seen_total = len(seen_gen)
    else:
        mappings["_appendonly_seen"] = seen_keys
        seen_total = len(seen_keys)

    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "delta_via_datastore",
            "rows_added": rows_inserted_total,
            "rows_total": seen_total,
            "key": append_key or "_hash",
            "windowed": windowed,
            "pages_processed": pages_processed,
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings=mappings,
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    await db.commit()
    logger.info(
        "delta_archiver: %s version %d committed (%d new rows, %d seen, %d pages)",
        ds.ckan_name, next_version, rows_inserted_total, seen_total,
        pages_processed,
    )
    return True
