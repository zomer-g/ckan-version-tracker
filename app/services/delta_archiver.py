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

import asyncio
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
from app.services import append_store

logger = logging.getLogger(__name__)

# Flush pending-new buffer this often. Bounds peak memory during the
# seeding poll where every row is "new" (~32k pages × 5k flushes ≈
# constant memory regardless of total dataset size).
PENDING_FLUSH_THRESHOLD = 5000

# httpx client timeout per page request. data.gov.il's datastore_search is
# usually fast per 32k-row page, but under load some pages take far longer or
# time out outright — and a single uncaught timeout used to abort the whole
# multi-million-row seed. Bumped + retried (below) so a transient blip doesn't
# kill a run; the checkpoint then only loses (at most) the in-flight page.
PAGE_TIMEOUT_SECONDS = 120.0
PAGE_MAX_RETRIES = 6


async def _fetch_datastore_page(
    client: httpx.AsyncClient, base_url: str, resource_id: str, page_size: int, offset: int,
) -> dict:
    """GET one datastore_search page, retrying transient timeouts / transport
    errors / 5xx with capped exponential backoff. Raises only after the retry
    budget is spent (so the caller's checkpoint holds and the next run resumes)."""
    last: Exception | None = None
    for attempt in range(1, PAGE_MAX_RETRIES + 1):
        try:
            resp = await client.get(
                f"{base_url}/datastore_search",
                params={"resource_id": resource_id, "limit": page_size, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"success=false: {data.get('error')}")
            return data
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                raise  # 4xx won't fix itself
            last = e
        except (httpx.TimeoutException, httpx.TransportError, RuntimeError) as e:
            last = e
        if attempt < PAGE_MAX_RETRIES:
            backoff = min(30.0, 2.0 ** attempt)
            logger.warning(
                "datastore_search retry %d/%d at offset=%d (%s) — backoff %.0fs",
                attempt, PAGE_MAX_RETRIES, offset, type(last).__name__, backoff,
            )
            await asyncio.sleep(backoff)
    raise RuntimeError(
        f"datastore_search failed after {PAGE_MAX_RETRIES} attempts at "
        f"offset={offset}: {type(last).__name__}: {last}"
    )


async def _stream_datastore_pages(
    resource_id: str,
    page_size: int = DATASTORE_PAGE_SIZE,
    start_offset: int = 0,
) -> AsyncGenerator[tuple[int, list[dict]], None]:
    """Yield ``(next_offset, page)`` for data.gov.il's datastore, starting at
    ``start_offset`` (a checkpoint, so a killed seed resumes instead of
    restarting). ``next_offset`` is the offset AFTER this page — persist it as
    the resume point. Strips the synthetic `_id` column on each row.

    Stops when a page comes back empty (datastore exhausted). Per-page transient
    failures are retried (see _fetch_datastore_page) so a single slow page can't
    abort a long seed."""
    base_url = settings.data_gov_il_url.rstrip("/") + "/api/3/action"
    offset = max(0, int(start_offset or 0))
    async with httpx.AsyncClient(timeout=PAGE_TIMEOUT_SECONDS) as client:
        while True:
            data = await _fetch_datastore_page(
                client, base_url, resource_id, page_size, offset,
            )
            records = data.get("result", {}).get("records") or []
            if not records:
                return
            offset += len(records)
            yield offset, [{k: v for k, v in r.items() if k != "_id"} for r in records]


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


async def _archive_streaming_to_db(
    *,
    ds: TrackedDataset,
    resource: dict,
    ds_info: dict,
    next_version: int,
    new_modified: str,
    db,
) -> bool:
    """Stream the dataset's datastore rows into the dedicated append Postgres.

    Dedup + first_seen are the DB's job (UNIQUE index + ON CONFLICT DO NOTHING,
    first_seen DEFAULT now()), so there's no seen-set to carry or window. Keyed
    (append_key) datasets dedup on that column; keyless ones on a row_hash. The
    pending buffer is flushed every PENDING_FLUSH_THRESHOLD rows so peak memory
    is bounded even on the 4M-row vehicle seed. Records a VersionIndex with the
    per-poll insert count + running total. Returns True on commit, False to let
    the caller fall through to the metadata stub."""
    append_key = (ds.scraper_config or {}).get("append_key")  # None → keyless
    keyless = not append_key

    target_rid = resource.get("id") or ds.resource_id
    if not target_rid:
        logger.info("append-db: %s no resource id", ds.ckan_name)
        return False

    fields = _fields_for_odata(ds_info.get("fields") or [])
    if not fields:
        logger.info("append-db: %s no usable fields", ds.ckan_name)
        return False
    source_cols = [f["id"] for f in fields]
    table = append_store.table_name(ds)

    try:
        await append_store.ensure_table(
            table, source_cols, key_col=append_key, keyless=keyless,
        )
    except Exception as e:
        logger.error("append-db: ensure_table failed for %s: %s", ds.ckan_name, e)
        ds.last_error = f"append-db ensure_table: {type(e).__name__}: {e}"[:2000]
        await db.commit()
        return False

    # Resume point: a prior run may have been killed mid-scan (e.g. a dyno
    # restart from a parallel deploy). seed_offset is the datastore offset the
    # last successful flush reached; start there instead of re-scanning from 0.
    start_offset = int((ds.scraper_config or {}).get("seed_offset") or 0)
    rows_inserted_total = 0
    pages_processed = 0
    last_offset = start_offset
    pending: list[dict] = []

    async def _flush_and_checkpoint() -> None:
        nonlocal rows_inserted_total, pending
        if not pending:
            return
        n = await append_store.append_rows(
            table, source_cols, pending, key_col=append_key, keyless=keyless,
        )
        rows_inserted_total += n
        pending = []
        # Persist the resume point ONLY after the rows are durably in the append
        # DB, so a kill resumes from here (ON CONFLICT makes a replayed page a
        # cheap no-op either way). Checkpoint lives in scraper_config (OVER DB).
        ds.scraper_config = {**(ds.scraper_config or {}), "seed_offset": last_offset}
        await db.commit()

    try:
        async for next_offset, batch in _stream_datastore_pages(
            target_rid, start_offset=start_offset,
        ):
            pages_processed += 1
            last_offset = next_offset
            pending.extend(batch)
            if len(pending) >= PENDING_FLUSH_THRESHOLD:
                await _flush_and_checkpoint()
        await _flush_and_checkpoint()
    except Exception as e:
        logger.error("append-db: streaming/insert aborted for %s at offset %d: %s",
                     ds.ckan_name, last_offset, e)
        ds.last_error = (
            f"append-db insert failed (offset {last_offset}): "
            f"{type(e).__name__}: {e}"
        )[:2000]
        await db.commit()  # keeps seed_offset so the next run resumes here
        return False

    # Datastore exhausted → the full scan finished. Clear the checkpoint so the
    # next scheduled poll starts a fresh full re-scan (catches new rows anywhere
    # in the registry), and record the completed append_db version so the UI
    # recognizes this dataset as a NEON-backed append archive.
    ds.scraper_config = {
        k: v for k, v in (ds.scraper_config or {}).items() if k != "seed_offset"
    } or None

    try:
        total = await append_store.table_count(table)
    except Exception:
        total = rows_inserted_total

    version = VersionIndex(
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=None,
        change_summary={
            "type": "append_db",
            "rows_added": rows_inserted_total,
            "rows_total": total,
            "key": append_key or "_hash",
            "pages_processed": pages_processed,
            "start_offset": start_offset,
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
        },
        resource_mappings={
            "_resource_ids": [target_rid],
            "append_table": table,
        },
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    ds.last_error = None
    await db.commit()
    logger.info(
        "append-db: %s v%d committed (%d new rows, %d total, %d pages from offset %d) → %s",
        ds.ckan_name, next_version, rows_inserted_total, total, pages_processed,
        start_offset, table,
    )
    return True


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

    STORAGE: when an append DB is configured (APPEND_DATABASE_URL), rows are
    written there — one table per dataset, deduped by the DB (UNIQUE index +
    ON CONFLICT), first_seen stamped by a column default. That's the supported
    path now that ODATA's write endpoint is down. The legacy ODATA datastore
    path below is kept only as a fallback for when no append DB is configured.
    """
    if append_store.is_configured():
        return await _archive_streaming_to_db(
            ds=ds, resource=resource, ds_info=ds_info,
            next_version=next_version, new_modified=new_modified, db=db,
        )

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
        async for _next_offset, batch in _stream_datastore_pages(target_rid):
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
