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

# Synthetic column stamped on every append-only row at insert time, so the
# shared resource records WHEN each row first entered the archive. Both append
# paths inject it: delta_archiver (large datasets, dedup by append_key) and
# poll_job._poll_append_only (small datasets, dedup by full-row hash). ASCII
# on purpose — odata's autopusher transliterates Hebrew column names to ASCII
# (`חשבון`→`khshbvn`), so a Hebrew header would land under an unpredictable
# name. Dedup runs on the SOURCE row before stamping, so the synthetic value
# never perturbs identity (critical for the full-row-hash flights case).
APPEND_FIRST_SEEN_FIELD = "first_seen"


def _iso_timestamp() -> str:
    """Israel-local ISO-8601 timestamp without offset: ``2026-06-26T14:30:00``.

    Used to stamp the ``first_seen`` column on append-only rows. Naive-local
    keeps CKAN's datastore ``timestamp`` parser happy and matches the Hebrew
    UI context users read these in (same tz rationale as ``_timestamp``)."""
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Jerusalem")
    except Exception:
        from datetime import timedelta
        tz = timezone(timedelta(hours=3))
    return datetime.now(tz).strftime("%Y-%m-%dT%H:%M:%S")


def stamp_first_seen(
    fields: list[dict], rows: list[dict], *, when: str | None = None
) -> tuple[list[dict], list[dict]]:
    """Add the ``first_seen`` timestamp column to an append push.

    Ensures ``fields`` contains the synthetic column exactly once (so the
    create-schema and every subsequent insert agree on the table shape), and
    stamps every row that doesn't already carry a value with ``when`` (default:
    now, Israel-local ISO). Rows that already have ``first_seen`` are left
    untouched so a row's original first-seen time can never be overwritten.
    Returns the (possibly extended) field list and the same rows (mutated in
    place)."""
    ts = when or _iso_timestamp()
    if not any(f.get("id") == APPEND_FIRST_SEEN_FIELD for f in fields):
        fields = list(fields) + [{"id": APPEND_FIRST_SEEN_FIELD, "type": "timestamp"}]
    for r in rows:
        if APPEND_FIRST_SEEN_FIELD not in r:
            r[APPEND_FIRST_SEEN_FIELD] = ts
    return fields, rows


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
    *,
    use_r2: bool = False,
    tracked_dataset_id: str | None = None,
) -> tuple[str, dict, list[str]]:
    """
    Upload a full version snapshot.

    - ODATA backend (default): tabular data (CSV) is pushed to the CKAN
      Datastore (queryable via API); other formats are uploaded as files.
    - R2 backend (``use_r2=True``): every resource — tabular or not — is
      stored verbatim as a downloadable object in the S3-compatible store
      (no datastore; the dataset page serves CSVs/files for download). The
      mapping value becomes ``r2:<key>`` (see ``storage_client.mark``).
      ``tracked_dataset_id`` (the OVER dataset UUID) is required in this mode
      to build the object key.

    Resource names include date for easy identification.
    Returns (metadata_resource_id, resource_mappings, errors). `errors` is
    a list of per-resource upload failure messages — empty when every
    resource was uploaded cleanly. The caller decides whether to persist
    a version when only some/none of the uploads succeeded.
    """
    ts = _timestamp()

    if use_r2 and not tracked_dataset_id:
        raise ValueError("create_version_snapshot: use_r2 requires tracked_dataset_id")

    # No separate metadata JSON upload — metadata is stored in version_index JSONB
    metadata_resource_id = None

    resource_mappings: dict[str, Any] = {}
    errors: list[str] = []

    # Carry forward unchanged resources from old mappings. This copies the
    # previous version's mapping VALUES verbatim — backend-agnostic, so an
    # unchanged resource keeps pointing at the same r2:<key> (R2) or ODATA
    # resource_id (ODATA) as the prior version, with zero new uploads.
    if old_mappings:
        for rid, prev_rid in old_mappings.items():
            if rid.startswith("_"):
                continue
            if rid not in {cr["resource"]["id"] for cr in changed_resources}:
                resource_mappings[rid] = prev_rid

    # Upload new/changed resources. Each cr now carries ``file_path``
    # (a temp file written by ckan_client.download_resource); we stream
    # straight from disk so peak memory stays ~chunk-size regardless
    # of resource size. The temp files are unlinked in a finally block
    # at the end so a partial failure can't leak files onto the dyno's
    # ephemeral disk.
    import os as _os

    for cr in changed_resources:
        resource = cr["resource"]
        rid = resource["id"]
        file_path: str | None = cr.get("file_path")
        # Back-compat: a few callers still hand us a small in-memory
        # blob (e.g. lightweight CSV exports built locally). If neither
        # is present, skip with an error rather than crashing.
        content: bytes | None = cr.get("content")
        fmt = (resource.get("format", "") or "").lower().strip()

        try:
            if use_r2:
                # R2 backend: store the resource bytes verbatim as a
                # downloadable object. No datastore — tabular and non-tabular
                # alike become file objects served from R2's public domain.
                from app.services import storage_client as storage
                from app.services.storage_client import storage_client

                res_name = resource.get("name", rid)
                ext = fmt or "dat"
                key = storage.build_key(
                    tracked_dataset_id, version_number, f"{res_name}.{ext}",
                )
                content_type = (
                    "text/csv; charset=utf-8" if fmt in TABULAR_FORMATS
                    else None
                )
                if file_path:
                    await storage_client.upload_object(
                        key, file_path=file_path, content_type=content_type,
                    )
                elif content is not None:
                    await storage_client.upload_object(
                        key, file_content=content, content_type=content_type,
                    )
                else:
                    raise RuntimeError("no content or file_path provided")
                resource_mappings[rid] = storage.mark(key)
                logger.info(
                    "Snapshot(R2): stored %s (%s) → %s", res_name, fmt or "?", key,
                )
                continue

            if fmt in TABULAR_FORMATS:
                # parse_csv reads the whole CSV into memory regardless,
                # so we either feed it the bytes we already have or load
                # the temp file once. The size cap upstream keeps this
                # from OOMing.
                if content is None and file_path:
                    with open(file_path, "rb") as fh:
                        content = fh.read()
                if content is None:
                    raise RuntimeError("no content or file_path provided for tabular resource")
                result = await _push_to_datastore(
                    odata_dataset_id, version_number, resource, content, ts,
                )
            else:
                if file_path:
                    result = await odata_client.upload_resource(
                        dataset_id=odata_dataset_id,
                        file_path=file_path,
                        filename=f"{ts}_v{version_number}_{resource.get('name', rid)}",
                        name=f"{ts} v{version_number} - {resource.get('name', rid)}",
                        description=f"Resource snapshot (version {version_number}, {ts})",
                        resource_format=fmt.upper(),
                    )
                elif content is not None:
                    result = await odata_client.upload_resource(
                        dataset_id=odata_dataset_id,
                        file_content=content,
                        filename=f"{ts}_v{version_number}_{resource.get('name', rid)}",
                        name=f"{ts} v{version_number} - {resource.get('name', rid)}",
                        description=f"Resource snapshot (version {version_number}, {ts})",
                        resource_format=fmt.upper(),
                    )
                else:
                    raise RuntimeError("no content or file_path provided")

            resource_mappings[rid] = result["id"]
        except Exception as e:
            # str(e) is empty for many httpx low-level errors (WriteError,
            # RemoteProtocolError, broken-pipe scenarios). Surface type
            # and repr so the user actually sees what failed.
            detail = str(e) or repr(e)
            etype = type(e).__name__
            resp_info = ""
            if hasattr(e, "response") and getattr(e, "response", None) is not None:
                try:
                    body = e.response.text[:500]
                    resp_info = f" [HTTP {e.response.status_code}: {body}]"
                except Exception:
                    pass
            logger.error("Failed to upload resource %s to odata: %s: %s%s", rid, etype, detail, resp_info)
            errors.append(f"upload {resource.get('name', rid[:8])} ({fmt or '?'}): {etype}: {detail}{resp_info}")
        finally:
            if file_path:
                try:
                    _os.unlink(file_path)
                except OSError:
                    pass

    # Store hashes and resource IDs in mappings for next comparison
    resource_mappings["_hashes"] = hash_map
    resource_mappings["_resource_ids"] = [r["id"] for r in metadata.get("resources", [])]

    # R2 backend: stamp a friendly per-file label (`_names`) and the archive
    # date (`_filedates`) so the dataset page shows "<title> — DD.MM.YYYY"
    # instead of a raw source UUID, and the date filter attributes each file to
    # this version (consistent with the one-off rebuild). Carry forward labels
    # for unchanged resources; (re)stamp changed ones with today's date.
    if use_r2:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            from zoneinfo import ZoneInfo
            today = datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%Y-%m-%d")
        except Exception:
            pass
        d, mo, y = today.split("-")[2], today.split("-")[1], today.split("-")[0]
        today_disp = f"{d}.{mo}.{y}"
        names = dict((old_mappings or {}).get("_names") or {}) if old_mappings else {}
        filedates = dict((old_mappings or {}).get("_filedates") or {}) if old_mappings else {}
        for cr in changed_resources:
            rid = cr["resource"]["id"]
            if rid in resource_mappings:
                base = (cr["resource"].get("name") or rid).strip()
                names[rid] = f"{base} — {today_disp}"
                filedates[rid] = today
        # keep only labels for resources still mapped this version
        mapped = {k for k in resource_mappings if not k.startswith("_")}
        names = {k: v for k, v in names.items() if k in mapped}
        filedates = {k: v for k, v in filedates.items() if k in mapped}
        if names:
            resource_mappings["_names"] = names
        if filedates:
            resource_mappings["_filedates"] = filedates

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
    add_first_seen: bool = False,
    first_seen_value: str | None = None,
) -> tuple[str | None, int]:
    """Append-only push: insert `new_rows` into a shared odata resource.

    - If `appendonly_resource_id` is None, the shared resource is created
      via the standard push_csv_to_datastore path (defines schema +
      uploads the CSV file). Caller must persist the returned id back to
      the dataset row.
    - Otherwise inserts new rows in batches into the existing resource via
      datastore_upsert(method="insert", force=True).

    When `add_first_seen` is set, every pushed row is stamped with a
    ``first_seen`` timestamp column (see stamp_first_seen). Stamping happens
    here — after the caller has already deduplicated against seen-keys — so
    the synthetic timestamp can't affect row identity. `first_seen_value`
    pins the timestamp (used by the streaming path so all rows in one poll
    share a value); defaults to now.

    Returns (resource_id, rows_inserted). resource_id may be None only when
    new_rows is empty AND there is no shared resource yet (caller skips).
    """
    from app.services.csv_parser import batch_records

    if not new_rows and not appendonly_resource_id:
        return None, 0

    if add_first_seen and new_rows:
        fields, new_rows = stamp_first_seen(fields, new_rows, when=first_seen_value)

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
