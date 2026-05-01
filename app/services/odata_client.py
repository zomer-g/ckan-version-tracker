import io
import json
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=60.0, pool=10.0)
DATASTORE_TIMEOUT = httpx.Timeout(connect=15.0, read=120.0, write=120.0, pool=10.0)
# Large file uploads (ZIP parts up to 80MB) need much more time for slow links.
UPLOAD_TIMEOUT = httpx.Timeout(connect=15.0, read=600.0, write=600.0, pool=10.0)


def _remap_keys(record: dict, key_map: dict[str, str]) -> dict:
    """Rename keys in a record dict according to ``key_map``.

    Used when CKAN's datastore_create normalised our column names
    (typically Hebrew → ASCII transliteration on odata.org.il) so we
    can keep upserting subsequent batches against the schema CKAN
    actually built, rather than the schema we asked for.
    """
    return {key_map.get(k, k): v for k, v in record.items()}


class ODataClient:
    """Async client for reading/writing to odata.org.il CKAN API."""

    def __init__(self, base_url: str | None = None, api_key: str | None = None):
        self.base_url = (base_url or settings.odata_url).rstrip("/")
        self.api_url = f"{self.base_url}/api/3/action"
        self.api_key = (api_key or settings.odata_api_key).strip()

    def _headers(self) -> dict:
        return {"Authorization": self.api_key} if self.api_key else {}

    async def _post(self, action: str, data: dict | None = None, timeout: httpx.Timeout | None = None) -> Any:
        async with httpx.AsyncClient(timeout=timeout or TIMEOUT) as client:
            url = f"{self.api_url}/{action}"
            resp = await client.post(url, json=data or {}, headers=self._headers())
            if resp.is_error:
                # Surface CKAN's structured error body — the JSON shape is
                # {"error": {"...": "..."}} and tells us things like the
                # actual field-type mismatch behind a 409 Conflict. Without
                # this the caller only ever sees "Client error '409 …'" and
                # can't tell which column or value tripped the validator.
                detail = ""
                try:
                    body = resp.json()
                    err = body.get("error") if isinstance(body, dict) else None
                    if err:
                        detail = json.dumps(err, ensure_ascii=False)[:600]
                except Exception:
                    detail = (resp.text or "")[:300]
                raise RuntimeError(
                    f"odata {action} {resp.status_code}: {detail or resp.reason_phrase}"
                )
            result = resp.json()
            if not result.get("success"):
                raise RuntimeError(f"odata API error: {result.get('error', 'unknown')}")
            return result["result"]

    async def _get(self, action: str, params: dict | None = None) -> Any:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            url = f"{self.api_url}/{action}"
            resp = await client.get(url, params=params, headers=self._headers())
            resp.raise_for_status()
            result = resp.json()
            if not result.get("success"):
                raise RuntimeError(f"odata API error: {result.get('error', 'unknown')}")
            return result["result"]

    # ── Dataset management ───────────────────────────────────────────────

    NOTES_CKAN = (
        "שיקוף היסטוריית גרסאות של מאגר מידע מ-data.gov.il, מנוהל אוטומטית על ידי "
        "[גרסאות לעם](https://over.org.il) — "
        "[קוד מקור](https://github.com/zomer-g/ckan-version-tracker)\n\n"
        "Dataset version history mirror from data.gov.il, auto-managed by "
        "[Versions for the People](https://over.org.il)"
    )

    NOTES_SCRAPER = (
        "גירוד אוטומטי של עמוד מאתר gov.il, מנוהל על ידי "
        "[גרסאות לעם](https://over.org.il) — "
        "[קוד מקור](https://github.com/zomer-g/ckan-version-tracker)\n\n"
        "Automated scraping of a gov.il page, managed by "
        "[Versions for the People](https://over.org.il)"
    )

    @staticmethod
    def build_notes(source_type: str, source_url: str | None, tracker_url: str) -> str:
        """Build the ODATA dataset `notes` (description) field with explicit
        links to:
          - the source page on data.gov.il / gov.il
          - the version-history view on over.org.il for THIS tracked dataset

        Both links are Markdown so the CKAN UI renders them as clickable.
        Falls back to a static notes template if source_url is missing.
        """
        if source_type == "scraper":
            source_label_he = "עמוד המקור באתר gov.il"
            source_label_en = "Source page on gov.il"
            intro_he = "גירוד אוטומטי של עמוד מאתר gov.il"
            intro_en = "Automated scraping of a gov.il page"
        else:
            source_label_he = "עמוד המקור ב-data.gov.il"
            source_label_en = "Source page on data.gov.il"
            intro_he = "שיקוף היסטוריית גרסאות של מאגר מידע מ-data.gov.il"
            intro_en = "Dataset version history mirror from data.gov.il"

        source_line_he = (
            f"[{source_label_he}]({source_url})" if source_url
            else source_label_he
        )
        source_line_en = (
            f"[{source_label_en}]({source_url})" if source_url
            else source_label_en
        )
        tracker_line_he = f"[היסטוריית גרסאות ב-over.org.il]({tracker_url})"
        tracker_line_en = f"[Version history on over.org.il]({tracker_url})"

        return (
            f"{intro_he}, מנוהל על ידי "
            f"[גרסאות לעם](https://over.org.il) — "
            f"[קוד מקור](https://github.com/zomer-g/ckan-version-tracker)\n\n"
            f"🔗 {source_line_he}\n\n"
            f"📜 {tracker_line_he}\n\n"
            f"---\n\n"
            f"{intro_en}, managed by "
            f"[Versions for the People](https://over.org.il)\n\n"
            f"🔗 {source_line_en}\n\n"
            f"📜 {tracker_line_en}"
        )

    async def create_dataset(self, name: str, title: str, owner_org: str | None = None,
                             extras: list | None = None, notes: str | None = None) -> dict:
        """Create a mirror dataset on odata.org.il."""
        payload: dict[str, Any] = {
            "name": name,
            "title": title,
            "notes": notes or self.NOTES_CKAN,
        }
        if owner_org:
            payload["owner_org"] = owner_org
        if extras:
            payload["extras"] = extras
        return await self._post("package_create", payload)

    async def package_patch(self, dataset_id: str, **kwargs) -> dict:
        """Patch (partial update) an existing dataset on odata.org.il."""
        payload = {"id": dataset_id, **kwargs}
        return await self._post("package_patch", payload)

    async def package_show(self, id_or_name: str) -> dict:
        return await self._get("package_show", {"id": id_or_name})

    async def package_delete(self, dataset_id: str, purge: bool = True) -> dict:
        """Delete a dataset on odata.org.il.

        CKAN's `package_delete` performs a soft delete by default (flips
        state='deleted' but rows stay in the DB). `purge=True` issues a
        follow-up `dataset_purge` for a hard delete — matches user expectation
        that "delete" actually removes the item from ODATA.
        """
        result = await self._post("package_delete", {"id": dataset_id})
        if purge:
            try:
                await self._post("dataset_purge", {"id": dataset_id})
            except Exception as e:
                logger.warning(
                    "package_delete succeeded for %s but dataset_purge failed: %s"
                    " — the package is soft-deleted and will hide from the UI",
                    dataset_id, e,
                )
        return result

    async def resource_delete(self, resource_id: str) -> dict:
        """Delete a resource from its parent dataset. Also wipes any datastore
        table that was attached to it (CKAN handles that cascade internally
        when force=True)."""
        return await self._post("resource_delete", {
            "id": resource_id,
            "force": True,
        })

    # ── Resource management (file upload) ────────────────────────────────

    async def create_resource(self, dataset_id: str, name: str, description: str = "",
                              resource_format: str = "", url: str = "") -> dict:
        """Create a CKAN resource without uploading a file. Used as a datastore
        target when the plain CSV would exceed CKAN's upload size limit.

        `url` can point to an external download (e.g. CKAN's datastore dump
        endpoint) so the CKAN UI Download button actually produces a CSV
        instead of a 404 / empty response.
        """
        return await self._post("resource_create", {
            "package_id": dataset_id,
            "name": name,
            "description": description,
            "format": resource_format,
            "url": url,
        })

    async def update_resource_url(self, resource_id: str, new_url: str) -> dict:
        """Patch just a resource's URL field (used after resource_create when
        we want the download link to point at a dynamic endpoint that needs
        the resource_id — a chicken/egg that can't be resolved in one call).
        """
        return await self._post("resource_patch", {
            "id": resource_id,
            "url": new_url,
        })

    async def upload_resource(
        self,
        dataset_id: str,
        file_content: bytes | None = None,
        filename: str = "",
        name: str = "",
        description: str = "",
        resource_format: str = "",
        file_path: str | None = None,
    ) -> dict:
        """Upload a file as a new resource to a dataset.

        Either pass `file_content` (in-memory bytes) or `file_path` (on-disk
        path — preferred for large files; httpx streams from disk instead of
        holding the full payload in memory).
        """
        if file_path is None and file_content is None:
            raise ValueError("upload_resource: need file_content or file_path")

        async with httpx.AsyncClient(timeout=UPLOAD_TIMEOUT) as client:
            url = f"{self.api_url}/resource_create"
            data = {
                "package_id": dataset_id,
                "name": name,
                "description": description,
                "format": resource_format,
            }
            if file_path is not None:
                # Stream from disk — constant memory even for 200MB+ CSVs
                with open(file_path, "rb") as fh:
                    files = {"upload": (filename, fh, "application/octet-stream")}
                    resp = await client.post(
                        url, data=data, files=files, headers=self._headers(),
                    )
            else:
                files = {"upload": (filename, io.BytesIO(file_content), "application/octet-stream")}
                resp = await client.post(
                    url, data=data, files=files, headers=self._headers(),
                )
            resp.raise_for_status()
            result = resp.json()
            if not result.get("success"):
                raise RuntimeError(f"odata upload error: {result.get('error', 'unknown')}")
            return result["result"]

    async def upload_metadata_snapshot(
        self, dataset_id: str, version_number: int, metadata: dict, timestamp: str = ""
    ) -> dict:
        """Upload metadata snapshot as a JSON resource."""
        ts = timestamp or "unknown"
        content = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
        return await self.upload_resource(
            dataset_id=dataset_id,
            file_content=content,
            filename=f"{ts}_v{version_number}_metadata.json",
            name=f"{ts} v{version_number} - Metadata Snapshot",
            description=f"Metadata snapshot for version {version_number} ({ts})",
            resource_format="JSON",
        )

    # ── Datastore API ────────────────────────────────────────────────────

    async def datastore_info(self, resource_id: str) -> dict:
        """Fetch the actual datastore schema CKAN built for a resource.

        Returns the full ``datastore_search`` result-shape (``fields``,
        ``total``, etc.). Used right after ``datastore_create`` so we can
        compare what we asked for against what CKAN actually committed.
        """
        return await self._get("datastore_search", {"resource_id": resource_id, "limit": 0})

    async def datastore_create(
        self,
        resource_id: str,
        fields: list[dict],
        records: list[dict],
        primary_key: str | list[str] | None = None,
        calculate_record_count: bool = False,
    ) -> dict:
        """
        Create a DataStore table and insert initial records.
        After this, data is queryable via datastore_search.

        `calculate_record_count=True` updates the stored row count so the
        CKAN UI footer shows the right total. Only useful when this is the
        only call (i.e. small datasets that fit in one batch) — otherwise
        set it on the final datastore_upsert instead.
        """
        payload: dict[str, Any] = {
            "resource_id": resource_id,
            "fields": fields,
            "records": records,
            "force": True,
        }
        if primary_key:
            payload["primary_key"] = primary_key
        if calculate_record_count:
            payload["calculate_record_count"] = True
        return await self._post("datastore_create", payload, timeout=DATASTORE_TIMEOUT)

    async def datastore_upsert(
        self,
        resource_id: str,
        records: list[dict],
        method: str = "insert",
        force: bool = True,
        calculate_record_count: bool = False,
    ) -> dict:
        """
        Insert/upsert additional records into an existing DataStore table.
        Methods: 'insert' (fast, no key check), 'upsert', 'update'.

        `force=True` by default — CKAN's _check_read_only raises
        ValidationError if the resource's url_type isn't in the writable
        list, and since our resources get their `url` patched to the
        datastore dump endpoint (which changes url_type), every upsert
        after the first would otherwise silently fail, leaving us with
        only the first batch in the table.

        `calculate_record_count=True` should be set on the LAST batch of
        a multi-batch push so the CKAN UI footer reflects the final row
        count. Doing it on every batch is wasteful (scans the table).
        """
        payload: dict[str, Any] = {
            "resource_id": resource_id,
            "method": method,
            "records": records,
            "force": force,
        }
        if calculate_record_count:
            payload["calculate_record_count"] = True
        return await self._post("datastore_upsert", payload, timeout=DATASTORE_TIMEOUT)

    async def _push_batch_with_retry(
        self,
        resource_id: str,
        fields: list[dict],
        records_batch: list[dict],
        *,
        create: bool,
        batch_num: int,
        is_last: bool,
        max_attempts: int = 3,
    ) -> None:
        """Push one batch to the datastore, retrying up to `max_attempts` times
        with linear backoff on transient failures.

        - `create=True` uses datastore_create (defines schema on first batch)
        - `create=False` uses datastore_upsert with `method="insert"`, always
          with `force=True` so CKAN's read-only check (url_type check) can't
          silently drop the batch.
        - `is_last=True` adds `calculate_record_count=True` so the CKAN UI
          footer matches the real row count after the full push.

        Raises RuntimeError after all attempts are exhausted. The caller
        typically logs and stops the stream, keeping the source CSV on disk
        for manual retry.
        """
        import asyncio

        last_err: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                if create:
                    await self.datastore_create(
                        resource_id=resource_id,
                        fields=fields,
                        records=records_batch,
                        calculate_record_count=is_last,
                    )
                else:
                    await self.datastore_upsert(
                        resource_id=resource_id,
                        records=records_batch,
                        method="insert",
                        force=True,
                        calculate_record_count=is_last,
                    )
                return
            except Exception as e:
                last_err = e
                if attempt < max_attempts:
                    wait = 5 * attempt  # 5s, 10s
                    logger.warning(
                        "Datastore batch %d attempt %d/%d failed (%s) — retrying in %ds",
                        batch_num, attempt, max_attempts, e, wait,
                    )
                    await asyncio.sleep(wait)
        raise RuntimeError(
            f"Datastore batch {batch_num} failed after {max_attempts} attempts: {last_err}"
        )

    async def push_csv_to_datastore(
        self,
        dataset_id: str,
        version_number: int,
        resource_name: str,
        fields: list[dict],
        records: list[dict],
        resource_format: str = "CSV",
        timestamp: str = "",
    ) -> dict:
        """
        Upload a CSV file and push parsed data into the datastore.
        The uploaded file makes the CKAN download button work; the datastore
        entries enable the filter/search/preview UI.
        Returns the created resource dict (with resource_id for querying).
        """
        import csv as _csv
        import io as _io
        from app.services.csv_parser import batch_records

        safe_name = resource_name.replace("/", "_").replace("\\", "_")
        ts = timestamp or "unknown"

        # Step 1: Generate CSV bytes from records
        buf = _io.StringIO()
        fieldnames = [f["id"] for f in fields]
        writer = _csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            writer.writerow({k: ("" if v is None else v) for k, v in row.items()})
        csv_bytes = buf.getvalue().encode("utf-8-sig")  # BOM for Excel Hebrew

        # Step 2: Upload as a real file resource (gives download URL)
        filename = f"{ts}_v{version_number}_{safe_name}.csv"
        resource = await self.upload_resource(
            dataset_id=dataset_id,
            file_content=csv_bytes,
            filename=filename,
            name=f"{ts} v{version_number} - {safe_name}",
            description=f"Version {version_number} ({ts}): {resource_name} ({len(records)} rows)",
            resource_format=resource_format,
        )
        resource_id = resource["id"]

        # Step 3: Push data into datastore in batches (for filter/search UI).
        #
        # odata.org.il runs CKAN's xloader/datapusher automatically on
        # every uploaded CSV — by the time we get here the datastore
        # table almost always already exists, with column names that
        # CKAN has normalised (Hebrew → ASCII transliteration:
        # `חשבון` → `khshbvn`, etc.). If we blindly call
        # datastore_create with our original Hebrew field list it
        # races/conflicts with the autopusher and returns 409
        # "Supplied field 'חשבון' not present or in wrong order".
        #
        # So we probe first: ask CKAN what schema (if any) already
        # exists. If it's there, build a position-based remap from our
        # original column names to the names CKAN settled on, skip the
        # create entirely, and upsert every batch with remapped keys.
        # Only when no datastore exists yet do we run datastore_create
        # ourselves (and even then we re-probe afterwards in case
        # datastore_create itself normalised our names — we've seen
        # that on ckanext-datastore_normalize too).
        import asyncio
        batches = batch_records(records)
        total_batches = len(batches)

        # Wait briefly for the autopusher to commit a schema + rows, then
        # decide what we still need to push ourselves. Returns (fields,
        # total_rows) or (None, 0) if nothing landed in time.
        async def _probe_schema() -> tuple[list[str] | None, int]:
            for attempt in range(6):  # ~12s total
                try:
                    info = await self.datastore_info(resource_id)
                except Exception:
                    info = None
                if info:
                    fs = [f["id"] for f in info.get("fields", []) if f["id"] != "_id"]
                    total = info.get("total", 0) or 0
                    if fs:
                        return fs, int(total)
                await asyncio.sleep(2)
            return None, 0

        expected_fields = [f["id"] for f in fields]
        actual_fields, autoloaded_rows = await _probe_schema()

        # If the autopusher already loaded EVERYTHING (or close to it),
        # an extra upsert would only duplicate rows — datastore_upsert
        # method=insert just appends, it doesn't dedupe by content. So
        # bail out: the file is uploaded, the datastore has the right
        # rows, the version-history record can point at this resource_id
        # and we're done.
        if actual_fields and autoloaded_rows >= len(records):
            logger.info(
                "Autopusher fully populated %s (%d rows ≥ %d uploaded); "
                "skipping our datastore push to avoid duplicates",
                resource_id, autoloaded_rows, len(records),
            )
            return resource

        if actual_fields is None:
            # No datastore yet — create it ourselves.
            logger.info("No autopusher schema for %s; running datastore_create", resource_id)
            await self._push_batch_with_retry(
                resource_id=resource_id,
                fields=fields,
                records_batch=batches[0],
                create=True,
                batch_num=1,
                is_last=(total_batches == 1),
            )
            start_batch = 2
            # Re-probe in case datastore_create renamed columns.
            try:
                info = await self.datastore_info(resource_id)
                actual_fields = [f["id"] for f in info.get("fields", []) if f["id"] != "_id"]
            except Exception:
                actual_fields = expected_fields
        else:
            logger.info(
                "Autopusher provided partial schema for %s "
                "(%d rows already in, %d to push); upserting all batches",
                resource_id, autoloaded_rows, len(records),
            )
            start_batch = 1

        # Build a position-based remap if column names differ.
        key_map: dict[str, str] | None = None
        if (
            actual_fields
            and len(actual_fields) == len(expected_fields)
            and actual_fields != expected_fields
        ):
            key_map = dict(zip(expected_fields, actual_fields))
            logger.info("Datastore renamed columns; remap = %s", key_map)
        elif actual_fields and len(actual_fields) != len(expected_fields):
            logger.warning(
                "Schema field count mismatch (we have %d, datastore has %d) "
                "for %s — upsert may drop columns",
                len(expected_fields), len(actual_fields), resource_id,
            )

        # Upsert remaining batches (or all of them if autopusher loaded
        # only the schema but no rows).
        for i in range(start_batch, total_batches + 1):
            batch = batches[i - 1]
            logger.info(
                "Pushing batch %d/%d (%d records) to %s",
                i, total_batches, len(batch), resource_id,
            )
            batch_to_push = (
                [_remap_keys(r, key_map) for r in batch] if key_map else batch
            )
            await self._push_batch_with_retry(
                resource_id=resource_id,
                fields=fields,
                records_batch=batch_to_push,
                create=False,  # always upsert when the schema already exists
                batch_num=i,
                is_last=(i == total_batches),
            )
            if i < total_batches:
                await asyncio.sleep(1)  # brief settle pause between batches

        logger.info(
            "Datastore resource %s created with %d records",
            resource_id, len(records),
        )
        return resource

    async def push_records_to_datastore_from_file(
        self,
        resource_id: str,
        fields: list[dict],
        csv_path: str,
        delete_when_done: bool = True,
        batch_size: int = 2500,
    ) -> None:
        """Stream records from a CSV on disk into the datastore in batches.

        Used for very large datasets where loading the whole record set into
        memory would risk OOM on small dynos. Peak memory is ~batch_size rows
        (few MB) regardless of the CSV's total size.

        Resilience features:
        - Each batch is retried up to 3 times with exponential backoff.
        - On total failure of a batch, the CSV file is KEPT on disk so it
          can be manually or programmatically retried from batch 1. The
          caller can see the path in the logs and re-invoke this method.
        - delete_when_done=True only deletes on success; failures always
          preserve the file.
        """
        import asyncio
        import csv as _csv
        import os

        total_pushed = 0
        batch_num = 0
        had_failure = False

        # Two-slot buffer: we hold one "pending" batch and only flush it once
        # we've read the next row (or hit EOF). That way we always know
        # whether the batch we're flushing is the LAST one — which lets us
        # set calculate_record_count=True only on that call, matching CKAN's
        # recommended best practice.
        pending: list[dict] | None = None

        async def _flush(records_batch: list[dict], create: bool, is_last: bool):
            nonlocal total_pushed, batch_num
            if not records_batch:
                return
            batch_num += 1
            await self._push_batch_with_retry(
                resource_id=resource_id,
                fields=fields,
                records_batch=records_batch,
                create=create,
                batch_num=batch_num,
                is_last=is_last,
            )
            total_pushed += len(records_batch)
            logger.info(
                "Datastore batch %d (%d rows, cumulative %d)%s → %s",
                batch_num, len(records_batch), total_pushed,
                " [final, record count refreshed]" if is_last else "",
                resource_id,
            )
            # Ask the GC to reclaim batch + httpx buffers before we build the
            # next one. On a 512MB Render dyno, not doing this can leave tens
            # of MB of unreferenced-but-uncollected objects lingering per
            # batch, and by batch 8+ we can bump into the OOM limit while the
            # web server is still handling requests in the same process.
            import gc as _gc
            _gc.collect()
            # Brief pause between batches — ODATA/CKAN occasionally needs
            # a moment between sequential datastore writes.
            if not is_last:
                await asyncio.sleep(1)

        try:
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
                reader = _csv.DictReader(fh)

                current: list[dict] = []
                for row in reader:
                    current.append(dict(row))
                    if len(current) >= batch_size:
                        # If we have a pending batch, flush it now (not last —
                        # we just read another batch-worth, so more follows).
                        if pending is not None:
                            await _flush(pending, create=(batch_num == 0), is_last=False)
                        pending = current
                        current = []

                # Handle tail: flush pending (if any) then current.
                # Whichever is last gets is_last=True.
                if current:
                    if pending is not None:
                        await _flush(pending, create=(batch_num == 0), is_last=False)
                    await _flush(current, create=(batch_num == 0), is_last=True)
                elif pending is not None:
                    await _flush(pending, create=(batch_num == 0), is_last=True)

            logger.info(
                "Datastore stream complete: resource=%s, total_rows=%d, csv=%s",
                resource_id, total_pushed, csv_path,
            )
        except Exception as e:
            had_failure = True
            logger.exception(
                "Datastore stream FAILED after batch %d (pushed %d rows so far) "
                "for resource %s — CSV kept at %s for manual retry: %s",
                batch_num, total_pushed, resource_id, csv_path, e,
            )
        finally:
            # Only delete on clean success; keep the file on failure so a
            # later retry (manual or scheduled) can recover the missing rows.
            if delete_when_done and not had_failure:
                try:
                    os.remove(csv_path)
                    logger.info("Deleted temp CSV %s", csv_path)
                except OSError as e:
                    logger.warning("Failed to delete temp CSV %s: %s", csv_path, e)

    async def push_records_to_datastore(
        self,
        resource_id: str,
        fields: list[dict],
        records: list[dict],
    ) -> None:
        """Push records to an existing resource's datastore (no file upload).
        Used when the file was already uploaded separately and we just need
        to populate the queryable table. Safe to call in a background task
        — the caller doesn't need to await the result if they already have
        the resource_id.

        Shares the _push_batch_with_retry helper so force=True and retries
        apply consistently across all three datastore-push code paths.
        """
        import asyncio
        from app.services.csv_parser import batch_records

        batches = batch_records(records)
        if not batches:
            logger.info("Datastore push for %s: no records", resource_id)
            return

        total_batches = len(batches)
        logger.info(
            "Background datastore push: %d records → %s in %d batch(es)",
            len(records), resource_id, total_batches,
        )
        try:
            for i, batch in enumerate(batches, start=1):
                logger.info(
                    "Datastore batch %d/%d (%d records) → %s",
                    i, total_batches, len(batch), resource_id,
                )
                await self._push_batch_with_retry(
                    resource_id=resource_id,
                    fields=fields,
                    records_batch=batch,
                    create=(i == 1),
                    batch_num=i,
                    is_last=(i == total_batches),
                )
                if i < total_batches:
                    await asyncio.sleep(1)
            logger.info(
                "Background datastore push complete: %s (%d records)",
                resource_id, len(records),
            )
        except Exception as e:
            logger.exception(
                "Background datastore push FAILED for %s: %s",
                resource_id, e,
            )


odata_client = ODataClient()
