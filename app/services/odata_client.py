import io
import json
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=60.0, pool=10.0)
DATASTORE_TIMEOUT = httpx.Timeout(connect=15.0, read=120.0, write=120.0, pool=10.0)


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
            resp.raise_for_status()
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

    # ── Resource management (file upload) ────────────────────────────────

    async def create_resource(self, dataset_id: str, name: str, description: str = "", resource_format: str = "") -> dict:
        """Create an empty CKAN resource (no file upload). Used as datastore target."""
        return await self._post("resource_create", {
            "package_id": dataset_id,
            "name": name,
            "description": description,
            "format": resource_format,
            "url": "",  # Empty URL — data lives in datastore
        })

    async def upload_resource(
        self,
        dataset_id: str,
        file_content: bytes,
        filename: str,
        name: str,
        description: str = "",
        resource_format: str = "",
    ) -> dict:
        """Upload a file as a new resource to a dataset."""
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            url = f"{self.api_url}/resource_create"
            files = {"upload": (filename, io.BytesIO(file_content), "application/octet-stream")}
            data = {
                "package_id": dataset_id,
                "name": name,
                "description": description,
                "format": resource_format,
            }
            resp = await client.post(url, data=data, files=files, headers=self._headers())
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

    async def datastore_create(
        self,
        resource_id: str,
        fields: list[dict],
        records: list[dict],
        primary_key: str | list[str] | None = None,
    ) -> dict:
        """
        Create a DataStore table and insert initial records.
        After this, data is queryable via datastore_search.
        """
        payload: dict[str, Any] = {
            "resource_id": resource_id,
            "fields": fields,
            "records": records,
            "force": True,
        }
        if primary_key:
            payload["primary_key"] = primary_key
        return await self._post("datastore_create", payload, timeout=DATASTORE_TIMEOUT)

    async def datastore_upsert(
        self,
        resource_id: str,
        records: list[dict],
        method: str = "insert",
    ) -> dict:
        """
        Insert/upsert additional records into an existing DataStore table.
        Methods: 'insert' (fast, no key check), 'upsert', 'update'.
        """
        return await self._post("datastore_upsert", {
            "resource_id": resource_id,
            "method": method,
            "records": records,
        }, timeout=DATASTORE_TIMEOUT)

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

        # Step 3: Push data into datastore in batches (for filter/search UI)
        batches = batch_records(records)

        if batches:
            # First batch: use datastore_create to define schema + insert
            logger.info(
                "Pushing %d records to datastore %s (batch 1/%d)",
                len(batches[0]), resource_id, len(batches),
            )
            await self.datastore_create(
                resource_id=resource_id,
                fields=fields,
                records=batches[0],
            )

            # Remaining batches: use datastore_upsert (insert mode)
            for i, batch in enumerate(batches[1:], start=2):
                logger.info(
                    "Pushing batch %d/%d (%d records) to %s",
                    i, len(batches), len(batch), resource_id,
                )
                await self.datastore_upsert(
                    resource_id=resource_id,
                    records=batch,
                    method="insert",
                )

        logger.info(
            "Datastore resource %s created with %d records",
            resource_id, len(records),
        )
        return resource


odata_client = ODataClient()
