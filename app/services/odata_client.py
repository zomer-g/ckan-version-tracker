import io
import json
import logging
from typing import Any

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=60.0, pool=10.0)


class ODataClient:
    """Async client for reading/writing to odata.org.il CKAN API."""

    def __init__(self, base_url: str | None = None, api_key: str | None = None):
        self.base_url = (base_url or settings.odata_url).rstrip("/")
        self.api_url = f"{self.base_url}/api/3/action"
        self.api_key = api_key or settings.odata_api_key

    def _headers(self) -> dict:
        return {"Authorization": self.api_key} if self.api_key else {}

    async def _post(self, action: str, data: dict | None = None) -> Any:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
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

    async def create_dataset(self, name: str, title: str, owner_org: str | None = None, extras: list | None = None) -> dict:
        """Create a mirror dataset on odata.org.il."""
        payload: dict[str, Any] = {
            "name": name,
            "title": title,
            "notes": f"Version history mirror - auto-managed by CKAN Version Tracker",
        }
        if owner_org:
            payload["owner_org"] = owner_org
        if extras:
            payload["extras"] = extras
        return await self._post("package_create", payload)

    async def package_show(self, id_or_name: str) -> dict:
        return await self._get("package_show", {"id": id_or_name})

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
        self, dataset_id: str, version_number: int, metadata: dict
    ) -> dict:
        """Upload metadata snapshot as a JSON resource."""
        content = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
        return await self.upload_resource(
            dataset_id=dataset_id,
            file_content=content,
            filename=f"v{version_number}_metadata.json",
            name=f"v{version_number} - Metadata Snapshot",
            description=f"Metadata snapshot for version {version_number}",
            resource_format="JSON",
        )

    async def upload_resource_snapshot(
        self,
        dataset_id: str,
        version_number: int,
        resource_name: str,
        file_content: bytes,
        resource_format: str = "",
    ) -> dict:
        """Upload a resource data file as a version snapshot."""
        safe_name = resource_name.replace("/", "_").replace("\\", "_")
        filename = f"v{version_number}_{safe_name}"
        return await self.upload_resource(
            dataset_id=dataset_id,
            file_content=file_content,
            filename=filename,
            name=f"v{version_number} - {resource_name}",
            description=f"Resource snapshot: {resource_name} (version {version_number})",
            resource_format=resource_format,
        )


odata_client = ODataClient()
