import csv
import hashlib
import io
import ipaddress
import json
import logging
from typing import Any
from urllib.parse import urlparse

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TIMEOUT = httpx.Timeout(connect=15.0, read=60.0, write=30.0, pool=10.0)
DOWNLOAD_TIMEOUT = httpx.Timeout(connect=15.0, read=300.0, write=30.0, pool=10.0)

# Allowed schemes and blocked IP ranges for SSRF protection
ALLOWED_SCHEMES = {"http", "https"}
BLOCKED_NETWORKS = [
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]

# data.gov.il datastore page size (max records per request)
DATASTORE_PAGE_SIZE = 32000


def _validate_url(url: str) -> None:
    """Validate URL to prevent SSRF attacks."""
    parsed = urlparse(url)
    if parsed.scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"Blocked URL scheme: {parsed.scheme}")
    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL has no hostname")
    try:
        ip = ipaddress.ip_address(hostname)
        for network in BLOCKED_NETWORKS:
            if ip in network:
                raise ValueError(f"Blocked internal IP: {hostname}")
    except ValueError as e:
        if "Blocked" in str(e) or "scheme" in str(e):
            raise


class CKANClient:
    """Async client for reading from data.gov.il CKAN API."""

    def __init__(self, base_url: str | None = None):
        self.base_url = (base_url or settings.data_gov_il_url).rstrip("/")
        self.api_url = f"{self.base_url}/api/3/action"

    async def _get(self, action: str, params: dict | None = None) -> Any:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            url = f"{self.api_url}/{action}"
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"CKAN API error: {data.get('error', 'unknown')}")
            return data["result"]

    async def package_search(self, query: str, rows: int = 20, start: int = 0) -> dict:
        return await self._get("package_search", {"q": query, "rows": rows, "start": start})

    async def package_show(self, id_or_name: str) -> dict:
        return await self._get("package_show", {"id": id_or_name})

    async def package_list(self, limit: int = 100, offset: int = 0) -> list[str]:
        return await self._get("package_list", {"limit": limit, "offset": offset})

    async def organization_list(self, all_fields: bool = False) -> list:
        return await self._get("organization_list", {"all_fields": all_fields})

    async def download_resource(self, url: str, resource_id: str = "") -> tuple[bytes, str]:
        """
        Download a resource. Strategy:
        1. Try datastore_search API (works even when direct URL is blocked by IAP)
        2. Fall back to direct URL download
        """
        # Strategy 1: Try datastore API (data.gov.il blocks direct downloads with Google IAP)
        if resource_id:
            try:
                content = await self._download_via_datastore(resource_id)
                if content and not content.startswith(b"<!"):  # Not an HTML error page
                    sha256 = hashlib.sha256(content).hexdigest()
                    logger.info("Downloaded %s via datastore API (%d bytes)", resource_id, len(content))
                    return content, sha256
            except Exception as e:
                logger.debug("Datastore download failed for %s: %s, trying direct URL", resource_id, e)

        # Strategy 2: Direct URL download (may fail on data.gov.il due to IAP)
        return await self._download_direct(url)

    async def _download_via_datastore(self, resource_id: str) -> bytes:
        """Download all records from data.gov.il datastore and convert to CSV bytes."""
        all_records: list[dict] = []
        fields: list[str] = []
        offset = 0

        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
            while True:
                url = f"{self.api_url}/datastore_search"
                params = {
                    "resource_id": resource_id,
                    "limit": DATASTORE_PAGE_SIZE,
                    "offset": offset,
                }
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

                if not data.get("success"):
                    raise RuntimeError(f"datastore_search error: {data.get('error')}")

                result = data["result"]

                if not fields and result.get("fields"):
                    fields = [f["id"] for f in result["fields"] if f["id"] != "_id"]

                records = result.get("records", [])
                if not records:
                    break

                all_records.extend(records)
                offset += len(records)

                # Check if we got all records
                total = result.get("total", 0)
                if offset >= total:
                    break

        if not all_records or not fields:
            raise RuntimeError("No data returned from datastore")

        # Convert to CSV bytes
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for record in all_records:
            # Remove _id field added by datastore
            clean = {k: v for k, v in record.items() if k != "_id"}
            writer.writerow(clean)

        return output.getvalue().encode("utf-8")

    async def _download_direct(self, url: str) -> tuple[bytes, str]:
        """Direct file download with SSRF protection."""
        _validate_url(url)
        max_size = settings.max_resource_download_size

        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
            try:
                head = await client.head(url)
                content_length = head.headers.get("content-length")
                if content_length and int(content_length) > max_size:
                    raise ValueError(f"Resource too large: {int(content_length)} bytes")
            except httpx.HTTPError:
                pass

            chunks = []
            total = 0
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    total += len(chunk)
                    if total > max_size:
                        raise ValueError(f"Resource exceeded size limit ({max_size} bytes)")
                    chunks.append(chunk)

            content = b"".join(chunks)

            # Detect if we got an HTML page instead of actual data (IAP redirect)
            if content[:15].lower().startswith(b"<!doctype") or content[:6].lower().startswith(b"<html"):
                raise RuntimeError("Got HTML instead of data — likely blocked by IAP/auth")

            sha256 = hashlib.sha256(content).hexdigest()
            return content, sha256

    async def head_resource(self, url: str) -> dict:
        """HEAD request to check resource metadata without downloading."""
        _validate_url(url)
        async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
            resp = await client.head(url)
            return {
                "content_length": resp.headers.get("content-length"),
                "last_modified": resp.headers.get("last-modified"),
                "etag": resp.headers.get("etag"),
                "status": resp.status_code,
            }


    async def datastore_info(self, resource_id: str) -> dict:
        """Get record count and fields without downloading any data. Instant."""
        result = await self._get("datastore_search", {
            "resource_id": resource_id,
            "limit": 0,
        })
        return {
            "total": result.get("total", 0),
            "fields": [f for f in result.get("fields", []) if f["id"] != "_id"],
        }

    async def datastore_sample(self, resource_id: str, head: int = 100, tail: int = 100) -> tuple[list[dict], list[dict]]:
        """Get first N and last N records for inspection without downloading everything."""
        # Get total count first
        info = await self.datastore_info(resource_id)
        total = info["total"]
        fields = info["fields"]

        # Head records (first N)
        head_result = await self._get("datastore_search", {
            "resource_id": resource_id,
            "limit": head,
            "offset": 0,
        })
        head_records = head_result.get("records", [])

        # Tail records (last N)
        tail_records = []
        if total > head + tail:
            tail_result = await self._get("datastore_search", {
                "resource_id": resource_id,
                "limit": tail,
                "offset": max(0, total - tail),
            })
            tail_records = tail_result.get("records", [])

        # Clean _id from records
        for r in head_records + tail_records:
            r.pop("_id", None)

        return head_records, tail_records


ckan_client = CKANClient()
