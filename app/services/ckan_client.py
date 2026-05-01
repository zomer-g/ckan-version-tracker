import csv
import hashlib
import io
import ipaddress
import json
import logging
import os
import tempfile
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

    async def download_resource(self, url: str, resource_id: str = "") -> tuple[str, str, int]:
        """
        Download a resource into a temporary file on disk.

        Returns ``(file_path, sha256, byte_count)``. The caller owns the
        file and is responsible for deleting it when done — typically
        via ``os.unlink(path)`` in a try/finally. Streaming straight to
        disk keeps peak memory at one HTTP chunk (~64KB) regardless of
        the resource size, so a 200MB ZIP no longer OOM-kills the 512MB
        Render dyno.

        Strategy:
        1. Try datastore_search API (works even when direct URL is
           blocked by IAP).
        2. Fall back to streaming direct URL download.
        """
        # Strategy 1: Try datastore API (data.gov.il blocks direct downloads with Google IAP)
        if resource_id:
            try:
                path, sha256, n = await self._download_via_datastore_to_file(resource_id)
                if n > 0:
                    logger.info(
                        "Downloaded %s via datastore API (%d bytes → %s)",
                        resource_id, n, path,
                    )
                    return path, sha256, n
            except Exception as e:
                logger.debug("Datastore download failed for %s: %s, trying direct URL", resource_id, e)

        # Strategy 2: Direct URL download (may fail on data.gov.il due to IAP)
        return await self._download_direct(url)

    async def _download_via_datastore_to_file(self, resource_id: str) -> tuple[str, str, int]:
        """Stream the datastore export to a temp CSV file, hashing as we go."""
        max_size = settings.max_resource_download_size

        fd, path = tempfile.mkstemp(prefix="ckan-ds-", suffix=".csv")
        os.close(fd)
        h = hashlib.sha256()
        total = 0
        offset = 0
        fields: list[str] = []
        any_records = False

        try:
            async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    writer: csv.DictWriter | None = None
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
                            if not fields:
                                break
                            writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
                            writer.writeheader()

                        records = result.get("records", [])
                        if not records:
                            break

                        for record in records:
                            assert writer is not None
                            clean = {k: v for k, v in record.items() if k != "_id"}
                            buf = io.StringIO()
                            csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore").writerow(clean)
                            line = buf.getvalue()
                            fh.write(line)
                            line_bytes = line.encode("utf-8")
                            h.update(line_bytes)
                            total += len(line_bytes)
                            any_records = True
                            if total > max_size:
                                raise ValueError(f"Resource exceeded size limit ({max_size} bytes)")
                        offset += len(records)
                        if offset >= result.get("total", 0):
                            break
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise

        if not any_records or not fields:
            try:
                os.unlink(path)
            except OSError:
                pass
            return "", "", 0

        return path, h.hexdigest(), total

    async def _download_direct(self, url: str) -> tuple[str, str, int]:
        """Stream a direct file download to a temp file, with SSRF protection."""
        _validate_url(url)
        max_size = settings.max_resource_download_size

        fd, path = tempfile.mkstemp(prefix="ckan-dl-")
        os.close(fd)
        h = hashlib.sha256()
        total = 0
        first_bytes = b""

        try:
            async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
                try:
                    head = await client.head(url)
                    content_length = head.headers.get("content-length")
                    if content_length and int(content_length) > max_size:
                        raise ValueError(f"Resource too large: {int(content_length)} bytes")
                except httpx.HTTPError:
                    pass

                with open(path, "wb") as fh:
                    async with client.stream("GET", url) as resp:
                        resp.raise_for_status()
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            total += len(chunk)
                            if total > max_size:
                                raise ValueError(f"Resource exceeded size limit ({max_size} bytes)")
                            if len(first_bytes) < 16:
                                first_bytes += chunk[: 16 - len(first_bytes)]
                            fh.write(chunk)
                            h.update(chunk)
        except Exception:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise

        # Detect if we got an HTML page instead of actual data (IAP redirect)
        head_lc = first_bytes.lower()
        if head_lc.startswith(b"<!doctype") or head_lc.startswith(b"<html"):
            try:
                os.unlink(path)
            except OSError:
                pass
            raise RuntimeError("Got HTML instead of data — likely blocked by IAP/auth")

        return path, h.hexdigest(), total

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
