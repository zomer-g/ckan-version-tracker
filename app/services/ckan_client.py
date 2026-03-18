import hashlib
import ipaddress
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
        # hostname is not an IP — that's fine (it's a domain name)


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

    async def download_resource(self, url: str) -> tuple[bytes, str]:
        """Download a resource file with SSRF protection and size limit."""
        _validate_url(url)
        max_size = settings.max_resource_download_size

        async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
            # Check size via HEAD first
            try:
                head = await client.head(url)
                content_length = head.headers.get("content-length")
                if content_length and int(content_length) > max_size:
                    raise ValueError(
                        f"Resource too large: {int(content_length)} bytes (max {max_size})"
                    )
            except httpx.HTTPError:
                pass  # HEAD may fail; proceed with streaming download

            # Stream download with size enforcement
            chunks = []
            total = 0
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=65536):
                    total += len(chunk)
                    if total > max_size:
                        raise ValueError(
                            f"Resource exceeded size limit during download ({max_size} bytes)"
                        )
                    chunks.append(chunk)

            content = b"".join(chunks)
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


ckan_client = CKANClient()
