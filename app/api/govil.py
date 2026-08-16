"""Gov.il URL validation and title extraction endpoint."""

import hashlib
import json
import logging
import re
from urllib.parse import parse_qs, urlparse

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/govil", tags=["govil"])

# URL patterns for gov.il collector pages
RE_DYNAMIC = re.compile(
    r"^https?://(www\.)?gov\.il/he/departments?/dynamiccollectors?/([^/?#]+)",
    re.IGNORECASE,
)
RE_TRADITIONAL = re.compile(
    r"^https?://(www\.)?gov\.il/he/collectors?/([^/?#]+)",
    re.IGNORECASE,
)
RE_CONTENT_PAGE = re.compile(
    r"^https?://(www\.)?gov\.il/he/pages/([^/?#]+)",
    re.IGNORECASE,
)
# Raw API URL for the DataCollector / Content Page collector backends.
# Two patterns are accepted:
#   * The current API host: openapi-gc.digital.gov.il (Google Cloud,
#     not behind Cloudflare) — what the gov.il SPA actually calls.
#   * The legacy www.gov.il/CollectorsWebApi/... path, which now serves
#     the SPA HTML shell. We still accept it because admins frequently
#     paste these in (and translate_to_api_url rewrites them).
RE_COLLECTOR_API = re.compile(
    r"^https?://(?:"
    r"openapi-gc\.digital\.gov\.il/"
    r"|(?:www\.)?gov\.il/(?:CollectorsWebApi|ContentPageWebApi)/api/"
    r")",
    re.IGNORECASE,
)


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _collector_api_slug(url: str) -> str:
    """Build a stable, readable slug for a raw CollectorsWebApi URL.

    Each (CollectorType, officeId, Type) triple is a distinct
    "collection" on gov.il — e.g. publications/<office>/<topic-uuid>.
    We surface the type prefix + a hash of the full URL so two URLs
    that differ only by query params produce different slugs.
    """
    parsed = urlparse(url)
    qs = {k.lower(): v for k, v in parse_qs(parsed.query).items()}
    collector_type = (qs.get("collectortype") or [""])[0].strip()
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:8]
    prefix = collector_type or "datacollector"
    return f"{prefix}-{digest}"


def _parse_govil_url(url: str) -> tuple[str | None, str | None]:
    """Parse a gov.il URL and return (page_type, collector_name) or (None, None)."""
    s = url.strip()
    m = RE_DYNAMIC.match(s)
    if m:
        return "dynamic_collector", m.group(2)
    m = RE_TRADITIONAL.match(s)
    if m:
        return "traditional_collector", m.group(2)
    m = RE_CONTENT_PAGE.match(s)
    if m:
        return "content_page", m.group(2)
    if RE_COLLECTOR_API.match(s):
        return "data_collector_api", _collector_api_slug(s)
    return None, None


def _format_collector_name(name: str) -> str:
    """Format a collector slug into a readable title (fallback when page is unreachable)."""
    return name.replace("-", " ").replace("_", " ").title()


# www.gov.il/ContentPageWebApi/api/... is behind Cloudflare and answers a
# plain client with 403 + an "Attention Required" page, which is why this
# used to return None for every page and the form fell back to a title made
# out of the URL slug ("Grants Grants"). The SPA itself calls an Apigee
# gateway that is NOT behind Cloudflare — it wants the site's public client
# id plus a gov.il Origin (with the id but no Origin it answers
# RF-OriginError; with neither, FailedToResolveAPIKey). Both the gateway
# host and the id come from a static client-config.js that IS servable to a
# plain client.
_GOVIL_CLIENT_CONFIG_JS = "https://www.gov.il/ContentpageWebApi/client-config.js"
_GOVIL_RUN_CONFIG_RE = re.compile(r"govilRunConfig'\]\s*=\s*(\{.*?\})\s*;", re.DOTALL)
_GOVIL_API_HEADERS = {"Origin": "https://www.gov.il", "Accept": "application/json"}
_GOVIL_API_FALLBACK = "https://www.gov.il/ContentPageWebApi/api/content-pages"

# (api_base, client_id), resolved once per process. gov.il rotates neither
# often, and a stale value only costs us a title.
_content_page_api_cache: tuple[str, str] | None = None


async def _content_page_api(client: httpx.AsyncClient) -> tuple[str, str]:
    """Resolve the content-page API base + client id from client-config.js."""
    global _content_page_api_cache
    if _content_page_api_cache is not None:
        return _content_page_api_cache
    api_base, client_id = _GOVIL_API_FALLBACK, ""
    try:
        resp = await client.get(_GOVIL_CLIENT_CONFIG_JS)
        m = _GOVIL_RUN_CONFIG_RE.search(resp.text or "")
        if m:
            cfg = json.loads(m.group(1))
            base = (cfg.get("contentPageWebApi") or "").rstrip("/")
            if base:
                api_base = f"{base}/api/content-pages"
            client_id = (cfg.get("clientId") or "").strip()
    except Exception as e:
        logger.debug("govil client-config fetch failed, using www fallback: %s", e)
    _content_page_api_cache = (api_base, client_id)
    return _content_page_api_cache


def _chapter_index_of(url: str) -> str | None:
    """The ?chapterIndex=N a /he/pages/ URL was pasted with, if any."""
    qs = {k.lower(): v for k, v in parse_qs(urlparse(url).query).items()}
    value = (qs.get("chapterindex") or [""])[0].strip()
    return value if value.isdigit() else None


async def _fetch_content_page_title(collector_name: str, url: str = "") -> str | None:
    """Fetch the real title for /he/pages/{name} via ContentPageWebApi.

    These pages are React SPAs whose HTML <title> is just the generic shell
    ("גוב.איל" or similar). The API returns the actual page title in
    ``contentHead.title``.

    A page can be a stack of unrelated tabs, and ``contentHead.title`` names
    the stack rather than the tab: every one of grants-grants' eleven
    chapters reports "תמיכות ומענקים", though chapterIndex=5 is the balance
    grants and chapterIndex=7 is the minister's grant. Each tab is tracked as
    its own dataset (the slug hashes the full URL), so when the pasted URL
    pins a chapter, prefer that tab's own name from the side-nav.
    """
    try:
        chapter = _chapter_index_of(url)
        query = "?culture=he" + (f"&chapterIndex={chapter}" if chapter else "")
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; over.org.il)"},
        ) as client:
            api_base, client_id = await _content_page_api(client)
            resp = await client.get(
                f"{api_base}/{collector_name}{query}",
                headers=_GOVIL_API_HEADERS | ({"x-client-id": client_id} if client_id else {}),
            )
            if resp.status_code == 200 and "json" in (resp.headers.get("content-type") or ""):
                data = resp.json()
                content_main = data.get("contentMain") or {}
                page_title = ((data.get("contentHead") or {}).get("title") or "").strip()
                if chapter:
                    tabs = (content_main.get("sideNav") or {}).get("tagItems") or []
                    for tab in tabs:
                        m = re.search(r"chapterIndex=(\d+)", tab.get("url") or "")
                        if (m.group(1) if m else "1") != chapter:
                            continue
                        tab_title = (tab.get("title") or "").strip()
                        if tab_title:
                            return f"{page_title} — {tab_title}" if page_title else tab_title
                        break
                if page_title:
                    return page_title
    except Exception as e:
        logger.debug("Failed to fetch content-page title for %s: %s", collector_name, e)
    return None


async def _fetch_page_title(url: str) -> str | None:
    """Fetch the page title from a gov.il URL."""
    try:
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; over.org.il)"},
        ) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                # Extract <title> tag content
                match = re.search(r"<title[^>]*>([^<]+)</title>", resp.text, re.IGNORECASE)
                if match:
                    title = match.group(1).strip()
                    # Remove common suffixes like " | gov.il" or " - gov.il"
                    title = re.sub(r"\s*[|–-]\s*gov\.il.*$", "", title, flags=re.IGNORECASE)
                    title = re.sub(r"\s*[|–-]\s*אתר ממשלתי.*$", "", title)
                    if title:
                        return title
    except Exception as e:
        logger.debug("Failed to fetch page title from %s: %s", url, e)
    return None


async def _probe_collector_api(url: str) -> tuple[bool, str | None]:
    """Hit the raw collector API URL with proper JSON headers.

    Returns ``(ok, sample_title)``. ``ok`` is True when the endpoint
    responds with a JSON body that has a ``Results`` (or similar) list —
    proof we can actually collect from it. Without proper Accept and
    User-Agent headers gov.il serves the SPA shell as HTML; with them
    the same URL returns JSON.
    """
    # Share the same header set as the actual collector fetch so the
    # probe doesn't false-negative on URLs the collector could handle
    # (gov.il's Cloudflare returns 403 to short bot UAs but lets through
    # full-Chrome headers).
    from app.services.datacollector_client import REQUEST_HEADERS as _DC_HEADERS
    try:
        async with httpx.AsyncClient(
            timeout=15,
            follow_redirects=True,
            headers=_DC_HEADERS,
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return False, None
            ctype = (resp.headers.get("content-type") or "").lower()
            text = resp.text
            if "json" not in ctype and not text.lstrip().startswith(("{", "[")):
                return False, None
            data = resp.json()
    except Exception as e:
        logger.debug("Collector API probe failed for %s: %s", url, e)
        return False, None

    results = _extract_results(data)
    if not isinstance(results, list):
        return False, None
    sample_title = None
    if results:
        first = results[0]
        if isinstance(first, dict):
            for key in ("Title", "title", "Name", "name", "Subject", "subject"):
                v = first.get(key)
                if isinstance(v, str) and v.strip():
                    sample_title = v.strip()
                    break
    return True, sample_title


def _extract_results(data: object) -> list | None:
    """Pull the row list out of a gov.il collector JSON response.

    Different endpoints use different envelope names. Returns the first
    list-valued field that looks like a results array, or ``None``.
    """
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None
    for key in ("Results", "results", "Items", "items", "Records", "records", "Data", "data"):
        v = data.get(key)
        if isinstance(v, list):
            return v
    return None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_govil_url(request: Request, body: ValidateRequest):
    """Validate a gov.il collector URL and extract metadata."""
    url = body.url.strip()

    page_type, collector_name = _parse_govil_url(url)

    if not page_type or not collector_name:
        return ValidateResponse(
            valid=False,
            error="URL is not a recognized gov.il page. Supported: "
                  "/he/departments/dynamiccollectors/..., /he/collectors/..., "
                  "/he/pages/..., or "
                  "/CollectorsWebApi/api/DataCollector/GetResults?... (raw API URL)",
        )

    # Raw collector API URL: probe the endpoint as JSON so we both confirm
    # it's actually a collector (and not, e.g., the SPA shell) and pick up
    # a title from the first row when one is available.
    if page_type == "data_collector_api":
        ok, sample_title = await _probe_collector_api(url)
        if not ok:
            return ValidateResponse(
                valid=False,
                page_type=page_type,
                collector_name=collector_name,
                error="URL did not return a JSON collector payload. Check "
                      "the path, query params, and that the endpoint still "
                      "exists on gov.il.",
            )
        title = sample_title or _format_collector_name(collector_name)
        return ValidateResponse(
            valid=True,
            page_type=page_type,
            collector_name=collector_name,
            title=title,
            url=url,
        )

    # Try to fetch the page title. For content_page (React SPA), use the
    # ContentPageWebApi since the raw HTML title is just the shell.
    title = None
    if page_type == "content_page":
        title = await _fetch_content_page_title(collector_name, url)
    if not title:
        title = await _fetch_page_title(url)
    if not title:
        title = _format_collector_name(collector_name)

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=collector_name,
        title=title,
        url=url,
    )
