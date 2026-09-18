"""Gov.il URL validation and title extraction endpoint."""

import logging
import re
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.api.utils import sanitize_ckan_name
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


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_govil_url(url: str) -> tuple[str | None, str | None]:
    """Parse a gov.il URL and return (page_type, collector_name) or (None, None)."""
    m = RE_DYNAMIC.match(url.strip())
    if m:
        return "dynamic_collector", m.group(2)
    m = RE_TRADITIONAL.match(url.strip())
    if m:
        return "traditional_collector", m.group(2)
    m = RE_CONTENT_PAGE.match(url.strip())
    if m:
        return "content_page", m.group(2)
    return None, None


def derive_source_name(url: str) -> str | None:
    """Derive a collector-like stem from a source URL that is not a gov.il collector.

    Scraper datasets are normally registered from one of the gov.il collector
    pages matched above, and the collector slug becomes the dataset's
    ``ckan_name``. A scraper whose source lives elsewhere (a different gov.il
    subdomain such as ``nadlan.taxes.gov.il``, or another site entirely) has no
    such slug, so build one from the host and the first path segments:

        https://nadlan.taxes.gov.il/svinfonadlan2010/startpage.aspx
        -> nadlan-taxes-gov-il-svinfonadlan2010

    Callers pass the result through :func:`app.api.utils.scraper_url_slug`,
    which appends a hash of the full URL — so two URLs that reduce to the same
    stem still get distinct slugs. Returns None if ``url`` is not http(s),
    which keeps ``javascript:`` and other schemes out of a field that is
    rendered as a link on over.org.il and on the ODATA mirror.
    """
    url = url.strip()
    try:
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
    except ValueError:
        # urlsplit rejects some malformed URLs outright (e.g. an unclosed IPv6
        # bracket). Treat those as unusable rather than letting them 500.
        return None
    if parts.scheme.lower() not in ("http", "https"):
        return None
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]
    # Segments that look like a file (startpage.aspx, index.html) carry no
    # identity of their own — drop them and keep at most two real segments.
    segments = [seg for seg in parts.path.split("/") if seg and "." not in seg][:2]
    # Cap the stem so that the ODATA mirror name built from it
    # ("gov-versions-scraper-" + stem + "-" + 8-char hash) stays within CKAN's
    # 100-character limit on dataset names.
    stem = sanitize_ckan_name("-".join([host, *segments]))[:60].strip("-")
    return stem or None


def _format_collector_name(name: str) -> str:
    """Format a collector slug into a readable title (fallback when page is unreachable)."""
    return name.replace("-", " ").replace("_", " ").title()


async def _fetch_content_page_title(collector_name: str) -> str | None:
    """Fetch the real title for /he/pages/{name} via ContentPageWebApi.

    These pages are React SPAs whose HTML <title> is just the generic shell
    ("גוב.איל" or similar). The API returns the actual page title in
    ``contentHead.title``.
    """
    try:
        api_url = f"https://www.gov.il/ContentPageWebApi/api/content-pages/{collector_name}?culture=he"
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; over.org.il)"},
        ) as client:
            resp = await client.get(api_url)
            if resp.status_code == 200:
                data = resp.json()
                title = ((data.get("contentHead") or {}).get("title") or "").strip()
                if title:
                    return title
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
                  "or /he/pages/...",
        )

    # Try to fetch the page title. For content_page (React SPA), use the
    # ContentPageWebApi since the raw HTML title is just the shell.
    title = None
    if page_type == "content_page":
        title = await _fetch_content_page_title(collector_name)
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
