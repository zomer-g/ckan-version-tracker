"""Gov.il URL validation and title extraction endpoint."""

import logging
import re

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
