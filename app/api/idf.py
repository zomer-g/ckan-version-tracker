"""idf.il URL validation endpoint.

Mirror of ``app/api/govil.py``, but parser-only — we do not probe the
target URL from the server side. idf.il sits behind Imperva Incapsula
which returns a 212-byte JS challenge HTML to anything that isn't a
real browser, so a probe from the Render dyno would always
false-negative. The actual scraping (which needs Playwright to clear
the challenge) lives in the external govil-scraper worker; this
module's job is just to recognise the URL shape and surface a
reasonable title for the request form.

V1 scope: only Military Prosecution unit pages
(``/אתרי-יחידות/הפרקליטות-הצבאית/…``). Broader idf.il coverage is a
follow-up once we see whether the layout varies per unit.
"""

import hashlib
import logging
import re
from urllib.parse import unquote, urlparse

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/idf", tags=["idf"])


# Hebrew path prefixes we accept. We match against the URL-decoded path
# so both raw-Hebrew and percent-encoded forms work. Anchored at the
# host+path start so other idf.il pages (irrelevant to this scraper)
# can't slip through.
_UNIT_SITES = "אתרי-יחידות"
_MIL_PROSECUTION = "הפרקליטות-הצבאית"
IDF_PROSECUTION_RE = re.compile(
    rf"^/{re.escape(_UNIT_SITES)}/{re.escape(_MIL_PROSECUTION)}/(.+)$"
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


def _decoded_path(url: str) -> str:
    """Return the URL's path component, with percent-encoded Hebrew
    decoded so a single regex catches both forms.

    Strips a trailing slash so ``…/פרק-1-כללי/`` and ``…/פרק-1-כללי``
    parse identically.
    """
    parsed = urlparse(url.strip())
    path = unquote(parsed.path)
    if path.endswith("/") and path != "/":
        path = path[:-1]
    return path


def _idf_slug(path_tail: str) -> str:
    """Build a stable, readable slug for an IDF URL.

    Last meaningful Hebrew segment + 8-char hash of the *decoded* path.
    Hashing the decoded path (not the raw URL) means raw-Hebrew and
    percent-encoded forms of the same page collapse to the same slug —
    so a user who pastes the same URL twice in different encodings
    won't end up with two duplicate datasets.

    The hash suffix exists because different prosecution sub-pages can
    share the same trailing segment (``/הוראות`` etc.) and we don't
    want them to collide on ``ckan_id``/mirror name.
    """
    # path_tail is everything after /אתרי-יחידות/הפרקליטות-הצבאית/ —
    # e.g. "הנחיות-תצ-ר/פרק-1-כללי". Take the last segment as the
    # human-readable part.
    last_segment = path_tail.rstrip("/").rsplit("/", 1)[-1] or "idf"
    digest = hashlib.sha1(path_tail.encode("utf-8")).hexdigest()[:8]
    return f"{last_segment}-{digest}"


def _parse_idf_url(url: str) -> tuple[str | None, str | None]:
    """Parse an idf.il URL.

    Returns ``("idf_prosecution", slug)`` for Military Prosecution unit
    pages, ``(None, None)`` for anything else (so other idf.il areas
    fall through cleanly until we add per-unit handling).
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in {"idf.il", "www.idf.il"}:
        return None, None

    m = IDF_PROSECUTION_RE.match(_decoded_path(s))
    if not m:
        return None, None
    path_tail = m.group(1)
    return "idf_prosecution", _idf_slug(path_tail)


def _format_idf_title(slug: str) -> str:
    """Last-resort title when the page is unreachable (Incapsula etc.).

    The slug carries a hash suffix; strip it for display. Hyphens →
    spaces because the URL slugs are kebab-cased Hebrew.
    """
    # Drop trailing "-<8hex>" hash if present
    name = re.sub(r"-[0-9a-f]{8}$", "", slug)
    return name.replace("-", " ").replace("_", " ").strip()


async def _fetch_idf_title(url: str) -> str | None:
    """Best-effort: try to extract <title> from the live page.

    Incapsula usually serves a 212-byte challenge shell to non-browser
    clients, in which case the <title> is "Request unsuccessful." or
    nothing useful — we detect that and bail. When (rarely) the page
    returns real HTML, take its <title>.
    """
    try:
        async with httpx.AsyncClient(
            timeout=10,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "he-IL,he;q=0.9,en;q=0.5",
            },
        ) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None
            text = resp.text
            # Incapsula challenge HTML is ~200 bytes and contains an
            # _Incapsula_Resource script tag.
            if "_Incapsula_Resource" in text or len(text) < 500:
                return None
            match = re.search(r"<title[^>]*>([^<]+)</title>", text, re.IGNORECASE)
            if not match:
                return None
            title = match.group(1).strip()
            # Drop common suffixes like " | צה\"ל" / " - IDF"
            title = re.sub(r"\s*[|–-]\s*(IDF|צה[\"׳]ל).*$", "", title, flags=re.IGNORECASE)
            return title or None
    except Exception as e:
        logger.debug("Failed to fetch IDF page title from %s: %s", url, e)
        return None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_idf_url(request: Request, body: ValidateRequest):
    """Validate an idf.il scraper URL and extract a title for the form."""
    url = body.url.strip()

    page_type, slug = _parse_idf_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported IDF page. v1 scope: "
                "https://www.idf.il/אתרי-יחידות/הפרקליטות-הצבאית/…"
            ),
        )

    title = await _fetch_idf_title(url) or _format_idf_title(slug)

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )
