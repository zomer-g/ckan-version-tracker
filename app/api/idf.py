"""idf.il URL validation endpoint.

Mirror of ``app/api/govil.py``, but parser-only — we do not probe the
target URL from the server side. idf.il sits behind Imperva Incapsula
which returns a 212-byte JS challenge HTML to anything that isn't a
real browser, so a probe from the Render dyno would always
false-negative. The actual scraping (which needs Playwright to clear
the challenge) lives in the external govil-scraper worker; this
module's job is just to recognise the URL shape and surface a
reasonable title for the request form.

Supported sections (allowlisted in ``IDF_ALLOWED_SECTIONS``):
  - ``/אתרי-יחידות/הפרקליטות-הצבאית/…``  Military Prosecution
  - ``/אתרי-יחידות/אתר-הפקודות/…``       Orders portal (פקודות מטכ"ל etc.)

Both go through the same external Playwright scraper module
(``govscraper.scrapers.idf``) — the page layout is consistent across
unit sites on idf.il (Sitecore-rendered subtree of <a> tags pointing
at PDFs/DOCs), so no per-section logic is needed in the crawler.

To add a new section: append its kebab-cased Hebrew slug to
``IDF_ALLOWED_SECTIONS`` and update the error message. No other
code changes required.
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
# Sections under /אתרי-יחידות/ that are in scope for the scraper.
# Order matters only for the error message — the regex tries all
# alternatives. New sections: append below and update the user-
# facing error message in validate_idf_url.
IDF_ALLOWED_SECTIONS = (
    "הפרקליטות-הצבאית",   # Military Prosecution
    "אתר-הפקודות",         # Orders portal (פקודות מטכ"ל etc.)
)
_section_alt = "|".join(re.escape(s) for s in IDF_ALLOWED_SECTIONS)
IDF_UNIT_RE = re.compile(
    rf"^/{re.escape(_UNIT_SITES)}/({_section_alt})/(.+)$"
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
    # path_tail is everything after /אתרי-יחידות/<section>/ —
    # e.g. "הנחיות-תצ-ר/פרק-1-כללי" for prosecution or
    # "פקודות-מטכ-ל" for the orders portal. Take the last segment as
    # the human-readable part.
    last_segment = path_tail.rstrip("/").rsplit("/", 1)[-1] or "idf"
    digest = hashlib.sha1(path_tail.encode("utf-8")).hexdigest()[:8]
    return f"{last_segment}-{digest}"


def _parse_idf_url(url: str) -> tuple[str | None, str | None]:
    """Parse an idf.il URL.

    Returns ``("idf_unit:<section>", slug)`` for any URL under a
    whitelisted section in ``IDF_ALLOWED_SECTIONS``, ``(None, None)``
    otherwise. The section is embedded in the page_type so downstream
    callers in ``app/api/datasets.py`` can pick section-appropriate
    scraper config (e.g. the Orders portal needs a deeper crawl and a
    higher document cap than the Prosecution section — it's a forest
    of ~60 category pages, each containing many orders).

    Compatibility:
      - "idf_unit:<section>" matches ``startswith("idf_")``, so the
        switch in datasets.py keeps working for both this format and
        the legacy bare ``"idf_unit"`` / ``"idf_prosecution"``.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in {"idf.il", "www.idf.il"}:
        return None, None

    m = IDF_UNIT_RE.match(_decoded_path(s))
    if not m:
        return None, None
    # m.group(1) is the section name, m.group(2) is everything after.
    # The slug only needs the tail — same hash collision risk applies
    # equally to all sections, and the section name doesn't
    # disambiguate two tails with identical text.
    section = m.group(1)
    path_tail = m.group(2)
    return f"idf_unit:{section}", _idf_slug(path_tail)


# Section → (max_depth, max_docs) tuned per-section. The Prosecution
# section we already supported is a flat "page with a list of files"
# pattern — depth 3 + 500 docs is plenty. The Orders portal
# (אתר-הפקודות) is a category tree: ~60 sub-pages under the root, each
# of which is itself a list of orders. We need both deeper recursion
# and a much higher document cap to cover the whole tree on first scrape.
#
# Fallback default applies to any future section we whitelist before
# we measure it — better to overshoot than to silently truncate.
IDF_SECTION_LIMITS: dict[str, tuple[int, int]] = {
    "הפרקליטות-הצבאית": (3, 500),
    "אתר-הפקודות": (5, 3000),
}
IDF_DEFAULT_LIMITS = (5, 3000)


def get_idf_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for an ``"idf_unit:<section>"``
    page_type. Bare ``"idf_unit"`` / ``"idf_prosecution"`` fall back
    to the conservative default.
    """
    if not page_type or ":" not in page_type:
        return IDF_DEFAULT_LIMITS
    section = page_type.split(":", 1)[1]
    return IDF_SECTION_LIMITS.get(section, IDF_DEFAULT_LIMITS)


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
        # Surface the supported list so the form's error message
        # tells the user exactly what to paste. The list grows as
        # we whitelist more sections in IDF_ALLOWED_SECTIONS.
        allowed = ", ".join(IDF_ALLOWED_SECTIONS)
        return ValidateResponse(
            valid=False,
            error=(
                f"URL is not a supported IDF page. Supported sections "
                f"under https://www.idf.il/אתרי-יחידות/: {allowed}"
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
