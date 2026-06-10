"""avodata.labor.gov.il URL validation endpoint.

Mirror of ``app/api/health.py``. avodata.labor.gov.il is the Ministry
of Labor's occupation-exploration site: ~1,000+ occupations organised
under 22 user-facing "scopes" (UI category buttons — אדריכלות והנדסה,
פיתוח/תכנות/דיגיטל, …). Each scope's page lives at
``/search?scope=<hebrew-slug>``.

The occupation pages themselves are fully server-rendered HTML, so the
external scraper can fetch them with plain ``httpx`` — no Playwright
needed for the per-occupation walk. This module's job is just to
recognise the scope URL shape and surface a reasonable title for the
OVER request form.

Per-scope tracking model: each scope URL is registered as its own
TrackedDataset (one of 22), mirroring the practitioners model where
each registry id is a separate dataset. The bare ``/search`` index,
the ``/occupations`` listing, and naked ``/isco_group/{X}[/{Y}]``
URLs are intentionally rejected — the scope is the unit OVER tracks.
"""

import logging
import re
from urllib.parse import parse_qs, unquote, urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/avodata", tags=["avodata"])


AVODATA_HOSTS = {"avodata.labor.gov.il"}
# Anchored path: only the bare /search endpoint with a non-empty
# ?scope= query is in scope. Trailing slash tolerated.
AVODATA_SEARCH_RE = re.compile(r"^/search/?$")

# Mapping of UI Hebrew label → canonical kebab-cased Hebrew slug used
# in the URL. We accept both the raw label (with spaces) and the
# kebab form (with hyphens) at validate time; the canonical kebab
# slug is what gets stamped into ``ckan_id`` and ``scraper_config``.
#
# The 22 scopes are reproduced verbatim from the Avodata homepage
# category-button list captured 2026-06-10 via Chrome DevTools. If
# the Ministry of Labor adds a 23rd scope (or renames one) we'll
# need to refresh this list — it's the only place in either project
# that depends on this list at all.
AVODATA_SCOPES_HE: tuple[str, ...] = (
    "אדריכלות והנדסה",
    "אחזקת מבנים וסביבה",
    "אכיפה ושירותי ביטחון",
    "בישול, מסעדנות והארחה",
    "בנייה ותשתיות",
    "בריאות, טיפול ורפואה משלימה",
    "חוק ומשפט",
    "חינוך והשכלה גבוהה",
    "טיפוח ומתן שירותים אישיים",
    "טכנאים ומכונאים",
    "לוגיסטיקה ותובלה",
    "מדיה, תרבות ועיצוב",
    "מדעי הטבע וחקלאות",
    "מחקר ואקדמיה",
    "ניהול",
    "ספורט ופנאי",
    "פיננסים וכלכלה",
    "פיתוח, תכנות ודיגיטל",
    "שיווק ומכירות",
    "שירותים קהילתיים וחברתיים",
    "תעשייה וייצור",
    "תפעול ושירות לקוחות",
)


def _scope_to_kebab(label_or_slug: str) -> str:
    """Normalise a Hebrew scope label OR a pre-slugified scope to the
    canonical kebab form the URL uses.

    Examples (round-trip):
      "פיתוח, תכנות ודיגיטל"  → "פיתוח-תכנות-ודיגיטל"
      "פיתוח-תכנות-ודיגיטל"   → "פיתוח-תכנות-ודיגיטל" (idempotent)
      "פיתוח תכנות ודיגיטל"   → "פיתוח-תכנות-ודיגיטל"
    """
    s = label_or_slug.strip()
    # Drop commas (the kebab form on the site omits them entirely)
    s = s.replace(",", "")
    # Spaces → hyphens
    s = re.sub(r"\s+", "-", s)
    # Collapse runs of hyphens
    s = re.sub(r"-+", "-", s)
    return s.strip("-")


# Pre-compute the kebab form of every accepted scope once, for the
# membership check. Built from the labels above, so the two stay in
# sync automatically.
AVODATA_SCOPE_SLUGS: frozenset[str] = frozenset(
    _scope_to_kebab(label) for label in AVODATA_SCOPES_HE
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


def _parse_avodata_url(url: str) -> tuple[str | None, str | None]:
    """Parse an avodata.labor.gov.il scope URL.

    Returns ``("avodata_scope:<slug>", "avodata-<slug>")`` for a
    URL whose path is ``/search`` and whose ``scope`` query parameter
    decodes to one of the 22 known scope labels (in any of the three
    accepted forms: raw label, space-form, or kebab slug).

    Returns ``(None, None)`` for everything else — the homepage, the
    bare ``/search`` index, naked ``/isco_group/...`` paths, the
    ``/occupations`` listing, etc.

    Compatibility:
      - ``"avodata_scope:<slug>"`` matches ``startswith("avodata_")``,
        keeping the dispatch switch in ``datasets.py`` symmetric with
        the existing ``idf_`` / ``health_`` prefixes.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in AVODATA_HOSTS:
        return None, None

    if not AVODATA_SEARCH_RE.match(parsed.path or ""):
        return None, None

    qs = parse_qs(parsed.query or "", keep_blank_values=False)
    raw_scope = (qs.get("scope") or [""])[0]
    raw_scope = unquote(raw_scope)
    if not raw_scope:
        return None, None

    slug = _scope_to_kebab(raw_scope)
    if slug not in AVODATA_SCOPE_SLUGS:
        # An unknown scope (typo or future scope we haven't whitelisted
        # yet). Reject rather than silently accept — silent acceptance
        # would let an admin register a typo'd URL that the scraper
        # then can't enumerate.
        return None, None

    return f"avodata_scope:{slug}", f"avodata-{slug}"


# Per-scope (max_depth, max_docs) limits. The biggest scope so far
# (פיתוח, תכנות ודיגיטל) holds on the order of 80–100 occupations;
# 5000 is a 50× safety margin and keeps the truncation marker
# meaningful if a scope ever explodes. max_depth is nominal — this
# scraper iterates a flat list, it doesn't BFS.
AVODATA_DEFAULT_LIMITS: tuple[int, int] = (3, 5000)
AVODATA_SCOPE_LIMITS: dict[str, tuple[int, int]] = {
    # Override per-scope once measured. Empty by default — the
    # generous default applies until we have real numbers.
}


def get_avodata_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for an
    ``"avodata_scope:<slug>"`` page_type.
    """
    if not page_type or ":" not in page_type:
        return AVODATA_DEFAULT_LIMITS
    slug = page_type.split(":", 1)[1]
    return AVODATA_SCOPE_LIMITS.get(slug, AVODATA_DEFAULT_LIMITS)


def _format_avodata_title(slug: str) -> str:
    """Last-resort title from the kebab slug. The 22 scope labels
    contain commas in some cases (e.g. ``פיתוח, תכנות ודיגיטל``) but
    the slug drops them — restoring the exact punctuation isn't
    necessary for a display title; spaces are good enough.
    """
    return f"עבודאטה — {slug.replace('-', ' ')}"


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_avodata_url(request: Request, body: ValidateRequest):
    """Validate an avodata.labor.gov.il scope URL.

    No live fetch — the scope list is hard-coded above, so we already
    know whether the URL is in scope. Returning fast also keeps the
    request form snappy when users paste a URL.
    """
    url = body.url.strip()

    page_type, slug = _parse_avodata_url(url)
    if not page_type or not slug:
        scopes_preview = ", ".join(list(AVODATA_SCOPES_HE)[:4]) + ", …"
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported avodata.labor.gov.il scope page. "
                "Expected https://avodata.labor.gov.il/search?scope=<scope> "
                f"where <scope> is one of the 22 known scopes ({scopes_preview}). "
                "The bare /search index, /occupations listing, and /isco_group/… "
                "paths are not registered directly — track each scope individually."
            ),
        )

    scope_slug = page_type.split(":", 1)[1]
    title = _format_avodata_title(scope_slug)

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )
