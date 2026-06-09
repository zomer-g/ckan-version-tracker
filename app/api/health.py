"""practitioners.health.gov.il URL validation endpoint.

Mirror of ``app/api/idf.py``. The Ministry of Health practitioners
portal is an Angular SPA backed by ``/api/Practitioners/*`` endpoints,
served from the same host but gated behind a WAF that won't reply to
non-browser clients (curl gets a 139-byte maintenance shell or a Hebrew
"operation not supported" page). So like idf.il we don't probe from
the Render dyno — this module just recognises the URL shape and
surfaces a reasonable title for the request form. Actual scraping
runs in the external govil-scraper worker via Playwright.

Supported URLs: ``https://practitioners.health.gov.il/Practitioners/{id}``
where ``{id}`` is the profession/registry numeric id (e.g. 1, 8, 27).
The bare ``/Practitioners`` index is intentionally rejected — per the
per-registry tracking model, each registry id is its own tracked
dataset.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/health", tags=["health"])


HEALTH_HOSTS = {"practitioners.health.gov.il"}
# Anchored at path start so other paths under the same host (if any
# ever appear) can't slip through unless explicitly whitelisted here.
HEALTH_PRACTITIONERS_RE = re.compile(r"^/Practitioners/(\d+)/?$")


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_health_url(url: str) -> tuple[str | None, str | None]:
    """Parse a practitioners.health.gov.il URL.

    Returns ``("health_practitioners:<id>", "health-practitioners-<id>")``
    for an in-scope per-registry URL; ``(None, None)`` otherwise. The
    registry id is embedded in the page_type so downstream callers in
    ``app/api/datasets.py`` can attach registry-aware scraper config
    (per-registry limits, registry_id pass-through) the same way IDF
    sections do.

    Compatibility:
      - ``"health_practitioners:<id>"`` matches
        ``startswith("health_")`` so the dispatch switch in
        datasets.py works the same way it does for ``idf_``.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in HEALTH_HOSTS:
        return None, None

    m = HEALTH_PRACTITIONERS_RE.match(parsed.path or "")
    if not m:
        return None, None
    registry_id = m.group(1)
    return (
        f"health_practitioners:{registry_id}",
        f"health-practitioners-{registry_id}",
    )


# Per-registry limits. The portal is a search-backed SPA where each
# registry id holds anywhere from a few hundred (small specialties)
# to tens of thousands (e.g. nurses, MDs) of practitioners. Start
# generous — the worker logs a truncation marker if it hits the cap,
# so we can tune after the first run rather than silently undercount.
#
# max_depth is mostly nominal for this scraper — the engine doesn't
# do BFS, it iterates the list endpoint and then fetches per-item
# details. Kept in the config for forward-compat / parity with IDF.
HEALTH_DEFAULT_LIMITS: tuple[int, int] = (3, 50000)
HEALTH_REGISTRY_LIMITS: dict[str, tuple[int, int]] = {
    # Override per registry once measured. Empty by default — the
    # generous default applies until we have real numbers.
}


def get_health_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a
    ``"health_practitioners:<id>"`` page_type.
    """
    if not page_type or ":" not in page_type:
        return HEALTH_DEFAULT_LIMITS
    registry_id = page_type.split(":", 1)[1]
    return HEALTH_REGISTRY_LIMITS.get(registry_id, HEALTH_DEFAULT_LIMITS)


def _format_health_title(registry_id: str) -> str:
    """Last-resort title when we can't probe the live page."""
    return f"רישום בעלי מקצועות בריאות — מאגר {registry_id}"


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_health_url(request: Request, body: ValidateRequest):
    """Validate a practitioners.health.gov.il URL and surface a title.

    We don't fetch the page — the WAF treats non-browser clients as
    abusive and the SPA shell carries no readable title anyway.
    """
    url = body.url.strip()

    page_type, slug = _parse_health_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported practitioners.health.gov.il page. "
                "Expected https://practitioners.health.gov.il/Practitioners/<id> "
                "(e.g. /Practitioners/1, /Practitioners/8, /Practitioners/27). "
                "The bare /Practitioners index is not supported — register "
                "each registry id separately."
            ),
        )

    registry_id = page_type.split(":", 1)[1]
    title = _format_health_title(registry_id)

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )
