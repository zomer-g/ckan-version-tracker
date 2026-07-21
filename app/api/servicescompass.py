"""gov.il/apps/servicescompass URL validation endpoint.

Mirror of ``app/api/avodata.py`` — a whole-corpus, single-URL source.

"מצפן השירותים הממשלתיים" is the National Digital Agency's (מערך הדיגיטל
הלאומי) weekly dashboard of every public government service — ministry,
type, delivery channels, digital-maturity level and usage estimates
(~4,560 services). The whole table, plus the finished Excel export, is
baked into the single page at
``https://www.gov.il/apps/servicescompass/``, which is regenerated about
once a week (the page carries a "עודכן בתאריך …" stamp).

The external scraper fetches that one page, decodes the embedded ``.xlsx``
(attached unchanged → R2) and parses its sheet into rows (tracked/diffed
→ NEON). So there is exactly ONE trackable URL and ONE dataset.

This module just recognises that URL shape and surfaces a Hebrew title for
the request form.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/servicescompass", tags=["servicescompass"])


SERVICESCOMPASS_HOSTS = {"www.gov.il", "gov.il"}
# The single trackable URL — the whole compass is one dataset. Trailing
# slash and query string are tolerated.
SERVICESCOMPASS_PATH_RE = re.compile(r"^/apps/servicescompass/?$", re.IGNORECASE)


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_servicescompass_url(url: str) -> tuple[str | None, str | None]:
    """Parse a services-compass URL.

    Returns ``("servicescompass_services", "servicescompass-services")`` for
    the app page, or ``(None, None)`` for everything else on gov.il (so any
    other gov.il URL falls through to the gov.il parser).

    The page_type matches ``startswith("servicescompass_")``, keeping the
    dispatch switch in ``datasets.py`` symmetric with the ``avodata_`` /
    ``health_`` prefixes.
    """
    parsed = urlparse(url.strip())
    host = (parsed.hostname or "").lower().lstrip(".")
    if host not in SERVICESCOMPASS_HOSTS:
        return None, None
    if SERVICESCOMPASS_PATH_RE.match(parsed.path or ""):
        return "servicescompass_services", "servicescompass-services"
    return None, None


# (max_depth, max_docs). One page, no recursion; max_docs is a generous
# ceiling above the ~4,560 service rows (0 in the engine means "all").
SERVICESCOMPASS_DEFAULT_LIMITS: tuple[int, int] = (1, 100000)


def get_servicescompass_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` — single dataset, always the default."""
    return SERVICESCOMPASS_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_servicescompass_url(request: Request, body: ValidateRequest):
    """Validate a gov.il/apps/servicescompass URL.

    No live fetch — the only accepted URL is the app page, recognised by
    shape alone (a live fetch would hit Cloudflare for no benefit).
    """
    url = body.url.strip()

    page_type, slug = _parse_servicescompass_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not the government services-compass page. Expected "
                "https://www.gov.il/apps/servicescompass/ (מצפן השירותים "
                "הממשלתיים, מערך הדיגיטל הלאומי) — tracked as a single dataset."
            ),
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title="מצפן השירותים הממשלתיים — מערך הדיגיטל הלאומי",
        url=url,
    )
