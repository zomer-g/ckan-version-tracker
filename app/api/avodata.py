"""avodata.labor.gov.il URL validation endpoint.

Mirror of ``app/api/health.py``. avodata.labor.gov.il is the Ministry
of Labor's occupation-exploration site: ~780 occupations, each with a
fully server-rendered HTML page at ``/isco_group/{ISCO4}/{id}``.

Originally we planned per-scope tracking (one dataset per the 22
user-facing scopes), but the scope→occupation mapping lives only in an
Elasticsearch endpoint that returns 403 to every client — including
the site's own frontend, whose search page currently shows zero
results. So there is no reliable way to filter occupations by scope.

Instead we track the whole corpus as ONE dataset: the user registers
``https://avodata.labor.gov.il/occupations`` and the external scraper
walks the sitemap, fetching every occupation page. Each row carries
its ISCO group, which is the available grouping dimension.

This module just recognises the ``/occupations`` URL shape and surfaces
a title for the request form.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/avodata", tags=["avodata"])


AVODATA_HOSTS = {"avodata.labor.gov.il"}
# The single trackable URL: the occupations index. Trailing slash ok.
AVODATA_OCCUPATIONS_RE = re.compile(r"^/occupations/?$")


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
    """Parse an avodata.labor.gov.il URL.

    Returns ``("avodata_occupations", "avodata-occupations")`` for the
    occupations index, ``(None, None)`` for everything else (the
    homepage, ``/search?scope=...`` pages, naked ``/isco_group/...``
    paths).

    Compatibility:
      - ``"avodata_occupations"`` matches ``startswith("avodata_")``,
        keeping the dispatch switch in ``datasets.py`` symmetric with
        the existing ``idf_`` / ``health_`` prefixes.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in AVODATA_HOSTS:
        return None, None
    if not AVODATA_OCCUPATIONS_RE.match(parsed.path or ""):
        return None, None
    return "avodata_occupations", "avodata-occupations"


# (max_depth, max_docs). max_depth is nominal — the scraper iterates a
# flat sitemap. The Hebrew occupation count is ~780; 2000 is a generous
# margin that still surfaces a truncation marker if the site grows.
AVODATA_DEFAULT_LIMITS: tuple[int, int] = (3, 2000)


def get_avodata_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for an avodata page_type.

    Single dataset, so there's nothing to vary per-page_type — always
    the default. Kept as a function for symmetry with the idf / health
    parsers so ``datasets.py`` calls all four the same way.
    """
    return AVODATA_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_avodata_url(request: Request, body: ValidateRequest):
    """Validate an avodata.labor.gov.il occupations-index URL.

    No live fetch — the only accepted URL is the ``/occupations``
    index, which we recognise by shape alone.
    """
    url = body.url.strip()

    page_type, slug = _parse_avodata_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported avodata.labor.gov.il page. "
                "Expected the occupations index: "
                "https://avodata.labor.gov.il/occupations — this tracks "
                "the entire occupation corpus as one dataset. (Per-scope "
                "/search?scope=… pages can't be tracked: the site's scope "
                "filter is backed by a blocked Elasticsearch endpoint.)"
            ),
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title="עבודאטה — מאגר העיסוקים (כל העיסוקים)",
        url=url,
    )
