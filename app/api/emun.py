"""govextra.gov.il/pmo/emun URL validation endpoint — "מערכת אמו״ן".

Mirror of ``app/api/munidata.py`` / ``app/api/avodata.py``. אמו״ן is the
Prime Minister's Office (אגף ממשל וחברה) public dashboard for follow-up on
the implementation of government decisions: how many decisions and tasks
each government produced, what share each ministry completed, which
barriers held tasks up, and how budget-linked tasks fared — from 2015
onwards.

The whole mini-site is a single page whose body is one embedded **Looker
Studio report** over BigQuery. There are no per-item pages, no HTML tables
and no CSV export, so there is nothing to slice into several datasets:
the trackable URL is the dashboard itself and it maps to ONE OVER
dataset::

    https://govextra.gov.il/pmo/emun/home/

Any path under ``/pmo/emun`` is accepted and canonicalised to that URL —
the site's other paths are chrome around the same report.

The GOVSCRAPER worker (``govscraper.scrapers.emun``) reads the report's
published configuration and pulls every data component of the four visible
pages, emitting one tidy row per value cell plus a per-component CSV.
``/validate`` needs no live fetch: the URL shape alone identifies the
dashboard.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/emun", tags=["emun"])


EMUN_HOSTS = {"govextra.gov.il", "www.govextra.gov.il"}
# The mini-site root. /home/ is the landing page; other paths under
# /pmo/emun are the same dashboard's chrome.
EMUN_PATH_RE = re.compile(r"^/pmo/emun(?:/|$)", re.IGNORECASE)

EMUN_CANONICAL_URL = "https://govextra.gov.il/pmo/emun/home/"
EMUN_TITLE = 'מערכת אמו"ן — מעקב אחר יישום החלטות הממשלה (משרד ראש הממשלה)'


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_emun_url(url: str) -> tuple[str | None, str | None]:
    """Parse a govextra.gov.il/pmo/emun URL.

    Returns ``("emun_dashboard", "emun-dashboard")`` for any path under
    ``/pmo/emun`` on govextra.gov.il, and ``(None, None)`` otherwise —
    including other govextra mini-sites, which are unrelated dashboards.

    The page_type matches ``startswith("emun_")``, keeping the dispatch
    switch in ``datasets.py`` symmetric with the existing ``health_`` /
    ``avodata_`` / ``munidata_`` prefixes.
    """
    try:
        parsed = urlparse((url or "").strip())
    except ValueError:
        return None, None
    if (parsed.hostname or "").lower() not in EMUN_HOSTS:
        return None, None
    if not EMUN_PATH_RE.match(parsed.path or ""):
        return None, None
    return "emun_dashboard", "emun-dashboard"


# (max_depth, max_docs). Both are nominal here: the engine walks the
# report's own component list rather than paging a corpus. max_docs caps
# the number of data components pulled — the report currently has ~120 on
# its visible pages, so 400 leaves generous room for it to grow while
# still bounding a runaway.
EMUN_DEFAULT_LIMITS: tuple[int, int] = (1, 400)


def get_emun_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for an emun page_type.

    Single dataset, so there is nothing to vary — always the default. Kept
    as a function for symmetry with the other parsers so ``datasets.py``
    calls them all the same way.
    """
    return EMUN_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_emun_url(request: Request, body: ValidateRequest):
    """Validate a govextra.gov.il/pmo/emun URL.

    No live fetch — the dashboard is recognised by URL shape alone, and
    its content is behind a Looker Studio embed that would cost several
    RPCs to probe.
    """
    url = body.url.strip()

    page_type, slug = _parse_emun_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported מערכת אמו\"ן page. Expected the PMO "
                "government-decision follow-up dashboard: "
                "https://govextra.gov.il/pmo/emun/home/ — the whole dashboard "
                "is tracked as one dataset."
            ),
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=EMUN_TITLE,
        url=EMUN_CANONICAL_URL,
    )
