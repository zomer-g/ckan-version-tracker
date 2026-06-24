"""geo.mot.gov.il (חצב) URL validation endpoint.

Mirror of ``app/api/avodata.py``. חצב is the Ministry of Transport's
public GovMap-based map viewer. It isn't a data API of its own — it is a
single-page map application whose entire layer catalog ships as two
static JS files (``DataNew.js`` / ``LayersInformationNew.js``). Each
layer carries its maintaining body, an update quarter (e.g. "רבעון 1
2026"), a description, a downloadable flag, and — for the downloadable
ones — links to the layer's actual feature data hosted on
``data.gov.il`` (shp / kml / csv / metadata).

The actual feature data of any single layer is already trackable by
pasting its ``data.gov.il`` dataset URL (OVER is a CKAN tracker). So
what חצב uniquely adds is the **catalog itself**: the user registers
``https://geo.mot.gov.il/`` and the external scraper fetches the two JS
files, parses every layer, and emits one row per layer plus the
layer's data.gov.il files as version attachments. A version diff then
surfaces when MOT adds/removes a layer, bumps an update quarter,
rewrites a description, or relinks a dataset.

There is no per-layer URL (the layer is selected via in-app UI state),
so — like avodata's ``/occupations`` — the whole portal is tracked as
ONE dataset. This module just recognises the portal-root URL shape and
surfaces a title for the request form.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/hatzav", tags=["hatzav"])


HATZAV_HOSTS = {"geo.mot.gov.il"}
# Only the portal root is trackable — the whole layer catalog is one
# OVER dataset. Accept "/", "" and the explicit index document; reject
# any deeper path (the viewer has no real per-layer routes).
HATZAV_ROOT_RE = re.compile(r"^/?(?:index\.html?|default\.aspx?)?/?$", re.IGNORECASE)


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_hatzav_url(url: str) -> tuple[str | None, str | None]:
    """Parse a geo.mot.gov.il URL.

    Returns:
      - ``("hatzav_catalog", "hatzav-catalog")`` for the portal root.
      - ``(None, None)`` for everything else (wrong host, deeper paths).

    Compatibility:
      - the page_type matches ``startswith("hatzav_")``, keeping the
        dispatch switch in ``datasets.py`` symmetric with the existing
        ``idf_`` / ``health_`` / ``avodata_`` prefixes.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in HATZAV_HOSTS:
        return None, None
    path = parsed.path or ""
    if HATZAV_ROOT_RE.match(path):
        return "hatzav_catalog", "hatzav-catalog"
    return None, None


# (max_depth, max_docs). max_depth is nominal — the scraper reads a flat
# JS catalog. The catalog has ~180 named layers today; 2000 is a
# generous margin that still surfaces a truncation marker if it grows.
HATZAV_DEFAULT_LIMITS: tuple[int, int] = (3, 2000)


def get_hatzav_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a hatzav page_type.

    Single dataset, so there's nothing to vary per-page_type — always
    the default. Kept as a function for symmetry with the avodata /
    health parsers so ``datasets.py`` calls all of them the same way.
    """
    return HATZAV_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_hatzav_url(request: Request, body: ValidateRequest):
    """Validate a geo.mot.gov.il (חצב) portal URL.

    No live fetch — the only accepted URL is the portal root, which we
    recognise by shape alone.
    """
    url = body.url.strip()

    page_type, slug = _parse_hatzav_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported geo.mot.gov.il (חצב) page. "
                "Expected the portal root: https://geo.mot.gov.il/ — the "
                "whole layer catalog is tracked as a single dataset (the "
                "viewer has no per-layer URLs). To track one layer's actual "
                "feature data, register its data.gov.il dataset directly."
            ),
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title="חצב — קטלוג שכבות המידע המרחבי (משרד התחבורה)",
        url=url,
    )
