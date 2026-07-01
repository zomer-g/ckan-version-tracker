"""חוזרי מנכ"ל משרד החינוך URL validation endpoint.

Mirror of ``app/api/avodata.py``. apps.education.gov.il/Mankal is the
Ministry of Education's Director-General Circulars portal. The whole
corpus is tracked as ONE dataset: the user registers the portal index
(``default.aspx`` / the ``/Mankal`` root / ``EtzNosim.aspx``) and the
external scraper walks the three ``?siduri=`` sequences (Horaa / Hodaa /
Chozer), emitting one row per item plus fan-out rows for prior versions
and attachments.

This module just recognises the index URL shape and surfaces a title for
the request form.
"""

import logging
import re
from urllib.parse import urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/mankal", tags=["mankal"])


MANKAL_HOSTS = {"apps.education.gov.il"}
# The trackable index paths (case-insensitive, trailing slash optional).
MANKAL_INDEX_PATHS = {"/mankal", "/mankal/default.aspx", "/mankal/etznosim.aspx"}


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_mankal_url(url: str) -> tuple[str | None, str | None]:
    """Parse an apps.education.gov.il/Mankal URL.

    Returns ``("mankal_all", "mankal-all")`` for the portal index, or
    ``(None, None)`` for everything else (naked ?siduri= item pages,
    wrong host). The page_type matches ``startswith("mankal_")`` so the
    dispatch switch in ``datasets.py`` stays symmetric with the existing
    ``idf_`` / ``health_`` / ``avodata_`` prefixes.
    """
    parsed = urlparse(url.strip())
    host = (parsed.hostname or "").lower()
    if host not in MANKAL_HOSTS:
        return None, None
    path = (parsed.path or "").lower().rstrip("/")
    if path in {p.rstrip("/") for p in MANKAL_INDEX_PATHS}:
        return "mankal_all", "mankal-all"
    return None, None


# (max_depth, max_docs). max_depth is nominal — the scraper walks flat
# ?siduri= sequences. The full corpus is ~1,100 items; 5000 is a generous
# margin that still surfaces a truncation marker if the site grows.
MANKAL_DEFAULT_LIMITS: tuple[int, int] = (3, 5000)


def get_mankal_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a mankal page_type. Single
    dataset, so always the default — kept as a function for symmetry with
    the other parsers so ``datasets.py`` calls them all the same way."""
    return MANKAL_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_mankal_url(request: Request, body: ValidateRequest):
    """Validate an apps.education.gov.il/Mankal index URL.

    No live fetch — the accepted URL is the portal index, recognised by
    shape alone. Naked item pages are rejected with guidance.
    """
    url = body.url.strip()

    page_type, slug = _parse_mankal_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported חוזרי מנכ\"ל page. Expected the "
                "portal index: https://apps.education.gov.il/Mankal/default.aspx "
                "(the whole corpus is tracked as one dataset — the scraper "
                "walks every הוראה / הודעה / חוזר). Individual "
                "Horaa/Hodaa/Chozer.aspx?siduri=… pages can't be tracked on "
                "their own."
            ),
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title="חוזרי מנכ\"ל משרד החינוך — כל החוזרים, ההוראות וההודעות",
        url=url,
    )
