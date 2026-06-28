"""mevaker.gov.il (State Comptroller) URL validation endpoint.

Mirror of ``app/api/avodata.py``. The State Comptroller's public audit
reports live in a SharePoint Digital Library at
``library.mevaker.gov.il``; the public site ``www.mevaker.gov.il`` just
links into it. The whole corpus is tracked as ONE dataset: the user
registers ``https://www.mevaker.gov.il/subjects`` and the external
scraper walks every publication volume → audit task (מטלה), one row per
task, downloading each task's PDF + Word documents.

This module recognises the ``/subjects`` URL shape and surfaces a title
for the request form. No live fetch — the URL is recognised by shape.
"""

import logging
import re
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/mevaker", tags=["mevaker"])


MEVAKER_HOSTS = {"www.mevaker.gov.il", "mevaker.gov.il"}
# The trackable index URL — the "דוחות לפי נושאים" landing page.
MEVAKER_SUBJECTS_RE = re.compile(r"^/subjects/?$")

# The whole corpus (~37 GB of PDF+Word) is too big for one dataset, so we
# split it by publication type. The service's ``publicationTypes`` endpoint
# advertises 10 types, but one of them — "דוחות בינלאומיים" (international)
# — has ZERO actual publications in the library (verified 2026-06-28 by
# scanning all ~446 volumes, pages 1-697: 9 of the 10 types are populated,
# international is not). It's a dropdown label with nothing behind it, so we
# DON'T expose it as a trackable type — registering it only produced empty
# versions every poll. The 9 populated types below each map a ``?type=<slug>``
# to the exact Hebrew ``Type`` string the scraper matches against each
# volume. Bare /subjects (no type) still means the whole corpus.
MEVAKER_TYPES: dict[str, str] = {
    "annual": "דוחות שנתיים",
    "special": "דוחות מיוחדים",
    "local-government": "ביקורת על השלטון המקומי",
    "ombudsman": "דוחות נציב תלונות הציבור",
    "unions": "ביקורת על האיגודים",
    "party-funding": "מימון מפלגות",
    "primaries-funding": "מימון בחירות מקדימות (פריימריז)",
    "local-elections-funding": "מימון בחירות ברשויות המקומיות",
    "studies": "עיונים, מאמרים, ספרים",
}


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def _parse_mevaker_url(url: str) -> tuple[str | None, str | None]:
    """Parse a mevaker.gov.il URL.

    Returns:
      - ``("mevaker_reports:<slug>", "mevaker-<slug>")`` for a per-type
        ``/subjects?type=<slug>`` URL (the recommended split),
      - ``("mevaker_reports", "mevaker-reports")`` for bare ``/subjects``
        (the whole corpus),
      - ``(None, None)`` otherwise (incl. an unknown ``type`` slug, so a
        typo can't silently scrape all 37 GB).

    The page_type matches ``startswith("mevaker_")``, keeping the dispatch
    switch in ``datasets.py`` symmetric with the ``avodata_`` / ``health_``
    prefixes.
    """
    s = url.strip()
    parsed = urlparse(s)
    host = (parsed.hostname or "").lower()
    if host not in MEVAKER_HOSTS:
        return None, None
    if not MEVAKER_SUBJECTS_RE.match(parsed.path or ""):
        return None, None
    type_vals = parse_qs(parsed.query or "").get("type")
    if type_vals:
        slug = type_vals[0].strip().lower()
        if slug not in MEVAKER_TYPES:
            return None, None
        return f"mevaker_reports:{slug}", f"mevaker-{slug}"
    return "mevaker_reports", "mevaker-reports"


def type_hebrew_of(page_type: str) -> str | None:
    """Map a ``mevaker_reports:<slug>`` page_type to the Hebrew
    publication-type string the scraper filters on. Bare
    ``mevaker_reports`` (whole corpus) returns None."""
    if page_type and page_type.startswith("mevaker_reports:"):
        return MEVAKER_TYPES.get(page_type.split(":", 1)[1])
    return None


# (max_depth, max_docs). max_depth is nominal. The corpus is a few
# thousand audit tasks; 20000 is a generous ceiling that still surfaces a
# truncation marker if the library grows.
MEVAKER_DEFAULT_LIMITS: tuple[int, int] = (3, 20000)


def get_mevaker_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a mevaker page_type. Single
    dataset, so always the default — kept as a function for symmetry with
    the avodata / health parsers."""
    return MEVAKER_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_mevaker_url(request: Request, body: ValidateRequest):
    """Validate a mevaker.gov.il reports-index URL. No live fetch — the
    only accepted URL is the ``/subjects`` index, recognised by shape."""
    url = body.url.strip()

    page_type, slug = _parse_mevaker_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported mevaker.gov.il page. Expected the "
                "reports index https://www.mevaker.gov.il/subjects with a "
                "publication-type filter, e.g. ?type=annual (the corpus is "
                "split by type — one dataset each). Known types: "
                + ", ".join(MEVAKER_TYPES.keys())
            ),
        )

    hebrew_type = type_hebrew_of(page_type)
    title = (
        f"מבקר המדינה — {hebrew_type}"
        if hebrew_type else "מבקר המדינה — דוחות ביקורת"
    )
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )
