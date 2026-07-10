"""knesset.gov.il committee-protocols URL validation endpoint.

Each OVER dataset is one *committee*, tracked from the Knesset's open
ODATA-v4 feed (``https://knesset.gov.il/OdataV4/ParliamentInfo``). Committee
protocols live across three tables — ``KNS_Committee`` (one row per committee
per Knesset; ``CategoryID`` is the persistent identity), ``KNS_CommitteeSession``
and ``KNS_DocumentCommitteeSession`` (protocols = ``GroupTypeID 23``, direct
files on ``fs.knesset.gov.il``).

The trackable URL is an honest ODATA query against ``KNS_Committee`` that
returns exactly the committee being tracked:

    https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=CategoryID eq 2   # ועדת הכספים (all Knessets)
    https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=Id eq 4187          # a single committee

``CategoryID eq N`` → the persistent committee across all Knessets (incl. its
sub-committees sharing the category); ``Id eq N`` → a single committee (used
for one-off inquiry/joint committees that carry no category). Unlike
practitioners/idf, the feed is fully open, so ``/validate`` probes it live for
the real committee name.

The actual scrape runs in the external govil-scraper worker
(``govscraper.scrapers.knesset``), which dispatches on
``scraper_config.kind == "knesset"``.
"""

import logging
import re
from urllib.parse import unquote, urlparse

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knesset", tags=["knesset"])


KNESSET_HOSTS = {"knesset.gov.il"}
# Anchored at the KNS_Committee entity set (lower-cased for the compare).
_ODATA_PATH = "/odatav4/parliamentinfo/kns_committee"
KNESSET_ODATA_BASE = "https://knesset.gov.il/OdataV4/ParliamentInfo"

# Tolerant of surrounding whitespace / other clauses / percent-encoding
# (the query is unquoted before matching). ``\bId`` avoids false-matching
# the ``ID`` tail of ``CommitteeTypeID`` / ``CategoryID``.
_CATEGORY_RE = re.compile(r"CategoryID\s+eq\s+(\d+)", re.IGNORECASE)
_ID_RE = re.compile(r"\bId\s+eq\s+(\d+)", re.IGNORECASE)


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None


def committee_scope_of(url: str) -> tuple[str, int] | None:
    """Return ``("category", N)`` or ``("single", N)`` for an in-scope
    ``KNS_Committee`` query URL, else ``None``. Mirrors the worker engine's
    classifier (``govscraper.scrapers.knesset._engine.committee_scope_of``);
    the backend copy is authoritative for what OVER accepts."""
    if not url:
        return None
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in KNESSET_HOSTS:
        return None
    if (parsed.path or "").lower().rstrip("/") != _ODATA_PATH:
        return None
    query = unquote(parsed.query or "")
    m = _CATEGORY_RE.search(query)
    if m:
        return ("category", int(m.group(1)))
    m = _ID_RE.search(query)
    if m:
        return ("single", int(m.group(1)))
    return None


def _parse_knesset_url(url: str) -> tuple[str | None, str | None]:
    """Parse a knesset.gov.il committee URL.

    Returns ``("knesset_committee:<N>", "knesset-committee-cat-<N>")`` for a
    ``CategoryID`` scope, ``("knesset_committee_single:<N>",
    "knesset-committee-single-<N>")`` for an ``Id`` scope, or ``(None, None)``.

    Both page_types match ``startswith("knesset_")`` so the dispatch switch in
    ``datasets.py`` works the same way it does for ``idf_`` / ``health_``.
    """
    scope = committee_scope_of(url)
    if scope is None:
        return None, None
    kind, n = scope
    if kind == "category":
        return f"knesset_committee:{n}", f"knesset-committee-cat-{n}"
    return f"knesset_committee_single:{n}", f"knesset-committee-single-{n}"


def scope_of_page_type(page_type: str) -> tuple[str, int] | None:
    """Recover the committee scope from a ``knesset_committee[...]:<N>``
    page_type — used by ``datasets.py`` to stamp category/committee ids into
    scraper_config so the worker needn't re-parse the URL."""
    if not page_type or ":" not in page_type:
        return None
    head, _, tail = page_type.partition(":")
    try:
        n = int(tail)
    except ValueError:
        return None
    if head == "knesset_committee":
        return ("category", n)
    if head == "knesset_committee_single":
        return ("single", n)
    return None


# (max_depth, max_docs). max_depth is nominal (the engine paginates ODATA, it
# doesn't recurse). ועדת הכספים — the largest committee — has ~13k protocols
# across all Knessets, so 100k is a generous cap that still surfaces a
# truncation marker if a committee somehow exceeds it.
KNESSET_DEFAULT_LIMITS: tuple[int, int] = (3, 100_000)


def get_knesset_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a knesset page_type. Single cap for
    all committees; kept as a function for symmetry with the other parsers."""
    return KNESSET_DEFAULT_LIMITS


async def _probe_committee_name(scope: tuple[str, int]) -> str | None:
    """Live-probe the ODATA feed for the committee's display name (the Name of
    its most-recent instance). Best-effort: any failure returns ``None`` and
    the caller falls back to a generic title. The feed is open (no WAF), so
    this is a cheap single request."""
    kind, n = scope
    field = "CategoryID" if kind == "category" else "Id"
    url = (
        f"{KNESSET_ODATA_BASE}/KNS_Committee"
        f"?$filter={field} eq {n}"
        f"&$select=Name,KnessetNum&$orderby=KnessetNum desc&$top=1"
    )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"Accept": "application/json"})
            resp.raise_for_status()
            rows = (resp.json() or {}).get("value") or []
            if rows:
                name = re.sub(r"\s+", " ", (rows[0].get("Name") or "").strip())
                return name or None
    except Exception as e:  # noqa: BLE001
        logger.info("knesset committee-name probe failed for %s=%s: %s", kind, n, e)
    return None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_knesset_url(request: Request, body: ValidateRequest):
    """Validate a knesset.gov.il committee URL and surface the committee name.

    The ODATA feed is open, so we probe it for the real committee name; if the
    probe fails we still validate the URL and return a generic Hebrew title.
    """
    url = body.url.strip()

    scope = committee_scope_of(url)
    if scope is None:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported knesset.gov.il committee page. Expected "
                "an ODATA KNS_Committee query with a committee scope, e.g. "
                "https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_Committee?$filter=CategoryID eq 2 "
                "(a committee across all Knessets — כאן ועדת הכספים) or "
                "?$filter=Id eq 4187 (a single committee). Each committee is "
                "tracked as its own dataset."
            ),
        )

    page_type, slug = _parse_knesset_url(url)
    name = await _probe_committee_name(scope)
    if name:
        title = f"{name} — פרוטוקולי ועדה"
    else:
        kind, n = scope
        title = (
            f"פרוטוקולי ועדת הכנסת (קטגוריה {n})"
            if kind == "category"
            else f"פרוטוקולי ועדת הכנסת (ועדה {n})"
        )

    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=title,
        url=url,
    )
