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
# Trackable URLs, each its own OVER dataset (mirrors avodata's
# /occupations vs /education split). The portal index → the whole corpus;
# the bare per-type paths → one document category each.
MANKAL_ALL_PATHS = {"/mankal", "/mankal/default.aspx", "/mankal/etznosim.aspx"}
MANKAL_CORPUS_BY_PATH = {
    "/mankal/horaa.aspx": "horaot",
    "/mankal/hodaa.aspx": "hodaot",
    "/mankal/chozer.aspx": "chozarim",
}
# Hebrew titles surfaced on the request form, per corpus.
MANKAL_TITLES = {
    "all": "חוזרי מנכ\"ל משרד החינוך — כל החוזרים, ההוראות וההודעות",
    "horaot": "חוזרי מנכ\"ל משרד החינוך — הוראות (הוראות קבע ושעה)",
    "hodaot": "חוזרי מנכ\"ל משרד החינוך — הודעות",
    "chozarim": "חוזרי מנכ\"ל משרד החינוך — חוזרים (עלונים חודשיים)",
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


def _corpus_of_url(url: str) -> str | None:
    """Return the corpus (``all`` / ``horaot`` / ``hodaot`` / ``chozarim``)
    for a trackable Mankal URL, or ``None``. A per-type path carrying a
    ``?siduri=`` is a naked item page → ``None``."""
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in MANKAL_HOSTS:
        return None
    path = (parsed.path or "").lower().rstrip("/")
    if path in {p.rstrip("/") for p in MANKAL_ALL_PATHS}:
        return "all"
    corpus = MANKAL_CORPUS_BY_PATH.get(path)
    if corpus and "siduri=" not in (parsed.query or "").lower():
        return corpus
    return None


def _parse_mankal_url(url: str) -> tuple[str | None, str | None]:
    """Parse an apps.education.gov.il/Mankal URL.

    Returns ``("mankal_<corpus>", "mankal-<corpus>")`` for a trackable URL
    (corpus one of all/horaot/hodaot/chozarim), or ``(None, None)`` for
    everything else (naked ?siduri= item pages, wrong host). The page_type
    matches ``startswith("mankal_")`` so the dispatch switch in
    ``datasets.py`` stays symmetric with the existing ``idf_`` / ``health_``
    / ``avodata_`` prefixes.
    """
    corpus = _corpus_of_url(url)
    if not corpus:
        return None, None
    return f"mankal_{corpus}", f"mankal-{corpus}"


def corpus_of_page_type(page_type: str) -> str:
    """Map a mankal page_type (``mankal_horaot`` etc.) to its engine
    corpus name. Defaults to ``all``."""
    corpus = (page_type or "").split("_", 1)[1] if "_" in (page_type or "") else ""
    return corpus if corpus in ("all", "horaot", "hodaot", "chozarim") else "all"


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
                "URL is not a supported חוזרי מנכ\"ל page. Track a whole "
                "category as its own dataset by pasting one of: "
                "https://apps.education.gov.il/Mankal/default.aspx (all), "
                "https://apps.education.gov.il/Mankal/Horaa.aspx (הוראות), "
                "https://apps.education.gov.il/Mankal/Hodaa.aspx (הודעות), or "
                "https://apps.education.gov.il/Mankal/Chozer.aspx (חוזרים). "
                "Individual …?siduri=… item pages can't be tracked on their own."
            ),
        )

    corpus = corpus_of_page_type(page_type)
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=MANKAL_TITLES.get(corpus, MANKAL_TITLES["all"]),
        url=url,
    )
