"""jda.gov.il (הרשות לפיתוח ירושלים) URL validation endpoint.

Mirror of ``app/api/mankal.py``. The Jerusalem Development Authority
publishes three separate WordPress archive pages, each a single
non-paginated listing covering its full history back to 2020:

  - מכרזים (tenders)   — accordion of ~40 tenders, each with several
    attached PDF documents (the invitation, clarifications, forms,
    postponement notices...).
  - הודעות לפי תקנות חובת המכרזים (single-supplier / joint-venture
    notices) — a flat list of ~150 single-PDF notices.
  - החלטות ועדת המכרזים (tenders-committee decisions) — a flat list of
    ~200 single-PDF committee protocols.

Each page is tracked as its own OVER dataset (mirrors mankal's
horaot/hodaot/chozarim split / avodata's occupations/education split).
The corpus is derived from the URL's (Hebrew) path slug.

This module just recognises the three index URL shapes and surfaces a
title for the request form. No live fetch — recognised by shape alone,
same as mankal/avodata.
"""

import logging
from urllib.parse import unquote, urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/jda", tags=["jda"])


JDA_HOSTS = {"jda.gov.il", "www.jda.gov.il"}

# WordPress permalink slugs (decoded, no leading/trailing slash), each its
# own trackable index page / OVER dataset.
_CORPUS_BY_SLUG = {
    "מכרזיםפנימי": "tenders",
    "הודעות-לפי-תקנות-חובת-המכרזים": "notices",
    "החלטות-ועדת-המכרזים": "decisions",
}
JDA_CORPORA = ("tenders", "notices", "decisions")

# Hebrew titles surfaced on the request form, per corpus.
JDA_TITLES = {
    "tenders": "הרשות לפיתוח ירושלים — מכרזים",
    "notices": "הרשות לפיתוח ירושלים — הודעות לפי תקנות חובת המכרזים",
    "decisions": "הרשות לפיתוח ירושלים — החלטות ועדת המכרזים",
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


def corpus_of(url: str) -> str | None:
    """Return the corpus (``tenders`` / ``notices`` / ``decisions``) for a
    trackable jda.gov.il URL, or ``None``.

    The path segment is Hebrew — ``unquote()`` first so both a
    percent-encoded (typical browser copy-paste) and an already-decoded
    (raw Hebrew typed into a JSON body) path match the same lookup.
    """
    if not url:
        return None
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in JDA_HOSTS:
        return None
    path = unquote(parsed.path or "").strip("/")
    return _CORPUS_BY_SLUG.get(path)


def _parse_jda_url(url: str) -> tuple[str | None, str | None]:
    """Parse a jda.gov.il URL.

    Returns ``("jda_<corpus>", "jda-<corpus>")`` for one of the three
    trackable index pages, or ``(None, None)`` otherwise. The page_type
    matches ``startswith("jda_")``, keeping the dispatch switch in
    ``datasets.py`` symmetric with the existing ``avodata_`` / ``mankal_``
    prefixes.
    """
    corpus = corpus_of(url)
    if not corpus:
        return None, None
    return f"jda_{corpus}", f"jda-{corpus}"


def corpus_of_page_type(page_type: str) -> str:
    """Map a jda page_type (``jda_tenders`` etc.) to its engine corpus
    name. Defaults to ``tenders``."""
    corpus = (page_type or "").split("_", 1)[1] if "_" in (page_type or "") else ""
    return corpus if corpus in JDA_CORPORA else "tenders"


# (max_depth, max_docs). max_depth is nominal — each corpus is a single
# static page, not a paginated walk. Corpus sizes are ~40 / ~150 / ~200;
# 1000 is a generous margin that still surfaces a truncation marker if the
# site grows dramatically.
JDA_DEFAULT_LIMITS: tuple[int, int] = (1, 1000)


def get_jda_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for a jda page_type. Same default
    for every corpus — kept as a function for symmetry with the other
    parsers so ``datasets.py`` calls them all the same way."""
    return JDA_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_jda_url(request: Request, body: ValidateRequest):
    """Validate a jda.gov.il tenders-domain index URL.

    No live fetch — the three accepted URLs are recognised by shape alone.
    """
    url = body.url.strip()

    page_type, slug = _parse_jda_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported jda.gov.il page. Track a whole "
                "category as its own dataset by pasting one of the three "
                "archive pages: מכרזים, הודעות לפי תקנות חובת המכרזים, or "
                "החלטות ועדת המכרזים (see jda.gov.il navigation)."
            ),
        )

    corpus = corpus_of_page_type(page_type)
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=JDA_TITLES.get(corpus, JDA_TITLES["tenders"]),
        url=url,
    )
