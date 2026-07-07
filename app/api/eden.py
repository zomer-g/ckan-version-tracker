"""jeden.co.il (חברת עדן) URL validation endpoint.

Mirror of ``app/api/jda.py``. Eden (the Jerusalem municipal development
company) publishes ALL its categories as tabs on ONE custom-theme
WordPress page — the five ``.swiper-slide.info-category`` tabs share a
single URL. We track the two procurement tabs, each as its own OVER
dataset, selected by a ``?category=`` marker on the URL:

  - ``?category=tenders``   → מכרזים (one item per tender + fan-out docs)
  - ``?category=decisions`` → החלטות וועדת מכרזים (one row per protocol)

Because the tabs share one page, the ``?category=`` convention is what lets
each become a distinct dataset (a bare URL is ambiguous → rejected with
guidance). Runs in the shared incremental-archive mode (archive_type=eden).

This module just recognises the URL shape and surfaces a title for the
request form. No live fetch.
"""

import logging
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter, Request
from pydantic import BaseModel

from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/eden", tags=["eden"])


EDEN_HOSTS = {"jeden.co.il", "www.jeden.co.il"}
EDEN_CORPORA = ("tenders", "decisions")

# Hebrew titles surfaced on the request form, per corpus.
EDEN_TITLES = {
    "tenders": "חברת עדן — מכרזים",
    "decisions": "חברת עדן — החלטות ועדת מכרזים",
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
    """Return ``tenders`` / ``decisions`` for a trackable jeden.co.il URL, or
    ``None``. The corpus comes from a ``?category=`` marker (the two tabs
    share one page, so this is what distinguishes the two datasets)."""
    if not url:
        return None
    parsed = urlparse(url.strip())
    if (parsed.hostname or "").lower() not in EDEN_HOSTS:
        return None
    cat = (parse_qs(parsed.query or "").get("category") or [""])[0].strip().lower()
    return cat if cat in EDEN_CORPORA else None


def _parse_eden_url(url: str) -> tuple[str | None, str | None]:
    """Parse a jeden.co.il URL.

    Returns ``("eden_<corpus>", "eden-<corpus>")`` for a URL carrying a valid
    ``?category=`` marker, or ``(None, None)`` otherwise. The page_type
    matches ``startswith("eden_")`` so the dispatch switch in ``datasets.py``
    stays symmetric with the ``jda_`` / ``mankal_`` prefixes.
    """
    corpus = corpus_of(url)
    if not corpus:
        return None, None
    return f"eden_{corpus}", f"eden-{corpus}"


def corpus_of_page_type(page_type: str) -> str:
    """Map an eden page_type (``eden_tenders`` etc.) to its engine corpus
    name. Defaults to ``tenders``."""
    corpus = (page_type or "").split("_", 1)[1] if "_" in (page_type or "") else ""
    return corpus if corpus in EDEN_CORPORA else "tenders"


# (max_depth, max_docs). max_depth is nominal — the corpus is one static
# page. Sizes are ~40 tenders / ~160 protocols; 2000 is a generous margin.
EDEN_DEFAULT_LIMITS: tuple[int, int] = (1, 2000)


def get_eden_limits(page_type: str) -> tuple[int, int]:
    """Return ``(max_depth, max_docs)`` for an eden page_type. Same default
    for every corpus — kept as a function for symmetry with the other
    parsers so ``datasets.py`` calls them all the same way."""
    return EDEN_DEFAULT_LIMITS


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("10/minute")
async def validate_eden_url(request: Request, body: ValidateRequest):
    """Validate a jeden.co.il tenders/decisions URL. No live fetch — the
    corpus is read from the ``?category=`` marker."""
    url = body.url.strip()

    page_type, slug = _parse_eden_url(url)
    if not page_type or not slug:
        return ValidateResponse(
            valid=False,
            error=(
                "URL is not a supported jeden.co.il (חברת עדן) page. The "
                "tenders and committee-decisions tabs share one page, so add "
                "a ?category= marker to pick which to track: append "
                "'?category=tenders' for מכרזים or '?category=decisions' for "
                "החלטות ועדת מכרזים. Each becomes its own tracked dataset."
            ),
        )

    corpus = corpus_of_page_type(page_type)
    return ValidateResponse(
        valid=True,
        page_type=page_type,
        collector_name=slug,
        title=EDEN_TITLES.get(corpus, EDEN_TITLES["tenders"]),
        url=url,
    )
