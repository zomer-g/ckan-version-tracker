"""Public source-URL resolver — the backend of the /direct deep link.

    GET /direct/<source url>   → 302 to the versions page, or to the request form
    GET /api/resolve?url=…     → the same answer as JSON

Both ask one question: does OVER already track the thing this URL points at?
A hit carries the dataset's versions page; a miss is the caller's cue to open
the collection request form. Matching is by identity, so a copied link with a
viewport, a ``www.`` or a tracking param still finds the dataset — see
app/services/url_identity.py.
"""

import re

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from urllib.parse import quote

from app.database import get_db
from app.rate_limit import limiter
from app.services.dataset_lookup import find_datasets_for_url

router = APIRouter(prefix="/api/resolve", tags=["resolve"])

# The /direct/<url> form. Not under /api: it is a human/extension-facing link,
# not a data endpoint, and it answers with a redirect rather than JSON.
direct_router = APIRouter(tags=["resolve"])


class ResolveMatch(BaseModel):
    id: str
    title: str
    organization: str | None = None
    source_type: str | None = None
    source_url: str | None = None
    status: str | None = None
    is_active: bool = True
    version_count: int = 0
    # "identity" — this URL and the dataset's URL denote the same thing.
    # "path"     — same page, different query; only ever returned when no
    #              identity matched, and possibly more than one.
    match: str = "identity"


class ResolveResponse(BaseModel):
    url: str
    found: bool
    matches: list[ResolveMatch] = []


@router.get("", response_model=ResolveResponse)
@limiter.limit("60/minute")
async def resolve_source_url(
    request: Request,
    url: str = Query(..., description="A source URL, e.g. a govmap.gov.il layer link"),
    db: AsyncSession = Depends(get_db),
):
    """Look a source URL up in the catalog. Never 404s — ``found: false`` is a
    normal answer meaning "not tracked yet, offer the request form"."""
    cleaned = (url or "").strip()
    matches = await find_datasets_for_url(db, cleaned) if cleaned else []
    return ResolveResponse(
        url=cleaned,
        found=bool(matches),
        matches=[ResolveMatch(**m) for m in matches],
    )


# ── /direct/<source url> ──────────────────────────────────────────────────

def rebuild_source_url(path_part: str, query: str) -> str:
    """Reassemble the source URL from how it survives a URL path.

    ``/direct/https://www.govmap.gov.il/?c=1,2&lay=11`` reaches us split in
    two: the routed path part (``https://www.govmap.gov.il/``) and the request's
    own query string (``c=1,2&lay=11``). Percent-encoding the whole URL instead
    (``/direct/https%3A%2F%2F…``) arrives already decoded in one piece, with an
    empty query — both spellings must rebuild to the same thing so callers can
    use whichever their tooling produces.
    """
    url = (path_part or "").strip().lstrip("/")
    if query:
        # A percent-encoded URL already carries its own "?" — anything after it
        # is a second query fragment, not the start of one.
        url += ("&" if "?" in url else "?") + query
    # Some proxies collapse the "//" in "https://" when it sits inside a path.
    url = re.sub(r"^(https?):/+", r"\1://", url, flags=re.IGNORECASE)
    # A ROUTE FRAGMENT ONLY SURVIVES THE PERCENT-ENCODED SPELLING. Browsers
    # never put anything after "#" on the wire, so a verbatim
    # ``/direct/https://site/#/documents`` reaches us as ``https://site/`` — the
    # route is gone before any of our code runs. Since a hash-routed SPA's route
    # is part of its identity (see url_identity), a caller that wants the exact
    # dataset must percent-encode the URL (``%23`` for the "#"), which arrives
    # here decoded and intact. Without it the answer degrades to the path match:
    # the /lookup chooser listing every dataset cut from that site, which is the
    # right fallback but is not the same as a hit.
    return url


@direct_router.get("/direct/{path_part:path}")
@limiter.limit("60/minute")
async def direct_to_dataset(
    request: Request,
    path_part: str,
    db: AsyncSession = Depends(get_db),
):
    """One stable shape a tool can build mechanically: prefix + source URL.

    Redirects rather than rendering, so a browser extension can hand the link
    to the user without knowing anything about OVER's ids or its catalog:

      tracked            → /versions/<id>        (the versions they came for)
      tracked, ambiguous → /lookup?url=…         (several datasets, they pick)
      not tracked        → /?q=…                 (the collection request form)
    """
    url = rebuild_source_url(path_part, request.url.query)
    matches = await find_datasets_for_url(db, url) if url else []
    # Only an IDENTITY match is somewhere to send someone. A "path" match means
    # "some dataset is cut from this same page", which for a hash-routed SPA is
    # every route of the site — they all share one path. Sending a deep link
    # there lands on a corpus it does not point at, and the request form for the
    # one it DOES point at becomes unreachable: pasting the Jerusalem documents
    # route opened the register's versions page instead. The frontend callers
    # were fixed for this; this redirect is the third and last of them.
    identity = [m for m in matches if m.get("match") == "identity"]

    if len(identity) == 1:
        target = f"/versions/{identity[0]['id']}"
    elif identity:
        target = f"/lookup?url={quote(url, safe='')}"
    elif url:
        # Not tracked, or tracked only by page — the search box, which lists
        # whatever IS collected from this page as "did you mean" and still
        # opens the request form for the URL that was actually asked for.
        target = f"/?q={quote(url, safe='')}"
    else:
        target = "/lookup"
    # 302, not 301: whether a source is tracked changes over time, and a
    # permanent redirect would be cached in browsers long after it stops
    # being true.
    return RedirectResponse(target, status_code=302)
