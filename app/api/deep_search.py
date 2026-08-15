"""Public API for "שאלות לעם" — the cross-source deep search (חיפוש רוחבי).

Two endpoints:

* ``GET /api/deep-search/sources`` — the registry: what can be searched, which
  filters each corpus offers, and whether it is configured. The page calls this
  first to draw its source chips.
* ``GET /api/deep-search/search`` — run one query. The page issues ONE request
  per source (so each column paints the moment it lands, without SSE, which
  this app has nowhere), but the endpoint accepts any subset via ``sources``.

Per-source filters arrive with an ``f_`` prefix — ``?f_source_type=ckan``.
They are flat rather than namespaced per source, which is safe precisely
BECAUSE the page sends one source per request; keep that invariant if you ever
change the client.

NOTE: no ``from __future__ import annotations`` — with the slowapi
``@limiter.limit`` wrapper it stringifies the endpoint hints and FastAPI then
mis-reads parameters. Same trap as nl_query.py / cbs_ask.py.
"""
import logging

from fastapi import APIRouter, HTTPException, Query, Request

from app.config import settings
from app.rate_limit import limiter
from app.services import deep_search, deep_search_sources

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/deep-search", tags=["deep-search"])

MAX_QUERY_CHARS = 200


def _require_enabled() -> None:
    if not settings.deep_search_enabled:
        raise HTTPException(status_code=503, detail="החיפוש הרוחבי מושבת בשרת")


def _parse_filters(request: Request) -> dict:
    """Pull the ``f_<id>`` query params into a plain {id: value} dict."""
    return {
        k[2:]: v
        for k, v in request.query_params.items()
        if k.startswith("f_") and v is not None and str(v).strip() != ""
    }


@router.get("/sources")
@limiter.limit("60/minute")
async def list_sources(request: Request):
    """The registry the page renders its chips and filter boxes from.

    Only a ``configured`` boolean is exposed — never a token value.
    """
    _require_enabled()
    out = [s.as_dict(configured=deep_search.is_configured(s))
           for s in deep_search_sources.active_sources()]
    await _describe_full_text_sources(out)
    return {"sources": out}


async def _describe_full_text_sources(sources: list[dict]) -> None:
    """Let each TAG-IT column state its own coverage, and warm it.

    Two hand-written claims about these corpora have already been wrong — a
    coverage note taken from the first page of one query, and a date filter over
    a corpus that was 4/22,036 dated. The service reports doc_count, date range
    and has_dates per scope, so the label comes from the number and the date
    filter is withdrawn when the corpus cannot honour it.

    Warming happens here because this is the request that precedes a search: it
    is the only moment we know a query is coming. Best-effort throughout — if
    TAG-IT is unreachable the static hints stand and the page is unchanged.
    """
    from app.config import settings
    from app.services import tagit_meta

    by_scope: dict[int, dict] = {}
    for s, entry in zip(deep_search_sources.active_sources(), sources):
        if not s.scope_setting:
            continue
        try:
            by_scope[int(getattr(settings, s.scope_setting))] = entry
        except (TypeError, ValueError):
            continue
    if not by_scope:
        return

    tagit_meta.warm_in_background(by_scope)
    # Bounded: this request gates the search button, so it must return whether
    # or not TAG-IT is awake. A cold upstream costs the labels on this load, not
    # the page.
    meta = await tagit_meta.scopes(max_wait=tagit_meta.DEFAULT_MAX_WAIT_S)
    for scope_id, entry in by_scope.items():
        m = meta.get(scope_id)
        label = tagit_meta.coverage_label(m)
        if label:
            entry["hint"] = f"{entry['hint']} · {label}" if entry.get("hint") else label
        if tagit_meta.dates_usable(m) is False and entry.get("filters"):
            entry["filters"] = [f for f in entry["filters"]
                                if f["id"] not in ("date_from", "date_to")] or None


@router.get("/search")
@limiter.limit("60/minute")
async def search(
    request: Request,
    q: str = Query(..., description="טקסט חופשי לחיפוש בכל המקורות"),
    sources: str = Query("", description="רשימת מזהי מקורות מופרדת בפסיקים; ריק ⇒ הכול"),
    limit: int = Query(15, ge=1, le=50),
):
    # No `db` dependency on purpose: each local source opens its own session in
    # deep_search._local_caller, because several may run concurrently and an
    # AsyncSession is not safe to share across a gather.
    _require_enabled()
    query = (q or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="חסר טקסט לחיפוש")
    if len(query) > MAX_QUERY_CHARS:
        raise HTTPException(
            status_code=400, detail=f"השאילתה ארוכה מדי (עד {MAX_QUERY_CHARS} תווים)")

    limit = min(int(limit), int(settings.deep_search_max_limit))
    wanted = [s for s in (sources or "").split(",") if s.strip()]
    chosen = deep_search_sources.resolve(wanted)
    if not chosen:
        raise HTTPException(status_code=400, detail="לא נמצאו מקורות תואמים")

    columns = await deep_search.fan_out(
        request, chosen, query, limit, _parse_filters(request))
    return {"query": query, "sources": columns}
