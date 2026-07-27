"""Public read-only API over the settlement (יישוב) reference index.

The processed CBS-based index of official locality names + their inflections,
built by app/services/settlement_index.py. Also fully queryable as SQL on the
/data console (tables public.over_settlements + public.over_settlement_aliases).
"""
import logging

from fastapi import APIRouter, HTTPException, Request

from app.rate_limit import limiter
from app.services import append_store, settlement_index

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/settlements", tags=["settlements"])


def _require_enabled() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append DB is not configured")


@router.get("")
@limiter.limit("60/minute")
async def list_settlements(request: Request, q: str | None = None, limit: int = 25):
    """Search official settlements by name (Hebrew/English/transliteration)."""
    _require_enabled()
    limit = max(1, min(200, limit))
    return {"settlements": await settlement_index.search(q, limit=limit)}


@router.get("/stats")
@limiter.limit("60/minute")
async def settlement_stats(request: Request):
    _require_enabled()
    return await settlement_index.stats()


@router.get("/resolve")
@limiter.limit("120/minute")
async def resolve_settlement(request: Request, q: str):
    """Resolve a free-text locality value to its official settlement (or null).

    This is the same lookup the per-dataset ``Over_Settlement`` columns use —
    normalizes the input and matches it against the inflection index."""
    _require_enabled()
    match = await settlement_index.resolve(q)
    return {"query": q, "match": match}


@router.get("/{code}")
@limiter.limit("60/minute")
async def get_settlement(request: Request, code: int):
    """One settlement by CBS code (סמל יישוב), including all its known aliases."""
    _require_enabled()
    rec = await settlement_index.get(code)
    if not rec:
        raise HTTPException(status_code=404, detail="Unknown settlement code")
    return rec
