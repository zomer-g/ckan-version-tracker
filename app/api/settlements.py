"""Public read-only API over the settlement (יישוב) reference index.

The processed CBS-based index of official locality names + their inflections,
built by app/services/settlement_index.py. Also fully queryable as SQL on the
/data console (tables public.over_settlements + public.over_settlement_aliases).
"""
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from app.rate_limit import limiter
from app.services import append_store, settlement_index

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/settlements", tags=["settlements"])

# Hard caps for the batch normalizer — bound the work so a pasted list can't be
# used to exhaust the DB or memory.
_MAX_NAMES = 5000
_MAX_NAME_LEN = 120


class BatchBody(BaseModel):
    names: list[str] = []


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


@router.post("/resolve-batch")
@limiter.limit("20/minute")
async def resolve_batch(request: Request, body: BatchBody):
    """Resolve a LIST of locality names to their official names — for the
    paste-a-list normalizer. Public, read-only, and stateless: the pasted names
    are resolved and returned, never stored or logged. Safe by construction —
    inputs are normalized in Python and matched with a parameterized ``= ANY(...)``
    lookup (no SQL-injection surface) — and bounded (max names × length) so it
    can't be used to exhaust the DB."""
    _require_enabled()
    raw = body.names or []
    if not isinstance(raw, list):
        raise HTTPException(status_code=422, detail="names must be a list of strings")
    if len(raw) > _MAX_NAMES:
        raise HTTPException(status_code=413, detail=f"too many names (max {_MAX_NAMES})")
    names = [str(n)[:_MAX_NAME_LEN] for n in raw]
    results = await settlement_index.resolve_many(names)
    matched = sum(1 for r in results if r["matched"])
    return {"total": len(results), "matched": matched, "results": results}


@router.get("/{code}")
@limiter.limit("60/minute")
async def get_settlement(request: Request, code: int):
    """One settlement by CBS code (סמל יישוב), including all its known aliases."""
    _require_enabled()
    rec = await settlement_index.get(code)
    if not rec:
        raise HTTPException(status_code=404, detail="Unknown settlement code")
    return rec
