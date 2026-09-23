"""Public read API for שקיפות מחירים — every retail chain's prices, as one market.

The chains are tracked as separate datasets (one per retailer, see
app/services/prices_query.py); these routes query across all of them.

Endpoints (all public, rate-limited):
    GET  /api/prices/chains                     the chains, their store counts and freshness
    GET  /api/prices/products?q=…               find items by name or barcode, across chains
    GET  /api/prices/compare?item_code=…        current price of item(s) at every chain
    POST /api/prices/basket                     a basket's total at each store, cheapest first
    GET  /api/prices/stores?city=…              stores by city / name / chain
    GET  /api/prices/store/{chain}/{store_id}   what one store charges now
    GET  /api/prices/history?item_code=…        every price state of an item
    GET  /api/prices/promotions?item_code=…     promotions covering an item

``chains`` everywhere takes a comma-separated list of chain keys
(``shufersal:shufersal``), bare accounts (``ramilevi``) or names (``רמי לוי``).
Every answer carries ``caveats`` — properties of the published data that a
consumer must not forget.
"""
# No ``from __future__ import annotations`` here: every route is wrapped by
# slowapi, and FastAPI resolves string annotations against the wrapper's
# globals — see tests/test_route_annotations.py.
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import prices_query as pq

router = APIRouter(prefix="/api/prices", tags=["prices"])


def _chains(value: str | None) -> list[str] | None:
    items = [c.strip() for c in (value or "").split(",") if c.strip()]
    return items or None


async def _run(coro):
    try:
        data = await coro
    except pq.NotCollectedYet as e:
        raise HTTPException(status_code=503, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {**data, "caveats": pq.CAVEATS, "source": pq.SOURCE_PAGE,
            "processed": False}


@router.get("/chains")
@limiter.limit("60/minute")
async def chains(request: Request, db: AsyncSession = Depends(get_db)):
    async def go():
        return {"chains": await pq.list_chains(db)}
    return await _run(go())


@router.get("/products")
@limiter.limit("60/minute")
async def products(request: Request,
                   q: str = Query(..., min_length=2, max_length=100,
                                  description="שם מוצר (מילים) או ברקוד"),
                   chains: str | None = Query(None, max_length=500),
                   limit: int = Query(50, ge=1, le=200),
                   db: AsyncSession = Depends(get_db)):
    return await _run(pq.search_products(db, q=q, chains_filter=_chains(chains), limit=limit))


@router.get("/compare")
@limiter.limit("60/minute")
async def compare(request: Request,
                  item_code: str = Query(..., max_length=900,
                                         description="ברקוד, או כמה מופרדים בפסיק"),
                  city: str | None = Query(None, max_length=60),
                  chains: str | None = Query(None, max_length=500),
                  top: int = Query(5, ge=1, le=50),
                  db: AsyncSession = Depends(get_db)):
    codes = [c.strip() for c in item_code.split(",") if c.strip()]
    return await _run(pq.compare_prices(db, item_codes=codes, city=city,
                                        chains_filter=_chains(chains), top=top))


class BasketLine(BaseModel):
    item_code: str = Field(..., max_length=20)
    quantity: float = Field(1, gt=0, le=1000)


class BasketBody(BaseModel):
    items: list[BasketLine] = Field(..., min_length=1, max_length=40)
    city: str | None = Field(None, max_length=60)
    chains: list[str] | None = Field(None, max_length=40)
    top: int = Field(10, ge=1, le=50)


@router.post("/basket")
@limiter.limit("30/minute")
async def basket(request: Request, body: BasketBody, db: AsyncSession = Depends(get_db)):
    return await _run(pq.compare_basket(
        db, basket=[line.model_dump() for line in body.items], city=body.city,
        chains_filter=body.chains, top=body.top))


@router.get("/stores")
@limiter.limit("60/minute")
async def stores(request: Request,
                 city: str | None = Query(None, max_length=60),
                 q: str | None = Query(None, max_length=60, description="שם סניף או כתובת"),
                 chains: str | None = Query(None, max_length=500),
                 limit: int = Query(100, ge=1, le=1000),
                 db: AsyncSession = Depends(get_db)):
    return await _run(pq.find_stores(db, city=city, q=q, chains_filter=_chains(chains),
                                     limit=limit))


@router.get("/store/{chain}/{store_id}")
@limiter.limit("60/minute")
async def store(request: Request, chain: str, store_id: str,
                q: str | None = Query(None, max_length=100),
                limit: int = Query(100, ge=1, le=200),
                offset: int = Query(0, ge=0, le=200000),
                db: AsyncSession = Depends(get_db)):
    return await _run(pq.store_prices(db, chain=chain, store_id=store_id, q=q,
                                      limit=limit, offset=offset))


@router.get("/history")
@limiter.limit("60/minute")
async def history(request: Request,
                  item_code: str = Query(..., max_length=20),
                  chains: str | None = Query(None, max_length=500),
                  store_id: str | None = Query(None, max_length=10),
                  limit: int = Query(200, ge=1, le=2000),
                  db: AsyncSession = Depends(get_db)):
    return await _run(pq.price_history(db, item_code=item_code, chains_filter=_chains(chains),
                                       store_id=store_id, limit=limit))


@router.get("/promotions")
@limiter.limit("60/minute")
async def promotions(request: Request,
                     item_code: str = Query(..., max_length=20),
                     chains: str | None = Query(None, max_length=500),
                     active_only: bool = True,
                     limit: int = Query(100, ge=1, le=200),
                     db: AsyncSession = Depends(get_db)):
    return await _run(pq.item_promotions(db, item_code=item_code,
                                         chains_filter=_chains(chains),
                                         active_only=active_only, limit=limit))
