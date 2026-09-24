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

    GET  /api/prices/tables                     the six UNIFORM tables and their columns
    GET  /api/prices/table/{name}?<col>=…       rows of one uniform table, every chain
                                                in one schema (app/services/prices_unified.py)

``chains`` everywhere takes a comma-separated list of chain keys
(``shufersal:shufersal``), bare accounts (``ramilevi``) or names (``רמי לוי``).
Every answer carries ``caveats`` — properties of the published data that a
consumer must not forget.
"""
# No ``from __future__ import annotations`` here: every route is wrapped by
# slowapi, and FastAPI resolves string annotations against the wrapper's
# globals — see tests/test_route_annotations.py.
import csv
import io

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import prices_query as pq
from app.services import prices_unified as pu

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
    except TimeoutError as e:
        raise HTTPException(status_code=504, detail=str(e))
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


# ---------------------------------------------------------------------------
# The uniform tables — every chain in one schema
# ---------------------------------------------------------------------------

# Query-string keys that are not column filters.
_RESERVED = {"q", "city", "current", "date", "min_price", "max_price", "columns",
             "order", "limit", "offset", "format"}


@router.get("/tables")
@limiter.limit("60/minute")
async def tables(request: Request):
    """The six uniform tables, their columns, and how to query them."""
    return {"tables": pu.describe(),
            "usage": {
                "endpoint": "/api/prices/table/{name}",
                "column_filters": "כל עמודה בטבלה היא פרמטר: ?chain=…&store_id=…; כמה ערכים "
                                  "מופרדים בפסיק. item_code מתעלם מאפסים מובילים; chain "
                                  "מקבל מפתח, חשבון או שם.",
                "q": "חיפוש חופשי: שם מוצר (ב-prices_market מתורגם לברקודים דרך "
                     "prices_products), שם סניף/כתובת, או תיאור מבצע",
                "city": "שם עיר — מנורמל לסמל יישוב של הלמ\"ס",
                "current": "true (ברירת מחדל) = רק מה שבתוקף עכשיו; false = כל ההיסטוריה",
                "date": "YYYY-MM-DD — מה שהיה בתוקף ביום הזה (גובר על current)",
                "min_price / max_price": "טווח מחיר (item_price או discounted_price)",
                "columns": "אילו עמודות להחזיר, מופרדות בפסיק",
                "order": "עמודות למיון, מופרדות בפסיק; '-' לפני השם = יורד",
                "limit / offset": f"עד {pu.MAX_LIMIT} שורות בעמוד",
                "format": "json (ברירת מחדל) או csv",
                "sql": "אותן טבלאות זמינות בשמן בקונסולת ה-SQL (over.org.il/data) "
                       "ובשרת ה-SQL MCP; כל תשובה מחזירה console_sql — השאילתה המקבילה.",
            },
            "caveats": pq.CAVEATS, "source": pq.SOURCE_PAGE, "processed": False}


@router.get("/table/{name}")
@limiter.limit("60/minute")
async def table(request: Request, name: str,
                q: str | None = Query(None, max_length=100),
                city: str | None = Query(None, max_length=60),
                current: bool = True,
                date: str | None = Query(None, max_length=10,
                                         description="YYYY-MM-DD — המצב שבתוקף ביום הזה"),
                min_price: float | None = Query(None, ge=0),
                max_price: float | None = Query(None, ge=0),
                columns: str | None = Query(None, max_length=1000),
                order: str | None = Query(None, max_length=200),
                limit: int = Query(100, ge=1, le=pu.MAX_LIMIT),
                offset: int = Query(0, ge=0, le=pu.MAX_OFFSET),
                format: str = Query("json", pattern="^(json|csv)$"),
                db: AsyncSession = Depends(get_db)):
    """Rows of one uniform table. Every other query parameter is a column filter."""
    filters = {k: v for k, v in request.query_params.items() if k not in _RESERVED}
    cols = [c.strip() for c in (columns or "").split(",") if c.strip()] or None
    data = await _run(pu.query(db, name, filters=filters, q=q, city=city, current=current,
                               on_date=date, min_price=min_price, max_price=max_price,
                               columns=cols, order=order, limit=limit, offset=offset))
    if format == "csv":
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=data["columns"], extrasaction="ignore")
        w.writeheader()
        w.writerows(data["rows"])
        return Response(content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                        headers={"Content-Disposition": f'attachment; filename="{name}.csv"'})
    return data
