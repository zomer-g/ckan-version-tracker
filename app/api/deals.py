"""Public read API for "עסקאות נדל"ן" — the מיסוי מקרקעין deal register.

The register itself is published one גוש at a time behind a form. This serves
the same rows as a queryable corpus: filter, page, sort, and two aggregates
(per year, per deal type) computed under the SAME filter as the listing, so a
chart can never describe a different population than the table beside it.

Endpoints (all public, rate-limited):
    GET /api/deals/stats                   hero counters + the source dataset
    GET /api/deals/search?…                the browse, filtered and paged
    GET /api/deals/series?…                deals + median price per year
    GET /api/deals/breakdown?…             deals + median price per deal type
    GET /api/deals/compare?…               two years, every settlement, side by side
    GET /api/deals/settlements             every settlement, with its deal count
    GET /api/deals/natures                 the deal types, with their counts
    GET /api/deals/parcel/{gush}/{helka}   one parcel's deals, newest first

Conventions follow app/api/nadlan.py: ``request: Request`` first (slowapi needs
it), an explicit ``@limiter.limit`` on every route, ``_require_ready()`` → 503
before the register has been scraped, and ``processed``/``caveats`` travelling
with the data rather than being left for the UI to remember.

Unlike נדל"ן לעם this data is NOT derived: it is one publisher's rows, passed
through. ``processed`` is therefore false, and the caveats are about what the
register itself does and does not record.

The filter is ONE dependency shared by /search, /series and /breakdown. Those
three are three views of a single population, and a parameter that existed on
only one of them would quietly let them disagree.
"""
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from app.rate_limit import limiter
from app.services import deals_query, nadlan_query

router = APIRouter(prefix="/api/deals", tags=["deals"])

# Each is a property of the register, measured on all 3.84 M rows — not a
# limitation of this API, which is why they ship with every answer.
CAVEATS = [
    "המאגר מדווח לפי גוש וחלקה בלבד, ללא תת-גוש וללא כתובת: הקישור לנכס מסוים "
    "עובר דרך הצלבת גוש-חלקה ואינו מבחין בין חלקות שחולקות מספר.",
    "ל-674,340 עסקאות (17.5%) אין קוד יישוב במקור, ולכן הסינון לפי יישוב הוא "
    "לפי השם כפי שפורסם.",
    "שווי העסקה הוא הסכום המדווח לרשות המסים, לא מחיר שוק מאומת, ושורה אחת "
    "יכולה להיות דירה אחת או בניין שלם — ולכן כל חישוב כאן הוא חציון ולא ממוצע.",
]


async def _require_ready() -> None:
    if not await deals_query.is_ready():
        raise HTTPException(
            status_code=503,
            detail="מאגר עסקאות הנדל\"ן עדיין לא נטען.")


class DealFilters:
    """The filter, declared once and injected into all three read shapes.

    ``as_dict`` drops everything the caller did not set, which is what
    ``deals_query._where`` needs: an unset filter must not appear in the WHERE
    clause at all, or it defeats the index it was supposed to use."""

    def __init__(
        self,
        settlement: str | None = Query(None, max_length=100,
                                       description="שם היישוב כפי שפורסם במאגר"),
        settlement_code: str | None = Query(None, max_length=10,
                                            description="קוד יישוב (חסר ב-17.5% מהשורות)"),
        gush: int | None = Query(None, ge=1, le=99_999_999),
        helka: int | None = Query(None, ge=0, le=99_999_999),
        sub_parcel: str | None = Query(None, max_length=3, description="תת-חלקה"),
        nature: str | None = Query(None, max_length=60, description="מהות העסקה"),
        date_from: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
        date_to: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
        min_amount: int | None = Query(None, ge=0, le=10_000_000_000),
        max_amount: int | None = Query(None, ge=0, le=10_000_000_000),
        min_rooms: float | None = Query(None, ge=0, le=99),
        max_rooms: float | None = Query(None, ge=0, le=99),
    ):
        self.values = {
            "settlement": settlement, "settlement_code": settlement_code,
            "gush": gush, "helka": helka, "sub_parcel": sub_parcel,
            "nature": nature, "date_from": date_from, "date_to": date_to,
            "min_amount": min_amount, "max_amount": max_amount,
            "min_rooms": min_rooms, "max_rooms": max_rooms,
        }

    def as_dict(self) -> dict:
        return {k: v for k, v in self.values.items() if v not in (None, "")}


@router.get("/stats")
@limiter.limit("60/minute")
async def deals_stats(request: Request):
    """Hero counters: rows, span, settlements, parcels, and the tracked dataset
    they come from, so every number on the page is one click from its source."""
    await _require_ready()
    return await deals_query.stats()


@router.get("/settlements")
@limiter.limit("60/minute")
async def deals_settlements(request: Request):
    """Every settlement name in the register, with its deal count.

    The names are verbatim: they are the exact strings /search filters on, so a
    picker built from this list cannot produce an empty result by spelling."""
    await _require_ready()
    data = await deals_query.settlements()
    return {"data": data, "count": len(data)}


@router.get("/natures")
@limiter.limit("60/minute")
async def deals_natures(request: Request):
    await _require_ready()
    data = await deals_query.natures()
    return {"data": data, "count": len(data)}


@router.get("/search")
@limiter.limit("60/minute")
async def deals_search(
    request: Request,
    filters: DealFilters = Depends(),
    sort: str = Query("date_desc", pattern="^(date|amount|area)_(desc|asc)$"),
    limit: int = Query(50, ge=1, le=deals_query.MAX_LIMIT),
    offset: int = Query(0, ge=0, le=100_000),
):
    """The browse. ``total`` is counted to 10,000 and then reported as capped:
    an exact count of a city's whole history is a real cost for a number nobody
    reads past the first page."""
    await _require_ready()
    f = filters.as_dict()
    res = await deals_query.search(f, limit=limit, offset=offset, sort=sort)
    return {"query": f, **res, "count": len(res["data"]),
            "processed": False, "caveats": CAVEATS}


@router.get("/series")
@limiter.limit("30/minute")
async def deals_series(request: Request, filters: DealFilters = Depends()):
    """Deals and the MEDIAN reported price per year, under the same filter as
    /search. Median, because one sale of a whole building moves a mean by
    millions and the register mixes the two."""
    await _require_ready()
    f = filters.as_dict()
    data = await deals_query.series(f)
    return {"query": f, "data": data, "count": len(data),
            "processed": False, "caveats": CAVEATS}


@router.get("/breakdown")
@limiter.limit("30/minute")
async def deals_breakdown(
    request: Request,
    filters: DealFilters = Depends(),
    limit: int = Query(20, ge=1, le=60),
):
    await _require_ready()
    f = filters.as_dict()
    data = await deals_query.breakdown(f, limit=limit)
    return {"query": f, "data": data, "count": len(data),
            "processed": False, "caveats": CAVEATS}


@router.get("/compare")
@limiter.limit("20/minute")
async def deals_compare(
    request: Request,
    year_from: int = Query(..., ge=1998, le=2100),
    year_to: int = Query(..., ge=1998, le=2100),
    nature: str | None = Query(None, max_length=60,
                               description="מהות העסקה — מומלץ מאוד; ראו להלן"),
    min_deals: int = Query(30, ge=1, le=10_000,
                           description="מינימום עסקאות בכל אחת מהשנים"),
    order: str = Query("change_desc",
                       pattern="^(change_desc|change_asc|median_desc|deals_desc)$"),
    limit: int = Query(30, ge=1, le=200),
):
    """Two years, every settlement, side by side, with the change in percent.

    The one aggregate here that reads as authoritative enough to quote without
    qualification, so the qualification travels IN the response: run without a
    ``nature`` it warns, because a settlement whose mix shifted from flats to
    plots shows a price change that is a composition change."""
    await _require_ready()
    rows = await deals_query.compare_settlements(
        year_from, year_to, nature=nature, min_deals=min_deals,
        limit=limit, order=order)
    out = {"query": {"year_from": year_from, "year_to": year_to, "nature": nature,
                     "min_deals": min_deals, "order": order},
           "data": rows, "count": len(rows), "processed": False, "caveats": CAVEATS}
    if not nature:
        out["warning"] = ("ההשוואה רצה על כל מהויות העסקה יחד; תמהיל שהשתנה בין "
                          "השנים ייראה כמו שינוי מחיר. העבירו nature.")
    return out


@router.get("/parcel/{gush}/{helka}")
@limiter.limit("120/minute")
async def deals_parcel(
    request: Request,
    gush: int = Path(..., ge=1, le=99_999_999),
    helka: int = Path(..., ge=0, le=99_999_999),
    sub_parcel: str | None = Query(None, max_length=3, description="תת-חלקה"),
    limit: int = Query(50, ge=1, le=nadlan_query.MAX_DEALS),
    offset: int = Query(0, ge=0, le=100_000),
):
    """One parcel's deals, newest first — the same list נדל"ן לעם shows under a
    property, served from the same function so the two can never disagree."""
    await _require_ready()
    data, total = await nadlan_query.parcel_deals(
        gush, helka, limit=limit, offset=offset,
        sub_parcel=sub_parcel.zfill(3) if sub_parcel else None)
    return {"query": {"gush": gush, "helka": helka, "sub_parcel": sub_parcel},
            "data": data, "count": len(data), "total": total,
            "limit": limit, "offset": offset,
            "processed": False, "caveats": CAVEATS}
