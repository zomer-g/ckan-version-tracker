"""Public read API for מכרזי רמ"י — the Israel Land Authority's land tenders.

The register is scraped daily into one tracked dataset with several tables (see
app/services/michrazim.py). This serves it as one corpus, joined the way a
question about it needs: a LOT (מתחם) with its tender's fields, its parcels,
and the per-unit figures that make one lot comparable with another.

Endpoints (all public, rate-limited):
    GET /api/michrazim/stats                    what is loaded, and its span
    GET /api/michrazim/lots?…                   lots, filtered and paged
    GET /api/michrazim/series?…                 per year, under the same filter
    GET /api/michrazim/settlements              settlement names as RMI spells them
    GET /api/michrazim/tender/{tender_id}       one tender in full
    GET /api/michrazim/parcel/{gush}/{helka}    every lot marketed on one parcel

Conventions follow app/api/deals.py: ``request: Request`` first (slowapi), an
explicit ``@limiter.limit`` on every route, 503 before the register is loaded,
and ``caveats`` / ``notes`` / ``console_sql`` travelling with the data — the
console SQL is the exact query that answered, so every number can be rerun at
/data. ``processed`` is false: these are the publisher's rows; the per-unit
figures are arithmetic on them, and say how they were computed.

/lots and /series take ONE filter dependency, so a chart and the table beside
it can never describe different populations.
"""
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request

from app.rate_limit import limiter
from app.services import michrazim

router = APIRouter(prefix="/api/michrazim", tags=["michrazim"])

CAVEATS = michrazim.CAVEATS
SOURCE = {"source": 'מכרזי רמ"י — אתר מכרזי המקרקעין של רשות מקרקעי ישראל, דרך גרסאות לעם',
          "source_url": michrazim.SOURCE_URL, "versions_url": michrazim.VERSIONS_URL,
          "processed": False}


async def _require_ready() -> None:
    if not await michrazim.is_ready():
        raise HTTPException(status_code=503, detail='מאגר מכרזי רמ"י עדיין לא נטען.')


class LotFilters:
    """The filter, declared once and injected into /lots and /series."""

    def __init__(
        self,
        settlement: str | None = Query(None, max_length=100,
                                       description="יישוב — בכתיב של רמ\"י או בשם הרשמי"),
        year_from: int | None = Query(None, ge=1990, le=2100, description="שנת המכרז, מ-"),
        year_to: int | None = Query(None, ge=1990, le=2100, description="שנת המכרז, עד"),
        tender_id: int | None = Query(None, ge=1, le=99_999_999),
        status: str | None = Query(None, max_length=40, description="סטטוס מכרז"),
        won_only: bool = Query(False, description="רק מתחמים שיש להם זוכה וסכום"),
        amount_basis: str | None = Query(None, pattern=r"^(lot_total|per_sqm_suspected)$",
                                         description="lot_total = מחיר קרקע; "
                                                     "per_sqm_suspected = מחיר מ\"ר דירה"),
        min_units: int | None = Query(None, ge=0, le=100_000),
        gush: int | None = Query(None, ge=1, le=99_999_999),
        helka: int | None = Query(None, ge=0, le=99_999_999),
        tender_type: str | None = Query(None, max_length=80,
                                        description="סוג מכרז (דורש את טבלת המכרזים)"),
        purpose: str | None = Query(None, max_length=80,
                                    description="ייעוד (דורש את טבלת המכרזים)"),
    ):
        self.values = {
            "settlement": (settlement or "").strip() or None,
            "year_from": year_from, "year_to": year_to, "tender_id": tender_id,
            "status": status, "won_only": won_only or None,
            "amount_basis": amount_basis, "min_units": min_units,
            "gush": gush, "helka": helka,
            "tender_type": tender_type, "purpose": purpose,
        }

    def as_dict(self) -> dict:
        return {k: v for k, v in self.values.items() if v not in (None, "")}


def _bad_filter(e: ValueError):
    raise HTTPException(status_code=422, detail=str(e))


@router.get("/stats")
@limiter.limit("60/minute")
async def michrazim_stats(request: Request):
    """Which of the register's tables are loaded here, and their span."""
    return {**SOURCE, **await michrazim.stats(), "caveats": CAVEATS}


@router.get("/lots")
@limiter.limit("60/minute")
async def michrazim_lots(
    request: Request,
    f: LotFilters = Depends(),
    sort: str = Query("year_desc", pattern=r"^(" + "|".join(michrazim.SORTS) + r")$"),
    limit: int = Query(50, ge=1, le=michrazim.MAX_LIMIT),
    offset: int = Query(0, ge=0, le=100_000),
):
    """One row per (tender, lot): its own published fields, the tender's
    fields when the tenders table is loaded, ``amount_basis``, and the per-unit
    land price and development costs."""
    await _require_ready()
    q = f.as_dict()
    try:
        res = await michrazim.search_lots(q, limit=limit, offset=offset, sort=sort)
    except ValueError as e:
        _bad_filter(e)
    return {**SOURCE, "query": q, **res, "caveats": CAVEATS}


@router.get("/series")
@limiter.limit("60/minute")
async def michrazim_series(request: Request, f: LotFilters = Depends()):
    """Per tender year, under the same filter as /lots: lots marketed, won and
    with no bids, units won, and the MEDIAN land price per unit, development
    costs per unit, their sum, and the sqm price of the per-sqm tenders."""
    await _require_ready()
    q = f.as_dict()
    try:
        res = await michrazim.series(q)
    except ValueError as e:
        _bad_filter(e)
    return {**SOURCE, "query": q, **res, "caveats": CAVEATS}


@router.get("/settlements")
@limiter.limit("30/minute")
async def michrazim_settlements(request: Request):
    """Settlement names exactly as the register spells them, with lot and tender
    counts and the code OVER resolves each to — the key to join tenders with
    the deals register and every other dataset."""
    await _require_ready()
    rows = await michrazim.settlements()
    return {**SOURCE, "data": rows, "count": len(rows)}


@router.get("/tender/{tender_id}")
@limiter.limit("60/minute")
async def michrazim_tender(request: Request,
                           tender_id: int = Path(..., ge=1, le=99_999_999)):
    """One tender: its row (when loaded), lots, bids on each lot, tender-level
    bids (without the bidders' ID numbers), parcels, plans and documents."""
    await _require_ready()
    out = await michrazim.tender(tender_id)
    if not out:
        raise HTTPException(status_code=404, detail="לא נמצא מכרז כזה")
    return {**SOURCE, **out, "caveats": CAVEATS}


@router.get("/parcel/{gush}/{helka}")
@limiter.limit("60/minute")
async def michrazim_parcel(
    request: Request,
    gush: int = Path(..., ge=1, le=99_999_999),
    helka: int = Path(..., ge=0, le=99_999_999),
    limit: int = Query(50, ge=1, le=michrazim.MAX_LIMIT),
):
    """Every tender lot marketed on this גוש/חלקה, newest tender first, with
    links to the property page and to that parcel's deals."""
    await _require_ready()
    res = await michrazim.parcel_lots(gush, helka, limit=limit)
    return {**SOURCE, "gush": gush, "helka": helka, **res,
            "property_page": f"{michrazim.SITE}/projects/nadlan?tab=gush&g={gush}&h={helka}",
            "deals_page": f"{michrazim.SITE}/projects/deals?gush={gush}&helka={helka}",
            "caveats": CAVEATS}
