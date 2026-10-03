"""Public read API for GovMap's deals (layer 16), loaded by govmap_deals.

    GET /api/deals/govmap/stats            counters + the version loaded
    GET /api/deals/govmap/settlements      every settlement, with its deal count
    GET /api/deals/govmap/search?…         the browse, filtered and paged
    GET /api/deals/govmap/points?…&bbox=   the deals inside a map view, one entry
                                           per point (a building) with its count

This is a second register beside the Tax Authority's (app/api/deals.py), not a
copy of it: GovMap publishes the deals nadlan.gov.il shows, one point per
building, history included. Like the Tax Authority rows they are passed
through, so ``processed`` is false and the caveats are about the source.
"""

import asyncio
import time

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.rate_limit import limiter
from app.services import append_store, govmap_deals

router = APIRouter(prefix="/api/deals/govmap", tags=["deals"])

T = f"public.{govmap_deals.TABLE}"
STATE = f"public.{govmap_deals.STATE_TABLE}"
_TIMEOUT_MS = 8000
_AGGREGATE_TIMEOUT_MS = 25000
COUNT_CAP = 10_000
MAX_LIMIT = 200
POINTS_CAP = 3000

CAVEATS = [
    "העסקאות כפי ש-GovMap מפרסם אותן בשכבת עסקאות הנדל\"ן (שכבה 16), אותן "
    "עסקאות שמציג אתר הנדל\"ן הממשלתי — בלי עיבוד, בלי השלמה ובלי תיקון.",
    "כל העסקאות של בניין מסומנות על נקודה אחת — נקודת הבניין — ולכן המפה מציגה "
    "נקודה לכל בניין עם מספר העסקאות בו, ולא את קווי המתאר של הבניינים.",
    "ב-27 נקודות (חלקות ללא כתובת) רשומות יותר מ-500 עסקאות, ו-GovMap מחזיר "
    "מהן רק 500; פירוט הנקודות מופיע בדף המאגר.",
    "תאריכי העסקה במקור נעים בין 1900 ל-2048: אלה ערכים כפי שפורסמו, לא שגיאת "
    "טעינה. לכ-84 אלף עסקאות אין שם יישוב במקור, ולכן הן נמצאות לפי גוש וחלקה.",
]


async def _fetch(sql: str, *args, timeout_ms: int = _TIMEOUT_MS) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            rows = await conn.fetch(sql, *args)
    return [dict(r) for r in rows]


async def _require_ready() -> dict:
    try:
        rows = await _fetch(f"SELECT version_number, rows, with_point, loaded_at FROM {STATE}")
    except Exception:  # noqa: BLE001 — table not created yet
        rows = []
    if not rows:
        raise HTTPException(status_code=503,
                            detail="עסקאות GovMap עדיין לא נטענו למסד הנתונים.")
    return rows[0]


class GovmapFilters:
    def __init__(
        self,
        settlement: str | None = Query(None, max_length=100),
        gush: int | None = Query(None, ge=1, le=99_999_999),
        helka: int | None = Query(None, ge=0, le=99_999_999),
        sub_parcel: int | None = Query(None, ge=0, le=99_999),
        street: str | None = Query(None, max_length=80),
        house: str | None = Query(None, max_length=10),
        property_type: str | None = Query(None, max_length=60),
        date_from: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
        date_to: str | None = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
        min_amount: int | None = Query(None, ge=0, le=10_000_000_000),
        max_amount: int | None = Query(None, ge=0, le=10_000_000_000),
        min_rooms: float | None = Query(None, ge=0, le=99),
        max_rooms: float | None = Query(None, ge=0, le=99),
    ):
        self.values = {k: v for k, v in {
            "settlement": (settlement or "").strip() or None, "gush": gush,
            "helka": helka, "sub_parcel": sub_parcel,
            "street": (street or "").strip() or None,
            "house": (house or "").strip() or None,
            "property_type": (property_type or "").strip() or None,
            "date_from": date_from, "date_to": date_to,
            "min_amount": min_amount, "max_amount": max_amount,
            "min_rooms": min_rooms, "max_rooms": max_rooms,
        }.items() if v not in (None, "")}


def where_of(f: dict) -> tuple[str, list]:
    """The WHERE clause and its parameters. Every filter is one indexed or
    narrow predicate; an unset filter is absent, not ``IS NULL OR``."""
    clauses, args = [], []

    def arg(v):
        args.append(v)
        return f"${len(args)}"

    if f.get("settlement"):
        clauses.append(f"settlement = {arg(f['settlement'])}")
    if f.get("gush"):
        clauses.append(f"gush = {arg(int(f['gush']))}")
    if f.get("helka") is not None and f.get("gush"):
        clauses.append(f"parcel = {arg(int(f['helka']))}")
    if f.get("sub_parcel") is not None and f.get("gush"):
        clauses.append(f"sub_parcel = {arg(int(f['sub_parcel']))}")
    if f.get("street"):
        clauses.append(f"street = {arg(f['street'])}")
    if f.get("house") and f.get("street"):
        clauses.append(f"house_num = {arg(f['house'])}")
    if f.get("property_type"):
        clauses.append(f"property_type = {arg(f['property_type'])}")
    if f.get("date_from"):
        clauses.append(f"deal_date >= {arg(f['date_from'])}::date")
    if f.get("date_to"):
        clauses.append(f"deal_date <= {arg(f['date_to'])}::date")
    if f.get("min_amount") is not None:
        clauses.append(f"deal_amount >= {arg(int(f['min_amount']))}")
    if f.get("max_amount") is not None:
        clauses.append(f"deal_amount <= {arg(int(f['max_amount']))}")
    if f.get("min_rooms") is not None:
        clauses.append(f"rooms >= {arg(float(f['min_rooms']))}")
    if f.get("max_rooms") is not None:
        clauses.append(f"rooms <= {arg(float(f['max_rooms']))}")
    return (" AND ".join(clauses) or "true"), args


SORTS = {
    "date_desc": "deal_date DESC NULLS LAST, objectid",
    "date_asc": "deal_date ASC NULLS LAST, objectid",
    "amount_desc": "deal_amount DESC NULLS LAST, objectid",
    "amount_asc": "deal_amount ASC NULLS LAST, objectid",
}

_ROW = ("objectid, deal_id, deal_date, deal_date_raw, deal_amount, settlement, street, "
        "house_num, floor, asset_area, rooms, property_type, deal_nature, gush, "
        "parcel, sub_parcel, polygon_id, lon, lat")


def _row(r: dict) -> dict:
    out = dict(r)
    for k in ("asset_area", "rooms"):
        if out.get(k) is not None:
            out[k] = float(out[k])
    if out.get("deal_date") is not None:
        out["deal_date"] = out["deal_date"].isoformat()
    return out


_cache: dict[str, tuple[float, object]] = {}
_TTL = 3600.0


async def _cached(key: str, producer):
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < _TTL:
        return hit[1]
    value = await producer()
    _cache[key] = (time.monotonic(), value)
    return value


@router.get("/stats")
@limiter.limit("60/minute")
async def govmap_stats(request: Request):
    state = await _require_ready()

    async def produce():
        rows = await _fetch(f"""
            SELECT count(*) AS deals,
                   count(DISTINCT settlement) AS settlements,
                   count(DISTINCT polygon_id) AS points,
                   min(deal_date) FILTER (WHERE deal_date >= DATE '1990-01-01') AS first_deal,
                   max(deal_date) FILTER (WHERE deal_date <= now()::date) AS last_deal
            FROM {T}""", timeout_ms=_AGGREGATE_TIMEOUT_MS)
        r = rows[0]
        return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in r.items()}

    stats = await _cached(f"stats:{state['version_number']}", produce)
    return {**stats, "version": state["version_number"],
            "loaded_at": state["loaded_at"].isoformat(),
            "dataset_id": govmap_deals.DATASET_ID,
            "source_url": "https://www.govmap.gov.il/?lay=16",
            "table": T, "caveats": CAVEATS}


@router.get("/settlements")
@limiter.limit("30/minute")
async def govmap_settlements(request: Request):
    state = await _require_ready()

    async def produce():
        return await _fetch(f"""
            SELECT settlement, count(*) AS deals FROM {T}
            WHERE settlement IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""",
                            timeout_ms=_AGGREGATE_TIMEOUT_MS)

    data = await _cached(f"settlements:{state['version_number']}", produce)
    return {"data": data, "count": len(data)}


@router.get("/search")
@limiter.limit("60/minute")
async def govmap_search(
    request: Request,
    filters: GovmapFilters = Depends(),
    sort: str = Query("date_desc", pattern="^(date|amount)_(desc|asc)$"),
    limit: int = Query(50, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0, le=100_000),
):
    await _require_ready()
    f = filters.values
    where, args = where_of(f)
    rows, counted = await asyncio.gather(
        _fetch(f"SELECT {_ROW} FROM {T} WHERE {where} ORDER BY {SORTS[sort]} "
               f"LIMIT {limit} OFFSET {offset}", *args),
        _fetch(f"SELECT count(*) AS n FROM (SELECT 1 FROM {T} WHERE {where} "
               f"LIMIT {COUNT_CAP + 1}) capped", *args),
    )
    total = counted[0]["n"] if counted else 0
    return {"query": f, "data": [_row(r) for r in rows], "count": len(rows),
            "total": min(total, COUNT_CAP), "total_capped": total > COUNT_CAP,
            "limit": limit, "offset": offset, "sort": sort,
            "processed": False, "caveats": CAVEATS}


@router.get("/points")
@limiter.limit("60/minute")
async def govmap_points(
    request: Request,
    filters: GovmapFilters = Depends(),
    bbox: str | None = Query(None, pattern=r"^-?[\d.]+,-?[\d.]+,-?[\d.]+,-?[\d.]+$",
                             description="minLon,minLat,maxLon,maxLat"),
):
    """One entry per point (a building) with its deal count, inside the view.

    Every deal of a building shares the building's point, so drawing deals
    one by one would stack hundreds of markers on one spot; this answers the
    map with what it can show. Capped, with the cap reported."""
    await _require_ready()
    f = filters.values
    where, args = where_of(f)
    if bbox:
        x0, y0, x1, y1 = (float(v) for v in bbox.split(","))
        if (x1 - x0) * (y1 - y0) > 4.0 and not f:
            raise HTTPException(status_code=422,
                                detail="התקרבו למפה או סננו לפני הצגת נקודות.")
        args += [x0, x1, y0, y1]
        n = len(args)
        where += f" AND lon BETWEEN ${n-3} AND ${n-2} AND lat BETWEEN ${n-1} AND ${n}"
    elif not f:
        raise HTTPException(status_code=422, detail="יש לסנן או לתחום אזור במפה.")
    rows = await _fetch(f"""
        SELECT lon, lat, count(*) AS deals,
               max(deal_date) AS last_deal,
               (array_agg(settlement ORDER BY deal_date DESC NULLS LAST))[1] AS settlement,
               (array_agg(street ORDER BY deal_date DESC NULLS LAST))[1] AS street,
               (array_agg(house_num ORDER BY deal_date DESC NULLS LAST))[1] AS house_num,
               min(gush) AS gush, min(parcel) AS parcel, min(polygon_id) AS polygon_id
        FROM {T} WHERE {where} AND lon IS NOT NULL
        GROUP BY lon, lat ORDER BY count(*) DESC LIMIT {POINTS_CAP + 1}""", *args)
    capped = len(rows) > POINTS_CAP
    data = [{**r, "last_deal": r["last_deal"].isoformat() if r.get("last_deal") else None}
            for r in rows[:POINTS_CAP]]
    return {"query": f, "data": data, "count": len(data), "capped": capped}
