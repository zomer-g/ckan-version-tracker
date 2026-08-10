"""Public read API for "נדל"ן לעם" — the property-level spatial crosswalk.

One question, four ways in. Whichever spatial identity the caller has — a point
on the map (optionally with a radius), a postal code, an address, or a
gush/helka — the answer comes back in the SAME envelope, carrying that property's
identity in every other codespace plus a per-source block linking to that
source's untouched full row on /data.

Endpoints (all public, rate-limited):
    GET /api/nadlan/stats                      hero counters + coverage
    GET /api/nadlan/resolve?q=                 omnibox — sniffs the mode
    GET /api/nadlan/parcel/{gush}/{helka}      by גוש/חלקה (?suffix=)
    GET /api/nadlan/parcel/{g}/{h}/geometry    the polygon, as GeoJSON
    GET /api/nadlan/point?lat=&lon=&radius_m=  by map point / radius
    GET /api/nadlan/zip/{zip}                  by מיקוד (5 or 7 digits)
    GET /api/nadlan/address?city=&street=&…    by כתובת
    GET /api/nadlan/streets?q=&settlement=     street autocomplete

Conventions follow app/api/ocal.py: ``request: Request`` first (slowapi needs
it), an explicit ``@limiter.limit`` on every route, ``_require_ready()`` → 503
before the index has been built, and a ``{"data": …, "query": …}`` envelope.

The data here is DERIVED — the crosswalk between four sources, not any one of
them — so every response carries ``processed: true`` and the coverage caveats
travel with it rather than being left for the UI to remember.
"""
from fastapi import APIRouter, HTTPException, Path, Query, Request

from app.rate_limit import limiter
from app.services import nadlan_query, nadlan_text

router = APIRouter(prefix="/api/nadlan", tags=["nadlan"])

# Stated on every response so a caller cannot mistake the crosswalk for a
# primary source. Each is a measured limit, not a guess — see docs/nadlan.md.
CAVEATS = [
    "המיקוד זמין ל-91 יישובים בלבד (קובץ המיקוד של דואר ישראל).",
    "גזטיר הנכסים מקשר גוש-חלקה לרחוב בלבד — לא למספר בית.",
    "כ-30% מרשימת הכתובות ללא קואורדינטות, ולכן ללא שיוך לחלקה.",
]


async def _require_ready() -> None:
    if not await nadlan_query.is_ready():
        raise HTTPException(
            status_code=503,
            detail="נדל\"ן לעם עדיין לא נבנה (האינדקס המוצלב טרם הופק).")


def _envelope(mode: str, parsed: dict, data: list, **extra) -> dict:
    return {"query": {"mode": mode, **parsed}, "data": data,
            "count": len(data), "processed": True, "caveats": CAVEATS, **extra}


@router.get("/stats")
@limiter.limit("60/minute")
async def nadlan_stats(request: Request):
    """Hero counters and the published coverage percentages."""
    await _require_ready()
    return await nadlan_query.stats()


@router.get("/parcel/{gush}/{helka}")
@limiter.limit("120/minute")
async def nadlan_parcel(
    request: Request,
    gush: int = Path(..., ge=1, le=99_999_999),
    helka: int = Path(..., ge=0, le=99_999_999),
    suffix: int | None = Query(None, ge=0, le=999, description="תת-גוש"),
):
    await _require_ready()
    parcels = await nadlan_query.by_gush_helka(gush, helka, suffix)
    data = await nadlan_query.property_envelope(parcels)
    return _envelope("gush_helka", {"gush": gush, "helka": helka, "suffix": suffix}, data)


@router.get("/parcel/{gush}/{helka}/geometry")
@limiter.limit("30/minute")
async def nadlan_parcel_geometry(
    request: Request,
    gush: int = Path(..., ge=1, le=99_999_999),
    helka: int = Path(..., ge=0, le=99_999_999),
    suffix: int = Query(0, ge=0, le=999),
):
    """The parcel polygon as GeoJSON — the only response that touches the 4.58 GB
    source table, so it is the most tightly rate-limited route here."""
    await _require_ready()
    row = await nadlan_query.parcel_geometry(gush, suffix, helka)
    if not row:
        raise HTTPException(status_code=404, detail="לא נמצאה חלקה כזו")
    return {"gush": gush, "gush_suffix": suffix, "helka": helka,
            "geojson": row["geojson"], "legal_area": row.get("legal_area"),
            "status_text": row.get("status_text"), "processed": True}


@router.get("/point")
@limiter.limit("60/minute")
async def nadlan_point(
    request: Request,
    lat: float = Query(..., ge=29.0, le=34.0, description="קו רוחב (WGS84)"),
    lon: float = Query(..., ge=33.0, le=36.5, description="קו אורך (WGS84)"),
    radius_m: float = Query(0, ge=0, le=nadlan_query.MAX_RADIUS_M),
    limit: int = Query(50, ge=1, le=nadlan_query.MAX_LIMIT),
):
    """radius_m=0 answers "which parcel is this point inside"; anything larger
    returns the parcels whose centre lies within that many metres."""
    await _require_ready()
    parcels = await nadlan_query.by_point(lat, lon, radius_m, limit)
    data = await nadlan_query.property_envelope(parcels)
    return _envelope("point", {"lat": lat, "lon": lon, "radius_m": radius_m}, data)


@router.get("/zip/{zip_code}")
@limiter.limit("120/minute")
async def nadlan_zip(request: Request, zip_code: str = Path(..., min_length=5, max_length=7)):
    await _require_ready()
    if not zip_code.isdigit() or len(zip_code) not in (5, 7):
        raise HTTPException(status_code=422, detail="מיקוד חייב להיות 5 או 7 ספרות")
    addrs, parcels = await nadlan_query.by_zip(zip_code)
    data = await nadlan_query.property_envelope(parcels, addresses=addrs)
    return _envelope("zip", {"zip": zip_code}, data, addresses=addrs)


@router.get("/address")
@limiter.limit("120/minute")
async def nadlan_address(
    request: Request,
    city: str = Query(..., min_length=1, max_length=100),
    street: str = Query(..., min_length=1, max_length=120),
    number: str | None = Query(None, max_length=40),
):
    await _require_ready()
    addrs, parcels = await nadlan_query.by_address(city, street, number)
    data = await nadlan_query.property_envelope(parcels, addresses=addrs)
    return _envelope("address", {"city": city, "street": street, "number": number},
                     data, addresses=addrs)


@router.get("/streets")
@limiter.limit("120/minute")
async def nadlan_streets(
    request: Request,
    q: str = Query(..., min_length=1, max_length=80),
    settlement: int | None = Query(None, ge=1, le=99_999),
    limit: int = Query(20, ge=1, le=50),
):
    await _require_ready()
    return {"data": await nadlan_query.suggest_streets(q, settlement, limit)}


@router.get("/resolve")
@limiter.limit("60/minute")
async def nadlan_resolve(
    request: Request,
    q: str = Query(..., min_length=1, max_length=200),
    radius_m: float = Query(0, ge=0, le=nadlan_query.MAX_RADIUS_M),
):
    """The omnibox: one free-text box onto whichever mode the text implies.

    Ambiguity is surfaced rather than resolved by guessing — a bare 5-digit
    number is a legitimate ZIP5 *and* a legitimate gush, so both readings come
    back in ``alternatives`` and the caller can offer the choice."""
    await _require_ready()
    sniff = nadlan_text.sniff_mode(q)
    mode, parsed = sniff["mode"], sniff["parsed"]

    if mode == "point":
        parcels = await nadlan_query.by_point(parsed["lat"], parsed["lon"], radius_m)
        data = await nadlan_query.property_envelope(parcels)
    elif mode == "gush_helka":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], parsed["helka"])
        data = await nadlan_query.property_envelope(parcels)
    elif mode == "zip":
        addrs, parcels = await nadlan_query.by_zip(parsed["zip"])
        data = await nadlan_query.property_envelope(parcels, addresses=addrs)
    elif mode == "gush":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], 0)
        data = await nadlan_query.property_envelope(parcels)
    else:
        # Free-text address: the resolver needs the parts, which the client's
        # address form supplies. Say so rather than guessing at a split.
        return _envelope("address_text", parsed, [],
                         hint="השתמשו ב-/api/nadlan/address עם city/street/number",
                         alternatives=sniff["alternatives"])

    return _envelope(mode, parsed, data, alternatives=sniff["alternatives"])
