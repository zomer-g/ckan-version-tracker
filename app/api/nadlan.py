"""Public read API for "נדל"ן לעם" — the property-level spatial crosswalk.

One question, four ways in. Whichever spatial identity the caller has — a point
on the map (optionally with a radius), a postal code, an address, or a
gush/helka — the answer comes back in the SAME envelope, carrying that property's
identity in every other codespace plus a per-source block linking to that
source's untouched full row on /data.

Endpoints (all public, rate-limited):
    GET /api/nadlan/stats                      hero counters + coverage
    GET /api/nadlan/lookup?…&fields=           ONE endpoint, any identifier in,
                                               any subset of the answer out
    GET /api/nadlan/resolve?q=                 omnibox — sniffs the mode
    GET /api/nadlan/parcel/{gush}/{helka}      by גוש/חלקה (?suffix=)
    GET /api/nadlan/parcel/{g}/{h}/geometry    the polygon, as GeoJSON
    GET /api/nadlan/parcel/{g}/{h}/deals       that parcel's מיסוי מקרקעין deals
    GET /api/nadlan/point?lat=&lon=&radius_m=  by map point / radius
    GET /api/nadlan/zip/{zip}                  by מיקוד (5 or 7 digits)
    GET /api/nadlan/address?city=&street=&…    by כתובת
    GET /api/nadlan/streets?q=&settlement=     street autocomplete

Beside the four identities, every answer also carries the two layers that
DESCRIBE the property rather than identify it: its CBS statistical area (א"ס),
resolved by putting the parcel centroid inside an area polygon, and a summary of
the מיסוי מקרקעין deals reported on its גוש/חלקה. Both are on by default and
both can be switched off.

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
    # Israeli postal reality: only the big cities get a zip per street+house.
    # Everywhere else has ONE zip for the whole locality — a weaker fact, and
    # flagged as such (`zip_level`) rather than passed off as the doorway's own.
    "מיקוד ברמת הכתובת קיים ל-91 יישובים בלבד; בשאר היישובים המיקוד הוא "
    "מיקוד כלל-יישובי אחד (מסומן zip_level=locality).",
    "גזטיר הנכסים מקשר גוש-חלקה לרחוב בלבד — לא למספר בית.",
    "כ-42% מרשימת הכתובות ללא קואורדינטות, ולכן ללא שיוך מדויק לחלקה.",
    # The deal register is keyed on גוש+חלקה with no suffix, exactly like the
    # gazetteer — so it inherits the same ambiguity, and says so.
    "עסקאות מיסוי מקרקעין מקושרות לפי גוש וחלקה בלבד (המאגר אינו מפרסם תת-גוש), "
    "ולכן בחלקות שחולקות מספר ייתכן שאותן עסקאות יוצגו ביותר מחלקה אחת.",
    # ...and the א"ס is two different divisions, which must not read as one.
    "האזור הסטטיסטי נקבע לפי מרכז החלקה בתוך שכבת האזורים הסטטיסטיים של הלמ\"ס "
    "לשנת 2022; המדד החברתי-כלכלי מתפרסם רק על חלוקת 2011, ולכן הוא מחושב בנפרד "
    "ומסומן בשנת החלוקה שלו.",
]


# Declared once: the same two switches appear on every mode, and a switch that
# meant something slightly different on one of them would be a trap.
_STAT_AREA = Query(True, description='לצרף את האזור הסטטיסטי (א"ס) לכל תוצאה')
_DEALS = Query(True, description="לצרף סיכום עסקאות מיסוי מקרקעין לכל תוצאה")


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
    geometry: bool = Query(False, description="לצרף את פוליגון החלקה לכל תוצאה"),
    stat_area: bool = _STAT_AREA,
    deals: bool = _DEALS,
):
    await _require_ready()
    parcels = await nadlan_query.by_gush_helka(gush, helka, suffix)
    data = await nadlan_query.property_envelope(
        parcels, with_geometry=geometry, with_stat_area=stat_area, with_deals=deals)
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


@router.get("/parcel/{gush}/{helka}/deals")
@limiter.limit("120/minute")
async def nadlan_parcel_deals(
    request: Request,
    gush: int = Path(..., ge=1, le=99_999_999),
    helka: int = Path(..., ge=0, le=99_999_999),
    sub_parcel: str | None = Query(None, max_length=3, description="תת-חלקה"),
    limit: int = Query(50, ge=1, le=nadlan_query.MAX_DEALS),
    offset: int = Query(0, ge=0, le=100_000),
):
    """Every deal reported on this גוש/חלקה, newest first.

    The envelope carries only a SUMMARY because a parcel in a condo tower holds
    hundreds of deals (1,850 on the busiest one measured). This is the full list,
    paged — the same split the polygon already uses. ``sub_parcel`` narrows to
    one תת-חלקה, which in such a tower is the single apartment: the only grain
    at which a price series means anything."""
    await _require_ready()
    data, total = await nadlan_query.parcel_deals(
        gush, helka, limit=limit, offset=offset,
        sub_parcel=sub_parcel.zfill(3) if sub_parcel else None)
    return {"query": {"gush": gush, "helka": helka, "sub_parcel": sub_parcel},
            "data": data, "count": len(data), "total": total,
            "limit": limit, "offset": offset, "processed": True,
            "caveats": CAVEATS}


@router.get("/point")
@limiter.limit("60/minute")
async def nadlan_point(
    request: Request,
    lat: float = Query(..., ge=29.0, le=34.0, description="קו רוחב (WGS84)"),
    lon: float = Query(..., ge=33.0, le=36.5, description="קו אורך (WGS84)"),
    radius_m: float = Query(0, ge=0, le=nadlan_query.MAX_RADIUS_M),
    limit: int = Query(50, ge=1, le=nadlan_query.MAX_LIMIT),
    geometry: bool = Query(False, description="לצרף את פוליגון החלקה לכל תוצאה"),
    stat_area: bool = _STAT_AREA,
    deals: bool = _DEALS,
):
    """radius_m=0 answers "which parcel is this point inside"; anything larger
    returns the parcels whose centre lies within that many metres.

    A radius of 0 that finds nothing widens ONCE, to 150 m, and says so in
    ``radius_used``/``widened``: parcels do not tile the country, and in a lot of
    it a tap lands between them — 94 of 150 random points inside Dimona's own
    envelope are inside a parcel, against 149 of 150 in Tel Aviv."""
    await _require_ready()
    parcels, used = await nadlan_query.by_point_or_near(lat, lon, radius_m, limit)
    data = await nadlan_query.property_envelope(
        parcels, with_geometry=geometry, with_stat_area=stat_area, with_deals=deals)
    # Open ground: nothing under the point and nothing within the fallback
    # either. Say what was tried — an empty list on a map reads as a broken map.
    extra = {} if data else {"miss": {
        "reason": "no_parcel_near",
        "message": (
            "לא נמצאה חלקה רשומה בנקודה הזו"
            + (f" ואף לא ברדיוס {nadlan_query.POINT_FALLBACK_RADIUS_M:.0f} מ׳"
               if not radius_m else f" ברדיוס {radius_m:.0f} מ׳")
            + ". חלקות אינן מרצפות את השטח: כבישים, שטחים פתוחים וקרקע שאינה "
              "מוסדרת נמצאים ביניהן. נסו נקודה קרובה יותר לבינוי, או רדיוס גדול יותר."),
        "radius_tried_m": used or (radius_m or nadlan_query.POINT_FALLBACK_RADIUS_M),
    }}
    return _envelope("point", {"lat": lat, "lon": lon, "radius_m": radius_m,
                               "radius_used": used, "widened": used > radius_m},
                     data, **extra)


@router.get("/zip/{zip_code}")
@limiter.limit("120/minute")
async def nadlan_zip(
    request: Request,
    zip_code: str = Path(..., min_length=5, max_length=7),
    geometry: bool = Query(False, description="לצרף את פוליגון החלקה לכל תוצאה"),
    stat_area: bool = _STAT_AREA,
    deals: bool = _DEALS,
):
    await _require_ready()
    if not zip_code.isdigit() or len(zip_code) not in (5, 7):
        raise HTTPException(status_code=422, detail="מיקוד חייב להיות 5 או 7 ספרות")
    addrs, parcels = await nadlan_query.by_zip(zip_code)
    data = await nadlan_query.property_envelope(
        parcels, addresses=addrs, with_geometry=geometry,
        with_stat_area=stat_area, with_deals=deals)
    return _envelope("zip", {"zip": zip_code}, data, addresses=addrs)


@router.get("/address")
@limiter.limit("120/minute")
async def nadlan_address(
    request: Request,
    city: str = Query(..., min_length=1, max_length=100),
    street: str = Query(..., min_length=1, max_length=120),
    number: str | None = Query(None, max_length=40),
    geometry: bool = Query(False, description="לצרף את פוליגון החלקה לכל תוצאה"),
    stat_area: bool = _STAT_AREA,
    deals: bool = _DEALS,
):
    await _require_ready()
    addrs, parcels = await nadlan_query.by_address(city, street, number)
    data = await nadlan_query.property_envelope(
        parcels, addresses=addrs, with_geometry=geometry,
        with_stat_area=stat_area, with_deals=deals)
    # An empty list is the one answer a person cannot act on: it does not say
    # whether the town, the spelling or the street itself is the problem, and
    # each of those is a different next move.
    # `addrs` is passed in on purpose: an address that matched and simply has
    # no parcel behind it is the COMMON empty answer, not a miss, and the reader
    # is owed what we do hold about it rather than a blank screen.
    extra = ({} if data else
             {"miss": await nadlan_query.explain_address_miss(city, street, addrs)})
    return _envelope("address", {"city": city, "street": street, "number": number},
                     data, addresses=addrs, **extra)


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


# ── one endpoint for all of it ────────────────────────────────────────────────
async def _by_mode(mode: str, parsed: dict, radius_m: float, limit: int,
                   geometry: bool, stat_area: bool, deals: bool) -> list[dict]:
    """Run whichever mode the caller's parameters chose.

    The four modes reach the database differently and then converge on ONE
    envelope builder, which is the whole point of the project: the answer must
    not depend on which identity you happened to hold."""
    addrs: list[dict] | None = None
    if mode == "point":
        parcels = await nadlan_query.by_point(parsed["lat"], parsed["lon"],
                                              radius_m, limit)
    elif mode == "gush_helka":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], parsed["helka"],
                                                   parsed.get("suffix"))
    elif mode == "gush":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], 0)
    elif mode == "zip":
        addrs, parcels = await nadlan_query.by_zip(parsed["zip"])
    elif mode == "address":
        addrs, parcels = await nadlan_query.by_address(
            parsed["city"], parsed["street"], parsed.get("number"))
    else:
        return []
    return await nadlan_query.property_envelope(
        parcels[:limit], addresses=addrs, with_geometry=geometry,
        with_stat_area=stat_area, with_deals=deals)


@router.get("/lookup")
@limiter.limit("60/minute")
async def nadlan_lookup(
    request: Request,
    q: str | None = Query(None, max_length=200,
                          description="טקסט חופשי — נקודה, מיקוד או גוש/חלקה"),
    lat: float | None = Query(None, ge=29.0, le=34.0),
    lon: float | None = Query(None, ge=33.0, le=36.5),
    radius_m: float = Query(0, ge=0, le=nadlan_query.MAX_RADIUS_M),
    gush: int | None = Query(None, ge=1, le=99_999_999),
    helka: int | None = Query(None, ge=0, le=99_999_999),
    suffix: int | None = Query(None, ge=0, le=999, description="תת-גוש"),
    zip_code: str | None = Query(None, alias="zip", min_length=5, max_length=7),
    city: str | None = Query(None, max_length=100),
    street: str | None = Query(None, max_length=120),
    number: str | None = Query(None, max_length=40),
    fields: str = Query(",".join(nadlan_query.DEFAULT_FIELDS),
                        description="אילו חלקים להחזיר, מופרדים בפסיק, או all"),
    limit: int = Query(50, ge=1, le=nadlan_query.MAX_LIMIT),
):
    """Any identifier in, any subset of the answer out.

    This is the whole project as one call: give it whichever identity you hold —
    a point, a כתובת, a מיקוד or a גוש/חלקה — and ask with ``fields`` for
    whichever of the others you need, including the CBS statistical area and the
    deal history. The four dedicated endpoints remain, and return exactly the
    same envelope; this one exists so a caller does not have to know which of
    them to call.

    ``fields`` is validated, never ignored: a name that is not part of the
    envelope is a 422 rather than a silently missing block.
    """
    await _require_ready()

    wanted = {f.strip() for f in fields.split(",") if f.strip()}
    if "all" in wanted:
        wanted = set(nadlan_query.FIELDS)
    unknown = sorted(wanted - set(nadlan_query.FIELDS))
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"שדות לא מוכרים: {', '.join(unknown)}. "
                   f"אפשריים: {', '.join(nadlan_query.FIELDS)}")

    # Explicit parameters beat the free-text box: a caller that named a גוש and
    # a חלקה has said what it means, and sniffing over that could only get it
    # wrong. ``q`` is the fallback, not the default.
    if city and street:
        mode, parsed = "address", {"city": city, "street": street, "number": number}
    elif gush is not None and helka is not None:
        mode, parsed = "gush_helka", {"gush": gush, "helka": helka, "suffix": suffix}
    elif zip_code:
        if not zip_code.isdigit() or len(zip_code) not in (5, 7):
            raise HTTPException(status_code=422, detail="מיקוד חייב להיות 5 או 7 ספרות")
        mode, parsed = "zip", {"zip": zip_code}
    elif lat is not None and lon is not None:
        mode, parsed = "point", {"lat": lat, "lon": lon}
    elif gush is not None:
        mode, parsed = "gush", {"gush": gush}
    elif q:
        sniff = nadlan_text.sniff_mode(q)
        mode, parsed = sniff["mode"], sniff["parsed"]
        if mode == "address_text":
            return _envelope("address_text", {**parsed, "fields": sorted(wanted)}, [],
                             hint="כתובת חופשית — השתמשו ב-city/street/number",
                             alternatives=sniff["alternatives"])
    else:
        raise HTTPException(
            status_code=422,
            detail="נדרש מזהה אחד לפחות: q, lat+lon, gush+helka, zip או city+street")

    data = await _by_mode(mode, parsed, radius_m, limit,
                          geometry="geometry" in wanted,
                          stat_area="stat_area" in wanted,
                          deals="deals" in wanted)
    return _envelope(mode, {**parsed, "fields": sorted(wanted)},
                     [nadlan_query.project_property(d, wanted) for d in data])
