"""נדל"ן לעם — the read path.

Every lookup here is answered from the thin ``over_re_*`` crosswalk through an
index, never by scanning a source table (the parcels layer alone is 4.58 GB and
the gazetteer is 3.68 M rows). The one deliberate exception is
``parcel_geometry()``, which reaches into the parcels table by gush/parcel — one
row, on an index — because the polygon is the one thing the spine does not copy.

The envelope every mode returns is built by :func:`property_envelope`, so the
four entry points (gush/helka, address, zip, point+radius) are genuinely the same
answer reached four ways rather than four different shapes.

**"Extra fields from each source" without copying them.** Each source block
carries a ``console_sql`` and a ``row_url`` — a /data console deep-link holding
the exact query that returns that source's FULL row. The crosswalk stays thin as
required, the user still reaches every column of every source, and the link
doubles as the verification trail.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import time
from urllib.parse import quote

from app.services import append_store, nadlan_text
from app.services.append_store import _qi
from app.services.index_mirror import GEOM_SRID, PG_EXT_SCHEMA
from app.services import nadlan_index
from app.services.nadlan_index import (
    ADDRESSES_TABLE, DEAL_SORT_KEY, GAZ_TABLE, PARCELS_SRC,
    PARCELS_TABLE, STAT_AREA_POP_SRC, STAT_AREA_SOCIO_SRC, STAT_AREA_SRC,
    STREETS_TABLE, ZIP5_TABLE, GAZTIR_SRC, POSTAL_SRC, ADDR_SRC, _t,
)

logger = logging.getLogger(__name__)

SITE = "https://www.over.org.il"
# Tighter than the console's 10s: an interactive lookup that needs longer is a
# missing index, and should fail loudly rather than hold a Neon compute open.
_TIMEOUT_MS = 5000
MAX_RADIUS_M = 2000
MAX_LIMIT = 200


async def _fetch(sql: str, *args) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {_TIMEOUT_MS}")
            # The read-only pool carries no search_path of its own. Functions and
            # casts here are schema-qualified, but OPERATORS resolve only through
            # the path — so name it explicitly rather than depend on the role's
            # default (see the OPERATOR(extensions.&&) note in nadlan_index).
            await conn.execute(f"SET LOCAL search_path = public, {PG_EXT_SCHEMA}")
            rows = await conn.fetch(sql, *args)
    return [dict(r) for r in rows]


def _console_url(sql: str) -> str:
    """A /data deep-link that runs ``sql``.

    The console reads the query from ``?q=`` as BASE64 (``sqlFromUrl`` in
    DataSqlPage.tsx): the plain ``?sql=`` form leaked SELECT/FROM/UNION into the
    Referer header of every subsequent request, which Cloudflare's SQLi rules
    answered with 403. The base64 must be percent-encoded too — it contains
    ``+``, ``/`` and ``=``."""
    b64 = base64.b64encode(sql.encode("utf-8")).decode("ascii")
    return f"{SITE}/data?q=" + quote(b64, safe="")


# The console only re-serialises a query into a shared link below ~1800 chars,
# so a deep-link that exceeds it would survive one click and then quietly stop
# round-tripping. Keep the generated WHERE clauses well under that.
_MAX_LINK_SQL = 1500


def _src_block(schema: str, table: str, where: str, fields: dict | None = None) -> dict:
    sql = f'SELECT * FROM {_qi(schema)}.{_qi(table)} WHERE {where} LIMIT 200'
    if len(sql) > _MAX_LINK_SQL:
        sql = f'SELECT * FROM {_qi(schema)}.{_qi(table)} LIMIT 200'
    return {
        "table": f"{schema}.{table}",
        "fields": fields or {},
        "console_sql": sql,
        "row_url": _console_url(sql),
    }


# ── the four entry modes ──────────────────────────────────────────────────────
async def by_gush_helka(gush: int, helka: int, suffix: int | None = None) -> list[dict]:
    sql = f"""
        SELECT p.* FROM public.{_qi(PARCELS_TABLE)} p
        WHERE p.gush = $1 AND p.parcel = $2
          AND ($3::int IS NULL OR p.gush_suffix = $3)
        ORDER BY p.gush_suffix
        LIMIT 50
    """
    return await _fetch(sql, gush, helka, suffix)


async def by_point(lat: float, lon: float, radius_m: float = 0.0,
                   limit: int = 50) -> list[dict]:
    """Radius 0 means "the parcel I am standing in" — an exact containment test
    against the source polygons (one point, GiST-indexed, verified fast).
    Anything larger is answered from the spine's centroids, so a wide radius
    never touches the 4.58 GB table."""
    limit = max(1, min(int(limit), MAX_LIMIT))
    if radius_m and radius_m > 0:
        radius_m = min(float(radius_m), MAX_RADIUS_M)
        sql = f"""
            SELECT p.*,
                   {_qi(PG_EXT_SCHEMA)}.ST_Distance(
                     p.centroid::{_qi(PG_EXT_SCHEMA)}.geography,
                     {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                       {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1),
                       {GEOM_SRID})::{_qi(PG_EXT_SCHEMA)}.geography) AS distance_m
            FROM public.{_qi(PARCELS_TABLE)} p
            WHERE {_qi(PG_EXT_SCHEMA)}.ST_DWithin(
                    p.centroid::{_qi(PG_EXT_SCHEMA)}.geography,
                    {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                      {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1),
                      {GEOM_SRID})::{_qi(PG_EXT_SCHEMA)}.geography, $3)
            ORDER BY distance_m
            LIMIT {limit}
        """
        return await _fetch(sql, lat, lon, radius_m)

    sql = f"""
        WITH hit AS (
          SELECT public.over_parcel_key(s."GUSH_NUM", s."GUSH_SUFFI", s."PARCEL") AS pk
          FROM {_t(PARCELS_SRC)} s
          WHERE s.geom OPERATOR({_qi(PG_EXT_SCHEMA)}.&&) {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                            {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1), {GEOM_SRID})
            AND {_qi(PG_EXT_SCHEMA)}.ST_Contains(
                  s.geom, {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                            {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1), {GEOM_SRID}))
          LIMIT 5
        )
        SELECT p.*, 0::double precision AS distance_m
        FROM hit JOIN public.{_qi(PARCELS_TABLE)} p ON p.parcel_key = hit.pk
    """
    return await _fetch(sql, lat, lon)


# What a tap on the map falls back to when it lands on nothing. Parcels do not
# tile the country: roads, open ground and unregistered land sit between them,
# and how much is between them varies enormously. Measured 2026-09-20 on random
# points inside a settlement's own envelope: 149/150 inside a parcel in Tel
# Aviv, 94/150 in Dimona. So an exact-containment answer of "nothing" is the
# normal case in a lot of the country, and returning it bare reads as a broken
# map rather than as a gap in the cadastre.
POINT_FALLBACK_RADIUS_M = 150.0


async def by_point_or_near(lat: float, lon: float, radius_m: float = 0.0,
                           limit: int = 50) -> tuple[list[dict], float]:
    """The parcel under the point, or the nearest ones, with which it was.

    Only an EXPLICIT radius of 0 widens: a caller that asked for 500 m and got
    nothing has been answered, and quietly re-asking a different question is how
    a result set stops meaning what the query said. The returned radius is what
    the answer actually used, so the caller can say which of the two it got."""
    parcels = await by_point(lat, lon, radius_m, limit)
    if parcels or radius_m:
        return parcels, radius_m
    parcels = await by_point(lat, lon, POINT_FALLBACK_RADIUS_M, limit)
    return parcels, POINT_FALLBACK_RADIUS_M if parcels else 0.0


async def by_zip(zip_code: str) -> tuple[list[dict], list[dict]]:
    """Return (addresses, parcels) for a ZIP5 or ZIP7.

    Coverage warning belongs with the caller: the postal file covers 91
    localities only, so a valid Israeli zip outside them simply has no rows."""
    z = (zip_code or "").strip()
    col = "zip7" if len(z) == 7 else "zip5"
    addrs = await _fetch(
        f"""SELECT * FROM public.{_qi(ADDRESSES_TABLE)}
            WHERE {col} = $1 ORDER BY street_name, house_num LIMIT 200""", z)
    keys = sorted({a["parcel_key"] for a in addrs if a.get("parcel_key")})
    parcels = await _fetch(
        f"""SELECT * FROM public.{_qi(PARCELS_TABLE)}
            WHERE parcel_key = ANY($1::text[]) LIMIT 200""", keys) if keys else []
    return addrs, parcels


async def by_address(city: str, street: str, number: str | None = None
                     ) -> tuple[list[dict], list[dict]]:
    house, _suffix = nadlan_text.parse_house_number(number) if number else (None, None)
    addrs = await _fetch(
        f"""
        WITH sc AS (SELECT public.over_settlement_code($1) AS code)
        SELECT a.* FROM public.{_qi(ADDRESSES_TABLE)} a, sc
        WHERE a.settlement_code = sc.code
          AND a.street_key = public.over_street_key(sc.code, $2)
          AND ($3::int IS NULL OR a.house_num = $3)
        ORDER BY a.house_num, a.house_suffix
        LIMIT 200
        """, city, street, house)
    keys = sorted({a["parcel_key"] for a in addrs if a.get("parcel_key")})
    parcels = await _fetch(
        f"""SELECT * FROM public.{_qi(PARCELS_TABLE)}
            WHERE parcel_key = ANY($1::text[]) LIMIT 200""", keys) if keys else []
    return addrs, parcels


# How near a parcel of the SETTLEMENT WE ASKED FOR a geocoded street point
# must land to be believed. GovMap answers the street name and ignores the
# locality — asked for הר הצופים in Dimona under the address index it returns
# sixteen streets in Kiryat Shmona, 240 km away — so this is not a formality,
# it is the whole difference between placing a street and inventing one.
# 2 km is generous for a street inside a town and nowhere near another town.
STREET_GEOCODE_GUARD_M = 2000
# What to show around a street we placed. Not "the address": the parcels the
# street runs through, said as that.
STREET_GEOCODE_PARCELS = 6


async def locate_register_only_street(settlement_code: int, settlement_name: str,
                                      street: str) -> dict | None:
    """A point for a street only the register knows, and the parcels around it.

    None unless GovMap answers AND the answer is inside the settlement that was
    asked for. Best-effort throughout: this runs in a request path, on a query
    that has already failed, and a geocoder that is slow or down must cost the
    reader nothing beyond the answer they were already getting."""
    from app.services import street_geocode
    try:
        hit = await street_geocode.locate_street(settlement_name, street)
    except Exception as e:  # noqa: BLE001
        logger.info("nadlan: street geocode failed for %s: %s", street, e)
        return None
    if not hit:
        return None

    near = await _fetch(
        f"""
        SELECT p.parcel_key, p.gush, p.gush_suffix, p.parcel, p.settlement_code,
               round({_qi(PG_EXT_SCHEMA)}.ST_Distance(
                 p.centroid::{_qi(PG_EXT_SCHEMA)}.geography,
                 {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                   {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1),
                   {GEOM_SRID})::{_qi(PG_EXT_SCHEMA)}.geography)::numeric, 0) AS distance_m
        FROM public.{_qi(PARCELS_TABLE)} p
        WHERE p.settlement_code = $3
          AND {_qi(PG_EXT_SCHEMA)}.ST_DWithin(
                p.centroid::{_qi(PG_EXT_SCHEMA)}.geography,
                {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                  {_qi(PG_EXT_SCHEMA)}.ST_MakePoint($2, $1),
                  {GEOM_SRID})::{_qi(PG_EXT_SCHEMA)}.geography, $4)
        ORDER BY distance_m
        LIMIT {STREET_GEOCODE_PARCELS}
    """, hit["lat"], hit["lon"], settlement_code, float(STREET_GEOCODE_GUARD_M))

    if not near:
        # The point is not in the settlement we asked about. This is the Kiryat
        # Shmona case, and it is REFUSED rather than shown with a caveat.
        logger.info("nadlan: geocoded %s outside settlement %s — refused",
                    street, settlement_code)
        return None

    return {"lat": hit["lat"], "lon": hit["lon"], "label": hit.get("label"),
            "source": "GovMap — אינדקס הרחובות",
            "parcels": [{"parcel_key": r["parcel_key"], "gush": r["gush"],
                         "gush_suffix": r["gush_suffix"], "helka": r["parcel"],
                         "distance_m": float(r["distance_m"])} for r in near]}


async def explain_address_miss(city: str, street: str,
                               addresses: list[dict] | None = None) -> dict:
    """Why an address lookup came back empty, and what to try instead.

    An empty list is the one answer a person cannot act on: it does not say
    whether the town is unknown, the street is spelled differently, or the
    street is real and simply has nothing behind it. Each of those is a
    different next move, and the index can tell them apart.

    The third case is not rare. The street index is built from the address list
    and the postal file, so a street that only רשות האוכלוסין's register knows
    is absent by construction — 29,454 of 63,567 official streets as measured on
    2026-09-20. הר הצופים in Dimona is one of them: it is in the register, and
    in neither the gazetteer, the postal file nor the address list, so nothing
    we hold can place it.

    ``addresses`` is what the lookup actually matched. It matters because the
    most common empty answer is not a miss at all: the address was found and has
    no PARCEL behind it, which the envelope cannot represent because it is built
    from parcels. 187,819 of 617,876 addresses (30.4%) are in that state and
    186,313 of them carry a zip. Reporting that as "no matching address" blamed
    the reader for a question they had asked correctly."""
    # Answered from what the lookup ACTUALLY matched, before any second query.
    # If there are rows, the settlement and street resolved by construction, and
    # the reader is owed them whether or not the probe below succeeds. This is
    # the common empty answer: 187,819 of 617,876 addresses (30.4%) have no
    # parcel, and 186,313 of those have a zip.
    if addresses:
        zips = sorted({a["zip7"] for a in addresses if a.get("zip7")})
        points = [a for a in addresses
                  if a.get("point") is not None or a.get("lat") is not None]

        # "The point does not fall inside a registered parcel" is a claim about
        # THIS address, and it is the wrong claim for a locality the parcel
        # register does not cover at all: there is nothing there to fall into.
        # 51 of 321 localities with addresses hold no parcels under their own
        # settlement code, covering 13,681 addresses (measured 2026-09-21).
        # Some are beyond the Green Line, where the register genuinely stops;
        # others file their parcels under a parent locality's code, and there
        # the point-in-polygon link succeeds anyway — which is why this asks the
        # register rather than assuming either.
        sc = addresses[0].get("settlement_code")
        covered = True
        if sc is not None:
            probe = await _fetch(
                f"SELECT EXISTS (SELECT 1 FROM public.{_qi(PARCELS_TABLE)} "
                "WHERE settlement_code = $1) AS ok", sc)
            covered = bool(probe and probe[0]["ok"])

        if not covered:
            why = ("מרשם החלקות שבידינו אינו כולל חלקות ביישוב הזה כלל, ולכן "
                   "אין גוש-חלקה להציג עבורה. זו מגבלת כיסוי של המרשם ולא כשל "
                   "בהתאמה — הכתובת עצמה נמצאה, והמיקום שלה תקף.")
        else:
            why = ("הכתובת אינה משויכת לחלקה, ולכן אין חלקה להציג. שיוך נעשה "
                   "לפי נקודה בתוך פוליגון, "
                   + ("ולכתובת הזו אין קואורדינטות." if not points
                      else "והנקודה של הכתובת הזו אינה נופלת בתוך חלקה רשומה.")
                   + " כ-29% מהכתובות בישראל במצב הזה. אפשר לאתר את החלקה "
                     "בלשונית המפה.")
        return {
            "reason": ("locality_not_in_parcel_register" if not covered
                       else "addresses_without_parcel"),
            "settlement_name": (addresses[0].get("settlement_name") or city),
            "street_name": (addresses[0].get("street_name") or street),
            "n_addresses": len(addresses),
            "zip7": zips,
            "has_point": bool(points),
            "parcel_register_covers_locality": covered,
            "message": "הכתובת נמצאה במאגר. " + why,
        }

    rows = await _fetch(
        """
        WITH sc AS (SELECT public.over_settlement_code($1) AS code)
        SELECT sc.code AS settlement_code,
               public.over_settlement($1) AS settlement_name,
               s.street_key, s.name AS street_name,
               s.in_address_list, s.in_postal, s.in_gazetteer, s.official_code
        FROM sc
        LEFT JOIN public.over_re_streets s
               ON s.settlement_code = sc.code
              AND s.street_key = public.over_street_key(sc.code, $2)
        """, city, street)
    r = rows[0] if rows else {}
    if not r.get("settlement_code"):
        return {"reason": "settlement_unknown",
                "message": f"לא זוהה יישוב בשם \"{city}\"."}

    sc = r["settlement_code"]
    # Where to send a reader we cannot help directly. Averaged over the
    # settlement's parcel centroids on the settlement_code btree — cheap, and
    # precise enough to open a map on the right town, which is the difference
    # between an honest dead end and an honest next step.
    town = await _fetch(
        f"""SELECT round(avg(lat)::numeric, 6) AS lat, round(avg(lon)::numeric, 6) AS lon
            FROM public.{_qi(PARCELS_TABLE)}
            WHERE settlement_code = $1 AND lat IS NOT NULL""", sc)
    point = ({"lat": float(town[0]["lat"]), "lon": float(town[0]["lon"])}
             if town and town[0].get("lat") is not None else None)

    if not r.get("street_key"):
        near = await suggest_streets(street, sc, 8)
        return {"reason": "street_unknown", "settlement_code": sc,
                "settlement_name": r.get("settlement_name"),
                "message": (f"לא נמצא רחוב בשם \"{street}\" ב{r.get('settlement_name') or city}."
                            + (" האם התכוונתם לאחד מאלה?" if near else "")),
                "suggestions": [n["name"] for n in near],
                "settlement_point": point}

    located = any(r.get(k) for k in ("in_address_list", "in_postal", "in_gazetteer"))
    if not located:
        # The register knows the street and nothing we hold places it — so ask
        # the one index that can, and only believe it inside this settlement.
        placed = await locate_register_only_street(
            sc, r.get("settlement_name") or city, r.get("street_name") or street)
        if placed:
            return {
                "reason": "street_located_externally",
                "settlement_code": sc,
                "settlement_name": r.get("settlement_name"),
                "street_name": r.get("street_name"),
                "official_code": r.get("official_code"),
                "street_point": {"lat": placed["lat"], "lon": placed["lon"]},
                "street_point_source": placed["source"],
                "nearby_parcels": placed["parcels"],
                "settlement_point": point,
                "message": (
                    f"הרחוב \"{r.get('street_name')}\" קיים במרשם הרחובות הרשמי "
                    f"(קוד {r.get('official_code')}) ואינו מופיע באף אחד משלושת "
                    f"המקורות שאנחנו מצליבים, ולכן אין לנו כתובות משלו. "
                    f"מיקום הרחוב נלקח מאינדקס הרחובות של GovMap ואומת שהוא "
                    f"בתוך {r.get('settlement_name') or city}. אלה החלקות "
                    f"שבסביבתו — לא בהכרח החלקה של מספר הבית שחיפשתם."),
            }

        return {"reason": "street_not_located", "settlement_code": sc,
                "settlement_name": r.get("settlement_name"),
                "street_name": r.get("street_name"),
                "official_code": r.get("official_code"),
                "message": (f"הרחוב \"{r.get('street_name')}\" קיים במרשם הרחובות הרשמי "
                            f"(קוד {r.get('official_code')}), אבל אף אחד משלושת המקורות "
                            f"שנושאים מיקום — רשימת הכתובות, קובץ המיקוד וגזטיר הנכסים — "
                            f"אינו מזכיר אותו, ולכן אין באפשרותנו למקם אותו על המפה. "
                            f"אפשר לאתר את החלקה בלשונית המפה."),
                "settlement_point": point}

    return {"reason": "no_house_match", "settlement_code": sc,
            "settlement_name": r.get("settlement_name"),
            "street_name": r.get("street_name"),
            "message": (f"הרחוב \"{r.get('street_name')}\" מוכר, אך לא נמצאה כתובת "
                        f"תואמת במספר הבית הזה. נסו בלי מספר בית.")}


# ── the two layers that hang off a parcel ─────────────────────────────────────
# Neither identifies the property, they describe it, so both are attached to the
# envelope rather than folded into ``identity``, and both are computed per lookup
# instead of being stamped onto the 1.1 M-row spine: each is one index-backed
# read over a result set capped at 200 parcels, so a build stage (and the compute
# bill that comes with it) would buy nothing here.


def _iso(yyyymmdd: str | None) -> str | None:
    """The register's DD/MM/YYYY date, already rearranged to YYYYMMDD by
    ``DEAL_SORT_KEY``, handed out as ISO."""
    v = (yyyymmdd or "").strip()
    return f"{v[0:4]}-{v[4:6]}-{v[6:8]}" if len(v) == 8 and v.isdigit() else None


def _cbs_int(v) -> int | None:
    """CBS publishes every field of a .gdb layer as float text ('613.0')."""
    try:
        return int(float(str(v).strip()))
    except (TypeError, ValueError):
        return None


async def stat_areas(parcel_keys: list[str]) -> dict[str, dict]:
    """parcel_key -> its CBS statistical area (א"ס).

    Resolved SPATIALLY, by putting the parcel centroid inside an area polygon: a
    settlement holds dozens of areas, so its code cannot answer this, and the
    2022 layer is the only place the division is drawn.

    Two divisions are read and they are NOT merged. Identity and population come
    from the 2022 division (the population file is published on the same
    geometry, so it attaches by code, 3,847 of 3,857 areas). The socio-economic
    cluster exists only on the older 2011 division, which draws different
    boundaries, so it gets its own point-in-polygon against its own polygons and
    is labelled with its own year rather than being passed off as a property of
    the 2022 area.
    """
    keys = list(dict.fromkeys(k for k in parcel_keys if k))[:MAX_LIMIT]
    if not keys:
        return {}
    rows = await _fetch(f"""
        WITH k AS (
          SELECT parcel_key, centroid FROM public.{_qi(PARCELS_TABLE)}
          WHERE parcel_key = ANY($1::text[]) AND centroid IS NOT NULL
        )
        SELECT k.parcel_key,
               sa.stat_code, sa.yishuv_stat, sa.area_name, sa.rova, sa.tat_rova,
               nullif(btrim(pop."Pop_Total"), '')         AS pop_total,
               nullif(btrim(pop."Main_Function_Txt"), '') AS main_function,
               nullif(btrim(so.eshkol_madad2021), '')     AS eshkol_2021,
               nullif(btrim(so."YISHUV_STAT11"), '')      AS yishuv_stat_2011
        FROM k
        LEFT JOIN LATERAL (
          SELECT nullif(btrim(a."STAT_2022"), '')        AS stat_code,
                 nullif(btrim(a."YISHUV_STAT_2022"), '') AS yishuv_stat,
                 nullif(btrim(a."SHEM_YISHUV"), '')      AS area_name,
                 nullif(btrim(a."ROVA"), '')             AS rova,
                 nullif(btrim(a."TAT_ROVA"), '')         AS tat_rova
          FROM {_t(STAT_AREA_SRC)} a
          WHERE a.geom OPERATOR({_qi(PG_EXT_SCHEMA)}.&&) k.centroid
            AND {_qi(PG_EXT_SCHEMA)}.ST_Contains(a.geom, k.centroid)
          LIMIT 1
        ) sa ON true
        LEFT JOIN {_t(STAT_AREA_POP_SRC)} pop
               ON btrim(pop."YISHUV_STAT22") = sa.yishuv_stat
        LEFT JOIN LATERAL (
          SELECT b.eshkol_madad2021, b."YISHUV_STAT11"
          FROM {_t(STAT_AREA_SOCIO_SRC)} b
          WHERE b.geom OPERATOR({_qi(PG_EXT_SCHEMA)}.&&) k.centroid
            AND {_qi(PG_EXT_SCHEMA)}.ST_Contains(b.geom, k.centroid)
          LIMIT 1
        ) so ON true
    """, keys)

    out: dict[str, dict] = {}
    for r in rows:
        code = _cbs_int(r.get("stat_code"))
        full = _cbs_int(r.get("yishuv_stat"))
        socio = _cbs_int(r.get("eshkol_2021"))
        if code is None and full is None and socio is None:
            continue
        out[r["parcel_key"]] = {
            # The area's own number inside its settlement, and the national
            # 8-digit form (settlement * 10000 + area) that CBS tables key on.
            "code": code,
            "yishuv_stat": full,
            "settlement_name": r.get("area_name"),
            "rova": _cbs_int(r.get("rova")),
            "tat_rova": _cbs_int(r.get("tat_rova")),
            "division": "2022",
            "population": _cbs_int(r.get("pop_total")),
            "population_year": 2024,
            "main_function": r.get("main_function"),
            # Kept apart on purpose: a DIFFERENT division, so it carries the year
            # of both the boundaries and the index.
            "socio": ({"eshkol": socio, "index_year": 2021, "division": "2011",
                       "yishuv_stat": _cbs_int(r.get("yishuv_stat_2011"))}
                      if socio is not None else None),
        }
    return out


# A parcel in a condo tower carries hundreds of deals (1,850 on the busiest one
# measured), so the envelope gets a SUMMARY and the full list is its own paged
# endpoint, the same split the polygon already uses.
MAX_DEALS = 200


def _deal_row(r: dict) -> dict:
    """One deal, typed. Every numeric column in the register is clean integer
    text (measured over all 3.84 M rows), so a value that fails to parse means
    the publisher changed the format: the field is dropped, never guessed at."""
    def num(key):
        try:
            return int(str(r.get(key) or "").strip())
        except ValueError:
            return None

    d = (r.get("deal_date") or "").strip()
    amount, area = num("deal_amount"), num("asset_area")
    # `portion` is the share of the asset that changed hands (0.000-1.000, all
    # 3.84 M rows clean), while `asset_area` is the WHOLE asset's area and the
    # amount pays only for the share. amount/area therefore halves on a 50%
    # sale; amount/(area*portion) is the price per sqm actually bought, the one
    # figure comparable across deals. A share of 0.000 is a sub-0.0005 share
    # rounded away and gets no normalised price rather than a division by zero.
    try:
        share = float((r.get("portion") or "").strip())
    except ValueError:
        share = None
    return {
        "date": f"{d[6:10]}-{d[3:5]}-{d[0:2]}" if len(d) == 10 else None,
        "date_src": d or None,
        "amount": amount,
        "declared_amount": num("declared_amount"),
        "nature": (r.get("deal_nature") or "").strip() or None,
        "area_sqm": area,
        "rooms": num("room_num"),
        "year_built": num("year_built"),
        "portion": (r.get("portion") or "").strip() or None,
        "portion_fraction": share,
        "price_per_sqm": round(amount / area) if amount and area else None,
        "price_per_sqm_normalized": (round(amount / (area * share))
                                     if amount and area and share else None),
        "sub_parcel": (r.get("sub_chelka") or "").strip() or None,
    }


async def deal_summaries(parcels: list[dict]) -> dict[str, dict]:
    """parcel_key -> a summary of its מיסוי מקרקעין deals.

    The register publishes no gush suffix, so deals attach on גוש+חלקה, the
    suffix-less ``gp_key``. Where that pair covers several real parcels the same
    deals are reported against each of them, which is why ``gp_ambiguous``
    downgrades this block exactly as it downgrades the gazetteer."""
    pairs = list(dict.fromkeys((str(p["gush"]), str(p["parcel"])) for p in parcels))[:MAX_LIMIT]
    src = await nadlan_index.deals_table()
    if not pairs or not src:
        return {}
    rows = await _fetch(f"""
        WITH k AS (SELECT * FROM unnest($1::text[], $2::text[]) AS t(g, h))
        SELECT k.g, k.h, agg.deals, agg.first_deal, agg.last_deal, agg.sub_parcels,
               newest.deal_date, newest.deal_amount, newest.declared_amount,
               newest.deal_nature, newest.asset_area, newest.room_num,
               newest.year_built, newest.portion, newest.sub_chelka
        FROM k
        JOIN LATERAL (
          SELECT count(*) AS deals,
                 min({DEAL_SORT_KEY}) AS first_deal,
                 max({DEAL_SORT_KEY}) AS last_deal,
                 count(DISTINCT sub_chelka) AS sub_parcels
          FROM {_t(src)} WHERE gush = k.g AND chelka = k.h
        ) agg ON agg.deals > 0
        LEFT JOIN LATERAL (
          SELECT deal_date, deal_amount, declared_amount, deal_nature, asset_area,
                 room_num, year_built, portion, sub_chelka
          FROM {_t(src)} WHERE gush = k.g AND chelka = k.h
          ORDER BY {DEAL_SORT_KEY} DESC LIMIT 1
        ) newest ON true
    """, [g for g, _ in pairs], [h for _, h in pairs])

    by_gp = {f"{r['g']}-{r['h']}": {
        "deals": r["deals"],
        "first_deal": _iso(r["first_deal"]),
        "last_deal": _iso(r["last_deal"]),
        "sub_parcels": r["sub_parcels"],
        "latest": _deal_row(r),
    } for r in rows}
    return {p["parcel_key"]: by_gp[p["gp_key"]]
            for p in parcels if p.get("gp_key") in by_gp}


async def parcel_deals(gush: int, helka: int, limit: int = 50, offset: int = 0,
                       sub_parcel: str | None = None) -> tuple[list[dict], int]:
    """Every deal reported on one גוש/חלקה, newest first, plus the total.

    ``sub_parcel`` narrows to one תת-חלקה, which in a condo tower is the single
    apartment: the only grain at which a price series means anything."""
    limit = max(1, min(int(limit), MAX_DEALS))
    src = await nadlan_index.deals_table()
    if not src:
        return [], 0
    rows = await _fetch(f"""
        SELECT settlement_code, settlement, gush, chelka, sub_chelka, deal_date,
               deal_amount, declared_amount, deal_nature, portion, year_built,
               asset_area, room_num, count(*) OVER () AS total
        FROM {_t(src)}
        WHERE gush = $1 AND chelka = $2
          AND ($4::text IS NULL OR sub_chelka = $4)
        ORDER BY {DEAL_SORT_KEY} DESC
        LIMIT {limit} OFFSET $3::int
    """, str(gush), str(helka), int(max(0, offset)), sub_parcel)
    total = rows[0]["total"] if rows else 0
    return [_deal_row(r) | {"settlement": (r.get("settlement") or "").strip() or None,
                            "settlement_code": _cbs_int(r.get("settlement_code"))}
            for r in rows], total


# ── the unified envelope ──────────────────────────────────────────────────────
async def property_envelope(parcels: list[dict], *, addresses: list[dict] | None = None,
                            include_addresses: bool = True,
                            with_geometry: bool = False,
                            with_stat_area: bool = True,
                            with_deals: bool = True) -> list[dict]:
    """Turn parcel rows into the full cross-source answer.

    One shape for all four entry modes: identity in every codespace, one block
    per source (each with a deep-link to its untouched full row), and an explicit
    ``match`` block saying how certain the link is — ``gp_ambiguous`` downgrades
    the gazetteer to "approximate" instead of pretending the suffix was known.

    ``stat_area`` and ``deals`` are on by default because they are what a caller
    asking about a point actually wants, and both are index-backed reads over a
    capped result set. They are still switchable: a caller that only needs the
    identity should not pay for either."""
    if not parcels:
        return []
    keys = [p["parcel_key"] for p in parcels]

    # The polygon rides on the SAME envelope as everything else, so a property
    # found by zip or by address is as locatable on the map as one found by
    # clicking it — the identity and its shape never diverge.
    geoms = await parcel_geometries(keys) if with_geometry else {}

    # Independent of each other and of everything above, so they go together
    # rather than one round trip after the other.
    # Best-effort, and deliberately not fatal: both corpora are tracked
    # datasets like any other, so a deployment that does not track one — or one
    # mid-reseed — must still answer the four identity sources rather than 500.
    areas, deals = await asyncio.gather(
        stat_areas(keys) if with_stat_area else _none(),
        deal_summaries(parcels) if with_deals else _none(),
        return_exceptions=True,
    )
    for name, value in (("stat_area", areas), ("deals", deals)):
        if isinstance(value, BaseException):
            logger.warning("nadlan: %s lookup unavailable: %s", name, value)
    areas = areas if isinstance(areas, dict) else {}
    deals = deals if isinstance(deals, dict) else {}
    deals_src = await nadlan_index.deals_table() if with_deals else None

    gaz = {g["parcel_key"]: g for g in await _fetch(
        f"""SELECT * FROM public.{_qi(GAZ_TABLE)}
            WHERE parcel_key = ANY($1::text[])""", keys)}

    addr_by_parcel: dict[str, list[dict]] = {}
    if include_addresses:
        rows = addresses if addresses is not None else await _fetch(
            f"""SELECT * FROM public.{_qi(ADDRESSES_TABLE)}
                WHERE parcel_key = ANY($1::text[])
                ORDER BY street_name, house_num LIMIT 500""", keys)
        for a in rows:
            if a.get("parcel_key"):
                addr_by_parcel.setdefault(a["parcel_key"], []).append(a)

    out = []
    for p in parcels:
        pk = p["parcel_key"]
        g = gaz.get(pk) or {}
        addrs = addr_by_parcel.get(pk, [])
        zips = sorted({a["zip7"] for a in addrs if a.get("zip7")})
        zip5s = sorted({a["zip5"] for a in addrs if a.get("zip5")})
        notes = []
        confidence = "exact"
        if p.get("gp_ambiguous"):
            confidence = "approximate"
            notes.append("הגזטיר אינו מפרסם תת-גוש, ולגוש-חלקה הזה יש יותר מחלקה אחת "
                         "— נתוני הגזטיר עשויים להשתייך לחלקה אחרת באותו מספר.")
        if not addrs:
            notes.append("לא נמצאו כתובות מקושרות לחלקה זו.")

        out.append({
            "parcel_key": pk,
            "identity": {
                "gush": p["gush"], "gush_suffix": p["gush_suffix"], "helka": p["parcel"],
                "gp_key": p["gp_key"],
                "settlement": {"code": p.get("settlement_code"),
                               "name": p.get("locality_name")},
                "region": {"reg_mun": p.get("reg_mun_name"),
                           "county": p.get("county_name"),
                           "region": p.get("region_name")},
                "point": ({"lat": p["lat"], "lon": p["lon"]}
                          if p.get("lat") is not None else None),
                "distance_m": p.get("distance_m"),
                "zip7": zips, "zip5": zip5s,
                "streets": sorted({a["street_name"] for a in addrs if a.get("street_name")})
                           or ([g["street_name_src"]] if g.get("street_name_src") else []),
                "addresses": [
                    {"street": a.get("street_name"), "house": a.get("house_num"),
                     "suffix": a.get("house_suffix"), "entrance": a.get("entrance"),
                     "zip7": a.get("zip7"), "neighbourhood": a.get("neighbourhood"),
                     "lat": a.get("lat"), "lon": a.get("lon"),
                     "match": a.get("parcel_match")}
                    for a in addrs[:100]
                ],
            },
            "sources": {
                "parcels": _src_block(
                    *PARCELS_SRC,
                    where=(f'public.over_parcel_key("GUSH_NUM","GUSH_SUFFI","PARCEL") '
                           f"= {_lit(pk)}"),
                    fields={"legal_area": p.get("legal_area"),
                            "status": p.get("status_text"),
                            "locality": p.get("locality_name")}),
                "gazetteer": _src_block(
                    *GAZTIR_SRC,
                    where=(f'"GushNum" ~ \'^[0-9]+(\\.0*)?$\' AND "ParcelNum" ~ \'^[0-9]+(\\.0*)?$\' '
                           f'AND split_part("GushNum",\'.\',1)::int = {p["gush"]} '
                           f'AND split_part("ParcelNum",\'.\',1)::int = {p["parcel"]}'),
                    fields={k: g.get(k) for k in
                            ("n_assets", "n_dwellings", "n_subparcels", "floors_max",
                             "building_year_min", "building_year_max", "apartments_est",
                             "street_name_src", "street_code")} if g else {}),
                "postal": _src_block(
                    *POSTAL_SRC,
                    where=(f'"ZIP 7" = ANY(ARRAY[{",".join(_lit(z) for z in zips)}])'
                           if zips else "false"),
                    fields={"zip7": zips, "zip5": zip5s}),
                "address_list": _src_block(
                    *ADDR_SRC,
                    where=(" OR ".join(
                        f"(city = {_lit(a.get('settlement_name') or '')} "
                        f"AND street = {_lit(a.get('street_name') or '')})"
                        for a in addrs[:20]) or "false"),
                    fields={"n_addresses": len(addrs)}),
                # Absent, not empty, on a deployment that does not track the
                # deals corpus: a link to a table that is not there is worse
                # than no block.
                **({"deals": _src_block(
                    *deals_src,
                    where=f'gush = {_lit(p["gush"])} AND chelka = {_lit(p["parcel"])}',
                    fields={k: (deals.get(pk) or {}).get(k)
                            for k in ("deals", "first_deal", "last_deal",
                                      "sub_parcels")})} if deals_src else {}),
                "stat_area": _src_block(
                    *STAT_AREA_SRC,
                    where=(f'"YISHUV_STAT_2022" = {_lit((areas.get(pk) or {}).get("yishuv_stat"))}'
                           if (areas.get(pk) or {}).get("yishuv_stat") else "false"),
                    fields={k: (areas.get(pk) or {}).get(k)
                            for k in ("code", "yishuv_stat", "rova", "tat_rova",
                                      "population", "main_function")}),
            },
            # The two descriptive layers. ``stat_area`` is spatial (the centroid
            # inside a CBS area) and so is exact wherever the parcel has a point;
            # ``deals`` attaches on גוש+חלקה and therefore inherits gp_ambiguous.
            "stat_area": areas.get(pk),
            "deals": deals.get(pk),
            "match": {"method": "gp_key" if g else None,
                      "confidence": confidence, "notes": notes},
            "geometry": geoms.get(pk),
        })
    return out


# ── choosing what comes back ──────────────────────────────────────────────────
# The unified lookup lets a caller say which parts of the answer it wants. The
# names are the envelope's own keys, so ``fields=`` is readable against a sample
# response rather than against this list — and an unknown name is refused rather
# than silently ignored, because a typo that quietly drops a block is the worst
# failure mode an API like this has.
FIELDS = ("identity", "point", "zip", "streets", "addresses", "stat_area",
          "deals", "geometry", "sources", "match")
# Everything except the polygon: it is the one part that reads the 4.58 GB
# source table, so it is always asked for explicitly.
DEFAULT_FIELDS = tuple(f for f in FIELDS if f != "geometry")


def project_property(prop: dict, fields: set[str]) -> dict:
    """One property, narrowed to the requested parts.

    ``parcel_key`` and the גוש/חלקה identity always survive: an answer you
    cannot tie back to a parcel is not an answer."""
    ident = prop["identity"]
    out = {"parcel_key": prop["parcel_key"], "identity": {
        k: ident[k] for k in ("gush", "gush_suffix", "helka", "gp_key")}}
    if "identity" in fields:
        out["identity"].update({"settlement": ident["settlement"],
                                "region": ident["region"],
                                "distance_m": ident["distance_m"]})
    if "point" in fields:
        out["identity"]["point"] = ident["point"]
    if "zip" in fields:
        out["identity"]["zip7"] = ident["zip7"]
        out["identity"]["zip5"] = ident["zip5"]
    if "streets" in fields:
        out["identity"]["streets"] = ident["streets"]
    if "addresses" in fields:
        out["identity"]["addresses"] = ident["addresses"]
    for key in ("stat_area", "deals", "geometry", "sources", "match"):
        if key in fields:
            out[key] = prop[key]
    return out


async def _none() -> dict:
    """An awaitable empty result, so a disabled enrichment still fits the
    ``asyncio.gather`` above instead of branching around it."""
    return {}


def _lit(v) -> str:
    """A single-quoted SQL literal for the deep-link text (never executed here)."""
    return "'" + str(v).replace("'", "''") + "'"


# ── detail + support ──────────────────────────────────────────────────────────
# Bulk map geometry. A coarser tolerance than the single-parcel detail view
# (~2 m vs ~0.5 m) because this draws many parcels at once: measured on real
# urban parcels it takes a 3,067-char polygon down to 257 without a visible
# difference at neighbourhood zoom, so 200 parcels cost ~60 KB rather than MBs.
BULK_SIMPLIFY = 0.00002
DETAIL_SIMPLIFY = 0.000005
MAX_GEOMETRIES = 200


async def parcel_geometries(parcel_keys, simplify: float = BULK_SIMPLIFY) -> dict[str, str]:
    """parcel_key → GeoJSON polygon, for drawing results on the map.

    The polygons are the one thing the spine deliberately does not carry, so this
    reaches into the 4.58 GB source table — but bounded twice: the gush list
    drives the existing ``"GUSH_NUM"`` btree (a text equality, so no cast defeats
    the index), and the key list then narrows to the exact parcels."""
    keys = list(dict.fromkeys(parcel_keys))[:MAX_GEOMETRIES]
    if not keys:
        return {}
    gushes = sorted({k.split("-")[0] for k in keys if k})
    # The source writes plain integers today; accept the float-text form too so a
    # future re-scrape that reformats them cannot silently return nothing.
    gushes += [f"{g}.0" for g in gushes]
    rows = await _fetch(
        f"""
        SELECT public.over_parcel_key(s."GUSH_NUM", s."GUSH_SUFFI", s."PARCEL") AS parcel_key,
               {_qi(PG_EXT_SCHEMA)}.ST_AsGeoJSON(
                 {_qi(PG_EXT_SCHEMA)}.ST_SimplifyPreserveTopology(s.geom, $3)) AS geojson
        FROM {_t(PARCELS_SRC)} s
        WHERE s."GUSH_NUM" = ANY($1::text[])
          AND s.geom IS NOT NULL
          AND public.over_parcel_key(s."GUSH_NUM", s."GUSH_SUFFI", s."PARCEL") = ANY($2::text[])
        """, gushes, keys, simplify)
    return {r["parcel_key"]: r["geojson"] for r in rows if r.get("geojson")}


async def parcel_geometry(gush: int, suffix: int, helka: int,
                          simplify: float = DETAIL_SIMPLIFY) -> dict | None:
    """The polygon — the one thing the spine deliberately does not carry.

    Reached on the source table's ``GUSH_NUM`` btree (one row), and simplified
    before ``ST_AsGeoJSON`` so a large rural parcel does not ship megabytes."""
    rows = await _fetch(
        f"""
        SELECT {_qi(PG_EXT_SCHEMA)}.ST_AsGeoJSON(
                 {_qi(PG_EXT_SCHEMA)}.ST_SimplifyPreserveTopology(s.geom, $4)) AS geojson,
               s."LEGAL_AREA" AS legal_area, s."STATUS_TEX" AS status_text
        FROM {_t(PARCELS_SRC)} s
        WHERE s."GUSH_NUM" = $1::text
          AND coalesce(nullif(split_part(coalesce(s."GUSH_SUFFI",'0'),'.',1),'')::int,0) = $2
          AND split_part(s."PARCEL",'.',1)::int = $3
        LIMIT 1
        """, str(gush), suffix, helka, simplify)
    return rows[0] if rows else None


async def suggest_streets(q: str, settlement_code: int | None = None,
                          limit: int = 20) -> list[dict]:
    """Streets whose name starts with ``q``, best first.

    A prefix match on the WHOLE name answers nothing for a name we do not carry
    — "הר הצופים" starts with הרהצופים and nothing does — so a query of more
    than one word that finds nothing retries on its first word. That is what
    turns a blank suggestion list into הר ארבל / הר גולן / הר חרמון, which is
    the help a person who mistyped actually needs."""
    limit = max(1, min(int(limit), 50))
    rows = await _suggest_streets_prefix(q, settlement_code, limit)
    if rows:
        return rows
    head = (q or "").strip().split()[0] if (q or "").strip().split() else ""
    if head and head != (q or "").strip():
        return await _suggest_streets_prefix(head, settlement_code, limit)
    return rows


async def _suggest_streets_prefix(q: str, settlement_code: int | None,
                                  limit: int) -> list[dict]:
    return await _fetch(
        f"""
        SELECT s.street_key, s.name, s.settlement_code, st.name AS settlement_name,
               (s.in_address_list OR s.in_postal OR s.in_gazetteer) AS located
        FROM public.{_qi(STREETS_TABLE)} s
        LEFT JOIN public.over_settlements st ON st.code = s.settlement_code
        WHERE ($2::int IS NULL OR s.settlement_code = $2)
          AND s.name_norm LIKE public.over_settlement_norm($1) || '%'
        -- A street we can place ranks above one the register alone knows: both
        -- are real, only one of them can answer a lookup.
        ORDER BY (s.in_address_list OR s.in_postal OR s.in_gazetteer) DESC,
                 length(s.name_norm), s.name
        LIMIT {limit}
        """, q, settlement_code)


# stats() is 12 COUNT(*)s over 1.1M parcels and 622k addresses — measured at
# 2.1-2.7s against the 5s statement timeout, and the page calls it on EVERY load.
# That is both a 500 waiting for a cold Neon compute (one was observed) and a
# pointless compute bill on a plan that is ~98% compute. The numbers only move
# when a build runs, so serve them from a short process-local cache — the same
# convention data_catalog uses for the catalog.
_STATS_TTL_SECONDS = 300.0
_stats_cache: dict | None = None
_stats_cache_at: float = 0.0
_stats_lock = asyncio.Lock()


def invalidate_stats_cache() -> None:
    """Drop the cached counters so the next read recomputes them.

    Called at the end of a build, so a rebuild's numbers show up at once
    instead of up to five minutes later."""
    global _stats_cache, _stats_cache_at
    _stats_cache = None
    _stats_cache_at = 0.0


async def stats() -> dict:
    """Hero counters AND the coverage numbers.

    Coverage is published, not hidden: the whole point of the project is the
    crosswalk, and a crosswalk whose gaps are invisible is worse than none.

    Served from a 5-minute cache (see above); the lock means a burst of page
    loads after the cache expires runs the counts ONCE, not once per request."""
    global _stats_cache, _stats_cache_at
    if _stats_cache is not None and time.monotonic() - _stats_cache_at < _STATS_TTL_SECONDS:
        return _stats_cache
    async with _stats_lock:
        # Re-check: another request may have filled it while we waited.
        if _stats_cache is not None and time.monotonic() - _stats_cache_at < _STATS_TTL_SECONDS:
            return _stats_cache
        out = await _stats_uncached()
        _stats_cache, _stats_cache_at = out, time.monotonic()
        return out


async def _stats_uncached() -> dict:
    rows = await _fetch(f"""
        SELECT
          (SELECT count(*) FROM public.{_qi(PARCELS_TABLE)})                          AS parcels,
          (SELECT count(*) FROM public.{_qi(PARCELS_TABLE)} WHERE gp_ambiguous)       AS parcels_ambiguous,
          (SELECT count(*) FROM public.{_qi(PARCELS_TABLE)}
             WHERE settlement_code IS NOT NULL)                                       AS parcels_with_settlement,
          (SELECT count(*) FROM public.{_qi(GAZ_TABLE)})                              AS parcels_with_gazetteer,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)})                        AS addresses,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} WHERE point IS NOT NULL) AS addresses_with_point,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} WHERE zip7 IS NOT NULL)  AS addresses_with_zip,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}
             WHERE zip_level = 'address')                                             AS addresses_with_address_zip,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}
             WHERE zip_level = 'locality')                                            AS addresses_with_locality_zip,
          -- Linked to a parcel BY ANY ROUTE. point-in-polygon is no longer the
          -- only one: GovMap's parcel sweep arrives with the parcel already
          -- attached and stamps parcel_match='govmap_parcel', so counting pip
          -- alone understated the coverage the page publishes as "משויכות
          -- לחלקה" — 67.3% against a real 72.2% on 2026-09-23 — and the gap
          -- grew by roughly 4,000 addresses a day as the sweep advanced.
          (SELECT count(parcel_key) FROM public.{_qi(ADDRESSES_TABLE)})               AS addresses_with_parcel,
          -- Kept beside it as the METHOD breakdown, not as the headline: a
          -- computed containment and a parcel the source handed us are
          -- different facts, and the caveats say which is which.
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}
             WHERE parcel_match = 'pip')                                              AS addresses_linked_pip,
          (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}
             WHERE parcel_match = 'govmap_parcel')                                    AS addresses_linked_govmap_parcel,
          (SELECT count(*) FROM public.{_qi(STREETS_TABLE)})                          AS streets,
          -- The streets some source with a LOCATION knows. The rest come from
          -- רשות האוכלוסין's register alone: real streets that nothing we hold
          -- can place, and not a population the gazetteer could ever match.
          (SELECT count(*) FROM public.{_qi(STREETS_TABLE)}
             WHERE in_address_list OR in_postal OR in_gazetteer)                      AS streets_located,
          (SELECT count(*) FROM public.{_qi(STREETS_TABLE)}
             WHERE NOT (in_address_list OR in_postal OR in_gazetteer))                AS streets_register_only,
          (SELECT count(*) FROM public.{_qi(STREETS_TABLE)} WHERE in_gazetteer)       AS streets_in_gazetteer,
          (SELECT count(*) FROM public.{_qi(ZIP5_TABLE)})                             AS zip5_codes,
          (SELECT count(DISTINCT settlement_code) FROM public.{_qi(ADDRESSES_TABLE)}) AS localities_with_addresses
    """)
    s = rows[0] if rows else {}

    def pct(a, b):
        return round(100.0 * (s.get(a) or 0) / (s.get(b) or 1), 1)

    s["coverage"] = {
        "addresses_with_point_pct": pct("addresses_with_point", "addresses"),
        "addresses_with_zip_pct": pct("addresses_with_zip", "addresses"),
        # Split on purpose: a locality-wide zip is a weaker fact than the
        # doorway's own, and collapsing them would overstate precision.
        "addresses_with_address_zip_pct": pct("addresses_with_address_zip", "addresses"),
        "addresses_with_locality_zip_pct": pct("addresses_with_locality_zip", "addresses"),
        "addresses_linked_pct": pct("addresses_with_parcel", "addresses"),
        "parcels_with_gazetteer_pct": pct("parcels_with_gazetteer", "parcels"),
        # Over the streets it COULD match, not over every street in the index.
        # Seeding from the official register doubled the index, and reporting
        # this against the whole of it made a match rate that had risen from
        # 76.6% to 94.7% publish itself as a fall to 50.3%.
        "streets_in_gazetteer_pct": pct("streets_in_gazetteer", "streets_located"),
        "streets_register_only_pct": pct("streets_register_only", "streets"),
    }
    return s


async def is_ready() -> bool:
    """True once the spine has rows — the API 503s until the build has run."""
    try:
        rows = await _fetch(
            f"SELECT 1 FROM public.{_qi(PARCELS_TABLE)} LIMIT 1")
        return bool(rows)
    except Exception:  # noqa: BLE001 — table may not exist yet
        return False
