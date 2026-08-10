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

import base64
import logging
from urllib.parse import quote

from app.services import append_store, nadlan_text
from app.services.append_store import _qi
from app.services.index_mirror import GEOM_SRID, PG_EXT_SCHEMA
from app.services.nadlan_index import (
    ADDRESSES_TABLE, GAZ_TABLE, PARCELS_SRC, PARCELS_TABLE, STREETS_TABLE,
    ZIP5_TABLE, GAZTIR_SRC, POSTAL_SRC, ADDR_SRC, _t,
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
          WHERE s.geom && {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
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


# ── the unified envelope ──────────────────────────────────────────────────────
async def property_envelope(parcels: list[dict], *, addresses: list[dict] | None = None,
                            include_addresses: bool = True) -> list[dict]:
    """Turn parcel rows into the full cross-source answer.

    One shape for all four entry modes: identity in every codespace, one block
    per source (each with a deep-link to its untouched full row), and an explicit
    ``match`` block saying how certain the link is — ``gp_ambiguous`` downgrades
    the gazetteer to "approximate" instead of pretending the suffix was known."""
    if not parcels:
        return []
    keys = [p["parcel_key"] for p in parcels]

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
            },
            "match": {"method": "gp_key" if g else None,
                      "confidence": confidence, "notes": notes},
        })
    return out


def _lit(v) -> str:
    """A single-quoted SQL literal for the deep-link text (never executed here)."""
    return "'" + str(v).replace("'", "''") + "'"


# ── detail + support ──────────────────────────────────────────────────────────
async def parcel_geometry(gush: int, suffix: int, helka: int,
                          simplify: float = 0.000005) -> dict | None:
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
    limit = max(1, min(int(limit), 50))
    return await _fetch(
        f"""
        SELECT s.street_key, s.name, s.settlement_code, st.name AS settlement_name
        FROM public.{_qi(STREETS_TABLE)} s
        LEFT JOIN public.over_settlements st ON st.code = s.settlement_code
        WHERE ($2::int IS NULL OR s.settlement_code = $2)
          AND s.name_norm LIKE public.over_settlement_norm($1) || '%'
        ORDER BY length(s.name_norm), s.name
        LIMIT {limit}
        """, q, settlement_code)


async def stats() -> dict:
    """Hero counters AND the coverage numbers.

    Coverage is published, not hidden: the whole point of the project is the
    crosswalk, and a crosswalk whose gaps are invisible is worse than none."""
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
             WHERE parcel_match = 'pip')                                              AS addresses_linked_pip,
          (SELECT count(*) FROM public.{_qi(STREETS_TABLE)})                          AS streets,
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
        "addresses_linked_pct": pct("addresses_linked_pip", "addresses"),
        "parcels_with_gazetteer_pct": pct("parcels_with_gazetteer", "parcels"),
        "streets_in_gazetteer_pct": pct("streets_in_gazetteer", "streets"),
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
