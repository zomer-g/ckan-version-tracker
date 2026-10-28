"""נדל"ן לעם — a source's own rows for one parcel, on demand.

The envelope (``nadlan_query.property_envelope``) carries each source as a
SUMMARY: the gazetteer as "12 assets, 9 dwellings, up to 6 floors". That is
what a lookup of 200 parcels can afford. Someone who opened one parcel wants
the rows themselves — every asset the gazetteer lists, with its type, floor
and year — and this module returns them, paged, with Hebrew captions.

Every read goes through an index of its source table, and is bounded by
``MAX_ROWS``:

* gazetteer — the (``GushNum``, ``ParcelNum``) btree, matched as TEXT against
  the forms the source writes ("6319", "6319.0"), so no cast defeats it;
* parcels   — the ``GUSH_NUM`` btree, then the exact parcel key (one row);
* postal    — ``ZIP 7``, for the zip codes of the parcel's own addresses;
* stat_area — the area the parcel's point falls in (one row).

Rows come back through ``to_jsonb(row)`` minus the geometry, so a PostGIS
column never has to be decoded here, and in the table's own column order.
"""
from __future__ import annotations

import json
import time

from app.services import column_aliases
from app.services.append_store import _qi
from app.services.nadlan_index import (
    ADDRESSES_TABLE, GAZTIR_SRC, PARCELS_SRC, POSTAL_SRC, STAT_AREA_SRC, _t,
)
from app.services.nadlan_query import _console_url, _fetch, _lit, stat_areas

MAX_ROWS = 200

SOURCES = ("gazetteer", "parcels", "postal", "stat_area")

# Bookkeeping and geometry — never what a reader opened the rows for.
# Compared lower-case.
_NOISE = {
    "_id", "_row_hash", "_first_seen", "row_hash", "first_seen", "last_seen",
    "geom", "geometry", "geometry_wkt", "shape", "shape_area", "shape_length",
    "shape_leng", "objectid", "fid", "ogc_fid", "globalid",
}

# These sources publish machine names and no captions of their own (and none
# is in over_column_aliases). The names are the ones production returns, read
# off live rows on 2026-10-28; a column not listed keeps its own name rather
# than a guessed caption.
LABELS = {
    "gazetteer": {
        "Type": "סוג נכס",
        "SubParcelNum": "תת-חלקה",
        "BuildingFloors": "קומות בבניין",
        "BuildingYear": "שנת בנייה",
        "EstimatedAppartments": "אומדן דירות",
        "StreetNameHeb": "רחוב",
        "StreetCode": "סמל רחוב",
        "AssetID": "מזהה נכס",
        "Region_ID": "סמל מחוז",
    },
    "parcels": {
        "PARCEL_ID": "מזהה חלקה", "GUSH_NUM": "גוש", "GUSH_SUFFI": "תת-גוש",
        "PARCEL": "חלקה", "LEGAL_AREA": "שטח רשום (מ״ר)", "STATUS": "קוד סטטוס",
        "STATUS_TEX": "סטטוס", "LOCALITY_I": "סמל יישוב", "LOCALITY_N": "יישוב",
        "REG_MUN_ID": "סמל מועצה אזורית", "REG_MUN_NA": "מועצה אזורית",
        "COUNTY_ID": "סמל נפה", "COUNTY_NAM": "נפה", "REGION_ID": "סמל מחוז",
        "REGION_NAM": "מחוז", "TALAR_NUMB": "מספר תצ״ר", "TALAR_YEAR": "שנת תצ״ר",
        "SYS_DATE": "תאריך עדכון", "IS3D": "חלקה תלת-ממדית", "SHAPE_LEN": "היקף (מ׳)",
    },
    "postal": {
        "LocationID": "קוד יישוב (דואר)", "Location Name": "יישוב",
        "StreetID": "קוד רחוב (דואר)", "Street Name": "רחוב",
        "House Number": "מספר בית", "ZIP 5": "מיקוד 5", "ZIP 7": "מיקוד 7",
        "Updated": "עדכון אחרון",
    },
    "stat_area": {
        "SEMEL_YISHUV": "סמל יישוב", "SHEM_YISHUV": "יישוב",
        "STAT_2022": "אזור סטטיסטי", "YISHUV_STAT_2022": "קוד אזור מלא",
        "ROVA": "רובע", "TAT_ROVA": "תת-רובע", "COD_TIFKUD": "קוד תפקוד",
    },
}

# What a reader looks at first; the rest follow in the table's own order.
_FIRST = {
    "gazetteer": ["Type", "SubParcelNum", "BuildingFloors", "BuildingYear",
                  "EstimatedAppartments", "StreetNameHeb"],
}

# Said once on the card, repeated on every row, or an English copy of a
# Hebrew column beside it.
_REPEATED = {
    "gazetteer": {"GushNum", "ParcelNum", "SettlmentID", "SettlementNameHeb",
                  "SettlementNameEng", "StreetNameEng", "ID"},
    "stat_area": {"SHEM_YISHUV_ENGLISH"},
}


def _forms(n: int) -> list[str]:
    """The text forms a source writes an integer in — "6319" today, "6319.0"
    after a re-scrape that floats it — matched exactly, so the btree is used."""
    return [str(n), f"{n}.0"]


_COLS_TTL = 3600.0
_cols_cache: dict[tuple[str, str], tuple[float, list[str]]] = {}


async def _column_order(schema: str, table: str) -> list[str]:
    hit = _cols_cache.get((schema, table))
    if hit and time.monotonic() - hit[0] < _COLS_TTL:
        return hit[1]
    rows = await _fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position",
        schema, table)
    cols = [r["column_name"] for r in rows]
    _cols_cache[(schema, table)] = (time.monotonic(), cols)
    return cols


async def _rows(src: tuple[str, str], where: str, args: list, *, order: str = "",
                limit: int, offset: int) -> tuple[list[dict], int]:
    rows = await _fetch(f"""
        SELECT to_jsonb(s) - 'geom' - 'geometry' AS r, count(*) OVER () AS total
        FROM {_t(src)} s
        WHERE {where}
        {order}
        LIMIT {int(limit)} OFFSET {int(offset)}""", *args)
    total = int(rows[0]["total"]) if rows else 0
    return [json.loads(r["r"]) if isinstance(r["r"], str) else r["r"] for r in rows], total


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "nan"):
        return None
    # "4.0" in a column of counts and years is a float-typed integer, not a
    # fraction — show it as the integer it is.
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    # Israel Post pads house numbers and ids with zeros ("00008").
    if len(s) > 1 and s.startswith("0") and s.isdigit() and len(s) <= 5:
        return s.lstrip("0") or "0"
    return s


async def source_rows(source: str, parcel: dict, *, limit: int = 50,
                      offset: int = 0) -> dict:
    """One source's rows for one parcel: ``{columns, rows, total, ...}``.

    ``columns`` is ``[{key, label}]`` — only the columns that hold a value in
    at least one returned row, the reader-first ones leading; ``rows`` are
    lists aligned to it."""
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    limit = max(1, min(int(limit), MAX_ROWS))
    offset = max(0, int(offset))
    gush, helka, pk = parcel["gush"], parcel["parcel"], parcel["parcel_key"]

    if source == "gazetteer":
        src = GAZTIR_SRC
        where = '"GushNum" = ANY($1::text[]) AND "ParcelNum" = ANY($2::text[])'
        args = [_forms(gush), _forms(helka)]
        order = ("""ORDER BY nullif(substring("SubParcelNum" from '^[0-9]+'), '')::int
                    NULLS LAST, "ID" """)
        console = (f'SELECT * FROM {_t(src)} WHERE "GushNum" IN ({_lit(str(gush))}, '
                   f'{_lit(f"{gush}.0")}) AND "ParcelNum" IN ({_lit(str(helka))}, '
                   f'{_lit(f"{helka}.0")})')
    elif source == "parcels":
        src = PARCELS_SRC
        where = ('s."GUSH_NUM" = ANY($1::text[]) '
                 'AND public.over_parcel_key(s."GUSH_NUM", s."GUSH_SUFFI", s."PARCEL") = $2')
        args = [_forms(gush), pk]
        order = ""
        console = (f'SELECT * FROM {_t(src)} WHERE "GUSH_NUM" = {_lit(str(gush))} '
                   f'AND public.over_parcel_key("GUSH_NUM","GUSH_SUFFI","PARCEL") = {_lit(pk)}')
    elif source == "postal":
        src = POSTAL_SRC
        zips = [r["zip7"] for r in await _fetch(
            f"SELECT DISTINCT zip7 FROM public.{_qi(ADDRESSES_TABLE)} "
            f"WHERE parcel_key = $1 AND zip7 IS NOT NULL", pk)]
        where = '"ZIP 7" = ANY($1::text[])'
        args = [zips]
        order = 'ORDER BY "ZIP 7"'
        console = (f'SELECT * FROM {_t(src)} WHERE "ZIP 7" = ANY(ARRAY['
                   f'{",".join(_lit(z) for z in zips)}]::text[])' if zips
                   else f"SELECT * FROM {_t(src)} WHERE false")
    else:  # stat_area
        src = STAT_AREA_SRC
        code = ((await stat_areas([pk])).get(pk) or {}).get("yishuv_stat")
        where = '"YISHUV_STAT_2022" = $1'
        args = [str(code) if code is not None else None]
        order = ""
        console = f'SELECT * FROM {_t(src)} WHERE "YISHUV_STAT_2022" = {_lit(str(code))}'

    records, total = (await _rows(src, where, args, order=order, limit=limit, offset=offset)
                      if all(a not in (None, []) for a in args) else ([], 0))

    labels = {**(await column_aliases.load_map()).get(src, {}), **LABELS.get(source, {})}
    order_cols = await _column_order(*src)
    first = [c for c in _FIRST.get(source, []) if c in order_cols]
    keys = first + [c for c in order_cols if c not in first]
    keys = [k for k in keys
            if k.lower() not in _NOISE and k not in _REPEATED.get(source, set())
            and any(_clean(r.get(k)) is not None for r in records)]
    return {
        "source": source,
        "table": ".".join(src),
        "columns": [{"key": k, "label": labels.get(k) or k} for k in keys],
        "rows": [[_clean(r.get(k)) for k in keys] for r in records],
        "total": total,
        "offset": offset,
        "limit": limit,
        "row_url": _console_url(f"{console} LIMIT 200"),
    }
