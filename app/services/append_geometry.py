"""Give `public.append_*` tables a PostGIS `geom`, from what they actually hold.

`idx` layers were made spatial by parsing their `geometry_wkt`. The plan for the
append tables assumed the same shape. A census of the live append DB
(2026-08-07) says otherwise:

    geometry_wkt in public   0 tables
    geom         in public   0 tables
    geometry_wkt in idx    814 tables   (all already converted)

Not one append table has a WKT column, so a WKT-keyed step would match nothing.
What the spatial ones carry is COORDINATE PAIRS, in two spellings and — this is
the part that matters — three conventions mixed inside a single column:

    append_light_traffics_*   lat / lon                447 rows, clean degrees
    append_automated_devices  X_Coordinate/Y_Coordinate  2,992 swapped, 2 ITM,
                                                          1 correct, 4 junk
    append_branches_*         X_Coordinate/Y_Coordinate  2,810 swapped, 2 correct

`X_Coordinate` holds the LATITUDE (29.55–33.28) and `Y_Coordinate` the LONGITUDE
(34.56–35.85), systematically, in all three tables. The column names are simply
backwards at the source.

Which is why the classification below is per ROW and not per table. Build points
from X/Y as (lon, lat) — the obvious reading, and the one the column names
invite — and 5,802 of 5,833 land in Saudi Arabia and the Indian Ocean: valid
geometry, plausible on a map, wrong. That is the exact failure the PostGIS plan
named as the worst possible outcome here, so nothing is inferred from a column
name and nothing ambiguous is guessed. A pair that does not fall cleanly into
one of the three regimes gets a NULL `geom` and is re-offered next time.

The WKT path is kept, unused for now: when the worker starts returning features
extracted from the blocked SHP/KML files it will write `geometry_wkt` in
EPSG:4326, and that is the same shape `idx` already handles — so it is routed
straight to the tested code rather than reimplemented here.
"""
from __future__ import annotations

import logging

from app.config import settings
from app.services import index_mirror as _idx

logger = logging.getLogger(__name__)

SCHEMA = "public"

# Israel's envelope, deliberately NON-OVERLAPPING so no pair can satisfy two
# regimes at once. Lat tops out at 33.35 and lon starts at 34.2, so the reading
# of any given row is decided, never chosen.
LON_MIN, LON_MAX = 34.2, 35.95
LAT_MIN, LAT_MAX = 29.4, 33.35
# ITM (EPSG:6991) in metres. Easting and northing ranges do not overlap either.
ITM_E_MIN, ITM_E_MAX = 100_000, 300_000
ITM_N_MIN, ITM_N_MAX = 350_000, 800_000

# Column names that carry a coordinate. Which of the pair is which is NOT read
# from the name — see the module docstring — only the pairing is.
_FIRST = ("lon", "long", "longitude", "x_coordinate", "x", "east", "easting")
_SECOND = ("lat", "latitude", "y_coordinate", "y", "north", "northing")

# A text column becomes a number only if it is entirely one. '[0-9]' rather than
# '\d' because these expressions get copied into TypeScript template literals on
# the /data page, where a backslash-d silently becomes a d.
_NUMERIC = r"^-?[0-9]+(\.[0-9]+)?$"


def find_pair(columns: list[str]) -> tuple[str, str] | None:
    """The two columns holding a coordinate pair, or None.

    Returned in the table's own order; the caller must not read meaning into
    which came first.
    """
    lower = {c.lower(): c for c in columns}
    for a in _FIRST:
        if a not in lower:
            continue
        for b in _SECOND:
            if b in lower:
                return lower[a], lower[b]
    return None


def point_expr(col_a: str, col_b: str, *, ext: str = "extensions",
               srid: int = 4326, itm: int = 6991) -> str:
    """SQL that turns one row's coordinate pair into a point, or NULL.

    Reads the pair BOTH ways and lets the values decide, because the source
    swaps them. Anything outside the country in either reading — and anything
    that is not a plain number — yields NULL rather than a guess.
    """
    qa, qb = _idx._qi(col_a), _idx._qi(col_b)
    a = f"(CASE WHEN {qa} ~ '{_NUMERIC}' THEN {qa}::numeric END)"
    b = f"(CASE WHEN {qb} ~ '{_NUMERIC}' THEN {qb}::numeric END)"
    return (
        "CASE"
        # as written: first column is longitude
        f" WHEN {a} BETWEEN {LON_MIN} AND {LON_MAX}"
        f"  AND {b} BETWEEN {LAT_MIN} AND {LAT_MAX}"
        f" THEN {ext}.ST_SetSRID({ext}.ST_MakePoint({a}, {b}), {srid})"
        # swapped, which is how 99.5% of the corpus is actually published
        f" WHEN {a} BETWEEN {LAT_MIN} AND {LAT_MAX}"
        f"  AND {b} BETWEEN {LON_MIN} AND {LON_MAX}"
        f" THEN {ext}.ST_SetSRID({ext}.ST_MakePoint({b}, {a}), {srid})"
        # ITM metres — transformed, not relabelled, so the datum shift applies
        f" WHEN {a} BETWEEN {ITM_E_MIN} AND {ITM_E_MAX}"
        f"  AND {b} BETWEEN {ITM_N_MIN} AND {ITM_N_MAX}"
        f" THEN {ext}.ST_Transform("
        f"{ext}.ST_SetSRID({ext}.ST_MakePoint({a}, {b}), {itm}), {srid})"
        " ELSE NULL END"
    )


async def fill(conn, table: str, columns: list[str]) -> dict:
    """Build `geom` for the rows an append just added. Never raises.

    Geometry is an enhancement: a table whose coordinates will not convert must
    still get its new rows. Every failure is returned as a note for the caller
    to log, never propagated into the append.
    """
    if not settings.append_postgis_enabled:
        return {"skipped": "append postgis disabled"}
    if not await _idx._postgis_available(conn):
        return {"skipped": "postgis not installed on the append DB"}

    # A table that DOES carry WKT is `idx`'s shape, and `idx`'s tested code
    # handles it — including the ITM reprojection and the row-hash rewrite that
    # comes with it. Nothing to reimplement.
    if _idx.WKT_COLUMN in columns:
        return await _idx._fill_geometry(conn, table, columns, SCHEMA)

    pair = find_pair(columns)
    if not pair:
        return {"skipped": "no coordinate columns"}
    if not await _idx._srid_available(conn, 6991):
        # Only the ITM branch needs it, and that is a rounding error of the
        # corpus — but silently dropping those rows would be a lie, so say it.
        logger.info("append geometry: EPSG:6991 missing — ITM rows on %s will "
                    "stay NULL", table)

    geom = _idx._qi(_idx.GEOM_COLUMN)
    qt = _idx._qt(table, SCHEMA)
    has_geom = await conn.fetchval(
        "SELECT true FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
        SCHEMA, table, _idx.GEOM_COLUMN)

    try:
        if not has_geom:
            async with conn.transaction():   # DDL is transactional in Postgres
                await conn.execute(
                    f"ALTER TABLE {qt} ADD COLUMN {geom} "
                    f"{_idx._qi(_idx.PG_EXT_SCHEMA)}.geometry(Point, "
                    f"{_idx.GEOM_SRID})")
                await conn.execute(
                    f"CREATE INDEX {_idx._qi(_idx._geom_index_name(table))} "
                    f"ON {qt} USING GIST ({geom})")
        status = await conn.execute(
            f"UPDATE {qt} SET {geom} = {point_expr(*pair)} "
            f"WHERE {geom} IS NULL")
        # Rows no reading could place. They keep a NULL geom and are re-offered
        # by the WHERE above on every later append — so a source that starts
        # publishing them properly repairs itself without a reload.
        unplaced = await conn.fetchval(
            f"SELECT count(*) FROM {qt} WHERE {geom} IS NULL")
    except Exception as exc:  # noqa: BLE001 — the append must survive this
        logger.warning("append geometry: failed for %s", table, exc_info=True)
        return {"error": f"{type(exc).__name__}: {exc}"[:500]}

    try:
        attempted = int(str(status).rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        attempted = 0
    unplaced = int(unplaced or 0)
    out: dict = {"rows": attempted - unplaced, "columns": list(pair)}
    if unplaced:
        out["skipped"] = (f"{unplaced} row(s) held no coordinate this could "
                          f"place (kept as NULL geom)")
        logger.info("append geometry: %s — %d/%d rows unplaced",
                    table, unplaced, attempted)
    return out
