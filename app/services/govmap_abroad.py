"""Israel's outline as a processed index table, and which GovMap features lie
outside it.

Three ``public.over_*`` tables, built BY גרסאות לעם and published in /data with
its other processed indexes:

    over_israel_boundary        one row per region: ``israel`` (the six
                                Interior-Ministry districts, East Jerusalem and
                                the Golan included), ``judea_samaria`` (the
                                ministry's seventh, איו"ש) and ``maritime``
                                (the sea out to the maritime boundaries)
    over_israel_boundary_parts  the same outline cut into small pieces
                                (ST_Subdivide) under a GiST index — what every
                                point-in-polygon test actually runs against
    over_govmap_abroad          one row per mirrored GovMap layer: how many of
                                its features are in Israel, in איו"ש, at sea,
                                and outside all three
    over_govmap_abroad_features one row per feature outside all three, with its
                                geometry, its attributes and its distance from
                                the outline

Where the outline comes from. GovMap publishes no polygon of the state.

* Land: the Interior Ministry's district layer (GovMap layer 119, ``machoz``)
  is the one layer whose polygons tile the country, so the land regions are
  their union — ``israel`` for six districts, ``judea_samaria`` for the
  seventh. Gaza is outside, as in the source.
* Sea: the atlas layer "גבולות ישראל ומעמדם הבין-לאומי" (232711) draws the
  maritime boundaries as LINES. In the Mediterranean they chain end to end —
  the estimated boundary from the coast at Zikim out to the Cyprus line, the
  Israel–Cyprus EEZ boundary, the Israel–Lebanon maritime boundary back to the
  coast at Rosh HaNikra — and in the Gulf of Eilat the Jordan and Egypt
  maritime boundaries do the same between the two land borders. Each chain is
  closed into a ring by a short overland path through Israeli territory
  (MARITIME_CLOSURES), and the land is then subtracted, so ``maritime`` is sea
  only: the EEZ in the Mediterranean (about 25,000 km²) and the waters off
  Eilat.

Each region is kept as its own row, so a reader can count איו"ש or the sea
whichever way the question needs. "Abroad" means outside ALL of them.

Classification of one feature: it is in a region if it touches it at all
(ST_Intersects), so a line that crosses the border counts as inside. A feature
is "abroad" only if it touches no region AND lies farther than
settings.govmap_abroad_tolerance_m (default 10 m) from all of them: a point
drawn a few metres off a border line is "near the border" (``near_border``),
not abroad. The distance is to the nearest piece of the outline, in
kilometres on the spheroid.

The per-layer result also drives a content flag, ``has_abroad`` on
``tracked_datasets.field_flags`` — the same flags as locality/authority/date —
so "layers with features abroad" is a filter on the home list, the /data
console and the API like any other. Unlike those, it is computed from the
DATA, not from column names, so field_flags.recompute never touches it.

Re-checks are incremental: a layer is checked again only when its mirrored
version changed, the outline was rebuilt after its last check, or the
tolerance changed.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.tracked_dataset import TrackedDataset
from app.services import append_store, data_catalog
from app.services.append_store import _qi
from app.services import postgis_schema
from app.services.index_mirror import SCHEMA as IDX_SCHEMA

logger = logging.getLogger(__name__)

# Asked per statement: PostGIS may move schemas (see postgis_schema).
_X = postgis_schema.qschema

BOUNDARY_TABLE = "over_israel_boundary"
PARTS_TABLE = "over_israel_boundary_parts"
LAYERS_TABLE = "over_govmap_abroad"
FEATURES_TABLE = "over_govmap_abroad_features"
ALL_TABLES = (BOUNDARY_TABLE, PARTS_TABLE, LAYERS_TABLE, FEATURES_TABLE)

#: GovMap layer 119, "גבול מחוז משרד הפנים". Its mirror table is
#: ``govmap_119_<id8>``; found by prefix so a re-tracked dataset still resolves.
DISTRICTS_LAYER_ID = "119"
DISTRICT_COLUMN = "machoz"
JUDEA_SAMARIA_DISTRICT = "יהודה ושומרון"

REGION_ISRAEL = "israel"
REGION_JUDEA_SAMARIA = "judea_samaria"
REGION_MARITIME = "maritime"
REGION_NAMES = {
    REGION_ISRAEL: "ישראל — שישה מחוזות משרד הפנים (כולל ירושלים המזרחית והגולן)",
    REGION_JUDEA_SAMARIA: 'איו"ש — מחוז יהודה ושומרון של משרד הפנים',
    REGION_MARITIME: ("המרחב הימי — עד קווי הגבול הימי באטלס ישראל: האזור הכלכלי "
                      "הבלעדי בים התיכון והמים מול אילת"),
}

#: Bump when the regions are defined differently: a stored outline built under
#: another version is rebuilt on the next run, and every layer re-checked.
BOUNDARY_DEFINITION_VERSION = 2

#: The atlas layer that draws Israel's borders and maritime boundaries as lines.
MARITIME_LAYER_ID = "232711"

#: The two maritime chains, as ``englishname`` LIKE patterns in the order the
#: lines connect, each closed by an overland path through Israeli territory
#: (lon, lat) from the chain's last point back to its first. The paths only
#: have to stay inside Israel: whatever land they enclose is subtracted.
MARITIME_CLOSURES: list[dict] = [
    {   # Mediterranean: Zikim → Cyprus line → Lebanon line → Rosh HaNikra.
        "lines": ["Estimated Maritime Boundary", "%Israel and Cyprus%",
                  "%Lebanon%Maritime Boundary%"],
        "path": [(35.11, 33.08), (35.15, 33.0), (35.15, 31.6), (34.6, 31.6)],
    },
    {   # Gulf of Eilat: the Jordanian coast → the Egyptian coast at Taba.
        "lines": ["%Israel-Jordan Maritime Boundary%", "%Egypt Maritime Boundary%"],
        "path": [(34.915, 29.53), (34.96, 29.565)],
    },
]

#: Pieces of sea smaller than this are slivers between district polygons, not sea.
MARITIME_MIN_PIECE_KM2 = 1.0

#: Vertices per outline piece. Small pieces keep each bounding box tight, so
#: the GiST index rejects almost every piece before any exact test runs.
SUBDIVIDE_VERTICES = 256

#: Attributes kept per abroad feature: enough to recognise it, not an archive.
MAX_ATTRS_CHARS = 2000

#: Field flag written onto tracked_datasets.field_flags.
FLAG_KEY = "has_abroad"

#: Per-statement cap while checking one layer. The largest layers hold a few
#: hundred thousand features; with the parts index a check takes seconds.
STATEMENT_TIMEOUT_MS = 10 * 60 * 1000

def _ddl() -> list[str]:
    # A function, not a constant: the PostGIS schema is read at runtime.
    return [
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(BOUNDARY_TABLE)} (
        region        text PRIMARY KEY,
        name          text NOT NULL,
        districts     text[] NOT NULL,
        area_km2      double precision,
        geom          {_X()}.geometry(MultiPolygon, 4326) NOT NULL,
        source_table  text NOT NULL,
        built_at      timestamptz NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(PARTS_TABLE)} (
        region   text NOT NULL,
        part_id  integer NOT NULL,
        geom     {_X()}.geometry(Geometry, 4326) NOT NULL,
        PRIMARY KEY (region, part_id)
    )
    """,
    f"""CREATE INDEX IF NOT EXISTS {_qi(PARTS_TABLE + '_geom_gist')}
        ON public.{_qi(PARTS_TABLE)} USING gist (geom)""",
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(LAYERS_TABLE)} (
        dataset_id         text PRIMARY KEY,
        table_name         text NOT NULL,
        layer_id           text,
        title              text,
        version_number     integer,
        rows               bigint NOT NULL,
        rows_with_geom     bigint NOT NULL,
        in_israel          bigint NOT NULL,
        in_judea_samaria   bigint NOT NULL,
        abroad             bigint NOT NULL,
        max_distance_km    double precision,
        checked_at         timestamptz NOT NULL,
        boundary_built_at  timestamptz NOT NULL,
        duration_ms        bigint
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(FEATURES_TABLE)} (
        dataset_id   text NOT NULL,
        table_name   text NOT NULL,
        layer_id     text,
        row_hash     text NOT NULL,
        distance_km  double precision,
        attrs        jsonb NOT NULL,
        geom         {_X()}.geometry(Geometry, 4326) NOT NULL,
        PRIMARY KEY (dataset_id, row_hash)
    )
    """,
    f"""CREATE INDEX IF NOT EXISTS {_qi(FEATURES_TABLE + '_geom_gist')}
        ON public.{_qi(FEATURES_TABLE)} USING gist (geom)""",
    # Added with the maritime region (definition version 2).
    f"""ALTER TABLE public.{_qi(BOUNDARY_TABLE)}
        ADD COLUMN IF NOT EXISTS definition_version integer""",
    f"""ALTER TABLE public.{_qi(LAYERS_TABLE)}
        ADD COLUMN IF NOT EXISTS in_maritime bigint NOT NULL DEFAULT 0""",
    # Added with the border tolerance (settings.govmap_abroad_tolerance_m).
    f"""ALTER TABLE public.{_qi(LAYERS_TABLE)}
        ADD COLUMN IF NOT EXISTS near_border bigint NOT NULL DEFAULT 0""",
    f"""ALTER TABLE public.{_qi(LAYERS_TABLE)}
        ADD COLUMN IF NOT EXISTS tolerance_m double precision""",
]


def layer_id_of(table_name: str) -> str | None:
    """``govmap_376_41d39d22_fb79e65e`` → ``376``. None for a non-GovMap table."""
    parts = table_name.split("_")
    if len(parts) >= 3 and parts[0] == "govmap" and parts[1].isdigit():
        return parts[1]
    return None


def tolerance_m() -> float:
    """Metres from the outline within which a feature is not counted abroad
    (settings.govmap_abroad_tolerance_m; never negative)."""
    try:
        return max(0.0, float(getattr(settings, "govmap_abroad_tolerance_m", 10.0)))
    except (TypeError, ValueError):
        return 10.0


def needs_check(prev: dict | None, version_number, boundary_built_at,
                tolerance: float | None = None) -> bool:
    """Whether a layer must be (re)checked: never checked, a new mirrored
    version since, an outline rebuilt after its last check, or checked under
    another tolerance."""
    if prev is None:
        return True
    if version_number is not None and prev.get("version_number") != version_number:
        return True
    if tolerance is not None and prev.get("tolerance_m") != tolerance:
        return True
    built = prev.get("boundary_built_at")
    return built is None or (boundary_built_at is not None and built < boundary_built_at)


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for stmt in _ddl():
            await conn.execute(stmt)
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        if role:
            for t in ALL_TABLES:
                try:
                    await conn.execute(f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")
                except Exception:  # noqa: BLE001 — a missing role must not fail a build
                    logger.debug("govmap_abroad: grant on %s failed", t, exc_info=True)


async def _layer_table(conn, layer_id: str) -> str | None:
    """The live idx mirror table of a GovMap layer, or None."""
    return await conn.fetchval(
        """SELECT c.relname FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND c.relkind = 'r'
             AND c.relname LIKE $2
           ORDER BY c.relname LIMIT 1""",
        IDX_SCHEMA, f"govmap\\_{layer_id}\\_%")


def _maritime_sql(atlas_table: str) -> tuple[str, list[str]]:
    """The statement that builds the sea region from the atlas border lines.

    One ring per entry of MARITIME_CLOSURES: its lines in order, plus the
    closing overland path from the last line's end to the first line's start.
    The union of the rings minus the land regions, keeping only pieces of at
    least MARITIME_MIN_PIECE_KM2, is the sea. Returns (sql, params); the sql
    yields one row (geom, area_km2, line_names)."""
    src = f"{_qi(IDX_SCHEMA)}.{_qi(atlas_table)}"
    params: list[str] = []
    rings = []
    for c in MARITIME_CLOSURES:
        lines = []
        for pat in c["lines"]:
            params.append(pat)
            lines.append(
                f"(SELECT {_X()}.ST_SnapToGrid({_X()}.ST_GeometryN(geom, 1), 1e-7) FROM {src} "
                f"WHERE englishname LIKE ${len(params)} AND geom IS NOT NULL "
                f'ORDER BY "objectId" LIMIT 1)')
        pts = ", ".join(f"{_X()}.ST_SetSRID({_X()}.ST_MakePoint({x}, {y}), 4326)"
                        for x, y in c["path"])
        closing = (f"{_X()}.ST_MakeLine(ARRAY[{_X()}.ST_EndPoint({lines[-1]}), {pts}, "
                   f"{_X()}.ST_StartPoint({lines[0]})])")
        rings.append(f"{_X()}.ST_MakePolygon({_X()}.ST_LineMerge("
                     f"{_X()}.ST_Collect(ARRAY[{', '.join(lines)}, {closing}])))")
    n = len(params)
    sql = f"""
        WITH sea AS (SELECT {_X()}.ST_Union(ARRAY[{', '.join(rings)}]) AS g),
        land AS (SELECT {_X()}.ST_Union(geom) AS g FROM public.{_qi(BOUNDARY_TABLE)}
                 WHERE region IN ('{REGION_ISRAEL}', '{REGION_JUDEA_SAMARIA}')),
        pieces AS (SELECT (d).geom AS g
                   FROM (SELECT {_X()}.ST_Dump({_X()}.ST_Difference(sea.g, land.g)) AS d
                         FROM sea, land) s),
        kept AS (SELECT g FROM pieces
                 WHERE {_X()}.ST_Area(g::{_X()}.geography) / 1e6 >= {MARITIME_MIN_PIECE_KM2})
        SELECT {_X()}.ST_Multi({_X()}.ST_Union(g)) AS geom,
               sum({_X()}.ST_Area(g::{_X()}.geography)) / 1e6 AS area_km2,
               ARRAY(SELECT englishname FROM {src}
                     WHERE englishname LIKE ANY (ARRAY[{', '.join(f'${i}' for i in range(1, n + 1))}])
                     ORDER BY englishname) AS line_names
        FROM kept
    """
    return sql, params


async def build_boundary() -> dict:
    """(Re)build the outline from the district layer. Replaces both tables in
    one transaction, so a reader never sees half an outline."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    await ensure_tables()
    pool = await append_store.get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        src = await _layer_table(conn, DISTRICTS_LAYER_ID)
        if not src:
            return {"error": f"GovMap layer {DISTRICTS_LAYER_ID} is not mirrored in idx"}
        q_src = f"{_qi(IDX_SCHEMA)}.{_qi(src)}"
        districts = await conn.fetch(
            f"SELECT {_qi(DISTRICT_COLUMN)} AS d FROM {q_src} WHERE geom IS NOT NULL")
        names = sorted({r["d"] for r in districts if r["d"]})
        if JUDEA_SAMARIA_DISTRICT not in names or len(names) < 2:
            return {"error": f"unexpected districts in {src}: {names}"}
        now = datetime.now(timezone.utc)
        region_expr = (f"CASE WHEN {_qi(DISTRICT_COLUMN)} = $1 THEN '{REGION_JUDEA_SAMARIA}' "
                       f"ELSE '{REGION_ISRAEL}' END")
        async with conn.transaction():
            await conn.execute(f"DELETE FROM public.{_qi(PARTS_TABLE)}")
            await conn.execute(f"DELETE FROM public.{_qi(BOUNDARY_TABLE)}")
            await conn.execute(
                f"""INSERT INTO public.{_qi(BOUNDARY_TABLE)}
                        (region, name, districts, area_km2, geom, source_table, built_at,
                         definition_version)
                    SELECT region,
                           CASE region WHEN '{REGION_ISRAEL}' THEN $2 ELSE $3 END,
                           array_agg(DISTINCT d ORDER BY d),
                           {_X()}.ST_Area({_X()}.ST_Union(g)::{_X()}.geography) / 1e6,
                           {_X()}.ST_Multi({_X()}.ST_CollectionExtract({_X()}.ST_Union(g), 3)),
                           $4, $5, {BOUNDARY_DEFINITION_VERSION}
                    FROM (SELECT {region_expr} AS region, {_qi(DISTRICT_COLUMN)} AS d,
                                 {_X()}.ST_MakeValid(geom) AS g
                          FROM {q_src} WHERE geom IS NOT NULL) s
                    GROUP BY region""",
                JUDEA_SAMARIA_DISTRICT, REGION_NAMES[REGION_ISRAEL],
                REGION_NAMES[REGION_JUDEA_SAMARIA], src, now)
            maritime_note = await _build_maritime(conn, now)
            await conn.execute(
                f"""INSERT INTO public.{_qi(PARTS_TABLE)} (region, part_id, geom)
                    SELECT region, row_number() OVER (PARTITION BY region)::int, g
                    FROM (SELECT region,
                                 {_X()}.ST_Subdivide(geom, {SUBDIVIDE_VERTICES}) AS g
                          FROM public.{_qi(BOUNDARY_TABLE)}) s""")
        await conn.execute(f"ANALYZE public.{_qi(PARTS_TABLE)}")
        regions = await conn.fetch(
            f"""SELECT b.region, b.area_km2, b.districts,
                       (SELECT count(*) FROM public.{_qi(PARTS_TABLE)} p
                        WHERE p.region = b.region) AS parts
                FROM public.{_qi(BOUNDARY_TABLE)} b ORDER BY b.region""")
    data_catalog.invalidate_catalog_cache()
    out = {"source_table": src, "built_at": now.isoformat(), "maritime": maritime_note,
           "duration_ms": int((time.monotonic() - t0) * 1000),
           "regions": [{"region": r["region"], "area_km2": round(r["area_km2"] or 0),
                        "districts": list(r["districts"]), "parts": r["parts"]}
                       for r in regions]}
    logger.info("govmap_abroad.build_boundary: %s", out)
    return out


async def _build_maritime(conn, now) -> str:
    """Add the sea region inside the caller's transaction. A failure here (the
    atlas layer missing, its lines renamed) leaves the land regions standing:
    it is logged and reported, not raised."""
    atlas = await _layer_table(conn, MARITIME_LAYER_ID)
    if not atlas:
        logger.warning("govmap_abroad: atlas layer %s not mirrored; no maritime region",
                       MARITIME_LAYER_ID)
        return f"skipped: GovMap layer {MARITIME_LAYER_ID} is not mirrored in idx"
    sql, params = _maritime_sql(atlas)
    try:
        async with conn.transaction():   # a savepoint: the land rows survive a failure
            row = await conn.fetchrow(sql, *params)
            if row is None or row["geom"] is None:
                return "skipped: the maritime lines did not close into a ring"
            await conn.execute(
                f"""INSERT INTO public.{_qi(BOUNDARY_TABLE)}
                        (region, name, districts, area_km2, geom, source_table, built_at,
                         definition_version)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, {BOUNDARY_DEFINITION_VERSION})""",
                REGION_MARITIME, REGION_NAMES[REGION_MARITIME], list(row["line_names"]),
                row["area_km2"], row["geom"], atlas, now)
    except Exception as e:  # noqa: BLE001 — the land outline must not depend on it
        logger.warning("govmap_abroad: maritime region failed: %s", e)
        return f"failed: {str(e)[:200]}"
    return f"ok: {round(row['area_km2'] or 0)} km2 from {atlas}"


async def _boundary_state(conn) -> tuple:
    """(built_at, definition_version) of the stored outline, or (None, None)."""
    try:
        row = await conn.fetchrow(
            f"SELECT max(built_at) AS b, min(definition_version) AS v "
            f"FROM public.{_qi(BOUNDARY_TABLE)}")
        return (row["b"], row["v"]) if row else (None, None)
    except Exception:  # noqa: BLE001 — table not built yet
        return (None, None)


async def _boundary_built_at(conn):
    try:
        return await conn.fetchval(
            f"SELECT max(built_at) FROM public.{_qi(BOUNDARY_TABLE)}")
    except Exception:  # noqa: BLE001 — table not built yet
        return None


def _near_sql(parts: str, g: str, tol_m: float) -> str:
    """EXISTS a piece of any region within ``tol_m`` metres of ``g``. The
    bounding-box prefilter is generous (degrees of longitude are ~0.85 km
    shorter than degrees of latitude at these latitudes); ST_DWithin on the
    spheroid decides."""
    deg = tol_m / 111_000 * 2
    return (f"EXISTS (SELECT 1 FROM {parts} p WHERE "
            f"p.geom OPERATOR({_X()}.&&) {_X()}.ST_Expand({g}, {deg:.8f}) AND "
            f"{_X()}.ST_DWithin(p.geom::{_X()}.geography, {g}::{_X()}.geography, {tol_m}))")


def _check_sql(table: str, tol_m: float = 0.0) -> tuple[str, str]:
    """The two statements that check one layer: the abroad features (written)
    and the per-region counts (read). Both against the indexed outline pieces.

    Abroad = touches no piece of any region. A feature in several regions is
    counted once, in the first of israel → judea_samaria → maritime; the
    maritime count is what remains. In a region = touches any piece of it. Distance = to the nearest of the
    five pieces closest by bounding box, which is the nearest piece for any
    feature outside the outline.

    A feature that touches no region but lies within ``tol_m`` metres of one
    is "near the border": neither abroad nor in a region, counted on its own."""
    # The alias is deliberately unlikely: a layer with a column named like the
    # alias would make to_jsonb(alias) read that column instead of the row.
    src = f"{_qi(IDX_SCHEMA)}.{_qi(table)}"
    parts = f"public.{_qi(PARTS_TABLE)}"
    def touches(region: str | None, g: str) -> str:
        """EXISTS a piece of ``region`` (None = of ANY region) touching ``g``."""
        where = f"p.region = '{region}' AND " if region else ""
        return (f"EXISTS (SELECT 1 FROM {parts} p WHERE {where}"
                f"p.geom OPERATOR({_X()}.&&) {g} AND {_X()}.ST_Intersects(p.geom, {g}))")
    near_clause = (f"\n          AND NOT {_near_sql(parts, 'zz_src.geom', tol_m)}"
                   if tol_m > 0 else "")
    insert = f"""
        INSERT INTO public.{_qi(FEATURES_TABLE)}
            (dataset_id, table_name, layer_id, row_hash, distance_km, attrs, geom)
        SELECT $1, $2, $3, zz_src._row_hash::text,
               (SELECT min({_X()}.ST_Distance(n.geom::{_X()}.geography, zz_src.geom::{_X()}.geography))
                FROM (SELECT p.geom FROM {parts} p
                      ORDER BY p.geom OPERATOR({_X()}.<->) zz_src.geom LIMIT 5) n) / 1000.0,
               CASE WHEN length(a.j::text) > {MAX_ATTRS_CHARS}
                    THEN jsonb_build_object('_truncated', left(a.j::text, {MAX_ATTRS_CHARS}))
                    ELSE a.j END,
               zz_src.geom
        FROM {src} zz_src
        CROSS JOIN LATERAL (SELECT to_jsonb(zz_src) - 'geom' - 'geometry_wkt'
                                   - '_row_hash' - '_first_seen' AS j) a
        WHERE zz_src.geom IS NOT NULL
          AND NOT {touches(None, 'zz_src.geom')}{near_clause}
        ON CONFLICT (dataset_id, row_hash) DO NOTHING
    """
    counts = f"""
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE geom IS NOT NULL) AS rows_with_geom,
               count(*) FILTER (WHERE geom IS NOT NULL AND il) AS in_israel,
               count(*) FILTER (WHERE geom IS NOT NULL AND NOT il AND js) AS in_judea_samaria,
               count(*) FILTER (WHERE geom IS NOT NULL AND anyr) AS in_any
        FROM (SELECT zz_src.geom,
                     zz_src.geom IS NOT NULL AND {touches(None, 'zz_src.geom')} AS anyr,
                     zz_src.geom IS NOT NULL AND {touches(REGION_ISRAEL, 'zz_src.geom')} AS il,
                     zz_src.geom IS NOT NULL AND {touches(REGION_JUDEA_SAMARIA, 'zz_src.geom')} AS js
              FROM {src} zz_src) c
    """
    return insert, counts


async def check_layer(conn, dataset_id: str, table: str, *, title: str | None,
                      version_number, boundary_built_at, tol_m: float = 0.0) -> dict:
    """Check one mirrored layer and store its result. One transaction: the
    layer's previous abroad features are replaced, never mixed."""
    t0 = time.monotonic()
    layer_id = layer_id_of(table)
    insert, counts = _check_sql(table, tol_m)
    async with conn.transaction():
        await conn.execute(f"SET LOCAL statement_timeout = {STATEMENT_TIMEOUT_MS}")
        await conn.execute(
            f"DELETE FROM public.{_qi(FEATURES_TABLE)} WHERE dataset_id = $1", dataset_id)
        await conn.execute(insert, dataset_id, table, layer_id)
        c = await conn.fetchrow(counts)
        a = await conn.fetchrow(
            f"""SELECT count(*) AS n, max(distance_km) AS mx
                FROM public.{_qi(FEATURES_TABLE)} WHERE dataset_id = $1""", dataset_id)
        abroad = a["n"]
        in_js = c["in_judea_samaria"]
        in_sea = max(0, c["in_any"] - c["in_israel"] - in_js)
        near = max(0, c["rows_with_geom"] - c["in_any"] - abroad)
        ms = int((time.monotonic() - t0) * 1000)
        await conn.execute(
            f"""INSERT INTO public.{_qi(LAYERS_TABLE)}
                  (dataset_id, table_name, layer_id, title, version_number, rows,
                   rows_with_geom, in_israel, in_judea_samaria, in_maritime, abroad,
                   max_distance_km, checked_at, boundary_built_at, duration_ms,
                   near_border, tolerance_m)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,now(),$13,$14,$15,$16)
                ON CONFLICT (dataset_id) DO UPDATE SET
                  table_name = EXCLUDED.table_name, layer_id = EXCLUDED.layer_id,
                  title = EXCLUDED.title, version_number = EXCLUDED.version_number,
                  rows = EXCLUDED.rows, rows_with_geom = EXCLUDED.rows_with_geom,
                  in_israel = EXCLUDED.in_israel,
                  in_judea_samaria = EXCLUDED.in_judea_samaria,
                  in_maritime = EXCLUDED.in_maritime,
                  near_border = EXCLUDED.near_border,
                  tolerance_m = EXCLUDED.tolerance_m,
                  abroad = EXCLUDED.abroad, max_distance_km = EXCLUDED.max_distance_km,
                  checked_at = EXCLUDED.checked_at,
                  boundary_built_at = EXCLUDED.boundary_built_at,
                  duration_ms = EXCLUDED.duration_ms""",
            dataset_id, table, layer_id, title, version_number, c["rows"],
            c["rows_with_geom"], c["in_israel"], in_js, in_sea, abroad, a["mx"],
            boundary_built_at, ms, near, tol_m)
    return {"dataset_id": dataset_id, "table": table, "rows": c["rows"],
            "in_israel": c["in_israel"], "in_judea_samaria": in_js,
            "in_maritime": in_sea, "near_border": near, "abroad": abroad, "duration_ms": ms}


async def _govmap_layers(db: AsyncSession) -> list[dict]:
    """Mirrored GovMap layers that carry a PostGIS column, with their titles."""
    from app.services import index_mirror
    mirrored = await index_mirror.list_tables()
    if not mirrored:
        return []
    datasets = {str(d.id): d for d in
                (await db.execute(select(TrackedDataset))).scalars().all()}
    geo_tables = set()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for r in await conn.fetch(
                """SELECT table_name FROM information_schema.columns
                   WHERE table_schema = $1 AND column_name = 'geom'""", IDX_SCHEMA):
            geo_tables.add(r["table_name"])
    out = []
    for m in mirrored:
        ds = datasets.get(m["dataset_id"])
        if ds is None or (ds.source_type or "") != "govmap":
            continue
        if m["table"] not in geo_tables:
            continue
        if layer_id_of(m["table"]) == DISTRICTS_LAYER_ID:
            continue  # the outline's own source: inside by construction
        out.append({"dataset_id": m["dataset_id"], "table": m["table"],
                    "title": ds.title, "version_number": m.get("version_number")})
    return out


async def run(db: AsyncSession, *, limit: int | None = None,
              rebuild_boundary: bool = False, force: bool = False) -> dict:
    """Check every GovMap layer that needs it, then update the flags.

    Builds the outline first if it does not exist, was built under another
    BOUNDARY_DEFINITION_VERSION, or when asked. ``limit``
    caps how many layers this run checks; the rest wait for the next run."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        built_at, version = await _boundary_state(conn)
    boundary = None
    if built_at is None or rebuild_boundary or version != BOUNDARY_DEFINITION_VERSION:
        boundary = await build_boundary()
        if "error" in boundary or "skipped" in boundary:
            return {"boundary": boundary}
        async with pool.acquire() as conn:
            built_at = await _boundary_built_at(conn)

    tol = tolerance_m()
    layers = await _govmap_layers(db)
    async with pool.acquire() as conn:
        prev = {r["dataset_id"]: dict(r) for r in await conn.fetch(
            f"SELECT dataset_id, version_number, boundary_built_at, tolerance_m "
            f"FROM public.{_qi(LAYERS_TABLE)}")}
        live = {l["dataset_id"] for l in layers}
        # A layer no longer mirrored (or no longer spatial) leaves no stale rows.
        gone = [d for d in prev if d not in live]
        if gone:
            await conn.execute(
                f"DELETE FROM public.{_qi(FEATURES_TABLE)} WHERE dataset_id = ANY($1::text[])", gone)
            await conn.execute(
                f"DELETE FROM public.{_qi(LAYERS_TABLE)} WHERE dataset_id = ANY($1::text[])", gone)

    todo = [l for l in layers
            if force or needs_check(prev.get(l["dataset_id"]), l["version_number"], built_at, tol)]
    if limit is not None:
        todo = todo[:limit]
    checked, failed, abroad_layers = 0, [], 0
    t0 = time.monotonic()
    for l in todo:
        try:
            async with pool.acquire() as conn:
                res = await check_layer(conn, l["dataset_id"], l["table"], title=l["title"],
                                        version_number=l["version_number"],
                                        boundary_built_at=built_at, tol_m=tol)
            checked += 1
            abroad_layers += 1 if res["abroad"] else 0
        except Exception as e:  # noqa: BLE001 — one bad layer must not stop the run
            logger.warning("govmap_abroad: %s failed: %s", l["table"], e)
            failed.append({"table": l["table"], "error": str(e)[:300]})

    flags = await update_flags(db)
    async with pool.acquire() as conn:
        await conn.execute(f"ANALYZE public.{_qi(FEATURES_TABLE)}")
        totals = dict(await conn.fetchrow(
            f"""SELECT count(*) AS layers, coalesce(sum(rows), 0) AS rows,
                       coalesce(sum(in_judea_samaria), 0) AS in_judea_samaria,
                       coalesce(sum(in_maritime), 0) AS in_maritime,
                       coalesce(sum(near_border), 0) AS near_border,
                       coalesce(sum(abroad), 0) AS abroad,
                       count(*) FILTER (WHERE abroad > 0) AS layers_with_abroad
                FROM public.{_qi(LAYERS_TABLE)}"""))
    data_catalog.invalidate_catalog_cache()
    out = {"boundary": boundary, "tolerance_m": tol, "layers_total": len(layers), "to_check": len(todo),
           "checked": checked, "failed": failed, "flags": flags,
           "totals": {k: int(v) for k, v in totals.items()},
           "duration_ms": int((time.monotonic() - t0) * 1000)}
    logger.info("govmap_abroad.run: %s", {k: v for k, v in out.items() if k != "failed"})
    return out


async def update_flags(db: AsyncSession) -> dict:
    """Write ``has_abroad`` onto field_flags for every checked layer. Merges the
    one key; datasets never checked are left without it (= not flagged)."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT dataset_id, abroad FROM public.{_qi(LAYERS_TABLE)}")
    verdict = {r["dataset_id"]: r["abroad"] > 0 for r in rows}
    changed = 0
    for ds in (await db.execute(select(TrackedDataset))).scalars().all():
        did = str(ds.id)
        current = dict(ds.field_flags or {})
        if did in verdict:
            want = verdict[did]
        elif FLAG_KEY in current:
            want = False   # no longer checked (dropped from the mirror)
        else:
            continue
        if current.get(FLAG_KEY) != want:
            current[FLAG_KEY] = want
            ds.field_flags = current
            changed += 1
    await db.commit()
    data_catalog.invalidate_catalog_cache()
    return {"datasets_updated": changed, "flagged": sum(verdict.values())}


async def status() -> dict:
    """Outline + per-layer results, for the admin endpoint."""
    if not append_store.is_configured():
        return {"configured": False}
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            regions = [dict(r) for r in await conn.fetch(
                f"SELECT region, name, districts, area_km2, source_table, built_at "
                f"FROM public.{_qi(BOUNDARY_TABLE)} ORDER BY region")]
            top = [dict(r) for r in await conn.fetch(
                f"""SELECT dataset_id, table_name, layer_id, title, rows, in_israel,
                           in_judea_samaria, in_maritime, near_border, abroad,
                           max_distance_km, tolerance_m, checked_at
                    FROM public.{_qi(LAYERS_TABLE)} WHERE abroad > 0
                    ORDER BY abroad DESC LIMIT 50""")]
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(LAYERS_TABLE)}")
    except Exception as e:  # noqa: BLE001 — not built yet
        return {"configured": True, "built": False, "note": str(e)}
    return {"configured": True, "built": bool(regions), "regions": regions,
            "layers_checked": n, "top_abroad": top}


# ── Read side: the "חו\"ל" project page (/projects/abroad) ───────────────────
# Who to write to. GovMap's own catalog (over_govmap_layers) names the
# publisher of most layers; the atlas layers are not in it, and are labelled by
# the atlas they come from rather than guessed.
_PUBLISHER_SQL = (
    "coalesce(nullif(g.publisher, ''), "
    "CASE WHEN a.title LIKE '%(אטלס ישראל)%' THEN 'אטלס ישראל' END, 'לא ידוע')"
)

#: GovMap's zoom for a link to one feature: close enough to see the point.
GOVMAP_POINT_ZOOM = 9

#: Largest page of features one request may ask for.
MAX_FEATURES_PAGE = 5000


def govmap_point_url(layer_id: str | None, x_itm: float | None, y_itm: float | None) -> str | None:
    """A GovMap link centred on a point (ITM metres), with its layer switched on."""
    if x_itm is None or y_itm is None:
        return None
    lay = f"&lay={layer_id}" if layer_id else ""
    return (f"https://www.govmap.gov.il/?c={x_itm:.2f},{y_itm:.2f}"
            f"&z={GOVMAP_POINT_ZOOM}{lay}")


_tables_ensured = False


async def _ensure_once() -> None:
    """The read side may run before this process has done a check run, on a
    schema an older version created: add any missing column once."""
    global _tables_ensured
    if not _tables_ensured:
        await ensure_tables()
        _tables_ensured = True


async def summary() -> dict:
    """Totals, the publishers and the layers with features abroad, for the
    project page. Empty (not an error) before the first run."""
    try:
        await _ensure_once()
    except Exception:  # noqa: BLE001 — reported below as not ready
        logger.debug("govmap_abroad: ensure_tables failed", exc_info=True)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            totals = dict(await conn.fetchrow(f"""
                SELECT count(*) AS layers_checked,
                       count(*) FILTER (WHERE abroad > 0) AS layers_abroad,
                       coalesce(sum(rows_with_geom), 0) AS features_checked,
                       coalesce(sum(abroad), 0) AS abroad,
                       coalesce(sum(in_maritime), 0) AS in_maritime,
                       coalesce(sum(in_judea_samaria), 0) AS in_judea_samaria,
                       coalesce(sum(near_border), 0) AS near_border,
                       max(checked_at) AS checked_at,
                       max(tolerance_m) AS tolerance_m
                FROM public.{_qi(LAYERS_TABLE)}"""))
            regions = [dict(r) for r in await conn.fetch(
                f"SELECT region, name, area_km2 FROM public.{_qi(BOUNDARY_TABLE)} "
                f"ORDER BY region")]
            layers = [dict(r) for r in await conn.fetch(f"""
                SELECT a.dataset_id, a.table_name, a.layer_id, a.title,
                       {_PUBLISHER_SQL} AS publisher,
                       a.rows_with_geom, a.in_israel, a.in_judea_samaria,
                       a.in_maritime, a.near_border, a.abroad,
                       (SELECT min(distance_km) FROM public.{_qi(FEATURES_TABLE)} f
                        WHERE f.dataset_id = a.dataset_id) AS min_distance_km,
                       a.max_distance_km, a.checked_at
                FROM public.{_qi(LAYERS_TABLE)} a
                LEFT JOIN public.over_govmap_layers g ON g.layer_id = a.layer_id
                WHERE a.abroad > 0
                ORDER BY a.abroad DESC, a.title""")]
        except Exception as e:  # noqa: BLE001 — tables not built yet
            logger.debug("govmap_abroad.summary before first run", exc_info=True)
            return {"ready": False, "note": str(e)[:200]}
    pubs: dict[str, dict] = {}
    for l in layers:
        l["share"] = (l["abroad"] / l["rows_with_geom"]) if l["rows_with_geom"] else None
        p = pubs.setdefault(l["publisher"], {"publisher": l["publisher"], "layers": 0,
                                             "abroad": 0})
        p["layers"] += 1
        p["abroad"] += l["abroad"]
    publishers = sorted(pubs.values(), key=lambda p: (-p["abroad"], p["publisher"]))
    return {"ready": True, "totals": {k: (float(v) if k == "tolerance_m" and v is not None
                                          else v) for k, v in totals.items()},
            "regions": regions, "publishers": publishers, "layers": layers}


def _features_where(dataset_id: str | None, publisher: str | None) -> tuple[str, list]:
    clauses, params = [], []
    if dataset_id:
        params.append(dataset_id)
        clauses.append(f"f.dataset_id = ${len(params)}")
    if publisher:
        params.append(publisher)
        clauses.append(f"{_PUBLISHER_SQL} = ${len(params)}")
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


async def features(*, dataset_id: str | None = None, publisher: str | None = None,
                   limit: int = 500, offset: int = 0) -> dict:
    """The abroad features themselves, one row each: the layer and publisher,
    a representative point (WGS84 and ITM), the distance from the outline and
    the feature's own attributes. Ordered by publisher, layer, then distance
    (farthest first: the clearest errors lead)."""
    limit = max(1, min(int(limit), MAX_FEATURES_PAGE))
    offset = max(0, int(offset))
    where, params = _features_where(dataset_id, publisher)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"""
            SELECT count(*) FROM public.{_qi(FEATURES_TABLE)} f
            JOIN public.{_qi(LAYERS_TABLE)} a ON a.dataset_id = f.dataset_id
            LEFT JOIN public.over_govmap_layers g ON g.layer_id = a.layer_id{where}""",
            *params)
        rows = await conn.fetch(f"""
            SELECT f.dataset_id, f.layer_id, a.title, {_PUBLISHER_SQL} AS publisher,
                   f.row_hash, f.distance_km, f.attrs::text AS attrs,
                   {_X()}.GeometryType(f.geom) AS geom_type,
                   {_X()}.ST_X(pt) AS lon, {_X()}.ST_Y(pt) AS lat,
                   {_X()}.ST_X({_X()}.ST_Transform(pt, 2039)) AS x_itm,
                   {_X()}.ST_Y({_X()}.ST_Transform(pt, 2039)) AS y_itm
            FROM public.{_qi(FEATURES_TABLE)} f
            JOIN public.{_qi(LAYERS_TABLE)} a ON a.dataset_id = f.dataset_id
            LEFT JOIN public.over_govmap_layers g ON g.layer_id = a.layer_id
            CROSS JOIN LATERAL (SELECT {_X()}.ST_PointOnSurface(f.geom) AS pt) p{where}
            ORDER BY 4, a.title, f.distance_km DESC NULLS LAST, f.row_hash
            LIMIT {limit} OFFSET {offset}""", *params)
    import json as _json
    items = []
    for r in rows:
        d = dict(r)
        d["attrs"] = _json.loads(d["attrs"]) if d["attrs"] else {}
        d["govmap_url"] = govmap_point_url(d["layer_id"], d.pop("x_itm"), d.pop("y_itm"))
        items.append(d)
    return {"total": total, "limit": limit, "offset": offset, "items": items}
