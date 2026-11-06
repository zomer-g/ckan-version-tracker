"""Israel's outline as a processed index table, and which GovMap features lie
outside it.

Three ``public.over_*`` tables, built BY גרסאות לעם and published in /data with
its other processed indexes:

    over_israel_boundary        one row per region: ``israel`` (the six
                                Interior-Ministry districts, East Jerusalem and
                                the Golan included) and ``judea_samaria``
                                (the ministry's seventh, איו"ש)
    over_israel_boundary_parts  the same outline cut into small pieces
                                (ST_Subdivide) under a GiST index — what every
                                point-in-polygon test actually runs against
    over_govmap_abroad          one row per mirrored GovMap layer: how many of
                                its features are in Israel, in איו"ש, and
                                outside both
    over_govmap_abroad_features one row per feature outside both, with its
                                geometry, its attributes and its distance from
                                the outline

Where the outline comes from. GovMap publishes no polygon of the state: the
atlas layer "גבולות ישראל ומעמדם הבין-לאומי" is border LINES only. The
Interior Ministry's district layer (GovMap layer 119, ``machoz``) is the one
layer whose polygons tile the country, so the outline is their union. Its
limits are the source's: land only (no territorial sea), Gaza outside. איו"ש
is kept as a region of its own rather than folded into either side, so a
reader can count it whichever way the question needs; "abroad" here means
outside BOTH regions.

Classification of one feature: it is in a region if it touches it at all
(ST_Intersects), so a line that crosses the border counts as inside. A feature
is "abroad" only if it touches neither region. The distance is to the nearest
piece of the outline, in kilometres on the spheroid.

The per-layer result also drives a content flag, ``has_abroad`` on
``tracked_datasets.field_flags`` — the same flags as locality/authority/date —
so "layers with features abroad" is a filter on the home list, the /data
console and the API like any other. Unlike those, it is computed from the
DATA, not from column names, so field_flags.recompute never touches it.

Re-checks are incremental: a layer is checked again only when its mirrored
version changed or the outline was rebuilt after its last check.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.services import append_store, data_catalog
from app.services.append_store import _qi
from app.services.index_mirror import PG_EXT_SCHEMA, SCHEMA as IDX_SCHEMA

logger = logging.getLogger(__name__)

_X = _qi(PG_EXT_SCHEMA)

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
REGION_NAMES = {
    REGION_ISRAEL: "ישראל — שישה מחוזות משרד הפנים (כולל ירושלים המזרחית והגולן)",
    REGION_JUDEA_SAMARIA: 'איו"ש — מחוז יהודה ושומרון של משרד הפנים',
}

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

_DDL = [
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(BOUNDARY_TABLE)} (
        region        text PRIMARY KEY,
        name          text NOT NULL,
        districts     text[] NOT NULL,
        area_km2      double precision,
        geom          {_X}.geometry(MultiPolygon, 4326) NOT NULL,
        source_table  text NOT NULL,
        built_at      timestamptz NOT NULL
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(PARTS_TABLE)} (
        region   text NOT NULL,
        part_id  integer NOT NULL,
        geom     {_X}.geometry(Geometry, 4326) NOT NULL,
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
        geom         {_X}.geometry(Geometry, 4326) NOT NULL,
        PRIMARY KEY (dataset_id, row_hash)
    )
    """,
    f"""CREATE INDEX IF NOT EXISTS {_qi(FEATURES_TABLE + '_geom_gist')}
        ON public.{_qi(FEATURES_TABLE)} USING gist (geom)""",
]


def layer_id_of(table_name: str) -> str | None:
    """``govmap_376_41d39d22_fb79e65e`` → ``376``. None for a non-GovMap table."""
    parts = table_name.split("_")
    if len(parts) >= 3 and parts[0] == "govmap" and parts[1].isdigit():
        return parts[1]
    return None


def needs_check(prev: dict | None, version_number, boundary_built_at) -> bool:
    """Whether a layer must be (re)checked: never checked, a new mirrored
    version since, or an outline rebuilt after its last check."""
    if prev is None:
        return True
    if version_number is not None and prev.get("version_number") != version_number:
        return True
    built = prev.get("boundary_built_at")
    return built is None or (boundary_built_at is not None and built < boundary_built_at)


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for stmt in _DDL:
            await conn.execute(stmt)
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        if role:
            for t in ALL_TABLES:
                try:
                    await conn.execute(f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")
                except Exception:  # noqa: BLE001 — a missing role must not fail a build
                    logger.debug("govmap_abroad: grant on %s failed", t, exc_info=True)


async def _districts_table(conn) -> str | None:
    """The live idx mirror table of GovMap layer 119, or None."""
    return await conn.fetchval(
        """SELECT c.relname FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
           WHERE n.nspname = $1 AND c.relkind = 'r'
             AND c.relname LIKE $2
           ORDER BY c.relname LIMIT 1""",
        IDX_SCHEMA, f"govmap\\_{DISTRICTS_LAYER_ID}\\_%")


async def build_boundary() -> dict:
    """(Re)build the outline from the district layer. Replaces both tables in
    one transaction, so a reader never sees half an outline."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    await ensure_tables()
    pool = await append_store.get_pool()
    t0 = time.monotonic()
    async with pool.acquire() as conn:
        src = await _districts_table(conn)
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
                        (region, name, districts, area_km2, geom, source_table, built_at)
                    SELECT region,
                           CASE region WHEN '{REGION_ISRAEL}' THEN $2 ELSE $3 END,
                           array_agg(DISTINCT d ORDER BY d),
                           {_X}.ST_Area({_X}.ST_Union(g)::{_X}.geography) / 1e6,
                           {_X}.ST_Multi({_X}.ST_CollectionExtract({_X}.ST_Union(g), 3)),
                           $4, $5
                    FROM (SELECT {region_expr} AS region, {_qi(DISTRICT_COLUMN)} AS d,
                                 {_X}.ST_MakeValid(geom) AS g
                          FROM {q_src} WHERE geom IS NOT NULL) s
                    GROUP BY region""",
                JUDEA_SAMARIA_DISTRICT, REGION_NAMES[REGION_ISRAEL],
                REGION_NAMES[REGION_JUDEA_SAMARIA], src, now)
            await conn.execute(
                f"""INSERT INTO public.{_qi(PARTS_TABLE)} (region, part_id, geom)
                    SELECT region, row_number() OVER (PARTITION BY region)::int, g
                    FROM (SELECT region,
                                 {_X}.ST_Subdivide(geom, {SUBDIVIDE_VERTICES}) AS g
                          FROM public.{_qi(BOUNDARY_TABLE)}) s""")
        await conn.execute(f"ANALYZE public.{_qi(PARTS_TABLE)}")
        regions = await conn.fetch(
            f"""SELECT b.region, b.area_km2, b.districts,
                       (SELECT count(*) FROM public.{_qi(PARTS_TABLE)} p
                        WHERE p.region = b.region) AS parts
                FROM public.{_qi(BOUNDARY_TABLE)} b ORDER BY b.region""")
    data_catalog.invalidate_catalog_cache()
    out = {"source_table": src, "built_at": now.isoformat(),
           "duration_ms": int((time.monotonic() - t0) * 1000),
           "regions": [{"region": r["region"], "area_km2": round(r["area_km2"] or 0),
                        "districts": list(r["districts"]), "parts": r["parts"]}
                       for r in regions]}
    logger.info("govmap_abroad.build_boundary: %s", out)
    return out


async def _boundary_built_at(conn):
    try:
        return await conn.fetchval(
            f"SELECT max(built_at) FROM public.{_qi(BOUNDARY_TABLE)}")
    except Exception:  # noqa: BLE001 — table not built yet
        return None


def _check_sql(table: str) -> tuple[str, str]:
    """The two statements that check one layer: the abroad features (written)
    and the per-region counts (read). Both against the indexed outline pieces.

    In a region = touches any piece of it. Distance = to the nearest of the
    five pieces closest by bounding box, which is the nearest piece for any
    feature outside the outline."""
    # The alias is deliberately unlikely: a layer with a column named like the
    # alias would make to_jsonb(alias) read that column instead of the row.
    src = f"{_qi(IDX_SCHEMA)}.{_qi(table)}"
    parts = f"public.{_qi(PARTS_TABLE)}"
    touches = (lambda region, g:
               f"EXISTS (SELECT 1 FROM {parts} p WHERE p.region = '{region}' "
               f"AND p.geom OPERATOR({_X}.&&) {g} AND {_X}.ST_Intersects(p.geom, {g}))")
    insert = f"""
        INSERT INTO public.{_qi(FEATURES_TABLE)}
            (dataset_id, table_name, layer_id, row_hash, distance_km, attrs, geom)
        SELECT $1, $2, $3, zz_src._row_hash::text,
               (SELECT min({_X}.ST_Distance(n.geom::{_X}.geography, zz_src.geom::{_X}.geography))
                FROM (SELECT p.geom FROM {parts} p
                      ORDER BY p.geom OPERATOR({_X}.<->) zz_src.geom LIMIT 5) n) / 1000.0,
               CASE WHEN length(a.j::text) > {MAX_ATTRS_CHARS}
                    THEN jsonb_build_object('_truncated', left(a.j::text, {MAX_ATTRS_CHARS}))
                    ELSE a.j END,
               zz_src.geom
        FROM {src} zz_src
        CROSS JOIN LATERAL (SELECT to_jsonb(zz_src) - 'geom' - 'geometry_wkt'
                                   - '_row_hash' - '_first_seen' AS j) a
        WHERE zz_src.geom IS NOT NULL
          AND NOT {touches(REGION_ISRAEL, 'zz_src.geom')}
          AND NOT {touches(REGION_JUDEA_SAMARIA, 'zz_src.geom')}
        ON CONFLICT (dataset_id, row_hash) DO NOTHING
    """
    counts = f"""
        SELECT count(*) AS rows,
               count(*) FILTER (WHERE geom IS NOT NULL) AS rows_with_geom,
               count(*) FILTER (WHERE geom IS NOT NULL
                                AND {touches(REGION_ISRAEL, 'zz_src.geom')}) AS in_israel
        FROM {src} zz_src
    """
    return insert, counts


async def check_layer(conn, dataset_id: str, table: str, *, title: str | None,
                      version_number, boundary_built_at) -> dict:
    """Check one mirrored layer and store its result. One transaction: the
    layer's previous abroad features are replaced, never mixed."""
    t0 = time.monotonic()
    layer_id = layer_id_of(table)
    insert, counts = _check_sql(table)
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
        in_js = max(0, c["rows_with_geom"] - c["in_israel"] - abroad)
        ms = int((time.monotonic() - t0) * 1000)
        await conn.execute(
            f"""INSERT INTO public.{_qi(LAYERS_TABLE)}
                  (dataset_id, table_name, layer_id, title, version_number, rows,
                   rows_with_geom, in_israel, in_judea_samaria, abroad,
                   max_distance_km, checked_at, boundary_built_at, duration_ms)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now(),$12,$13)
                ON CONFLICT (dataset_id) DO UPDATE SET
                  table_name = EXCLUDED.table_name, layer_id = EXCLUDED.layer_id,
                  title = EXCLUDED.title, version_number = EXCLUDED.version_number,
                  rows = EXCLUDED.rows, rows_with_geom = EXCLUDED.rows_with_geom,
                  in_israel = EXCLUDED.in_israel,
                  in_judea_samaria = EXCLUDED.in_judea_samaria,
                  abroad = EXCLUDED.abroad, max_distance_km = EXCLUDED.max_distance_km,
                  checked_at = EXCLUDED.checked_at,
                  boundary_built_at = EXCLUDED.boundary_built_at,
                  duration_ms = EXCLUDED.duration_ms""",
            dataset_id, table, layer_id, title, version_number, c["rows"],
            c["rows_with_geom"], c["in_israel"], in_js, abroad, a["mx"],
            boundary_built_at, ms)
    return {"dataset_id": dataset_id, "table": table, "rows": c["rows"],
            "in_israel": c["in_israel"], "in_judea_samaria": in_js,
            "abroad": abroad, "duration_ms": ms}


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

    Builds the outline first if it does not exist (or when asked). ``limit``
    caps how many layers this run checks; the rest wait for the next run."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        built_at = await _boundary_built_at(conn)
    boundary = None
    if built_at is None or rebuild_boundary:
        boundary = await build_boundary()
        if "error" in boundary or "skipped" in boundary:
            return {"boundary": boundary}
        async with pool.acquire() as conn:
            built_at = await _boundary_built_at(conn)

    layers = await _govmap_layers(db)
    async with pool.acquire() as conn:
        prev = {r["dataset_id"]: dict(r) for r in await conn.fetch(
            f"SELECT dataset_id, version_number, boundary_built_at "
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
            if force or needs_check(prev.get(l["dataset_id"]), l["version_number"], built_at)]
    if limit is not None:
        todo = todo[:limit]
    checked, failed, abroad_layers = 0, [], 0
    t0 = time.monotonic()
    for l in todo:
        try:
            async with pool.acquire() as conn:
                res = await check_layer(conn, l["dataset_id"], l["table"], title=l["title"],
                                        version_number=l["version_number"],
                                        boundary_built_at=built_at)
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
                       coalesce(sum(abroad), 0) AS abroad,
                       count(*) FILTER (WHERE abroad > 0) AS layers_with_abroad
                FROM public.{_qi(LAYERS_TABLE)}"""))
    data_catalog.invalidate_catalog_cache()
    out = {"boundary": boundary, "layers_total": len(layers), "to_check": len(todo),
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
                           in_judea_samaria, abroad, max_distance_km, checked_at
                    FROM public.{_qi(LAYERS_TABLE)} WHERE abroad > 0
                    ORDER BY abroad DESC LIMIT 50""")]
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(LAYERS_TABLE)}")
    except Exception as e:  # noqa: BLE001 — not built yet
        return {"configured": True, "built": False, "note": str(e)}
    return {"configured": True, "built": bool(regions), "regions": regions,
            "layers_checked": n, "top_abroad": top}
