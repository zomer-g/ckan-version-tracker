"""נדל"ן לעם — the parcel crosswalk for GovMap layers too large to mirror.

The cubes (``nadlan_layers``) answer a spatial question against the layer's own
PostGIS table. That works for every layer the ``idx`` mirror holds — and the
mirror's size gate (``index_mirror_max_csv_mb``) keeps out exactly the national
planning layers a property question needs: מבא"ת's blue lines, the רצף layers of
רמ"י, משהב"ש plots. Raising the gate is not an option on a container that peaks
at 7.7 of its 8 GB: the gate exists because a big first load is what kills it.

So for those layers the LAYER is never stored. What is stored is the answer to
the only question the property page asks of it — which features touch which
parcel — computed once per layer version:

    over_re_layer_features   one row per feature that touches ANY parcel: its
                             attributes (no geometry), keyed by a content hash,
                             and the ARRAY of parcels it touches (GIN-indexed)

Why an array per feature and not a row per (parcel, feature) pair: a plan can
cover a whole city. Measured on a synthetic metro with 1.1 M parcels, 11,000
plans already made 10 M pairs — at ~200 bytes a pair with its index, מבא"ת
alone would have cost gigabytes. A GIN posting list stores the same pairs at a
couple of bytes each, and ``parcels @> ARRAY[key]`` is still an index lookup.

How it is computed, and why it costs constant memory however large the layer:

1. The version's index CSV ("נתוני הסורק", attributes + ``geometry_wkt``) is
   read as a STREAM straight from R2 — not downloaded, not loaded. Nothing ever
   holds more than one batch.
2. Each batch (bounded by rows AND bytes, as the mirror's are) is COPYed into a
   temporary table, and Postgres finds the parcels whose centroid lies inside
   each polygon (or within the cube's radius) through the GiST index that
   already covers all 1.1 M parcel centroids. Only the matches are written.
3. Every row carries a ``generation``. A build writes a NEW generation and only
   flips ``over_re_layer_link_state.generation`` when it has finished, so a
   reader never sees a half-built layer, and the previous generation is deleted
   only after the flip.

The geometry itself is not kept, so the page can list a plan's details but not
draw its outline. That is the price of not storing a 1.09 M-polygon layer.
"""
from __future__ import annotations

import asyncio
import codecs
import csv
import hashlib
import json
import logging
import time
import uuid

from app.config import settings
from app.services import append_store, column_aliases
from app.services.append_store import _qi
from app.services.index_mirror import (
    CSV_RESOURCE_KEY, GEOM_SRID, ITM_SRID, PG_EXT_SCHEMA, WKT_COLUMN,
    classify_wkt_crs, index_csv_value,
)
from app.services.nadlan_index import PARCELS_TABLE
from app.services.storage_client import key_of, storage_client

logger = logging.getLogger(__name__)

_X = _qi(PG_EXT_SCHEMA)

FEATURES_TABLE = "over_re_layer_features"
STATE_TABLE = "over_re_layer_link_state"
ALL_TABLES = (FEATURES_TABLE, STATE_TABLE)

#: A batch is bounded by rows and by bytes, whichever trips first — the same
#: rule as the mirror's COPY batches, for the same reason: a blue-line polygon
#: can be tens of KB of WKT, so a row count alone is not a memory bound.
BATCH_ROWS = 1_000
BATCH_BYTES = 8 * 1024 * 1024

#: Longest attribute value stored. A feature row is a summary for a card, not
#: an archive — the full row stays one click away in the version's files.
MAX_ATTR_CHARS = 400

# Bookkeeping and GIS plumbing — never what a reader opens a cube for.
# Compared lower-case; mirrors nadlan_layers._NOISE_COLS.
_NOISE = {
    "_id", "_row_hash", "_first_seen", "row_hash", "first_seen", "last_seen",
    WKT_COLUMN, "geom", "objectid", "object_id", "fid", "ogc_fid", "gid",
    "globalid", "global_id", "shape", "shape_area", "shape_length", "shape_leng",
    "shape__area", "shape__length", "shape_le_1", "st_area(shape)",
    "st_length(shape)", "symbol", "symbolid", "symbol_id",
}

csv.field_size_limit(10**8)

_DDL = [
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(STATE_TABLE)} (
        dataset_id      text PRIMARY KEY,
        generation      integer,          -- the generation readers use
        version_number  integer,          -- the layer version it was built from
        building        integer,          -- a generation in progress, else NULL
        status          text,
        features_read   bigint,
        features_linked bigint,
        links           bigint,
        labels          jsonb,            -- {{machine_name: hebrew caption}}
        started_at      timestamptz,
        finished_at     timestamptz,
        duration_ms     bigint,
        note            text
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(FEATURES_TABLE)} (
        dataset_id   text      NOT NULL,
        generation   integer   NOT NULL,
        feature_key  text      NOT NULL,
        attrs        jsonb     NOT NULL,
        parcels      text[]    NOT NULL,   -- every parcel the feature touches
        distances    real[],               -- radius layers only, aligned to parcels
        PRIMARY KEY (dataset_id, generation, feature_key)
    )
    """,
    # The read path: "which features touch this parcel" is a GIN lookup.
    f"""CREATE INDEX IF NOT EXISTS {_qi(FEATURES_TABLE + '_parcels_gin')}
        ON public.{_qi(FEATURES_TABLE)} USING gin (parcels)""",
]


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for stmt in _DDL:
            await conn.execute(stmt)
    from app.services.index_mirror import _readonly_role
    role = _readonly_role()
    if role:
        async with pool.acquire() as conn:
            for t in ALL_TABLES:
                try:
                    await conn.execute(f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")
                except Exception:  # noqa: BLE001 — a missing role must not fail a build
                    logger.debug("layer links: grant on %s failed", t, exc_info=True)


# ── reading a layer ──
def feature_key(row: list[str]) -> str:
    """A content hash of the source row. Stable across rebuilds of the same
    version, and the same feature in two versions only collides if it is
    byte-identical — in which case it IS the same feature."""
    h = hashlib.md5()
    for v in row:
        h.update((v or "").encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()


def attrs_of(header: list[str], row: list[str], keep: list[int]) -> dict:
    out = {}
    for i in keep:
        v = (row[i] if i < len(row) else "") or ""
        v = v.strip()
        if not v or v.lower() in ("null", "none", "nan"):
            continue
        out[header[i]] = v if len(v) <= MAX_ATTR_CHARS else v[:MAX_ATTR_CHARS] + "…"
    return out


def iter_csv_batches(lines, *, batch_rows: int = BATCH_ROWS,
                     batch_bytes: int = BATCH_BYTES):
    """Yield ``(header, [(key, wkt, attrs_json), ...])`` batches from an
    iterable of CSV text lines. Pure: the caller decides where lines come from
    (an R2 stream in production, a list in tests).

    Rows without geometry are skipped — a feature that cannot be placed cannot
    touch a parcel. The header is yielded with every batch so the caller never
    has to keep state between them."""
    reader = csv.reader(lines)
    header = next(reader, None)
    if not header:
        return
    header = [h.strip() for h in header]
    if WKT_COLUMN not in header:
        raise ValueError(f"index CSV has no {WKT_COLUMN} column")
    wi = header.index(WKT_COLUMN)
    keep = [i for i, h in enumerate(header) if h and h.lower() not in _NOISE]
    batch: list[tuple[str, str, str]] = []
    nbytes = 0
    for row in reader:
        wkt = (row[wi] if wi < len(row) else "") or ""
        if not wkt.strip():
            continue
        batch.append((feature_key(row), wkt,
                      json.dumps(attrs_of(header, row, keep), ensure_ascii=False)))
        nbytes += len(wkt)
        if len(batch) >= batch_rows or nbytes >= batch_bytes:
            yield header, batch
            batch, nbytes = [], 0
    if batch:
        yield header, batch


def _r2_lines(r2_value: str):
    """The object's text, line by line, read as a stream. Runs in a worker
    thread — boto3 is blocking — and never holds the whole object."""
    client = storage_client._get_client()
    body = client.get_object(Bucket=settings.s3_bucket, Key=key_of(r2_value))["Body"]
    try:
        reader = codecs.getreader("utf-8-sig")(body, errors="replace")
        # csv needs newline-preserving lines; StreamReader iteration keeps them.
        yield from reader
    finally:
        body.close()


async def _next(gen):
    """Advance a blocking generator on a worker thread; None at the end."""
    return await asyncio.to_thread(next, gen, None)


# ── which version, which labels ──
async def latest_version(dataset_id: str) -> tuple[int, dict] | None:
    """``(version_number, resource_mappings)`` of the newest version that
    carries an index CSV, or None."""
    from sqlalchemy import select

    from app.database import async_session
    from app.models.version_index import VersionIndex
    async with async_session() as db:
        rows = (await db.execute(
            select(VersionIndex.version_number, VersionIndex.resource_mappings)
            .where(VersionIndex.tracked_dataset_id == uuid.UUID(dataset_id))
            .order_by(VersionIndex.version_number.desc()).limit(5)
        )).all()
    for vnum, mappings in rows:
        if index_csv_value(mappings):
            return int(vnum), mappings or {}
    return None


# ── the build ──
_build_lock = asyncio.Lock()


def is_building() -> bool:
    return _build_lock.locked()


def _match_sql(radius_m: float) -> str:
    """The parcels each feature of the batch touches.

    Reads the batch's MATERIALISED ``g`` column, never the WKT. Written as a
    subquery over ``try_geom(wkt)`` the planner inlines the function into the
    join filter and re-parses the polygon once per candidate parcel (measured:
    1,215 parses per feature in a dense city) — and a re-parsed geometry is a
    new datum every time, so PostGIS's prepared-geometry cache, which is what
    makes testing thousands of points against ONE polygon cheap, never hits."""
    p = f"public.{_qi(PARCELS_TABLE)}"
    if radius_m <= 0:
        return f"""
            SELECT f.feature_key, p.parcel_key, 0::real AS distance_m
            FROM _layer_batch f
            JOIN {p} p
              ON p.centroid OPERATOR({_X}.&&) f.g
             AND {_X}.ST_Intersects(f.g, p.centroid)
            WHERE f.g IS NOT NULL"""
    deg = max(radius_m, 1.0) / 90_000.0
    return f"""
        SELECT f.feature_key, p.parcel_key,
               {_X}.ST_Distance(f.g::{_X}.geography, p.centroid::{_X}.geography)::real
                 AS distance_m
        FROM _layer_batch f
        JOIN {p} p
          ON p.centroid OPERATOR({_X}.&&) {_X}.ST_Expand(f.g, {deg})
         AND {_X}.ST_DWithin(f.g::{_X}.geography, p.centroid::{_X}.geography, {radius_m})
        WHERE f.g IS NOT NULL"""


async def build(dataset_id: str, *, radius_m: float = 0.0,
                force: bool = False) -> dict:
    """Compute the crosswalk for one layer's latest version. Never raises; the
    outcome is in the returned dict and in ``over_re_layer_link_state``."""
    if _build_lock.locked():
        return {"dataset_id": dataset_id, "status": "busy",
                "note": "another layer is being built; one at a time"}
    async with _build_lock:
        try:
            return await _build(dataset_id, radius_m=radius_m, force=force)
        except Exception as e:  # noqa: BLE001 — recorded, never propagated
            logger.exception("layer links: build of %s failed", dataset_id)
            await _set_state(dataset_id, status="failed", building=None,
                             note=f"{type(e).__name__}: {e}"[:500], finished=True)
            return {"dataset_id": dataset_id, "status": "failed", "note": str(e)[:500]}


async def _set_state(dataset_id: str, *, finished: bool = False, **fields) -> None:
    """Upsert one layer's state row; ``finished`` also stamps finished_at."""
    pool = await append_store.get_pool()
    cols = list(fields)
    sets = [f"{_qi(c)} = ${i + 2}" for i, c in enumerate(cols)]
    if finished:
        sets.append("finished_at = now()")
    vals = [json.dumps(v, ensure_ascii=False) if c == "labels" and v is not None else v
            for c, v in fields.items()]
    async with pool.acquire() as conn:
        await conn.execute(
            f"INSERT INTO public.{_qi(STATE_TABLE)} (dataset_id) VALUES ($1) "
            f"ON CONFLICT (dataset_id) DO NOTHING", dataset_id)
        if sets:
            await conn.execute(
                f"UPDATE public.{_qi(STATE_TABLE)} SET {', '.join(sets)} "
                f"WHERE dataset_id = $1", dataset_id, *vals)


async def get_state(dataset_id: str | None = None) -> list[dict]:
    try:
        async with append_store.readonly_txn() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM public.{_qi(STATE_TABLE)} "
                + ("WHERE dataset_id = $1 " if dataset_id else "")
                + "ORDER BY dataset_id", *([dataset_id] if dataset_id else []))
    except Exception:  # noqa: BLE001 — table not created yet
        return []
    out = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("labels"), str):
            d["labels"] = json.loads(d["labels"])
        out.append(d)
    return out


def _count(tag) -> int:
    """Rows affected, from an asyncpg command tag ("INSERT 0 412")."""
    try:
        return int(str(tag).rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


async def _build(dataset_id: str, *, radius_m: float, force: bool) -> dict:
    await ensure_tables()
    latest = await latest_version(dataset_id)
    if latest is None:
        await _set_state(dataset_id, status="no_index_csv", finished=True,
                         note="no version of this dataset carries an index CSV")
        return {"dataset_id": dataset_id, "status": "no_index_csv"}
    vnum, mappings = latest
    [state] = await get_state(dataset_id) or [{}]
    if not force and state.get("version_number") == vnum and state.get("generation"):
        return {"dataset_id": dataset_id, "status": "current", "version_number": vnum}

    gen = int(state.get("generation") or 0) + 1
    labels = {}
    try:
        labels = await column_aliases._bundle_dictionary(
            column_aliases._doc_values(mappings))
    except Exception:  # noqa: BLE001 — captions are a nicety
        logger.info("layer links: no captions for %s", dataset_id, exc_info=True)
    await _set_state(dataset_id, status="building", building=gen, features_read=0,
                     features_linked=0, links=0, note=None, finished_at=None)
    t0 = time.monotonic()
    pool = await append_store.get_pool()
    read = linked = links = batches = 0
    gen_iter = iter_csv_batches(_r2_lines(index_csv_value(mappings)),
                                batch_rows=BATCH_ROWS, batch_bytes=BATCH_BYTES)
    srid_expr = None
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE public.{_qi(STATE_TABLE)} SET started_at = now() "
                           f"WHERE dataset_id = $1", dataset_id)
        # Rows of this generation left by a build a restart cut short: it never
        # flipped, so no reader uses them, and starting clean keeps the counts true.
        await conn.execute(f"DELETE FROM public.{_qi(FEATURES_TABLE)} "
                           f"WHERE dataset_id = $1 AND generation = $2",
                           dataset_id, gen, timeout=1800)
        await conn.execute(f"""CREATE TEMP TABLE IF NOT EXISTS _layer_batch (
                                  feature_key text, wkt text, attrs text,
                                  g {_X}.geometry)""")
        try:
            while True:
                item = await _next(gen_iter)
                if item is None:
                    break
                _header, batch = item
                read += len(batch)
                if srid_expr is None:
                    crs = classify_wkt_crs(batch[0][1])
                    if crs == "itm":
                        srid_expr = (f"{_X}.ST_Transform(idx.try_geom(wkt, {ITM_SRID}), "
                                     f"{GEOM_SRID})")
                    elif crs == "degrees":
                        srid_expr = f"idx.try_geom(wkt, {GEOM_SRID})"
                    else:
                        raise ValueError(f"cannot tell the CRS of the geometry "
                                         f"({batch[0][1][:60]!r})")
                async with conn.transaction():
                    await conn.execute("TRUNCATE _layer_batch")
                    await conn.copy_records_to_table(
                        "_layer_batch", records=batch,
                        columns=["feature_key", "wkt", "attrs"])
                    # Parse each polygon ONCE, then drop the text: the join
                    # below must see a stored geometry (see _match_sql).
                    await conn.execute(f"UPDATE _layer_batch SET g = {srid_expr}, wkt = NULL",
                                       timeout=600)
                    await conn.execute("ANALYZE _layer_batch")
                    await conn.execute(f"""
                        CREATE TEMP TABLE _layer_hits ON COMMIT DROP AS
                        {_match_sql(radius_m)}""", timeout=1800)
                    # One row per feature, its parcels gathered into an array.
                    # A feature repeated in a later batch is the same content
                    # hash, so the conflict skips it rather than storing twice.
                    row = await conn.fetchrow(f"""
                        WITH h AS (
                          SELECT feature_key,
                                 array_agg(parcel_key ORDER BY parcel_key) AS parcels,
                                 array_agg(distance_m ORDER BY parcel_key) AS distances
                          FROM (SELECT feature_key, parcel_key, min(distance_m) AS distance_m
                                FROM _layer_hits GROUP BY feature_key, parcel_key) d
                          GROUP BY feature_key
                        ), ins AS (
                          INSERT INTO public.{_qi(FEATURES_TABLE)}
                              (dataset_id, generation, feature_key, attrs, parcels, distances)
                          SELECT DISTINCT ON (b.feature_key) $1, $2, b.feature_key,
                                 b.attrs::jsonb, h.parcels,
                                 CASE WHEN $3 THEN h.distances END
                          FROM _layer_batch b JOIN h USING (feature_key)
                          ON CONFLICT DO NOTHING
                          RETURNING cardinality(parcels) AS n
                        )
                        SELECT count(*) AS features, coalesce(sum(n), 0) AS links FROM ins""",
                        dataset_id, gen, radius_m > 0)
                    linked += int(row["features"])
                    links += int(row["links"])
                await conn.execute(
                    f"UPDATE public.{_qi(STATE_TABLE)} SET features_read = $2, "
                    f"features_linked = $3, links = $4 WHERE dataset_id = $1",
                    dataset_id, read, linked, links)
                batches += 1
                if batches % 20 == 0:
                    logger.info("layer links: %s building, %d read, %d linked, %d links, %.0fs",
                                dataset_id, read, linked, links, time.monotonic() - t0)
        finally:
            await conn.execute("DROP TABLE IF EXISTS _layer_batch")

        # The flip: readers move to the new generation in one statement, and
        # only then is the old one removed.
        await conn.execute(
            f"""UPDATE public.{_qi(STATE_TABLE)}
                SET generation = $2, version_number = $3, building = NULL,
                    status = 'ok', labels = $4::jsonb, finished_at = now(),
                    duration_ms = $5
                WHERE dataset_id = $1""",
            dataset_id, gen, vnum, json.dumps(labels, ensure_ascii=False),
            int((time.monotonic() - t0) * 1000))
        await conn.execute(
            f"DELETE FROM public.{_qi(FEATURES_TABLE)} "
            f"WHERE dataset_id = $1 AND generation <> $2",
            dataset_id, gen, timeout=1800)
        table_bytes = await conn.fetchval(
            "SELECT pg_total_relation_size(to_regclass($1))", f"public.{FEATURES_TABLE}")
    invalidate_generations()
    summary = {"dataset_id": dataset_id, "status": "ok", "version_number": vnum,
               "generation": gen, "features_read": read, "features_linked": linked,
               "links": links, "duration_s": round(time.monotonic() - t0, 1),
               "table_bytes": table_bytes}
    logger.info("layer links: %s", summary)
    return summary


async def refresh_stale() -> list[dict]:
    """Rebuild every layer that was built once and has a newer version since.

    Only layers an admin built at least once are refreshed: a first build is a
    deliberate act (it is the heaviest thing this module does), a refresh of a
    layer already in use is upkeep."""
    from app.services import nadlan_layers
    out = []
    for st in await get_state():
        if not st.get("generation"):
            continue
        latest = await latest_version(st["dataset_id"])
        if latest and latest[0] != st.get("version_number"):
            out.append(await build(st["dataset_id"],
                                   radius_m=nadlan_layers.radius_for_dataset(st["dataset_id"])))
    return out


def autobuild_ids() -> list[str]:
    return [d.strip() for d in (settings.nadlan_layer_links_autobuild or "").split(",")
            if d.strip()]


async def build_pending() -> dict | None:
    """The first build of the next layer listed in
    ``settings.nadlan_layer_links_autobuild`` that has none yet — one per call.

    A layer whose build failed is left alone (its note says why; an admin
    rebuilds it with ``force``), and so is everything after it: the list is an
    order, and the next layer is the larger one. A layer whose build a restart
    cut short reads as ``building`` with no generation, and is built again."""
    from app.services import nadlan_layers
    states = {s["dataset_id"]: s for s in await get_state()}
    for ds in autobuild_ids():
        st = states.get(ds) or {}
        if st.get("generation"):
            continue
        if st.get("status") in ("failed", "no_index_csv"):
            return None
        return await build(ds, radius_m=nadlan_layers.radius_for_dataset(ds))
    return None


# ── reading the crosswalk ──
_GEN_TTL = 60.0
_gen_cache: tuple[float, dict[str, dict]] | None = None


def invalidate_generations() -> None:
    global _gen_cache
    _gen_cache = None


async def current_generations() -> dict[str, dict]:
    """``{dataset_id: state}`` for every layer with a completed generation.

    Cached for a minute: a catalog call asks once per cube, and the answer only
    changes when a build finishes (which invalidates it in this process)."""
    global _gen_cache
    if _gen_cache and time.monotonic() - _gen_cache[0] < _GEN_TTL:
        return _gen_cache[1]
    gens = {s["dataset_id"]: s for s in await get_state() if s.get("generation")}
    _gen_cache = (time.monotonic(), gens)
    return gens
