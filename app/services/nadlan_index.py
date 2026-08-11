"""נדל"ן לעם — the property-level crosswalk index.

Four heavy layers already live in the append DB, each holding ONE leg of Israeli
property identity and none of them joined to the others:

===================  ==========================================  =========  ==================
layer                table                                       rows       identifies
===================  ==========================================  =========  ==================
חלקות shape          ``public.append_shape_ff3176b1``             1.10 M     גוש-חלקה → polygon
גזטיר הנכסים         ``odata.gaztir_41720377``                    3.68 M     גוש-חלקה → יישוב+רחוב
קובץ המיקוד          ``odata."00a9749e…_2a021675"``                515 k     כתובת → מיקוד
רשימת כתובות         ``odata."ac1ae1fa…_19c5be7f"``                548 k     כתובת → נקודה (ITM)
===================  ==========================================  =========  ==================

This module derives the THIN crosswalk between them. The parcels layer alone is
4.58 GB, so nothing here copies geometry or source attributes: the spine carries
identifiers plus one point, and the API hands back a /data deep-link for each
source's full row instead of duplicating it.

Tables (all ``public.over_re_*``, so ``data_catalog._over_index_records()`` puts
them in /data automatically and ``append_store.sql_helper_functions()``
advertises the ``over_*`` functions to the console and the MCP for free):

* ``over_re_parcels``            — the spine, one row per חלקה, PK ``parcel_key``
* ``over_re_parcel_gazetteer``   — the gazetteer rolled up to parcel grain
* ``over_re_streets`` / ``_street_aliases`` / ``_street_unmatched``
* ``over_re_postal_localities``  — Israel-Post locality id → CBS code
* ``over_re_addresses``          — the address spine (postal ∪ address list)
* ``over_re_zip5``               — zip → locality rollup
* ``over_re_build_state``        — per-stage watermark, timing and row counts

Design decisions that are load-bearing (each one was measured, see docs):

* **Two keys, not one.** ``parcel_key`` = ``gush-suffix-parcel``; the gazetteer
  publishes no gush suffix, so it attaches on the suffix-less ``gp_key`` and any
  pair covering several real parcels is flagged ``gp_ambiguous`` (0.63% of pairs)
  rather than silently collapsed.
* **Address→parcel is spatial, not attribute-based.** The gazetteer resolves to a
  STREET, never to a house number, so the precise link is point-in-polygon. On a
  3,000-point sample 90.4% of geocoded addresses landed inside a parcel.
* **Postal locality ids are Israel Post's own codespace** (0 of 91 match a CBS
  code), so they are resolved by NAME once, into a 91-row dimension table, and
  everything downstream joins on the CBS code only.
* **TRUNCATE+INSERT, never DROP.** The read-only console role keeps its grant
  (the ``site_index`` rationale) and ``over_gush_helka()`` keeps its return type.
"""
from __future__ import annotations

import logging
import time

from app.services import append_store, nadlan_text
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

# ── source tables ─────────────────────────────────────────────────────────────
PARCELS_SRC = ("public", "append_shape_ff3176b1")
GAZTIR_SRC = ("odata", "gaztir_41720377")
POSTAL_SRC = ("odata", "00a9749e_c112_4190_9c37_97918b5792cf_2a021675")
# The SAME Israel-Post dataset publishes a LOCALITY-level file too: one zip for
# each of 1,135 localities, plus `Location Symbol` — which IS the CBS code
# (981/995 resolve), i.e. the post→CBS crosswalk the street file lacks.
# Israeli postal reality: the big cities get a zip per street+house (the street
# file, 91 localities); everywhere else has ONE zip for the whole place. Loading
# only the street file left 98,139 addresses zip-less for no reason.
POSTAL_LOCALITY_SRC = ("odata", "00a9749e_c112_4190_9c37_97918b5792cf_65b5335b")
ADDR_SRC = ("odata", "ac1ae1fa_6d43_4685_8434_9953e950ca9b_19c5be7f")

# The repo's canonical ITM is 6991, not 2039 — they differ only in datum
# realisation (~0.4 mm), and index_mirror already standardised on 6991. The
# address list's X/Y are ITM easting/northing (X 157k–266k, Y 380k–798k).
from app.services.index_mirror import GEOM_SRID, ITM_SRID, PG_EXT_SCHEMA  # noqa: E402

PARCELS_TABLE = "over_re_parcels"
GAZ_TABLE = "over_re_parcel_gazetteer"
STREETS_TABLE = "over_re_streets"
STREET_ALIASES_TABLE = "over_re_street_aliases"
STREET_UNMATCHED_TABLE = "over_re_street_unmatched"
POSTAL_LOC_TABLE = "over_re_postal_localities"
ADDRESSES_TABLE = "over_re_addresses"
ZIP5_TABLE = "over_re_zip5"
STATE_TABLE = "over_re_build_state"

ALL_TABLES = [PARCELS_TABLE, GAZ_TABLE, STREETS_TABLE, STREET_ALIASES_TABLE,
              STREET_UNMATCHED_TABLE, POSTAL_LOC_TABLE, ADDRESSES_TABLE,
              ZIP5_TABLE, STATE_TABLE]

STAGES = ["source_indexes", "parcels", "gazetteer", "postal_localities",
          "streets", "addresses", "zip5", "pip"]

# The parcels scan and the index builds far exceed the pool's command_timeout=180.
_LONG_TIMEOUT = 3600
_PIP_BATCH = 2000


def _t(pair: tuple[str, str]) -> str:
    return f"{_qi(pair[0])}.{_qi(pair[1])}"


# ── DDL ───────────────────────────────────────────────────────────────────────
_DDL = [
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(PARCELS_TABLE)} (
        parcel_key      text PRIMARY KEY,
        gush            integer NOT NULL,
        gush_suffix     integer NOT NULL DEFAULT 0,
        parcel          integer NOT NULL,
        gp_key          text NOT NULL,
        gp_ambiguous    boolean NOT NULL DEFAULT false,
        settlement_code integer,
        locality_name   text,
        reg_mun_name    text,
        county_name     text,
        region_name     text,
        legal_area      double precision,
        status_text     text,
        -- lat/lon are stored ALONGSIDE the geometry on purpose: the asyncpg
        -- geometry codec hands back hex EWKB, so the API reads these two and
        -- only spatial predicates touch `centroid`.
        lat             double precision,
        lon             double precision,
        centroid        {_qi(PG_EXT_SCHEMA)}.geometry(Point, {GEOM_SRID}),
        refreshed_at    timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(GAZ_TABLE)} (
        parcel_key        text PRIMARY KEY,
        gp_key            text NOT NULL,
        settlement_code   integer,
        settlement_name   text,
        street_name_src   text,
        street_code       text,
        street_key        text,
        n_assets          integer,
        n_dwellings       integer,
        n_subparcels      integer,
        floors_max        integer,
        building_year_min integer,
        building_year_max integer,
        apartments_est    integer,
        refreshed_at      timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(STREETS_TABLE)} (
        street_key       text PRIMARY KEY,
        settlement_code  integer NOT NULL,
        name             text NOT NULL,
        name_norm        text NOT NULL,
        name_en          text,
        postal_street_id text,
        gaztir_street_code text,
        -- רשות האוכלוסין's official street code, where its file covers the
        -- street. This is the first REAL identifier streets have had here;
        -- everything else keys on a normalized name.
        official_code    integer,
        in_postal        boolean NOT NULL DEFAULT false,
        in_address_list  boolean NOT NULL DEFAULT false,
        in_gazetteer     boolean NOT NULL DEFAULT false,
        refreshed_at     timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(STREET_ALIASES_TABLE)} (
        settlement_code integer NOT NULL,
        variant         text NOT NULL,
        street_key      text NOT NULL,
        surface         text,
        kind            text,
        weight          integer NOT NULL DEFAULT 50,
        PRIMARY KEY (settlement_code, variant, street_key)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(STREET_UNMATCHED_TABLE)} (
        settlement_code integer,
        settlement_name text,
        source          text NOT NULL,
        raw_name        text NOT NULL,
        n_rows          integer,
        refreshed_at    timestamptz DEFAULT now(),
        PRIMARY KEY (source, settlement_code, raw_name)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(POSTAL_LOC_TABLE)} (
        post_location_id text PRIMARY KEY,
        post_name        text NOT NULL,
        settlement_code  integer,
        settlement_name  text,
        match_kind       text,
        n_rows           integer,
        refreshed_at     timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(ADDRESSES_TABLE)} (
        address_key     text PRIMARY KEY,
        settlement_code integer NOT NULL,
        settlement_name text,
        street_key      text,
        street_name     text,
        house_num       integer,
        house_suffix    text,
        entrance        text,
        house_raw       text,
        zip5            text,
        zip7            text,
        -- 'address'  → this doorway's own zip (street file, 91 localities)
        -- 'locality' → the ONE zip covering the whole locality. Kept distinct so
        --              a town-wide zip is never presented as the address's own.
        zip_level       text,
        neighbourhood   text,
        district        text,
        lat             double precision,
        lon             double precision,
        point           {_qi(PG_EXT_SCHEMA)}.geometry(Point, {GEOM_SRID}),
        parcel_key      text,
        -- 'pip'    → point-in-polygon, exact
        -- 'street' → inherited from the street's gazetteer parcels, approximate
        parcel_match    text,
        in_postal       boolean NOT NULL DEFAULT false,
        in_address_list boolean NOT NULL DEFAULT false,
        refreshed_at    timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(ZIP5_TABLE)} (
        zip5            text PRIMARY KEY,
        settlement_code integer,
        settlement_name text,
        n_addresses     integer,
        n_streets       integer,
        lat             double precision,
        lon             double precision,
        refreshed_at    timestamptz DEFAULT now()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS public.{_qi(STATE_TABLE)} (
        stage        text PRIMARY KEY,
        watermark    text,
        rows_in      bigint,
        rows_out     bigint,
        started_at   timestamptz,
        finished_at  timestamptz,
        duration_ms  bigint,
        status       text,
        note         text
    )
    """,
]

_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS {_qi(PARCELS_TABLE + '_gp_idx')} ON public.{_qi(PARCELS_TABLE)} (gp_key)",
    f"CREATE INDEX IF NOT EXISTS {_qi(PARCELS_TABLE + '_gush_idx')} ON public.{_qi(PARCELS_TABLE)} (gush, parcel)",
    f"CREATE INDEX IF NOT EXISTS {_qi(PARCELS_TABLE + '_sc_idx')} ON public.{_qi(PARCELS_TABLE)} (settlement_code)",
    f"CREATE INDEX IF NOT EXISTS {_qi(PARCELS_TABLE + '_centroid_gix')} ON public.{_qi(PARCELS_TABLE)} USING gist (centroid)",
    f"CREATE INDEX IF NOT EXISTS {_qi(GAZ_TABLE + '_gp_idx')} ON public.{_qi(GAZ_TABLE)} (gp_key)",
    f"CREATE INDEX IF NOT EXISTS {_qi(GAZ_TABLE + '_street_idx')} ON public.{_qi(GAZ_TABLE)} (street_key)",
    f"CREATE UNIQUE INDEX IF NOT EXISTS {_qi(STREETS_TABLE + '_sc_norm_uq')} ON public.{_qi(STREETS_TABLE)} (settlement_code, name_norm)",
    f"CREATE INDEX IF NOT EXISTS {_qi(STREET_ALIASES_TABLE + '_lookup_idx')} ON public.{_qi(STREET_ALIASES_TABLE)} (settlement_code, variant)",
    f"CREATE INDEX IF NOT EXISTS {_qi(ADDRESSES_TABLE + '_zip7_idx')} ON public.{_qi(ADDRESSES_TABLE)} (zip7)",
    f"CREATE INDEX IF NOT EXISTS {_qi(ADDRESSES_TABLE + '_zip5_idx')} ON public.{_qi(ADDRESSES_TABLE)} (zip5)",
    f"CREATE INDEX IF NOT EXISTS {_qi(ADDRESSES_TABLE + '_lookup_idx')} ON public.{_qi(ADDRESSES_TABLE)} (settlement_code, street_key, house_num)",
    f"CREATE INDEX IF NOT EXISTS {_qi(ADDRESSES_TABLE + '_parcel_idx')} ON public.{_qi(ADDRESSES_TABLE)} (parcel_key) WHERE parcel_key IS NOT NULL",
    f"CREATE INDEX IF NOT EXISTS {_qi(ADDRESSES_TABLE + '_point_gix')} ON public.{_qi(ADDRESSES_TABLE)} USING gist (point) WHERE point IS NOT NULL",
    f"CREATE INDEX IF NOT EXISTS {_qi(ZIP5_TABLE + '_sc_idx')} ON public.{_qi(ZIP5_TABLE)} (settlement_code)",
]


# CREATE TABLE IF NOT EXISTS never evolves a table that already exists, so a
# column added to _DDL after the table first shipped is silently absent in
# production — the build then dies on "column ... does not exist" (zip_level did
# exactly that). Every post-launch column therefore ALSO belongs here; Postgres
# has ADD COLUMN IF NOT EXISTS, so this stays idempotent.
_COLUMN_ADDITIONS = [
    (STREETS_TABLE, "official_code", "integer"),
    (ADDRESSES_TABLE, "zip_level", "text"),
]


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for stmt in _DDL:
            await conn.execute(stmt)
        for table, column, coltype in _COLUMN_ADDITIONS:
            await conn.execute(
                f"ALTER TABLE public.{_qi(table)} "
                f"ADD COLUMN IF NOT EXISTS {_qi(column)} {coltype}")
        for stmt in _INDEXES:
            await conn.execute(stmt, timeout=_LONG_TIMEOUT)
    await _grant_readonly()


async def _grant_readonly() -> None:
    """Re-grant SELECT to the public console's read-only role.

    Only needed for tables the role's provisioning script pre-dates; TRUNCATE
    keeps existing grants, which is exactly why the rebuild never DROPs."""
    from app.services.index_mirror import _readonly_role
    role = _readonly_role()
    if not role:
        return
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for t in ALL_TABLES:
            try:
                await conn.execute(f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")
            except Exception:  # noqa: BLE001 — a missing role must not fail a build
                logger.debug("nadlan: grant on %s failed", t, exc_info=True)


# ── source indexes ────────────────────────────────────────────────────────────
# None of the three odata tables carries a single index today, so every lookup is
# a full scan of up to 3.68M rows against a 10s timeout. These also speed up the
# /data console for everyone, not just this project.
_SOURCE_INDEXES = [
    (GAZTIR_SRC, "gaztir_gush_parcel_idx", '("GushNum", "ParcelNum")'),
    (GAZTIR_SRC, "gaztir_settlement_idx", '("SettlmentID")'),
    (GAZTIR_SRC, "gaztir_id_idx", '("ID")'),
    (POSTAL_SRC, "postal_zip7_idx", '("ZIP 7")'),
    (POSTAL_SRC, "postal_zip5_idx", '("ZIP 5")'),
    (POSTAL_SRC, "postal_loc_street_idx", '("Location Name", "Street Name")'),
    (ADDR_SRC, "addr_city_street_idx", "(city, street)"),
]


async def ensure_source_indexes() -> dict:
    """Index the three un-indexed odata source tables.

    Records a build_state row like every other stage. It used to be the ONE
    stage without one, so a run that reached it showed an EMPTY /state and looked
    like nothing was happening; and per-index failures — caught individually so
    one permission problem cannot abort the run — were visible only in the app
    log. Both now land in ``note``."""
    pool = await append_store.get_pool()
    made: list[str] = []
    failed: list[str] = []
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "source_indexes")
        for (schema, table), name, cols in _SOURCE_INDEXES:
            try:
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS {_qi(name)} ON {_qi(schema)}.{_qi(table)} {cols}",
                    timeout=_LONG_TIMEOUT)
                made.append(name)
            except Exception as e:  # noqa: BLE001
                failed.append(f"{name}: {e}")
                logger.warning("nadlan: source index %s failed: %s", name, e)
        for schema, table in (GAZTIR_SRC, POSTAL_SRC, ADDR_SRC):
            try:
                await conn.execute(f"ANALYZE {_qi(schema)}.{_qi(table)}", timeout=_LONG_TIMEOUT)
            except Exception:  # noqa: BLE001
                logger.debug("nadlan: analyze %s failed", table, exc_info=True)
        await _stage_done(conn, "source_indexes", t0, rows_out=len(made),
                          status="ok" if not failed else "partial",
                          note=f"created={len(made)}/{len(_SOURCE_INDEXES)}"
                               + (f" FAILED: {'; '.join(failed)[:400]}" if failed else ""))
    return {"indexes": made, "failed": failed}


# ── SQL cross-reference functions ─────────────────────────────────────────────
async def ensure_functions() -> None:
    """Expose the crosswalk to the /data console and the MCP as SQL functions.

    ``append_store.sql_helper_functions()`` reads ``over_%`` live out of
    ``pg_proc``, so everything created here shows up in every schema dump, the
    copy-to-AI button and ``describe_schema`` without any further wiring — which
    is the point: a model writing property SQL should never hand-roll a
    gush/helka string join."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_parcel_key(gush text, suffix text, parcel text)
            RETURNS text LANGUAGE sql IMMUTABLE AS $$
              SELECT CASE
                WHEN gush ~ '^[0-9]+(\\.0*)?$' AND parcel ~ '^[0-9]+(\\.0*)?$'
                THEN split_part(gush,'.',1)::int || '-' ||
                     coalesce(nullif(split_part(coalesce(suffix,'0'),'.',1),'')::int, 0) || '-' ||
                     split_part(parcel,'.',1)::int
              END
            $$;
            """)
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_parcel_key(gush text, parcel text)
            RETURNS text LANGUAGE sql IMMUTABLE AS $$
              SELECT public.over_parcel_key(gush, '0', parcel)
            $$;
            """)
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_house_num(q text)
            RETURNS integer LANGUAGE sql IMMUTABLE AS $$
              -- Mirrors nadlan_text.parse_house_number: leading digits win
              -- ('00043'→43), otherwise the LAST run does ('דוד אבידן 10'→10).
              SELECT CASE
                WHEN q ~ '^[0-9]' THEN (regexp_match(q, '^([0-9]+)'))[1]::int
                WHEN q ~ '[0-9]'  THEN (regexp_match(q, '([0-9]+)[^0-9]*$'))[1]::int
              END
            $$;
            """)
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_house_suffix(q text)
            RETURNS text LANGUAGE sql IMMUTABLE AS $$
              -- The letter glued to the house number ('12א' → 'א'), which makes
              -- 12א and 12ב distinct doorways rather than one merged address.
              SELECT CASE
                WHEN q ~ '^[0-9]' THEN nullif((regexp_match(q, '^[0-9]+\\s*([א-תA-Za-z])'))[1], '')
                WHEN q ~ '[0-9]'  THEN nullif((regexp_match(q, '[0-9]+\\s*([א-תA-Za-z])\\s*$'))[1], '')
              END
            $$;
            """)
        # NOTE ON PARAMETER NAMES: every parameter below is prefixed ``p_``.
        # In a LANGUAGE sql function an unqualified name that matches BOTH a
        # parameter and a column resolves to the COLUMN, so `WHERE p.gush = gush`
        # would silently be `gush = gush` — a tautology returning the whole
        # table. The prefix makes that collision impossible.
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_street_key(p_settlement_code integer, p_q text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT a.street_key
              FROM public.over_re_street_aliases a
              WHERE a.settlement_code = p_settlement_code
                AND a.variant = public.over_settlement_norm(p_q)
              ORDER BY a.weight DESC
              LIMIT 1
            $$;
            """)
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION public.over_street(p_settlement_code integer, p_q text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT s.name FROM public.over_re_streets s
              WHERE s.street_key = public.over_street_key(p_settlement_code, p_q)
            $$;
            """)
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.over_parcel_at(p_lat double precision,
                                                             p_lon double precision)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT public.over_parcel_key(p."GUSH_NUM", p."GUSH_SUFFI", p."PARCEL")
              FROM {_t(PARCELS_SRC)} p
              -- OPERATOR(extensions.&&), not a bare &&: PostGIS's operators
              -- live in the extensions schema and an OPERATOR cannot be
              -- qualified with dot notation the way a function can. The worker
              -- connection carries no search_path, so a bare && raises
              -- "operator does not exist: extensions.geometry && extensions.geometry".
              WHERE p.geom OPERATOR({_qi(PG_EXT_SCHEMA)}.&&) {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                                {_qi(PG_EXT_SCHEMA)}.ST_MakePoint(p_lon, p_lat), {GEOM_SRID})
                AND {_qi(PG_EXT_SCHEMA)}.ST_Contains(
                      p.geom,
                      {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                        {_qi(PG_EXT_SCHEMA)}.ST_MakePoint(p_lon, p_lat), {GEOM_SRID}))
              LIMIT 1
            $$;
            """)
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.over_parcels_near(
                p_lat double precision, p_lon double precision, p_radius_m double precision)
            RETURNS SETOF public.{_qi(PARCELS_TABLE)} LANGUAGE sql STABLE AS $$
              SELECT p.* FROM public.{_qi(PARCELS_TABLE)} p
              WHERE {_qi(PG_EXT_SCHEMA)}.ST_DWithin(
                      p.centroid::{_qi(PG_EXT_SCHEMA)}.geography,
                      {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                        {_qi(PG_EXT_SCHEMA)}.ST_MakePoint(p_lon, p_lat),
                        {GEOM_SRID})::{_qi(PG_EXT_SCHEMA)}.geography,
                      p_radius_m)
            $$;
            """)
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.over_gush_helka(p_gush integer, p_helka integer)
            RETURNS SETOF public.{_qi(PARCELS_TABLE)} LANGUAGE sql STABLE AS $$
              SELECT p.* FROM public.{_qi(PARCELS_TABLE)} p
              WHERE p.gush = p_gush AND p.parcel = p_helka
            $$;
            """)
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.over_zip(p_city text, p_street text, p_house text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT a.zip7 FROM public.{_qi(ADDRESSES_TABLE)} a
              WHERE a.settlement_code = public.over_settlement_code(p_city)
                AND a.street_key = public.over_street_key(
                      public.over_settlement_code(p_city), p_street)
                AND a.house_num = public.over_house_num(p_house)
                AND a.zip7 IS NOT NULL
              LIMIT 1
            $$;
            """)
        await conn.execute(
            f"""
            CREATE OR REPLACE FUNCTION public.over_address_parcel(p_city text, p_street text,
                                                                  p_house text)
            RETURNS text LANGUAGE sql STABLE AS $$
              SELECT a.parcel_key FROM public.{_qi(ADDRESSES_TABLE)} a
              WHERE a.settlement_code = public.over_settlement_code(p_city)
                AND a.street_key = public.over_street_key(
                      public.over_settlement_code(p_city), p_street)
                AND a.house_num = public.over_house_num(p_house)
                AND a.parcel_key IS NOT NULL
              ORDER BY (a.parcel_match = 'pip') DESC
              LIMIT 1
            $$;
            """)


# ── build-state bookkeeping ───────────────────────────────────────────────────
async def _stage_start(conn, stage: str) -> float:
    await conn.execute(
        f"""INSERT INTO public.{_qi(STATE_TABLE)} (stage, started_at, status)
            VALUES ($1, now(), 'running')
            ON CONFLICT (stage) DO UPDATE SET started_at = now(), status = 'running',
                                              finished_at = NULL, note = NULL""",
        stage)
    return time.monotonic()


async def _stage_done(conn, stage: str, t0: float, *, rows_in=None, rows_out=None,
                      watermark=None, status="ok", note=None) -> None:
    await conn.execute(
        f"""UPDATE public.{_qi(STATE_TABLE)}
            SET finished_at = now(), duration_ms = $2, rows_in = $3, rows_out = $4,
                watermark = coalesce($5, watermark), status = $6, note = $7
            WHERE stage = $1""",
        stage, int((time.monotonic() - t0) * 1000), rows_in, rows_out,
        watermark, status, note)


async def get_state() -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT * FROM public.{_qi(STATE_TABLE)} ORDER BY stage")
    return [dict(r) for r in rows]


# ── stage 1: the parcel spine ─────────────────────────────────────────────────
async def build_parcels() -> dict:
    """One server-side pass over the 4.58 GB parcels table.

    ``ST_PointOnSurface`` rather than ``ST_Centroid``: a concave or L-shaped
    parcel's centroid can fall OUTSIDE it, which would quietly make "the point of
    this property" wrong. Same cost, guaranteed inside."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "parcels")
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(PARCELS_TABLE)}")
            await conn.execute(
                f"""
                INSERT INTO public.{_qi(PARCELS_TABLE)}
                  (parcel_key, gush, gush_suffix, parcel, gp_key, settlement_code,
                   locality_name, reg_mun_name, county_name, region_name,
                   legal_area, status_text, lat, lon, centroid, refreshed_at)
                SELECT DISTINCT ON (k.parcel_key)
                       k.parcel_key, k.gush, k.sfx, k.parcel,
                       k.gush || '-' || k.parcel,
                       -- LOCALITY_I is the CBS code (spot-checked: 664 כרם בן זמרה,
                       -- 2063 דישון); fall back to resolving the NAME when the
                       -- code is blank or unknown to the settlement index.
                       coalesce(s.code, public.over_settlement_code(k.locality_name)),
                       k.locality_name, k.reg_mun_name, k.county_name, k.region_name,
                       k.legal_area, k.status_text,
                       {_qi(PG_EXT_SCHEMA)}.ST_Y(k.pt), {_qi(PG_EXT_SCHEMA)}.ST_X(k.pt),
                       k.pt, now()
                FROM (
                  SELECT split_part(p."GUSH_NUM",'.',1)::int AS gush,
                         coalesce(nullif(split_part(coalesce(p."GUSH_SUFFI",'0'),'.',1),'')::int, 0) AS sfx,
                         split_part(p."PARCEL",'.',1)::int AS parcel,
                         split_part(p."GUSH_NUM",'.',1)::int || '-' ||
                           coalesce(nullif(split_part(coalesce(p."GUSH_SUFFI",'0'),'.',1),'')::int, 0) || '-' ||
                           split_part(p."PARCEL",'.',1)::int AS parcel_key,
                         nullif(p."LOCALITY_I",'') AS locality_code,
                         p."LOCALITY_N" AS locality_name,
                         p."REG_MUN_NA" AS reg_mun_name,
                         p."COUNTY_NAM" AS county_name,
                         p."REGION_NAM" AS region_name,
                         nullif(p."LEGAL_AREA",'')::double precision AS legal_area,
                         p."STATUS_TEX" AS status_text,
                         p."SYS_DATE" AS sys_date,
                         {_qi(PG_EXT_SCHEMA)}.ST_PointOnSurface(p.geom) AS pt
                  FROM {_t(PARCELS_SRC)} p
                  WHERE p.geom IS NOT NULL
                    AND p."GUSH_NUM" ~ '^[0-9]+(\\.0*)?$'
                    AND p."PARCEL"   ~ '^[0-9]+(\\.0*)?$'
                ) k
                LEFT JOIN public.over_settlements s
                       ON k.locality_code ~ '^[0-9]+$' AND s.code = k.locality_code::int
                -- The append table keeps every version's rows; newest wins.
                ORDER BY k.parcel_key, k.sys_date DESC NULLS LAST
                """,
                timeout=_LONG_TIMEOUT)
            # Flag the gush/parcel pairs the gazetteer cannot disambiguate,
            # because it publishes no gush suffix.
            await conn.execute(
                f"""
                UPDATE public.{_qi(PARCELS_TABLE)} p SET gp_ambiguous = true
                FROM (SELECT gp_key FROM public.{_qi(PARCELS_TABLE)}
                      GROUP BY gp_key HAVING count(*) > 1) d
                WHERE p.gp_key = d.gp_key
                """, timeout=_LONG_TIMEOUT)
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(PARCELS_TABLE)}")
            amb = await conn.fetchval(
                f"SELECT count(*) FROM public.{_qi(PARCELS_TABLE)} WHERE gp_ambiguous")
            await _stage_done(conn, "parcels", t0, rows_out=n,
                              note=f"gp_ambiguous={amb}")
        await conn.execute(f"ANALYZE public.{_qi(PARCELS_TABLE)}", timeout=_LONG_TIMEOUT)
    return {"parcels": n, "gp_ambiguous": amb}


# ── stage 2: the gazetteer, rolled up to parcel grain ─────────────────────────
async def build_gazetteer() -> dict:
    """Fold 3.68M gazetteer assets onto the parcel spine.

    Only the aggregates land here (counts, floors, years) — the per-asset detail
    stays in the source table, reachable through the API's /data deep-link. The
    join is on ``gp_key`` because the gazetteer has no gush suffix; where that is
    ambiguous the spine already says so."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "gazetteer")
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(GAZ_TABLE)}")
            await conn.execute(
                f"""
                INSERT INTO public.{_qi(GAZ_TABLE)}
                  (parcel_key, gp_key, settlement_code, settlement_name,
                   street_name_src, street_code, n_assets, n_dwellings, n_subparcels,
                   floors_max, building_year_min, building_year_max, apartments_est,
                   refreshed_at)
                SELECT p.parcel_key, g.gp_key, g.sc, g.sname, g.sname_street, g.scode,
                       g.n_assets, g.n_dwellings, g.n_sub, g.floors_max,
                       g.year_min, g.year_max, g.apartments, now()
                FROM (
                  SELECT split_part("GushNum",'.',1)::int || '-' ||
                         split_part("ParcelNum",'.',1)::int AS gp_key,
                         min(nullif("SettlmentID",'')::int) AS sc,
                         min("SettlementNameHeb") AS sname,
                         min("StreetNameHeb") FILTER (WHERE "StreetNameHeb" <> '') AS sname_street,
                         min("StreetCode") FILTER (WHERE "StreetCode" <> '') AS scode,
                         count(*)::int AS n_assets,
                         count(*) FILTER (WHERE "Type" LIKE 'דירת%%')::int AS n_dwellings,
                         count(DISTINCT nullif("SubParcelNum",''))::int AS n_sub,
                         max(nullif("BuildingFloors",'')::int) AS floors_max,
                         min(nullif("BuildingYear",'')::int) AS year_min,
                         max(nullif("BuildingYear",'')::int) AS year_max,
                         sum(nullif("EstimatedAppartments",'')::int)::int AS apartments
                  FROM {_t(GAZTIR_SRC)}
                  WHERE "GushNum" ~ '^[0-9]+(\\.0*)?$' AND "ParcelNum" ~ '^[0-9]+(\\.0*)?$'
                    AND "SettlmentID" ~ '^[0-9]+$'
                  GROUP BY 1
                ) g
                JOIN public.{_qi(PARCELS_TABLE)} p ON p.gp_key = g.gp_key
                ON CONFLICT (parcel_key) DO NOTHING
                """,
                timeout=_LONG_TIMEOUT)
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(GAZ_TABLE)}")
            await _stage_done(conn, "gazetteer", t0, rows_out=n)
        await conn.execute(f"ANALYZE public.{_qi(GAZ_TABLE)}", timeout=_LONG_TIMEOUT)
    return {"gazetteer_parcels": n}


# ── stage 3: postal localities (the foreign codespace) ────────────────────────
async def build_postal_localities() -> dict:
    """Resolve Israel Post's 91 locality ids to CBS codes, ONCE, by name.

    ``LocationID`` is Israel Post's own codespace — verified: none of its
    (code, name) pairs matches a CBS (code, name) pair. Every downstream join
    therefore uses the CBS code from this dimension table and never the raw id."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "postal_localities")
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(POSTAL_LOC_TABLE)}")
            await conn.execute(
                f"""
                INSERT INTO public.{_qi(POSTAL_LOC_TABLE)}
                  (post_location_id, post_name, settlement_code, settlement_name,
                   match_kind, n_rows, refreshed_at)
                SELECT l.lid, l.lname,
                       coalesce(x.sc, public.over_settlement_code(l.lname)),
                       coalesce(public.over_settlement(l.lname), s.name),
                       CASE WHEN x.sc IS NOT NULL THEN 'symbol'
                            WHEN public.over_settlement_code(l.lname) IS NOT NULL THEN 'name'
                            ELSE 'unresolved' END,
                       l.n, now()
                FROM (SELECT "LocationID" AS lid, min("Location Name") AS lname, count(*)::int AS n
                      FROM {_t(POSTAL_SRC)} GROUP BY 1) l
                -- The locality file carries BOTH Israel Post's id and the CBS
                -- code ("Location Symbol"), i.e. the authoritative crosswalk.
                -- Prefer it; fall back to resolving the name, which is all the
                -- street file alone could offer.
                LEFT JOIN (
                  SELECT btrim("Location ID") AS lid,
                         max(nullif(btrim("Location Symbol"),'')::int) AS sc
                  FROM {_t(POSTAL_LOCALITY_SRC)}
                  WHERE btrim("Location Symbol") ~ '^[0-9]+$'
                    AND nullif(btrim("Location Symbol"),'')::int > 0
                  GROUP BY 1
                ) x ON x.lid = btrim(l.lid)
                LEFT JOIN public.over_settlements s ON s.code = x.sc
                """, timeout=_LONG_TIMEOUT)
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(POSTAL_LOC_TABLE)}")
            bad = await conn.fetchval(
                f"SELECT count(*) FROM public.{_qi(POSTAL_LOC_TABLE)} WHERE settlement_code IS NULL")
            await _stage_done(conn, "postal_localities", t0, rows_out=n,
                              note=f"unresolved={bad}")
    return {"postal_localities": n, "unresolved": bad}


# ── stage 4: the street index ─────────────────────────────────────────────────
# רשות האוכלוסין's street list WITH SYNONYMS (data.gov.il `israel-streets-synom`,
# resource bf185c7f…, 152,130 rows over 1,312 localities keyed by CBS code).
# Discovered by COLUMN SIGNATURE rather than by table name, because the resource
# can arrive either as a tracked CKAN dataset (public.append_*) or through the
# admin odata import (odata.*), and the physical name differs in each case.
# Absent → build_streets() just falls back to the heuristic ladder.
_OFFICIAL_STREET_COLS = {"city_code", "street_code", "street_name",
                         "street_name_status", "official_code"}


async def find_official_streets_table() -> tuple[str, str] | None:
    for schema in ("public", "odata", "idx"):
        try:
            cols_by_table = await append_store.schema_table_columns(schema)
        except Exception:  # noqa: BLE001
            continue
        for table, cols in cols_by_table.items():
            names = {c["name"] if isinstance(c, dict) else c for c in cols}
            if _OFFICIAL_STREET_COLS <= names:
                logger.info("nadlan: official street file found at %s.%s", schema, table)
                return schema, table
    return None


async def _load_official_streets() -> list[dict]:
    """Read the official street file, if it is tracked. Trailing spaces are
    stripped — every value in that file carries them ('official ', 'ירושלים ')."""
    loc = await find_official_streets_table()
    if not loc:
        return []
    schema, table = loc
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT nullif(btrim("city_code"::text),'')::int  AS sc,
                   btrim("street_name"::text)                AS name,
                   nullif(btrim("official_code"::text),'')::int AS official_code,
                   btrim("street_name_status"::text)         AS status
            FROM {_qi(schema)}.{_qi(table)}
            WHERE btrim("city_code"::text) ~ '^[0-9]+$'
              AND btrim("official_code"::text) ~ '^[0-9]+$'
              AND btrim("street_name"::text) <> ''
            """, timeout=_LONG_TIMEOUT)
    logger.info("nadlan: loaded %d official street rows from %s.%s", len(rows), schema, table)
    return [dict(r) for r in rows]


async def build_streets() -> dict:
    """Build the street reference + its alias index.

    The measured seam is narrow and directed: the address list and the postal
    file agree on street spelling 100% of the time, while the gazetteer agrees
    with them only 56–64%. So the CANONICAL spelling is taken from the
    postal/address side (it is also what a user types), and the gazetteer's
    spellings become weighted aliases.

    The whole street universe is ~93k rows, so the matcher runs in Python where
    it is unit-testable — no pg_trgm dependency, and the ambiguity rule below is
    expressible. ``last_token`` aliases ('שמואל יבניאלי' → 'יבניאלי') are the
    lever that closes the gap; any of them that is ambiguous WITHIN its
    settlement is dropped rather than allowed to mis-link
    ('דוד המלך' and 'שלמה המלך' both end in 'המלך')."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "streets")

        # Canonical side: address list ∪ postal, both resolved to a CBS code.
        # NOTE the GROUP BY comes BEFORE over_settlement_code(): resolving per
        # row would call the alias resolver 548k times instead of ~33k.
        canon = await conn.fetch(
            f"""
            WITH addr AS (
              SELECT city, street AS name, count(*)::int AS n
              FROM {_t(ADDR_SRC)} WHERE street <> '' GROUP BY 1,2
            ), post AS (
              SELECT p."LocationID" AS lid, p."Street Name" AS name,
                     count(*)::int AS n, min(p."StreetID") AS street_id
              FROM {_t(POSTAL_SRC)} p WHERE p."Street Name" <> '' GROUP BY 1,2
            )
            SELECT sc, name, max(n) AS n,
                   bool_or(src='addr') AS in_addr, bool_or(src='post') AS in_post,
                   max(street_id) AS street_id
            FROM (
              SELECT public.over_settlement_code(city) AS sc, name, n,
                     'addr' AS src, NULL::text AS street_id
              FROM addr
              UNION ALL
              SELECT pl.settlement_code, post.name, post.n, 'post', post.street_id
              FROM post
              JOIN public.{_qi(POSTAL_LOC_TABLE)} pl ON pl.post_location_id = post.lid
            ) u
            WHERE sc IS NOT NULL
            GROUP BY sc, name
            """, timeout=_LONG_TIMEOUT)

        gaz = await conn.fetch(
            f"""
            SELECT nullif("SettlmentID",'')::int AS sc,
                   "StreetNameHeb" AS name,
                   min("StreetCode") AS code,
                   min("StreetNameEng") AS name_en,
                   min("SettlementNameHeb") AS sname,
                   count(*)::int AS n
            FROM {_t(GAZTIR_SRC)}
            WHERE "StreetNameHeb" <> '' AND "SettlmentID" ~ '^[0-9]+$'
            GROUP BY 1,2
            """, timeout=_LONG_TIMEOUT)

    official = await _load_official_streets()
    streets, aliases, unmatched = _resolve_streets(canon, gaz, official)

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(STREETS_TABLE)}")
            await conn.execute(f"TRUNCATE public.{_qi(STREET_ALIASES_TABLE)}")
            await conn.execute(f"TRUNCATE public.{_qi(STREET_UNMATCHED_TABLE)}")
            await conn.executemany(
                f"""INSERT INTO public.{_qi(STREETS_TABLE)}
                    (street_key, settlement_code, name, name_norm, name_en,
                     postal_street_id, gaztir_street_code, in_postal,
                     in_address_list, in_gazetteer, official_code)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                    ON CONFLICT (street_key) DO NOTHING""", streets)
            await conn.executemany(
                f"""INSERT INTO public.{_qi(STREET_ALIASES_TABLE)}
                    (settlement_code, variant, street_key, surface, kind, weight)
                    VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (settlement_code, variant, street_key) DO UPDATE
                      SET weight = GREATEST(public.over_re_street_aliases.weight, EXCLUDED.weight)""",
                aliases)
            await conn.executemany(
                f"""INSERT INTO public.{_qi(STREET_UNMATCHED_TABLE)}
                    (settlement_code, settlement_name, source, raw_name, n_rows)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (source, settlement_code, raw_name) DO NOTHING""",
                unmatched)
            n_gaz = len(gaz)
            matched = n_gaz - len(unmatched)
            await _stage_done(
                conn, "streets", t0, rows_in=len(canon) + n_gaz, rows_out=len(streets),
                note=(f"aliases={len(aliases)} gazetteer_matched={matched}/{n_gaz} "
                      f"official_rows={len(official)}"))
        await conn.execute(f"ANALYZE public.{_qi(STREETS_TABLE)}")
        await conn.execute(f"ANALYZE public.{_qi(STREET_ALIASES_TABLE)}")
    return {"streets": len(streets), "aliases": len(aliases),
            "gazetteer_streets": n_gaz, "gazetteer_matched": matched,
            "unmatched": len(unmatched), "official_rows": len(official)}


def _official_code_index(official_rows) -> dict[tuple[int, str], int]:
    """(settlement_code, normalized name) → official street code.

    Built from רשות האוכלוסין's street file, which publishes every street once as
    ``official`` and again under each ``synonym of <code>`` spelling. A name that
    maps to more than one official code INSIDE its settlement is dropped: the
    source itself is ambiguous there, and guessing would be worse than falling
    back to the heuristic ladder."""
    multi: dict[tuple[int, str], set] = {}
    for r in official_rows:
        sc, name = r.get("sc"), (r.get("name") or "").strip()
        code = r.get("official_code")
        key = nadlan_text.norm(name)
        if sc is None or code is None or not key:
            continue
        multi.setdefault((sc, key), set()).add(int(code))
    return {k: next(iter(v)) for k, v in multi.items() if len(v) == 1}


def _resolve_streets(canon_rows, gaz_rows, official_rows=()) -> tuple[list, list, list]:
    """Pure matcher: canonical + gazetteer (+ the official street file) → rows.

    Split out of ``build_streets`` so the matching rules can be tested with plain
    dicts and no database.

    ``official_rows`` is רשות האוכלוסין's street list *with synonyms*
    (data.gov.il ``israel-streets-synom``): 152,130 rows over 1,312 localities,
    keyed by the CBS locality code, publishing each street once as ``official``
    and again under every synonym spelling — including both name orders
    (``יוסף ספיר`` / ``ספיר יוסף``), the type-prefixed form (``דרך ספיר``) and the
    bare surname (``ספיר``). That is the whole heuristic ladder, as published
    fact rather than inference.

    It does NOT replace the ladder, because the two fail on different names.
    Measured over the WHOLE corpus (30,995 canonical, 34,734 gazetteer, 152,130
    official rows) — not on a couple of big cities, where every source is at its
    richest and the rate flatters itself into the 90s:

    ========================  =============  ==========================
    matching gazetteer        all localities  where canon files reach
    ========================  =============  ==========================
    raw name                     61.1%        —
    heuristic ladder             72.7%        79.4%
    **ladder + official file**   **75.6%**    **82.6%**
    ========================  =============  ==========================

    The residue is COVERAGE, not naming: 2,938 gazetteer streets (8.5%) are in
    localities the address/postal files do not cover at all, so they cannot match
    by construction — and 99.7% of those ARE in the official file's 1,312
    localities, which is exactly what it adds beyond the match rate.

    So the file is layered ON TOP: its codes merge canonical spellings the ladder
    kept apart (39,825 → 37,815 streets country-wide) and give 35,168 of them a
    real identifier, and its synonyms become aliases outranking every heuristic
    guess."""
    code_of = _official_code_index(official_rows)

    # 1. canonical streets. Grouped by the OFFICIAL CODE where the street file
    #    knows one — that is what merges 'רוטשילד' and 'שדרות רוטשילד' into a
    #    single street even when the ladder would have kept them apart — and by
    #    the normalized name everywhere else.
    streets: dict[tuple[int, str], dict] = {}
    for r in canon_rows:
        sc, name = r["sc"], (r["name"] or "").strip()
        key_norm = nadlan_text.norm(name)
        if sc is None or not key_norm:
            continue
        code = code_of.get((sc, key_norm))
        k = (sc, f"c{code}" if code is not None else key_norm)
        cur = streets.get(k)
        if cur is None or (r["n"] or 0) > cur["n"]:
            keep = {"sc": sc, "name": name, "norm": key_norm, "n": r["n"] or 0,
                    "postal_id": r["street_id"], "gaz_code": None, "name_en": None,
                    "official_code": code,
                    "in_post": bool(r["in_post"]), "in_addr": bool(r["in_addr"]),
                    "in_gaz": False}
            if cur is not None:      # keep what the losing spelling contributed
                keep["in_post"] = keep["in_post"] or cur["in_post"]
                keep["in_addr"] = keep["in_addr"] or cur["in_addr"]
                keep["postal_id"] = keep["postal_id"] or cur["postal_id"]
            streets[k] = keep
        else:
            cur["in_post"] = cur["in_post"] or bool(r["in_post"])
            cur["in_addr"] = cur["in_addr"] or bool(r["in_addr"])
            cur["postal_id"] = cur["postal_id"] or r["street_id"]

    # street_key stays `{sc}-{norm of the representative spelling}` so its shape
    # does not depend on whether the official file happened to cover the street.
    # Two groups CAN land on the same representative norm; the second one keeps
    # its code in the key rather than silently colliding with the first.
    taken: dict[str, tuple] = {}

    def skey(sc: int, gkey: str) -> str:
        s = streets[(sc, gkey)]
        base = f"{sc}-{s['norm']}"
        owner = taken.setdefault(base, (sc, gkey))
        if owner == (sc, gkey):
            return base
        return f"{base}#c{s['official_code']}"

    # Resolve every key once, up front, so the mapping is stable.
    keys: dict[tuple[int, str], str] = {k: skey(*k) for k in streets}

    # 2. alias candidates from the canonical names
    alias: dict[tuple[int, str], tuple[str, str, str, int]] = {}

    def offer(sc: int, variant: str, street_key: str, surface: str, kind: str, weight: int):
        k = (sc, variant)
        cur = alias.get(k)
        if cur is None:
            alias[k] = (street_key, surface, kind, weight)
        elif cur[0] != street_key and weight >= cur[3]:
            # Ambiguous inside the settlement: a low-confidence variant must never
            # pick a winner. Mark it poisoned so neither street claims it.
            alias[k] = (None, surface, "ambiguous", weight)
        elif cur[0] == street_key and weight > cur[3]:
            alias[k] = (street_key, surface, kind, weight)

    for k, s in streets.items():
        for variant, surface, kind, weight in nadlan_text.street_aliases_for(s["name"]):
            offer(k[0], variant, keys[k], surface, kind, weight)

    # 2b. The official file's own spellings, outranking every heuristic guess.
    #     An `official` name is as trustworthy as an exact match (100); a
    #     published `synonym of <code>` sits at 92 — above no_paren/no_type and
    #     well above last_token, but still below an exact name hit.
    by_code: dict[tuple[int, int], str] = {
        (k[0], s["official_code"]): keys[k]
        for k, s in streets.items() if s.get("official_code") is not None
    }
    for r in official_rows:
        sc, name = r.get("sc"), (r.get("name") or "").strip()
        code = r.get("official_code")
        variant = nadlan_text.norm(name)
        if sc is None or code is None or not variant:
            continue
        sk = by_code.get((sc, int(code)))
        if not sk:
            continue                     # no canonical street carries this code
        is_official = str(r.get("status") or "").strip().lower().startswith("official")
        offer(sc, variant, sk, name,
              "official_file" if is_official else "official_synonym",
              100 if is_official else 92)

    # Reverse index street_key → the street dict. Without it, marking a matched
    # street would rescan the whole 93k-row table per gazetteer row.
    by_key: dict[str, dict] = {keys[k]: s for k, s in streets.items()}

    # 3. gazetteer names → an existing canonical street, or a new one
    unmatched: list[tuple] = []
    for r in gaz_rows:
        sc, name = r["sc"], (r["name"] or "").strip()
        nm = nadlan_text.norm(name)
        if sc is None or not nm:
            continue
        hit = None
        # Strongest variant first, so a formal-name hit always beats last_token.
        for variant, _surface, _kind, _w in sorted(
                nadlan_text.street_aliases_for(name), key=lambda a: -a[3]):
            cand = alias.get((sc, variant))
            if cand and cand[0]:
                hit = cand[0]
                break
        if hit is None:
            # No counterpart in the postal/address universe — the gazetteer
            # street becomes canonical itself (it is a real street; the other two
            # files simply do not list it). It still gets an official code when
            # the street file knows one, so a later run can merge it.
            code = code_of.get((sc, nm))
            k = (sc, f"c{code}" if code is not None else nm)
            if k not in streets:
                s = {"sc": sc, "name": name, "norm": nm, "n": r["n"] or 0,
                     "postal_id": None, "gaz_code": r["code"],
                     "name_en": r["name_en"], "official_code": code,
                     "in_post": False, "in_addr": False, "in_gaz": True}
                streets[k] = s
                keys[k] = skey(*k)
                by_key[keys[k]] = s
                for variant, surface, kind, weight in nadlan_text.street_aliases_for(name):
                    offer(sc, variant, keys[k], surface, kind, weight)
            unmatched.append((sc, r["sname"], "gazetteer", name, r["n"] or 0))
        else:
            s = by_key.get(hit)
            if s is not None:
                s["in_gaz"] = True
                s["gaz_code"] = s["gaz_code"] or r["code"]
                s["name_en"] = s["name_en"] or r["name_en"]
            # The gazetteer's own spelling becomes an alias of the canonical one.
            offer(sc, nm, hit, name, "gazetteer", 95)

    street_rows = [
        (keys[k], k[0], s["name"], s["norm"], s["name_en"], s["postal_id"],
         s["gaz_code"], s["in_post"], s["in_addr"], s["in_gaz"],
         s.get("official_code"))
        for k, s in streets.items()
    ]
    alias_rows = [
        (sc, variant, sk, surface, kind, weight)
        for (sc, variant), (sk, surface, kind, weight) in alias.items() if sk
    ]
    return street_rows, alias_rows, unmatched


# ── stage 5: the address spine ────────────────────────────────────────────────
async def build_addresses() -> dict:
    """Merge the address list and the postal file into ONE address per doorway.

    Keyed ``settlement_code|street_key|house|entrance``. The address list
    contributes the point (its ITM X/Y, 70% populated); the postal file
    contributes the zip (91 localities only). Rows present in both become one row
    carrying both, which is the entire purpose of the spine."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "addresses")
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(ADDRESSES_TABLE)}")
            await conn.execute(
                f"""
                -- The (locality, street) universe is ~33k pairs while the address
                -- rows are ~1.06M, so both resolvers run ONCE per pair here and
                -- the big scan just joins. Resolving inline cost a million
                -- alias-index lookups.
                WITH pair AS (
                  SELECT DISTINCT city, street FROM {_t(ADDR_SRC)} WHERE street <> ''
                ), pair_keyed AS (
                  SELECT city, street,
                         public.over_settlement_code(city) AS sc,
                         public.over_street_key(public.over_settlement_code(city), street) AS street_key
                  FROM pair
                ), post_pair AS (
                  SELECT DISTINCT p."LocationID" AS lid, p."Street Name" AS street
                  FROM {_t(POSTAL_SRC)} p WHERE p."Street Name" <> ''
                ), post_pair_keyed AS (
                  SELECT pp.lid, pp.street, pl.settlement_code AS sc,
                         public.over_street_key(pl.settlement_code, pp.street) AS street_key
                  FROM post_pair pp
                  JOIN public.{_qi(POSTAL_LOC_TABLE)} pl ON pl.post_location_id = pp.lid
                ), src AS (
                  SELECT pk.sc, a.street AS street_raw, pk.street_key,
                         public.over_house_num(a.number) AS house_num,
                         public.over_house_suffix(a.number) AS house_suffix,
                         NULL::text AS entrance,
                         a.number AS house_raw,
                         NULL::text AS zip5, NULL::text AS zip7,
                         nullif(a.neighbourhood,'') AS hood,
                         nullif(a.district,'') AS district,
                         CASE WHEN a."X" ~ '^[0-9]' AND a."X" <> '0'
                                   AND a."Y" ~ '^[0-9]' AND a."Y" <> '0'
                              THEN {_qi(PG_EXT_SCHEMA)}.ST_Transform(
                                     {_qi(PG_EXT_SCHEMA)}.ST_SetSRID(
                                       {_qi(PG_EXT_SCHEMA)}.ST_MakePoint(a."X"::double precision,
                                                                         a."Y"::double precision),
                                       {ITM_SRID}), {GEOM_SRID})
                         END AS pt,
                         'addr'::text AS src
                  FROM {_t(ADDR_SRC)} a
                  JOIN pair_keyed pk ON pk.city = a.city AND pk.street = a.street
                  WHERE a.street <> '' AND pk.sc IS NOT NULL
                  UNION ALL
                  SELECT ppk.sc, p."Street Name", ppk.street_key,
                         public.over_house_num(p."House Number"),
                         public.over_house_suffix(p."House Number"),
                         nullif(p."Entrance",''), p."House Number",
                         nullif(p."ZIP 5",''), nullif(p."ZIP 7",''),
                         NULL, NULL, NULL, 'post'
                  FROM {_t(POSTAL_SRC)} p
                  JOIN post_pair_keyed ppk
                       ON ppk.lid = p."LocationID" AND ppk.street = p."Street Name"
                  WHERE p."Street Name" <> '' AND ppk.sc IS NOT NULL
                ), keyed AS (
                  SELECT src.*,
                         -- Both files use '?' as "street unknown" (43,004 postal
                         -- rows and 7,055 address rows). Those must NOT collapse
                         -- into one bucket per house number: אבן יהודה '?' 1
                         -- carries THREE different ZIP7s, i.e. three real
                         -- addresses. So an unresolvable street keys on whatever
                         -- actually distinguishes the row — its zip, else its
                         -- coordinate, else the raw text.
                         coalesce(
                           src.street_key,
                           CASE
                             WHEN src.zip7 IS NOT NULL THEN 'zip:' || src.zip7
                             WHEN src.pt IS NOT NULL THEN
                               'xy:' || round({_qi(PG_EXT_SCHEMA)}.ST_Y(src.pt)::numeric, 6)
                                     || ',' || round({_qi(PG_EXT_SCHEMA)}.ST_X(src.pt)::numeric, 6)
                             ELSE 'raw:' || src.street_raw
                           END) AS key_part
                  FROM src
                )
                INSERT INTO public.{_qi(ADDRESSES_TABLE)}
                  (address_key, settlement_code, settlement_name, street_key, street_name,
                   house_num, house_suffix, entrance, house_raw, zip5, zip7, zip_level,
                   neighbourhood, district,
                   lat, lon, point, in_postal, in_address_list, refreshed_at)
                SELECT k.sc || '|' || k.key_part || '|' ||
                         coalesce(k.house_num::text,'?') || coalesce(k.house_suffix,'') ||
                         '|' || coalesce(k.entrance,''),
                       k.sc, s.name, k.street_key,
                       -- '?' is the sources' "unknown street" sentinel; surface
                       -- it as NULL rather than showing a literal question mark.
                       coalesce(st.name, nullif(min(k.street_raw), '?')),
                       k.house_num, k.house_suffix, k.entrance, min(k.house_raw),
                       max(k.zip5), max(k.zip7),
                       CASE WHEN max(k.zip7) IS NOT NULL THEN 'address' END,
                       max(k.hood), max(k.district),
                       {_qi(PG_EXT_SCHEMA)}.ST_Y((array_agg(k.pt) FILTER (WHERE k.pt IS NOT NULL))[1]),
                       {_qi(PG_EXT_SCHEMA)}.ST_X((array_agg(k.pt) FILTER (WHERE k.pt IS NOT NULL))[1]),
                       (array_agg(k.pt) FILTER (WHERE k.pt IS NOT NULL))[1],
                       bool_or(k.src='post'), bool_or(k.src='addr'), now()
                FROM keyed k
                LEFT JOIN public.over_settlements s ON s.code = k.sc
                LEFT JOIN public.{_qi(STREETS_TABLE)} st ON st.street_key = k.street_key
                GROUP BY k.sc, k.key_part, k.street_key, k.house_num, k.house_suffix,
                         k.entrance, s.name, st.name
                ON CONFLICT (address_key) DO NOTHING
                """, timeout=_LONG_TIMEOUT)
            # Locality-level zips for everywhere the street file does not reach.
            # Only 91 localities get a zip per street+house; the other ~1,000 have
            # ONE zip for the whole place, published in a separate file of the
            # same dataset. Marked zip_level='locality' so a town-wide zip is
            # never mistaken for the doorway's own.
            await conn.execute(
                f"""
                WITH loc AS (
                  SELECT nullif(btrim("Location Symbol"),'')::int AS sc,
                         max(nullif(btrim("ZIP 7"),'')) AS zip7,
                         max(nullif(btrim("ZIP5"),''))  AS zip5
                  FROM {_t(POSTAL_LOCALITY_SRC)}
                  WHERE btrim("Location Symbol") ~ '^[0-9]+$'
                    AND nullif(btrim("Location Symbol"),'')::int > 0
                    AND nullif(btrim("ZIP 7"),'') IS NOT NULL
                  GROUP BY 1
                )
                UPDATE public.{_qi(ADDRESSES_TABLE)} a
                SET zip7 = loc.zip7, zip5 = coalesce(a.zip5, loc.zip5),
                    zip_level = 'locality'
                FROM loc
                WHERE a.settlement_code = loc.sc AND a.zip7 IS NULL
                """, timeout=_LONG_TIMEOUT)
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}")
            with_pt = await conn.fetchval(
                f"SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} WHERE point IS NOT NULL")
            with_zip = await conn.fetchval(
                f"SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} WHERE zip7 IS NOT NULL")
            by_lvl = await conn.fetchval(
                f"""SELECT string_agg(lvl || '=' || c, ' ') FROM (
                      SELECT coalesce(zip_level,'none') AS lvl, count(*) AS c
                      FROM public.{_qi(ADDRESSES_TABLE)} GROUP BY 1 ORDER BY 1) x""")
            await _stage_done(conn, "addresses", t0, rows_out=n,
                              note=f"with_point={with_pt} with_zip={with_zip} [{by_lvl}]")
        await conn.execute(f"ANALYZE public.{_qi(ADDRESSES_TABLE)}", timeout=_LONG_TIMEOUT)
    return {"addresses": n, "with_point": with_pt, "with_zip": with_zip}


# ── stage 6: zip rollup ───────────────────────────────────────────────────────
async def build_zip5() -> dict:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "zip5")
        async with conn.transaction():
            await conn.execute(f"TRUNCATE public.{_qi(ZIP5_TABLE)}")
            await conn.execute(
                f"""
                INSERT INTO public.{_qi(ZIP5_TABLE)}
                  (zip5, settlement_code, settlement_name, n_addresses, n_streets, lat, lon)
                SELECT zip5, min(settlement_code), min(settlement_name),
                       count(*)::int, count(DISTINCT street_key)::int,
                       avg(lat), avg(lon)
                FROM public.{_qi(ADDRESSES_TABLE)}
                WHERE zip5 IS NOT NULL
                GROUP BY zip5
                """, timeout=_LONG_TIMEOUT)
            n = await conn.fetchval(f"SELECT count(*) FROM public.{_qi(ZIP5_TABLE)}")
            await _stage_done(conn, "zip5", t0, rows_out=n)
    return {"zip5": n}


# ── stage 7: point-in-polygon (the expensive one) ─────────────────────────────
async def build_pip(*, max_batches: int | None = None) -> dict:
    """Link each geocoded address to the parcel it physically sits in.

    The only genuinely expensive stage: ~384k probes against a 4.58 GB table.
    Two things keep it affordable and restartable:

    * **Batched** (``_PIP_BATCH`` at a time) so no single statement approaches a
      timeout and a dropped Neon connection costs one batch, not the run.
    * **Geohash-ordered** so consecutive batches touch spatially adjacent polygon
      pages and the buffer cache actually hits. Random order against a table this
      size is the difference between minutes and hours.

    Progress is the data itself — a row is done once ``parcel_match`` is set — so
    re-running simply continues."""
    pool = await append_store.get_pool()
    done = 0
    hits = 0
    batches = 0
    async with pool.acquire() as conn:
        t0 = await _stage_start(conn, "pip")
        while max_batches is None or batches < max_batches:
            # asyncpg's execute() returns the command tag ("UPDATE 2000"); its
            # trailing count is how many addresses this batch settled.
            tag = await conn.execute(
                f"""
                WITH todo AS (
                  SELECT address_key, point FROM public.{_qi(ADDRESSES_TABLE)}
                  WHERE point IS NOT NULL AND parcel_match IS NULL
                  ORDER BY {_qi(PG_EXT_SCHEMA)}.ST_GeoHash(point)
                  LIMIT {_PIP_BATCH}
                ), hit AS (
                  SELECT t.address_key,
                         public.over_parcel_key(p."GUSH_NUM", p."GUSH_SUFFI", p."PARCEL") AS pk
                  FROM todo t
                  LEFT JOIN LATERAL (
                    SELECT p."GUSH_NUM", p."GUSH_SUFFI", p."PARCEL"
                    FROM {_t(PARCELS_SRC)} p
                    WHERE p.geom OPERATOR({_qi(PG_EXT_SCHEMA)}.&&) t.point
                      AND {_qi(PG_EXT_SCHEMA)}.ST_Contains(p.geom, t.point)
                    LIMIT 1
                  ) p ON true
                )
                UPDATE public.{_qi(ADDRESSES_TABLE)} a
                SET parcel_key = hit.pk,
                    parcel_match = CASE WHEN hit.pk IS NULL THEN 'none' ELSE 'pip' END
                FROM hit WHERE a.address_key = hit.address_key
                """, timeout=_LONG_TIMEOUT)
            n = int(str(tag).rsplit(" ", 1)[-1]) if tag else 0
            if not n:
                break
            done += n
            batches += 1
            logger.info("nadlan pip: %d addresses linked (%d batches)", done, batches)
        hits = await conn.fetchval(
            f"SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} WHERE parcel_match = 'pip'")
        remaining = await conn.fetchval(
            f"""SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)}
                WHERE point IS NOT NULL AND parcel_match IS NULL""")
        await _stage_done(conn, "pip", t0, rows_out=hits,
                          status="ok" if not remaining else "partial",
                          note=f"linked={hits} remaining={remaining}")
    return {"processed": done, "linked": hits, "remaining": remaining}


# ── driver ────────────────────────────────────────────────────────────────────
_BUILDERS = {
    "source_indexes": ensure_source_indexes,
    "parcels": build_parcels,
    "gazetteer": build_gazetteer,
    "postal_localities": build_postal_localities,
    "streets": build_streets,
    "addresses": build_addresses,
    "zip5": build_zip5,
    "pip": build_pip,
}


async def build(stages: list[str] | None = None) -> dict:
    """Run the requested stages in dependency order.

    Default skips ``source_indexes`` and ``pip``: the first is a one-off that
    takes minutes and never needs repeating, the second is the only stage whose
    cost is material on a compute-billed Neon plan. Both stay explicit opt-ins."""
    todo = stages or [s for s in STAGES if s not in ("source_indexes", "pip")]
    unknown = [s for s in todo if s not in _BUILDERS]
    if unknown:
        raise ValueError(f"unknown stage(s): {', '.join(unknown)}")
    await ensure_tables()
    await ensure_functions()
    out: dict = {}
    for stage in STAGES:                     # keep dependency order regardless of input
        if stage not in todo:
            continue
        try:
            out[stage] = await _BUILDERS[stage]()
            logger.info("nadlan stage %s: %s", stage, out[stage])
        except Exception as e:  # noqa: BLE001
            logger.exception("nadlan stage %s failed", stage)
            out[stage] = {"error": str(e)}
            break
    from app.services import data_catalog, nadlan_query
    data_catalog.invalidate_catalog_cache()
    # A rebuild's numbers must show up at once, not up to 5 minutes later.
    nadlan_query.invalidate_stats_cache()
    return out
