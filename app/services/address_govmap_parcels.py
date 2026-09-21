"""נדל"ן לעם — folding GovMap's parcel address index into the address spine.

This is the third external source of addresses, and it is not a variant of the
other two. The geocoder (:mod:`app.services.geocode_queue`) takes an address we
already have and asks GovMap where it is. The municipal layers
(:mod:`app.services.address_municipal`) are seven towns publishing their own
register. This one **enumerates**: a long-running sweep on the worker walks
GovMap's parcel object ids against an endpoint that answers with every address
on that parcel, so it discovers addresses nobody here had a row for — and it
arrives with the parcel already attached.

What it actually contributes, measured 2026-09-21 on the first 36,344 rows
------------------------------------------------------------------------------
* **58% of sampled addresses have no row in the spine at all** (84 of 200 found
  an exact ``address_key``; the rest were new). The same sample found only
  **2 of those 84** missing a point and **2** missing a parcel — so enrichment
  of rows that already exist is a rounding error and *discovery* is the point.
* **39% of the localities it has already touched are absent from the spine** —
  170 of 439. The address list covers 321 localities; this sweep is not bounded
  by that file.
* **Its settlement code is the CBS code.** 438 of 439 codes resolve in
  ``over_settlements`` (only 10179 does not) and the names agree exactly
  (9000=באר שבע, 1061=נוף הגליל, 5000=תל אביב -יפו). So this source joins on the
  CODE and never on a name — the seam that
  :mod:`app.services.address_municipal` has to work around does not exist here.
* **Its parcel link is free and it resolves.** 60 of 60 sampled parcel
  centroids resolved through ``over_parcel_at()``. The centroid is inside its
  own polygon by construction, so this is a lookup, not a guess.

The coordinate is a PARCEL CENTROID, and that governs the whole design
----------------------------------------------------------------------
Every row the sweep produces is ``geometry_level='parcel_centroid'``: GovMap
returns the parcel's polygon and the sweep keeps its centre. It is **not** the
doorway. In the sample, one point carried **353 different addresses** — a whole
apartment block's worth of rows sharing one pair of coordinates.

So this source is deliberately ranked below the other two:

* it **never overwrites a point** (``point IS NULL`` only), the same rule the
  other two merges keep, but here it matters more: the register is exact to the
  centimetre and GovMap's geocoder measured a median 1.5 m, while this is
  "somewhere on the parcel";
* its points are stamped ``point_source = 'govmap_parcel'``, distinct from the
  geocoder's ``'govmap'``, precisely so a reader can tell a centroid from a
  geocode. Averaging the two into "a coordinate" is what ``point_source``
  exists to prevent;
* what it is authoritative about is the **parcel**, and that it fills
  unconditionally where the spine has none, stamped
  ``parcel_match = 'govmap_parcel'`` beside ``'pip'`` and ``'street'``.

An unresolvable street is dropped, not invented
-----------------------------------------------
11% of swept rows name their street as ``רח' 7002`` — GovMap's own street code
rendered as a name, for streets with no published name. ``over_street_key()``
cannot resolve those and this module does not try: the spine's build owns the
``xy:``/``zip:`` fallback keys for street-less addresses, and minting more of
them from here would create rows a later rebuild cannot reconcile. They stay in
the ingest table, counted and unmerged, so nothing is lost if a naming source
ever appears.

Why the ingest table is kept, rather than merging on arrival
------------------------------------------------------------
``build_addresses`` TRUNCATEs the spine. Every address this module adds would
vanish on the next rebuild, which is why it is a build STAGE (``govmap_parcels``
in :data:`app.services.nadlan_index.STAGES`, after ``addresses`` and before
``pip``) and why the raw rows have to survive independently of the spine. The
merge is idempotent and re-reads the whole ingest table, so re-running it is
always safe and always sufficient — there is no "already merged" flag to drift
out of step with a table that gets truncated underneath it.
"""
from __future__ import annotations

import logging

from app.services import append_store
from app.services.append_store import _qi
from app.services.nadlan_index import ADDRESSES_TABLE

logger = logging.getLogger(__name__)

TABLE = "over_re_govmap_parcels"

#: Stamped on the point and on the parcel link. NOT 'govmap' — that is the
#: geocoder's, and the two have entirely different precision (see the module
#: docstring). A reader tracing a coordinate has to be able to tell them apart.
POINT_SOURCE = "govmap_parcel"

#: How many distinct parcels one `resolve_parcels()` call will look up. The
#: sweep can only ingest ~15 parcels a minute (GovMap publishes
#: `X-RateLimit-Limit: 15` over a 60-second window), so this is never the
#: bottleneck; it is a bound on one statement, not a throttle.
RESOLVE_BATCH = 20_000

_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(TABLE)} (
    address_objectid  bigint PRIMARY KEY,
    parcel_object_id  bigint NOT NULL,
    settlement_code   integer,
    settlement_name   text,
    street_code       integer,
    street_name       text,
    house_num         integer,
    lat               double precision,
    lon               double precision,
    itm_x             double precision,
    itm_y             double precision,
    geometry_level    text,
    -- Resolved once per PARCEL, not per address: up to 353 addresses observed
    -- sharing one parcel centroid, and over_parcel_at() is a spatial lookup.
    parcel_key        text,
    -- Separate from `parcel_key IS NULL` on purpose: a parcel that falls
    -- outside the shape layer resolves to NULL legitimately, and without this
    -- flag every merge would ask about it again forever.
    parcel_resolved   boolean NOT NULL DEFAULT false,
    fetched_at        timestamptz DEFAULT now()
)
"""
_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS {_qi(TABLE + '_parcel_idx')} "
    f"ON public.{_qi(TABLE)} (parcel_object_id)",
    f"CREATE INDEX IF NOT EXISTS {_qi(TABLE + '_unresolved_idx')} "
    f"ON public.{_qi(TABLE)} (parcel_object_id) WHERE NOT parcel_resolved",
    f"CREATE INDEX IF NOT EXISTS {_qi(TABLE + '_settlement_idx')} "
    f"ON public.{_qi(TABLE)} (settlement_code)",
]


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_DDL)
        for stmt in _INDEXES:
            await conn.execute(stmt)
    await _revoke_from_public_console()


async def _revoke_from_public_console() -> None:
    """Keep the raw ledger out of the public SQL console.

    Same reasoning as the geocode ledger: the console runs free-form SQL, so
    leaving this out of the ``/data`` catalog hides the signpost but not the
    table. The read-only role inherits a grant from ``ALTER DEFAULT PRIVILEGES
    IN SCHEMA public``, which re-applies whenever the role is reprovisioned, so
    the revoke is re-asserted on every ``ensure_tables()`` rather than run once
    by hand. What the public gets is the merged result in
    ``over_re_addresses``, which is the part that has been reconciled.
    """
    from app.services.index_mirror import _readonly_role
    role = _readonly_role()
    if not role:
        return
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute(
                f"REVOKE ALL ON public.{_qi(TABLE)} FROM {_qi(role)}")
        except Exception:  # noqa: BLE001 — a missing role must not fail a build
            logger.debug("govmap_parcels: revoke on %s failed", TABLE, exc_info=True)


# ── ingest ────────────────────────────────────────────────────────────────────
def _row(a: dict) -> tuple | None:
    """One sweep row, or None if it cannot be keyed.

    ``address_objectid`` is GovMap's own id for the address and is the primary
    key here: the sweep is restartable and re-walks ids after a crash, so the
    same address WILL arrive twice and must land on the same row.
    """
    def _i(v):
        try:
            return int(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _f(v):
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    aoid = _i(a.get("address_objectid"))
    poid = _i(a.get("parcel_object_id"))
    if aoid is None or poid is None:
        return None
    return (aoid, poid, _i(a.get("settlement_code")),
            (a.get("settlement_name") or None), _i(a.get("street_code")),
            (a.get("street_name") or None), _i(a.get("house_num")),
            _f(a.get("lat")), _f(a.get("lon")),
            _f(a.get("itm_x")), _f(a.get("itm_y")),
            (a.get("geometry_level") or None))


async def record_batch(payload: dict) -> dict:
    """Store one checkpoint's worth of swept addresses.

    The sweep pushes what it has just written to its CSV, every window. An
    UPSERT rather than an insert because the sweep keeps no server-side
    checkpoint of what it has already sent: it is allowed to re-send, and
    re-sending must be a no-op rather than a conflict.

    Re-sending an address whose parcel was already resolved does NOT reset
    ``parcel_resolved`` — the geometry of a parcel does not change because the
    sweep walked past it twice, and re-resolving hundreds of thousands of
    parcels on every restart would be the whole cost of this feature.
    """
    rows = [r for r in (_row(a) for a in (payload.get("addresses") or [])) if r]
    if not rows:
        return {"received": len(payload.get("addresses") or []), "stored": 0}
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(f"""
                INSERT INTO public.{_qi(TABLE)}
                  (address_objectid, parcel_object_id, settlement_code,
                   settlement_name, street_code, street_name, house_num,
                   lat, lon, itm_x, itm_y, geometry_level, fetched_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12, now())
                ON CONFLICT (address_objectid) DO UPDATE SET
                  parcel_object_id = EXCLUDED.parcel_object_id,
                  settlement_code  = EXCLUDED.settlement_code,
                  settlement_name  = EXCLUDED.settlement_name,
                  street_code      = EXCLUDED.street_code,
                  street_name      = EXCLUDED.street_name,
                  house_num        = EXCLUDED.house_num,
                  lat = EXCLUDED.lat, lon = EXCLUDED.lon,
                  itm_x = EXCLUDED.itm_x, itm_y = EXCLUDED.itm_y,
                  geometry_level   = EXCLUDED.geometry_level,
                  fetched_at       = now()
            """, rows)
    out = {"received": len(payload.get("addresses") or []), "stored": len(rows)}
    logger.info("govmap_parcels: ingested %s", out)
    return out


# ── parcel resolution ─────────────────────────────────────────────────────────
async def resolve_parcels(limit: int = RESOLVE_BATCH) -> dict:
    """Turn GovMap's parcel object ids into gush-חלקה keys, once per parcel.

    ``over_parcel_at()`` is point-in-polygon against ``over_re_parcels``; the
    centroid is inside its own parcel by construction, which is why this is a
    lookup rather than a match — 60 of 60 sampled centroids resolved.

    Every parcel asked about is marked resolved even when the answer is NULL.
    GovMap's index and the shape layer are two different publications and a
    parcel present in one and absent from the other is a real, stable state;
    without the flag it would be re-asked on every merge forever.
    """
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        tag = await conn.execute(f"""
            WITH todo AS (
              SELECT DISTINCT ON (parcel_object_id) parcel_object_id, lat, lon
              FROM public.{_qi(TABLE)}
              WHERE NOT parcel_resolved AND lat IS NOT NULL AND lon IS NOT NULL
              ORDER BY parcel_object_id
              LIMIT {int(limit)}
            ), found AS (
              SELECT t.parcel_object_id, over_parcel_at(t.lat, t.lon) AS parcel_key
              FROM todo t
            )
            UPDATE public.{_qi(TABLE)} g
               SET parcel_key = f.parcel_key, parcel_resolved = true
              FROM found f
             WHERE g.parcel_object_id = f.parcel_object_id
        """, timeout=1800)
        updated = int(str(tag).rsplit(" ", 1)[-1] or 0)
        remaining = await conn.fetchval(f"""
            SELECT count(DISTINCT parcel_object_id) FROM public.{_qi(TABLE)}
             WHERE NOT parcel_resolved AND lat IS NOT NULL
        """) or 0
    logger.info("govmap_parcels: resolved parcels for %d rows, %d parcels left",
                updated, remaining)
    return {"rows_stamped": updated, "parcels_remaining": remaining}


# ── the merge ─────────────────────────────────────────────────────────────────
#: A projection of what ``resolve_parcels()`` WOULD store, computed without
#: storing it. Only the report uses this: the real merge resolves first, so by
#: the time it reads the table every parcel already has its answer.
_PROJECTED_RESOLUTION = f"""
    SELECT DISTINCT ON (parcel_object_id) parcel_object_id,
           over_parcel_at(lat, lon) AS parcel_key
    FROM public.{_qi(TABLE)}
    WHERE NOT parcel_resolved AND lat IS NOT NULL AND lon IS NOT NULL
    ORDER BY parcel_object_id
"""


def _keyed_cte(*, project_resolution: bool = False) -> str:
    """The ingest table, normalised onto the spine's own keys.

    ``settlement_code`` is used directly — it is the CBS code (438 of 439
    verified against ``over_settlements``), so unlike the municipal layers there
    is no name to resolve and no spelling to get wrong.

    ``project_resolution`` fills in the parcel a row has not been resolved to
    YET, by asking ``over_parcel_at()`` in the query rather than reading the
    stored answer. The report needs it and the merge does not: without it a dry
    run reports ``fills_a_parcel: 0`` no matter what the truth is, because
    resolution only happens inside the real merge — the first live run filled
    555 parcels against a forecast of zero, which is a forecast worth nothing.
    Costed per distinct PARCEL, not per address, and read-only.
    """
    parcel = ("coalesce(g.parcel_key, r.parcel_key)" if project_resolution
              else "g.parcel_key")
    join = ("LEFT JOIN _proj r ON r.parcel_object_id = g.parcel_object_id"
            if project_resolution else "")
    return f"""
        SELECT g.settlement_code                                     AS sc,
               over_street_key(g.settlement_code, g.street_name)     AS street_key,
               g.street_name                                         AS street_raw,
               g.house_num                                           AS house_num,
               g.lat, g.lon, {parcel}                                AS parcel_key
        FROM public.{_qi(TABLE)} g
        {join}
        WHERE g.settlement_code IS NOT NULL
          AND g.house_num IS NOT NULL
          AND g.lat IS NOT NULL AND g.lon IS NOT NULL
    """


def _distinct_cte() -> str:
    """One row per doorway.

    A parcel's addresses arrive per address object, and two objects can name the
    same doorway. ``DISTINCT ON`` collapses them on the key the insert actually
    uses, so the INSERT cannot meet its own duplicate.

    The ORDER BY is not decoration: ``DISTINCT ON`` without one keeps an
    arbitrary member of each group, so a doorway listed twice — once from a
    parcel that resolved and once from a parcel that did not — could keep the
    parcel-less copy and lose the link this source exists to supply.
    """
    return f"""
        SELECT DISTINCT ON (sc, street_key, house_num)
               sc, street_key, house_num, street_raw, lat, lon, parcel_key
        FROM k WHERE street_key IS NOT NULL
        ORDER BY sc, street_key, house_num, (parcel_key IS NULL), parcel_key
    """


async def report() -> dict:
    """What a merge would do, without doing it — including the parcels.

    It projects the parcel resolution instead of reading it (see
    ``_keyed_cte``), so the forecast covers all three effects rather than
    silently reporting zero for the one this source is best at.
    """
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            WITH _proj AS ({_PROJECTED_RESOLUTION}),
                 k AS ({_keyed_cte(project_resolution=True)}),
                 d AS ({_distinct_cte()})
            SELECT (SELECT count(*) FROM public.{_qi(TABLE)})            AS ingested_rows,
                   (SELECT count(*) FROM k WHERE street_key IS NULL)     AS unresolved_street,
                   (SELECT count(*) FROM d)                              AS doorways,
                   (SELECT count(*) FROM d WHERE NOT EXISTS (
                      SELECT 1 FROM public.{_qi(ADDRESSES_TABLE)} o
                       WHERE o.settlement_code = d.sc
                         AND o.street_key = d.street_key
                         AND o.house_num = d.house_num
                         AND o.house_suffix IS NULL))                    AS new_addresses,
                   (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} o
                     WHERE o.point IS NULL AND EXISTS (
                       SELECT 1 FROM d WHERE d.sc = o.settlement_code
                         AND d.street_key = o.street_key
                         AND d.house_num = o.house_num))                 AS fills_a_point,
                   (SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} o
                     WHERE o.parcel_key IS NULL AND EXISTS (
                       SELECT 1 FROM d WHERE d.sc = o.settlement_code
                         AND d.street_key = o.street_key
                         AND d.house_num = o.house_num
                         AND d.parcel_key IS NOT NULL))                  AS fills_a_parcel
        """, timeout=900)
    return dict(row or {})


async def merge(*, dry_run: bool = False) -> dict:
    """Fold the swept addresses into ``over_re_addresses``.

    Three writes, in this order and no other:

    1. **the parcel** — filled wherever the spine has none. This is the one leg
       this source is authoritative about: GovMap itself says the address sits
       on that parcel, and the centroid resolved it to a gush-חלקה. Stamped
       ``parcel_match='govmap_parcel'`` so it is distinguishable from ``pip``.
    2. **the point** — filled ONLY where there is none, stamped
       ``point_source='govmap_parcel'``. It is a parcel centroid, not a doorway
       (353 addresses observed on one point), so it must never displace the
       register's coordinate or a geocode.
    3. **the new addresses** — the rest of this source's value, 58% of it.

    Filling before inserting, as in the municipal merge: the other order lets a
    row inserted by step 3 be "filled" by step 1 in the same pass, which is
    harmless but makes the counts describe something that did not happen.
    """
    await ensure_tables()
    if not dry_run:
        # Resolve first: an unresolved parcel contributes no parcel_key, and a
        # merge that ran before resolution would silently fill nothing and then
        # find nothing left to fill on the next pass (the point is already set).
        await resolve_parcels()
    if dry_run:
        return {"dry_run": True, **await report()}

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            tag = await conn.execute(f"""
                WITH k AS ({_keyed_cte()}), d AS ({_distinct_cte()})
                UPDATE public.{_qi(ADDRESSES_TABLE)} o
                   SET parcel_key = d.parcel_key,
                       parcel_match = '{POINT_SOURCE}'
                  FROM d
                 WHERE o.settlement_code = d.sc
                   AND o.street_key = d.street_key
                   AND o.house_num = d.house_num
                   AND o.parcel_key IS NULL
                   AND d.parcel_key IS NOT NULL
            """, timeout=1800)
            parcels_filled = int(str(tag).rsplit(" ", 1)[-1] or 0)

            tag = await conn.execute(f"""
                WITH k AS ({_keyed_cte()}), d AS ({_distinct_cte()})
                UPDATE public.{_qi(ADDRESSES_TABLE)} o
                   SET lat = d.lat, lon = d.lon,
                       point = extensions.ST_SetSRID(
                                 extensions.ST_MakePoint(d.lon, d.lat), 4326),
                       point_source = '{POINT_SOURCE}'
                  FROM d
                 WHERE o.settlement_code = d.sc
                   AND o.street_key = d.street_key
                   AND o.house_num = d.house_num
                   AND o.point IS NULL
            """, timeout=1800)
            points_filled = int(str(tag).rsplit(" ", 1)[-1] or 0)

            tag = await conn.execute(f"""
                WITH k AS ({_keyed_cte()}), d AS ({_distinct_cte()})
                INSERT INTO public.{_qi(ADDRESSES_TABLE)}
                    (address_key, settlement_code, settlement_name, street_key,
                     street_name, house_num, lat, lon, point, point_source,
                     parcel_key, parcel_match,
                     in_postal, in_address_list, refreshed_at)
                SELECT d.sc || '|' || d.street_key || '|' ||
                         d.house_num::text || '|',
                       -- The name comes from the CODE. GovMap's own spelling
                       -- agrees with the register on every code checked, but
                       -- the spine's identity is the code and a second spelling
                       -- has no standing to disagree with it.
                       d.sc, t.name, d.street_key,
                       coalesce(s.name, d.street_raw),
                       d.house_num, d.lat, d.lon,
                       extensions.ST_SetSRID(
                         extensions.ST_MakePoint(d.lon, d.lat), 4326),
                       '{POINT_SOURCE}',
                       d.parcel_key,
                       CASE WHEN d.parcel_key IS NOT NULL
                            THEN '{POINT_SOURCE}' END,
                       false, false, now()
                FROM d
                LEFT JOIN public.over_re_streets s ON s.street_key = d.street_key
                LEFT JOIN public.over_settlements t ON t.code = d.sc
                ON CONFLICT (address_key) DO NOTHING
            """, timeout=1800)
            inserted = int(str(tag).rsplit(" ", 1)[-1] or 0)

    from app.services import nadlan_query
    nadlan_query.invalidate_stats_cache()
    out = {"parcels_filled": parcels_filled, "points_filled": points_filled,
           "inserted": inserted}
    logger.info("govmap_parcels: merged %s", out)
    return out


async def stats() -> dict:
    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            SELECT count(*)                                              AS rows,
                   count(DISTINCT parcel_object_id)                      AS parcels,
                   count(DISTINCT settlement_code)                       AS settlements,
                   count(*) FILTER (WHERE parcel_resolved)               AS parcel_resolved,
                   count(*) FILTER (WHERE parcel_key IS NOT NULL)        AS with_parcel_key,
                   max(fetched_at)                                       AS last_ingest
            FROM public.{_qi(TABLE)}
        """)
    return dict(row or {})
