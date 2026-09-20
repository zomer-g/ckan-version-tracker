"""נדל"ן לעם — folding the municipal address layers into the address spine.

Seven local authorities publish their own address register as a GovMap map
layer, and OVER already mirrors all seven into ``idx``. They are **not** the
same data as GovMap's national search box, and the difference is the point:
measured 2026-09-20, the national search returned **0 of 64** addresses OVER is
missing (against **28 of 32** on a control that OVER already has, so the service
was answering), while these seven layers hold **19,119 addresses OVER has no row
for at all** — 35.6% of their contents — each with a real point.

| layer | addresses | not in OVER |
|---|---|---|
| באר שבע 215337 | 28,059 | 8,635 |
| מודיעין מכבים רעות 224203 | 8,514 | 3,156 |
| ערד 236504 | 5,008 | 2,465 |
| שדרות 217145 | 4,474 | 2,262 |
| נס ציונה 212937 | 4,304 | 1,327 |
| בית אל 212868 | 724 | 702 |
| קרית טבעון 225724 | 2,684 | 572 |

Two rules this module keeps
---------------------------
* **It never overwrites a point.** Same rule as the GovMap geocode merge: a
  coordinate already in the spine came from the official register (exact to the
  centimetre) or from geocoding, and a municipality's own point is not
  automatically better. Only ``point IS NULL`` is filled.
* **Its points are labelled.** ``point_source = 'municipal_<id>'`` — not a bare
  'municipal', because these are seven separate publishers with seven separate
  update cycles and a reader tracing a suspect coordinate needs to know which
  one. The column exists precisely so that the register's centimetre and
  GovMap's ~1.5 m are not silently averaged into "a coordinate"; a third source
  must not undo that.

Street keys resolve, streets are not the gap
--------------------------------------------
Of 2,351 distinct street spellings across the seven layers, **2,344 already
resolve to a street in ``over_re_streets``** via ``over_street_key`` (which
normalises the layer's spelling onto OVER's canonical key — the layer's
``לוי אשכול`` in באר שבע becomes ``9000-שדאשכול``) and only **7** fail entirely.
So what these layers add is addresses, not street names, and the 7 that do not
resolve are skipped rather than guessed at.
"""
from __future__ import annotations

import logging

from app.services import append_store
from app.services.nadlan_index import ADDRESSES_TABLE

logger = logging.getLogger(__name__)

#: Each layer's own column names. There is no shared schema — every
#: municipality published whatever its GIS had, so the mapping is per layer and
#: has to be read off the mirrored table, not assumed.
#: (table, settlement name for over_settlement_code, street col, house col,
#:  entrance/letter col or None)
LAYERS: list[tuple[str, str, str, str, str | None]] = [
    ("govmap_215337_0f4aaa27_af7f32ff", "באר שבע", "str_name", "bldg_num", "ot"),
    ("govmap_224203_b25045c1_cc014b75", "מודיעין מכבים רעות", "streetname", "housenum", None),
    ("govmap_212937_d12074b9_faba7ce4", "נס ציונה", "street_he", "housenum", None),
    ("govmap_236504_949fcf23_a0b7403e", "ערד", "strname", "housenum", "letter"),
    ("govmap_217145_defba031_062e143f", "שדרות", "str_name", "house_num", "entry_letr"),
    ("govmap_225724_b482d54f_1dd55492", "קרית טבעון", "strname", "housenum", "entryletr"),
    ("govmap_212868_2256b608_d413802e", "בית אל", "str_name", "house_num", None),
]

#: A layer that suddenly matches almost nothing has usually been republished
#: with different column semantics, not emptied. Below this share of rows
#: keying successfully the layer is skipped and reported, rather than being
#: allowed to insert whatever it did manage to parse.
MIN_KEYED_RATIO = 0.5


def _layer_cte(table: str, town: str, st: str, hn: str, ot: str | None) -> str:
    """One layer, normalised onto the spine's own keys.

    The ``ot`` column is an entrance/building letter in every layer that has
    one, but they spell it differently and some write a blank rather than NULL.
    """
    suffix = f"nullif(btrim(coalesce(l.{ot}::text,'')),'')" if ot else "NULL::text"
    return f"""
        SELECT over_settlement_code('{town}')                        AS sc,
               over_street_key(over_settlement_code('{town}'),
                               l.{st}::text)                          AS street_key,
               over_house_num(l.{hn}::text)                           AS house_num,
               coalesce(over_house_suffix(l.{hn}::text), {suffix})    AS house_suffix,
               l.{st}::text                                           AS street_raw,
               l.{hn}::text                                           AS house_raw,
               extensions.ST_Y(l.geom)                                AS lat,
               extensions.ST_X(l.geom)                                AS lon,
               l.geom                                                 AS point
        FROM idx."{table}" l
        WHERE l.geom IS NOT NULL
          AND btrim(coalesce(l.{st}::text,'')) <> ''
          AND over_house_num(l.{hn}::text) IS NOT NULL
    """


async def layer_report() -> list[dict]:
    """What each layer would contribute, without contributing it."""
    pool = await append_store.get_pool()
    out: list[dict] = []
    async with pool.acquire() as conn:
        for table, town, st, hn, ot in LAYERS:
            row = await conn.fetchrow(f"""
                WITH l AS ({_layer_cte(table, town, st, hn, ot)}),
                     d AS (SELECT DISTINCT sc, street_key, house_num, house_suffix FROM l)
                SELECT (SELECT count(*) FROM idx."{table}")            AS layer_rows,
                       (SELECT count(*) FROM d)                        AS keyed,
                       (SELECT count(*) FROM d WHERE street_key IS NULL) AS unresolved_street,
                       (SELECT count(*) FROM d WHERE NOT EXISTS (
                          SELECT 1 FROM public."{ADDRESSES_TABLE}" o
                          WHERE o.settlement_code = d.sc
                            AND o.street_key = d.street_key
                            AND o.house_num = d.house_num)) AS new_addresses,
                       (SELECT count(*) FROM public."{ADDRESSES_TABLE}" o
                         WHERE o.point IS NULL AND EXISTS (
                           SELECT 1 FROM d WHERE d.sc = o.settlement_code
                             AND d.street_key = o.street_key
                             AND d.house_num = o.house_num)) AS fills_a_gap
            """, timeout=600)
            out.append({"table": table, "town": town, **dict(row)})
    return out


async def merge(*, dry_run: bool = False) -> dict:
    """Fold the seven layers into ``over_re_addresses``.

    Two writes per layer, in this order and no other: fill the points of rows
    that already exist and have none, then insert the addresses that have no row
    at all. Doing it the other way round would let a freshly inserted row be
    "filled" by its own source in the same pass, which is harmless but makes the
    counts lie about what happened.
    """
    pool = await append_store.get_pool()
    result: dict = {"layers": [], "filled": 0, "inserted": 0, "skipped": []}
    async with pool.acquire() as conn:
        for table, town, st, hn, ot in LAYERS:
            cte = _layer_cte(table, town, st, hn, ot)
            total = await conn.fetchval(f'SELECT count(*) FROM idx."{table}"')
            keyed = await conn.fetchval(
                f"WITH l AS ({cte}) SELECT count(*) FROM l WHERE street_key IS NOT NULL",
                timeout=600)
            # A layer republished with different columns parses as almost
            # nothing. Inserting "whatever parsed" from it would quietly put a
            # handful of rows in and report success.
            if not total or (keyed / total) < MIN_KEYED_RATIO:
                msg = (f"{town}: only {keyed:,} of {total:,} rows keyed "
                       f"({MIN_KEYED_RATIO:.0%} required) — the layer's columns "
                       f"have probably changed; skipped")
                logger.warning("address_municipal: %s", msg)
                result["skipped"].append(msg)
                continue
            if dry_run:
                result["layers"].append({"town": town, "keyed": keyed, "total": total})
                continue

            async with conn.transaction():
                filled = await conn.execute(f"""
                    WITH l AS ({cte}),
                         d AS (SELECT DISTINCT ON (sc, street_key, house_num, house_suffix)
                                      sc, street_key, house_num, house_suffix, lat, lon, point
                               FROM l WHERE street_key IS NOT NULL)
                    UPDATE public."{ADDRESSES_TABLE}" o
                       SET lat = d.lat, lon = d.lon, point = d.point,
                           point_source = 'municipal_{table.split("_")[1]}'
                      FROM d
                     WHERE o.settlement_code = d.sc
                       AND o.street_key = d.street_key
                       AND o.house_num = d.house_num
                       AND o.point IS NULL
                """, timeout=900)

                inserted = await conn.execute(f"""
                    WITH l AS ({cte}),
                         d AS (SELECT DISTINCT ON (sc, street_key, house_num, house_suffix)
                                      sc, street_key, house_num, house_suffix,
                                      street_raw, house_raw, lat, lon, point
                               FROM l WHERE street_key IS NOT NULL)
                    INSERT INTO public."{ADDRESSES_TABLE}"
                        (address_key, settlement_code, settlement_name, street_key,
                         street_name, house_num, house_suffix, house_raw,
                         lat, lon, point, point_source,
                         in_postal, in_address_list, refreshed_at)
                    SELECT d.sc || '|' || d.street_key || '|' ||
                             d.house_num::text || coalesce(d.house_suffix,'') || '|',
                           -- Name resolved from the CODE, not from the layer's
                           -- spelling: over_settlement() takes a name and
                           -- returns NULL for a numeric code, and the layers
                           -- spell towns their own way (מודיעין מכבים רעות vs
                           -- the canonical מודיעין-מכבים-רעות). Same join the
                           -- spine's own build uses.
                           d.sc, t.name, d.street_key,
                           coalesce(s.name, d.street_raw),
                           d.house_num, d.house_suffix, d.house_raw,
                           d.lat, d.lon, d.point,
                           'municipal_{table.split("_")[1]}',
                           false, false, now()
                    FROM d
                    LEFT JOIN public.over_re_streets s ON s.street_key = d.street_key
                    LEFT JOIN public.over_settlements t ON t.code = d.sc
                    ON CONFLICT (address_key) DO NOTHING
                """, timeout=900)

            nf = int(str(filled).rsplit(" ", 1)[-1] or 0)
            ni = int(str(inserted).rsplit(" ", 1)[-1] or 0)
            result["filled"] += nf
            result["inserted"] += ni
            result["layers"].append({"town": town, "filled": nf, "inserted": ni,
                                     "keyed": keyed, "total": total})
            logger.info("address_municipal: %s — filled %d, inserted %d",
                        town, nf, ni)
    return result
