"""עסקאות נדל"ן — the read path over the מיסוי מקרקעין deal register.

One source, one table, and the table is FOUND rather than named: its physical
name carries the tracked dataset's id, and the corpus was published as one file
per settlement before being merged, so the name has already changed once.
``nadlan_index.deals_table()`` resolves it by column signature (largest wins) and
memoises the answer; every query here goes through it, and a deployment that
does not track the corpus answers 503 rather than erroring on a missing table.

3.84 M reported deals back to 1998, scraped from nadlan.taxes.gov.il and kept in
the append DB like every other tracked dataset. Nothing here derives or corrects it — the project's
job is to make a register that the publisher only lets you read one גוש at a
time queryable as a whole, and to say plainly where it disagrees with
nadlan.gov.il (the "פערים" report that shares the page).

Three shapes of question, three shapes of query:

* **browse** — :func:`search`, filtered and paged, newest first;
* **shape** — :func:`series` (deals and median price per year) and
  :func:`breakdown` (per deal type), both under the SAME filters as the browse
  so a chart can never describe a different population than the table under it;
* **pick lists** — :func:`settlements` and :func:`natures`, cached, because both
  are whole-table aggregates that only move when the scraper appends.

**Every column in the register is text.** Dates are DD/MM/YYYY and ``to_date``
is only STABLE, so ordering and range filters go through
:data:`nadlan_index.DEAL_SORT_KEY` — the same immutable ``substr`` expression
the index is built on, imported rather than retyped so the two cannot drift.

**The settlement key is the NAME.** 674,340 rows (17.5%) carry an empty
``settlement_code`` while only 2,171 lack a name, so the code is an optional
extra here, never the filter.
"""
from __future__ import annotations

import asyncio
import logging
import time

from app.services import append_store
from app.services import nadlan_index
from app.services import nadlan_text
from app.services.nadlan_index import DEAL_SORT_KEY, _t
from app.services.nadlan_query import _console_url, _deal_row, _iso, _lit

logger = logging.getLogger(__name__)

# The dataset this whole project reads, so the UI and the API can link to its
# version history rather than describing it.
DATASET_ID = "fd06f5ae-8a4f-4120-b275-8a514ad23499"
# Raised by every read when the corpus is not tracked on this deployment. The
# API turns it into the same 503 as "not built yet", which is what it is.
class DealsUnavailable(RuntimeError):
    pass


async def _src() -> tuple[str, str]:
    src = await nadlan_index.deals_table()
    if not src:
        raise DealsUnavailable("מאגר עסקאות הנדל\"ן אינו נטען בסביבה הזו")
    return src
SOURCE_URL = "https://nadlan.taxes.gov.il/svinfonadlan2010/startpage.aspx"

# Same budget as the nadlan lookup: a browse that needs longer is a missing
# index, and should fail loudly rather than hold a Neon compute open.
_TIMEOUT_MS = 8000
# The cached whole-table aggregates get their own, longer ceiling. Measured in
# production: the browse, the series and the per-settlement list all answer in
# 1-5s, but stats() counts DISTINCT (gush||'-'||chelka) over 3.84 M rows and
# came in just over 8s — it 500'd on the first deploy. It is cached, so it runs
# a few times an hour rather than per page load, and the page renders without
# it either way; the ceiling is here to bound it, not to make it fail.
_AGGREGATE_TIMEOUT_MS = 25000
MAX_LIMIT = 200
# An exact COUNT over a settlement's whole history is a real cost for a number
# nobody reads past the first page, so the total is counted to here and then
# reported as "more than". 10,001 rows is a few milliseconds on the index.
COUNT_CAP = 10_000

# Price per sqm actually bought: amount / (whole-asset area × share sold). The
# register's area is the whole asset's while the amount pays for the share only,
# so the raw amount/area of a 50% sale reads as half the market price (measured:
# 19.3k vs 33.3k median for flats in the three big cities, 2024-25, same median
# area). NULL when the area or the share is zero — never a division by zero.
PPSQM_NORM = ("nullif(deal_amount, '')::numeric / nullif(nullif(asset_area, '')::numeric"
              " * nullif(portion, '')::numeric, 0)")

SORT_MODES = {
    "date_desc": f"{DEAL_SORT_KEY} DESC",
    "date_asc": f"{DEAL_SORT_KEY} ASC",
    "amount_desc": "nullif(deal_amount, '')::bigint DESC NULLS LAST",
    "amount_asc": "nullif(deal_amount, '')::bigint ASC NULLS LAST",
    "area_desc": "nullif(asset_area, '')::bigint DESC NULLS LAST",
}


async def _fetch(sql: str, *args, timeout_ms: int = _TIMEOUT_MS) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            rows = await conn.fetch(sql, *args)
    return [dict(r) for r in rows]


def _int(v):
    # numeric medians come back as Decimal; hand JSON a plain int.
    return int(v) if v is not None else None


def _num(v):
    return float(v) if v is not None else None


# ── the filter, in one place ──────────────────────────────────────────────────
# search(), series() and breakdown() all build from this, so the table and the
# charts above it always describe the same rows. A filter the caller did not set
# is not in the WHERE clause at all, rather than a `$n IS NULL OR …` that would
# defeat the index.
def _where(f: dict) -> tuple[str, list]:
    clauses: list[str] = []
    args: list = []

    def arg(v) -> str:
        args.append(v)
        return f"${len(args)}"

    if f.get("settlement"):
        clauses.append(f"settlement = {arg(f['settlement'])}")
    if f.get("settlement_code"):
        clauses.append(f"settlement_code = {arg(str(f['settlement_code']))}")
    if f.get("gush"):
        clauses.append(f"gush = {arg(str(f['gush']))}")
    if f.get("helka"):
        clauses.append(f"chelka = {arg(str(f['helka']))}")
    if f.get("sub_parcel"):
        clauses.append(f"sub_chelka = {arg(str(f['sub_parcel']).zfill(3))}")
    if f.get("nature"):
        clauses.append(f"deal_nature = {arg(f['nature'])}")
    # Dates arrive as ISO and are compared against the sortable YYYYMMDD form,
    # which is a plain text comparison on the indexed expression.
    if f.get("date_from"):
        clauses.append(f"{DEAL_SORT_KEY} >= {arg(f['date_from'].replace('-', ''))}")
    if f.get("date_to"):
        clauses.append(f"{DEAL_SORT_KEY} <= {arg(f['date_to'].replace('-', ''))}")
    if f.get("min_amount"):
        clauses.append(f"nullif(deal_amount, '')::bigint >= {arg(int(f['min_amount']))}")
    if f.get("max_amount"):
        clauses.append(f"nullif(deal_amount, '')::bigint <= {arg(int(f['max_amount']))}")
    if f.get("min_rooms"):
        clauses.append(f"nullif(room_num, '')::numeric >= {arg(float(f['min_rooms']))}")
    if f.get("max_rooms"):
        clauses.append(f"nullif(room_num, '')::numeric <= {arg(float(f['max_rooms']))}")
    # An address, already resolved to the גוש/חלקה pairs it stands on (see
    # resolve_address). Checked by presence, not truthiness: an address that
    # resolved to NO parcel must answer nothing, not drop out of the filter
    # and answer the whole settlement.
    if "parcels" in f:
        gs, hs = f["parcels"]
        clauses.append(f"(gush, chelka) IN (SELECT g, h FROM unnest({arg(list(gs))}::text[], "
                       f"{arg(list(hs))}::text[]) AS t(g, h))")
    return (" AND ".join(clauses) or "true"), args


# ── address → parcels ─────────────────────────────────────────────────────────
# The register has no address column at all: it is published per גוש/חלקה. An
# address therefore reaches it only through the נדל"ן לעם crosswalk, where each
# address in the national address list is linked to the parcel its point falls
# in. That link exists for a minority of the register (measured 2026-09-27 on a
# 2025 sample: 37% of the parcels that had a deal carry any address), because
# the address list covers the big cities and the geocoded remainder. A miss is
# therefore usually coverage, and resolve_address says which of the three
# failures it was rather than just "nothing found".
MAX_ADDRESS_PARCELS = 1000
# A whole street, pairs and all, is too long for the /data deep-link; the link
# carries the resolution as a subquery instead, which also shows HOW it was done.
_ADDR_T = f"public.{nadlan_index._qi(nadlan_index.ADDRESSES_TABLE)}"
_PARCEL_T = f"public.{nadlan_index._qi(nadlan_index.PARCELS_TABLE)}"


async def resolve_address(settlement: str, street: str, house: str | None) -> dict:
    """The גוש/חלקה pairs under an address (or a whole street), with how it went.

    ``house`` may carry a letter (12א); the letter is ignored, so 12א finds every
    entrance of 12 — the register could not tell them apart anyway, it has no
    address to tell them apart by."""
    num, _sfx = nadlan_text.parse_house_number(house) if house else (None, None)
    rows = await _fetch(f"""
        WITH sc AS (SELECT public.over_settlement_code($1) AS code),
             sk AS (SELECT sc.code, public.over_street_key(sc.code, $2) AS key FROM sc),
             a AS (
               SELECT a.parcel_key FROM {_ADDR_T} a, sk
               WHERE a.settlement_code = sk.code AND a.street_key = sk.key
                 AND ($3::int IS NULL OR a.house_num = $3)
             )
        SELECT sk.code, sk.key,
               (SELECT count(*) FROM a) AS addresses,
               (SELECT count(*) FROM a WHERE a.parcel_key IS NOT NULL) AS linked,
               (SELECT coalesce(array_agg(DISTINCT p.gush::text || '-' || p.parcel::text), '{{}}')
                  FROM a JOIN {_PARCEL_T} p ON p.parcel_key = a.parcel_key) AS pairs
        FROM sk
    """, settlement, street, num)
    r = rows[0] if rows else {}
    pairs = sorted(r.get("pairs") or [])[:MAX_ADDRESS_PARCELS]
    if not r.get("code"):
        status = "settlement_unknown"
    elif not r.get("key"):
        status = "street_unknown"
    elif not r.get("addresses"):
        status = "house_unknown" if num is not None else "street_not_located"
    elif not pairs:
        status = "not_linked"
    else:
        status = "ok"
    return {
        "status": status, "settlement": settlement, "street": street,
        "house": house or None, "addresses": r.get("addresses") or 0,
        "linked": r.get("linked") or 0, "parcels": pairs,
        "_pairs": ([p.split("-")[0] for p in pairs], [p.split("-")[1] for p in pairs]),
    }


def _address_console_where(f: dict) -> str:
    num, _ = nadlan_text.parse_house_number(f["house"]) if f.get("house") else (None, None)
    sc = f"public.over_settlement_code({_lit(f['settlement'])})"
    house = f" AND a.house_num = {int(num)}" if num is not None else ""
    return (f"(gush, chelka) IN (SELECT p.gush::text, p.parcel::text\n"
            f"  FROM {_ADDR_T} a JOIN {_PARCEL_T} p ON p.parcel_key = a.parcel_key\n"
            f"  WHERE a.settlement_code = {sc}\n"
            f"    AND a.street_key = public.over_street_key({sc}, {_lit(f['street'])}){house})")


async def addresses_for(pairs: list[tuple[str, str]], per_parcel: int = 3) -> dict[str, dict]:
    """``"gush-helka"`` -> the addresses the crosswalk links to that parcel.

    For DISPLAY beside a deal. A parcel is often a building with several
    entrances, or a block with several buildings, so it can carry many; the
    first few are shown and the rest counted. Best-effort: a deployment without
    the נדל"ן לעם tables still serves the register, just without the column."""
    keys = sorted({f"{g}-{h}" for g, h in pairs if g and h})
    if not keys:
        return {}
    try:
        rows = await _fetch(f"""
            SELECT DISTINCT p.gp_key, a.street_name, a.house_num, coalesce(a.house_suffix, '') AS sfx
            FROM {_PARCEL_T} p JOIN {_ADDR_T} a ON a.parcel_key = p.parcel_key
            WHERE p.gp_key = ANY($1::text[])
            ORDER BY p.gp_key, a.street_name, a.house_num, sfx
        """, keys)
    except Exception as e:  # noqa: BLE001
        logger.info("deals: address lookup unavailable: %s", e)
        return {}
    out: dict[str, dict] = {}
    for r in rows:
        name = (r.get("street_name") or "").strip()
        if not name or name == "?" or not r.get("gp_key"):
            continue
        house = r.get("house_num")
        label = name + (f" {house}{r.get('sfx') or ''}" if house is not None else "")
        slot = out.setdefault(r["gp_key"], {"addresses": [], "total": 0})
        slot["total"] += 1
        if len(slot["addresses"]) < per_parcel:
            slot["addresses"].append(label)
    return out


def _console_sql(src: tuple[str, str], f: dict) -> str:
    """The same filter as one runnable statement, for the /data deep-link.

    Literals rather than parameters because the console is handed text, not a
    prepared statement — this string is never executed here."""
    parts = []
    for col, key in (("settlement", "settlement"), ("settlement_code", "settlement_code"),
                     ("gush", "gush"), ("chelka", "helka"), ("deal_nature", "nature")):
        if f.get(key):
            parts.append(f"{col} = {_lit(f[key])}")
    if f.get("date_from"):
        parts.append(f"{DEAL_SORT_KEY} >= {_lit(f['date_from'].replace('-', ''))}")
    if f.get("date_to"):
        parts.append(f"{DEAL_SORT_KEY} <= {_lit(f['date_to'].replace('-', ''))}")
    if f.get("street") and f.get("settlement"):
        parts.append(_address_console_where(f))
    where = " AND ".join(parts) or "true"
    return (f'SELECT * FROM {_t(src)}\nWHERE {where}\n'
            f'ORDER BY {DEAL_SORT_KEY} DESC\nLIMIT 200')


# ── browse ────────────────────────────────────────────────────────────────────
async def search(filters: dict, limit: int = 50, offset: int = 0,
                 sort: str = "date_desc") -> dict:
    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    order = SORT_MODES.get(sort, SORT_MODES["date_desc"])
    where, args = _where(filters)
    src = await _src()

    rows, counted = await asyncio.gather(
        _fetch(f"""
            SELECT settlement_code, settlement, gush, chelka, sub_chelka, deal_date,
                   deal_amount, declared_amount, deal_nature, portion, year_built,
                   asset_area, room_num
            FROM {_t(src)}
            WHERE {where}
            ORDER BY {order}
            LIMIT {limit} OFFSET {offset}
        """, *args),
        _fetch(f"""
            SELECT count(*) AS n FROM (
              SELECT 1 FROM {_t(src)} WHERE {where} LIMIT {COUNT_CAP + 1}
            ) capped
        """, *args),
    )
    total = counted[0]["n"] if counted else 0
    pairs = [((r.get("gush") or "").strip(), (r.get("chelka") or "").strip()) for r in rows]
    addrs = await addresses_for(pairs)
    return {
        "data": [_deal_row(r) | {
            "settlement": (r.get("settlement") or "").strip() or None,
            "settlement_code": (r.get("settlement_code") or "").strip() or None,
            "gush": g or None,
            "helka": h or None,
            "addresses": addrs.get(f"{g}-{h}", {}).get("addresses", []),
            "addresses_total": addrs.get(f"{g}-{h}", {}).get("total", 0),
        } for r, (g, h) in zip(rows, pairs)],
        "total": min(total, COUNT_CAP),
        "total_capped": total > COUNT_CAP,
        "limit": limit, "offset": offset, "sort": sort,
        "console_sql": _console_sql(src, filters),
        "row_url": _console_url(_console_sql(src, filters)),
    }


async def series(filters: dict) -> list[dict]:
    """Deals and the MEDIAN reported price, per year, under the same filter.

    Median rather than mean: the register mixes a single flat with the sale of a
    whole building, and one such row moves an average by millions."""
    # No settlement or gush filter means no index narrows the rows: the page's
    # default, unfiltered view is three medians per year over all 3.84 M deals,
    # which fits under 10s with one median and not with three. It 500'd 85
    # times in the first 20 minutes after the deals indexes were restored. Such a
    # view is the same for everyone until the next sampling, so it is cached for
    # an hour, gets the aggregate ceiling, and concurrent callers share one query.
    if not any(filters.get(k) for k in ("settlement", "settlement_code", "gush")):
        key = "series:" + repr(sorted((k, v) for k, v in filters.items() if v))
        if sum(k.startswith("series:") for k in _cache) >= 200:
            for k in [k for k in _cache if k.startswith("series:")]:
                _cache.pop(k, None)
                _locks.pop(k, None)
        return await _cached(key, lambda: _series(filters, _AGGREGATE_TIMEOUT_MS))
    return await _series(filters, _TIMEOUT_MS)


async def _series(filters: dict, timeout_ms: int) -> list[dict]:
    where, args = _where(filters)
    src = await _src()
    rows = await _fetch(f"""
        SELECT substr(deal_date, 7, 4) AS year, count(*) AS deals,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY nullif(deal_amount, '')::bigint)
                 AS median_amount,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY nullif(asset_area, '')::bigint)
                 AS median_area,
               round(percentile_disc(0.5) WITHIN GROUP (ORDER BY {PPSQM_NORM}))
                 AS median_ppsqm_normalized
        FROM {_t(src)}
        WHERE {where} AND deal_date ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
        GROUP BY 1 ORDER BY 1
    """, *args, timeout_ms=timeout_ms)
    return [{"year": int(r["year"]), "deals": r["deals"],
             "median_amount": r["median_amount"], "median_area": r["median_area"],
             "median_ppsqm_normalized": _int(r["median_ppsqm_normalized"])}
            for r in rows if (r["year"] or "").isdigit()]


async def breakdown(filters: dict, limit: int = 20) -> list[dict]:
    """The deal types present under the filter, biggest first.

    Unfiltered this is a whole-table group-by (measured at 4.7s in production),
    which is why natures() caches it and why it gets the aggregate ceiling."""
    where, args = _where(filters)
    src = await _src()
    rows = await _fetch(f"""
        SELECT nullif(btrim(deal_nature), '') AS nature, count(*) AS deals,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY nullif(deal_amount, '')::bigint)
                 AS median_amount,
               round(percentile_disc(0.5) WITHIN GROUP (ORDER BY {PPSQM_NORM}))
                 AS median_ppsqm_normalized
        FROM {_t(src)}
        WHERE {where}
        GROUP BY 1 ORDER BY deals DESC LIMIT {max(1, min(int(limit), 60))}
    """, *args, timeout_ms=_AGGREGATE_TIMEOUT_MS if not filters else _TIMEOUT_MS)
    return [dict(r) | {"median_ppsqm_normalized": _int(r["median_ppsqm_normalized"])}
            for r in rows]


async def compare_settlements(year_from: int, year_to: int, *, nature: str | None = None,
                              min_deals: int = 30, limit: int = 30,
                              order: str = "change_desc") -> list[dict]:
    """Two years, every settlement, side by side: deals and median price in each.

    This is the question the register is most often asked and least able to
    answer one גוש at a time — "where did prices move, and by how much". It is
    one pass over the table rather than a query per settlement, and it is
    deliberately opinionated about honesty in three ways:

    * **Median, not mean**, for the reason stated at the top of this module.
    * **``min_deals`` on BOTH years.** A settlement with four sales in 2019 can
      show a 90% "rise" that is one unusual house. The floor applies to each
      year separately, so a place that stopped selling drops out rather than
      being compared against a handful.
    * **The change is reported alongside both raw medians and both counts**, so
      a caller can see what the percentage is made of.

    ``nature`` is almost always worth passing: comparing all deal types mixes a
    flat with a field, and the mix differs by settlement, which turns a
    composition change into a price change.
    """
    y1, y2 = str(int(year_from)), str(int(year_to))
    floor = max(1, min(int(min_deals), 10_000))
    cap = max(1, min(int(limit), 200))
    orders = {
        "change_desc": "change_pct DESC NULLS LAST",
        "change_asc": "change_pct ASC NULLS LAST",
        "median_desc": "median_to DESC NULLS LAST",
        "deals_desc": "deals_to DESC",
    }
    order_sql = orders.get(order, orders["change_desc"])
    src = await _src()

    where = [f"{DEAL_SORT_KEY} >= $1 || '0101'", f"{DEAL_SORT_KEY} <= $2 || '1231'",
             "btrim(settlement) <> ''"]
    args: list = [y1, y2] if y1 <= y2 else [y2, y1]
    if nature:
        args.append(nature)
        where.append(f"deal_nature = ${len(args)}")

    rows = await _fetch(f"""
        WITH f AS (
          SELECT btrim(settlement) AS settlement,
                 substr(deal_date, 7, 4) AS yr,
                 nullif(deal_amount, '')::bigint AS amt,
                 {PPSQM_NORM} AS ppsqm
          FROM {_t(src)}
          WHERE {' AND '.join(where)}
            AND substr(deal_date, 7, 4) IN ('{y1}', '{y2}')
        ), g AS (
          SELECT settlement,
                 count(*) FILTER (WHERE yr = '{y1}') AS deals_from,
                 count(*) FILTER (WHERE yr = '{y2}') AS deals_to,
                 percentile_disc(0.5) WITHIN GROUP (ORDER BY amt)
                   FILTER (WHERE yr = '{y1}') AS median_from,
                 percentile_disc(0.5) WITHIN GROUP (ORDER BY amt)
                   FILTER (WHERE yr = '{y2}') AS median_to,
                 round(percentile_disc(0.5) WITHIN GROUP (ORDER BY ppsqm)
                   FILTER (WHERE yr = '{y1}')) AS ppsqm_from,
                 round(percentile_disc(0.5) WITHIN GROUP (ORDER BY ppsqm)
                   FILTER (WHERE yr = '{y2}')) AS ppsqm_to
          FROM f GROUP BY 1
        )
        SELECT *, round(100.0 * (median_to - median_from)
                        / nullif(median_from, 0), 1) AS change_pct,
               round(100.0 * (ppsqm_to - ppsqm_from)
                     / nullif(ppsqm_from, 0), 1) AS ppsqm_change_pct
        FROM g
        WHERE deals_from >= {floor} AND deals_to >= {floor}
        ORDER BY {order_sql}
        LIMIT {cap}
    """, *args)
    return [dict(r) | {"ppsqm_from": _int(r["ppsqm_from"]), "ppsqm_to": _int(r["ppsqm_to"]),
                       "ppsqm_change_pct": _num(r["ppsqm_change_pct"]),
                       "change_pct": _num(r["change_pct"]),
                       "year_from": int(y1), "year_to": int(y2)} for r in rows]


# ── pick lists and counters, cached ───────────────────────────────────────────
# Each is a whole-table aggregate over 3.84 M rows. They only move when the
# scraper appends a new sampling, so they are served from a process-local cache
# on the same rationale (and with the same shape) as nadlan_query.stats().
_TTL_SECONDS = 3600.0
_cache: dict[str, tuple[float, object]] = {}
_locks: dict[str, asyncio.Lock] = {}


def invalidate_cache() -> None:
    _cache.clear()


async def _cached(key: str, producer):
    hit = _cache.get(key)
    if hit and time.monotonic() - hit[0] < _TTL_SECONDS:
        return hit[1]
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        hit = _cache.get(key)
        if hit and time.monotonic() - hit[0] < _TTL_SECONDS:
            return hit[1]
        value = await producer()
        _cache[key] = (time.monotonic(), value)
        return value


async def stats() -> dict:
    async def produce():
        src = await _src()
        # Each distinct count is its own subquery: four count(DISTINCT …) in one
        # pass are four sorts of 3.84 M rows, which ran past even the 25s
        # ceiling on 2026-09-27, while a DISTINCT subquery hashes (and min/max
        # ride the date index) — 1.8s measured for the same numbers.
        t = _t(src)
        rows = await _fetch(f"""
            SELECT (SELECT count(*) FROM {t}) AS deals,
                   (SELECT min({DEAL_SORT_KEY}) FROM {t}) AS first_deal,
                   (SELECT max({DEAL_SORT_KEY}) FROM {t}) AS last_deal,
                   (SELECT count(*) FROM (SELECT DISTINCT settlement FROM {t}
                     WHERE settlement IS NOT NULL) s) AS settlements,
                   (SELECT count(*) FROM (SELECT DISTINCT gush, chelka FROM {t}
                     WHERE gush IS NOT NULL AND chelka IS NOT NULL) p) AS parcels,
                   (SELECT count(*) FROM (SELECT DISTINCT deal_nature FROM {t}
                     WHERE deal_nature IS NOT NULL) n) AS natures,
                   (SELECT max(scraped_at) FROM {t}) AS scraped_at
        """, timeout_ms=_AGGREGATE_TIMEOUT_MS)
        s = dict(rows[0]) if rows else {}
        s["first_deal"] = _iso(s.get("first_deal"))
        s["last_deal"] = _iso(s.get("last_deal"))
        # The table is discovered, so the dataset link is only offered when the
        # table we actually read is the one that id names — an append table
        # carries the dataset id's first segment in its name. A link to the
        # wrong version history is worse than none.
        if DATASET_ID.split("-")[0] in src[1]:
            s["dataset_id"] = DATASET_ID
            s["source_url"] = SOURCE_URL
        s["table"] = f"{src[0]}.{src[1]}"
        return s

    return await _cached("stats", produce)


async def settlements() -> list[dict]:
    """Every settlement the register names, with its deal count.

    Grouped by NAME and not by code, and the name is handed back verbatim: it is
    the exact string :func:`search` filters on, so a picker built from this list
    can never produce an empty result through a spelling difference."""
    async def produce():
        src = await _src()
        rows = await _fetch(f"""
            SELECT btrim(settlement) AS settlement,
                   max(nullif(btrim(settlement_code), '')) AS settlement_code,
                   count(*) AS deals,
                   max({DEAL_SORT_KEY}) AS last_deal
            FROM {_t(src)}
            WHERE btrim(settlement) <> ''
            GROUP BY 1 ORDER BY deals DESC
        """, timeout_ms=_AGGREGATE_TIMEOUT_MS)
        return [{"settlement": r["settlement"], "settlement_code": r["settlement_code"],
                 "deals": r["deals"], "last_deal": _iso(r["last_deal"])} for r in rows]

    return await _cached("settlements", produce)


async def natures() -> list[dict]:
    async def produce():
        return await breakdown({}, limit=60)

    return await _cached("natures", produce)


async def is_ready() -> bool:
    """True once the register has rows; the API 503s until the scraper has run."""
    try:
        return bool(await _fetch(f"SELECT 1 FROM {_t(await _src())} LIMIT 1"))
    except Exception:  # noqa: BLE001 — untracked here, or not loaded yet
        return False
