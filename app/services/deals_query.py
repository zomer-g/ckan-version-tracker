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
    return (" AND ".join(clauses) or "true"), args


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
    return {
        "data": [_deal_row(r) | {
            "settlement": (r.get("settlement") or "").strip() or None,
            "settlement_code": (r.get("settlement_code") or "").strip() or None,
            "gush": (r.get("gush") or "").strip() or None,
            "helka": (r.get("chelka") or "").strip() or None,
        } for r in rows],
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
    where, args = _where(filters)
    src = await _src()
    rows = await _fetch(f"""
        SELECT substr(deal_date, 7, 4) AS year, count(*) AS deals,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY nullif(deal_amount, '')::bigint)
                 AS median_amount,
               percentile_disc(0.5) WITHIN GROUP (ORDER BY nullif(asset_area, '')::bigint)
                 AS median_area
        FROM {_t(src)}
        WHERE {where} AND deal_date ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
        GROUP BY 1 ORDER BY 1
    """, *args)
    return [{"year": int(r["year"]), "deals": r["deals"],
             "median_amount": r["median_amount"], "median_area": r["median_area"]}
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
                 AS median_amount
        FROM {_t(src)}
        WHERE {where}
        GROUP BY 1 ORDER BY deals DESC LIMIT {max(1, min(int(limit), 60))}
    """, *args, timeout_ms=_AGGREGATE_TIMEOUT_MS if not filters else _TIMEOUT_MS)
    return [dict(r) for r in rows]


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
                 nullif(deal_amount, '')::bigint AS amt
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
                   FILTER (WHERE yr = '{y2}') AS median_to
          FROM f GROUP BY 1
        )
        SELECT *, round(100.0 * (median_to - median_from)
                        / nullif(median_from, 0), 1) AS change_pct
        FROM g
        WHERE deals_from >= {floor} AND deals_to >= {floor}
        ORDER BY {order_sql}
        LIMIT {cap}
    """, *args)
    return [dict(r) | {"year_from": int(y1), "year_to": int(y2)} for r in rows]


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
        rows = await _fetch(f"""
            SELECT count(*) AS deals,
                   min({DEAL_SORT_KEY}) AS first_deal,
                   max({DEAL_SORT_KEY}) AS last_deal,
                   count(DISTINCT settlement) AS settlements,
                   count(DISTINCT (gush || '-' || chelka)) AS parcels,
                   count(DISTINCT deal_nature) AS natures,
                   max(scraped_at) AS scraped_at
            FROM {_t(src)}
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
