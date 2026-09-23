"""Cross-chain queries over the price-transparency source (שקיפות מחירים).

GOVSCRAPER's ``prices`` source publishes ONE dataset per retailer — Shufersal,
Rami Levy, Victory and ~30 more — each with the same six tables (see
govscraper/scrapers/prices/_engine.py in the worker repo):

    מחירים         a price STATE at a store: (sub_chain_id, store_id, item_code,
                   item_price, …, first_seen_date, last_seen_date)
    מוצרים         the chain's description of each item code
    מבצעים         a promotion at a store
    פריטי מבצעים   the items a promotion covers, chain-wide
    סניפים         the chain's stores
    כיסוי יומי     one row per store per snapshot read

Storage and refresh stay per chain on purpose — each chain publishes on its
own site, on its own schedule, and fails on its own. What a person asks is
across chains ("where is this cheapest", "which chain is cheapest for my
basket in Haifa"), so this module is where the chains become one market: every
query is a UNION ALL over the chains' tables, built at query time from the
datasets that exist, never hardcoded.

**Rows are states, not readings.** A row with an empty ``last_seen_date`` is in
force as of that store's latest snapshot (its newest ``כיסוי יומי`` row); a row
with a date ended on that date. "The current price" is therefore the open row,
and "as of when" is the coverage date — both are reported with every answer.

Every value is text in the append DB (empty string, not NULL, for missing).
Prices are parsed in Python (``_f``) rather than cast in SQL, so one chain's
"לא ידוע" in a price column cannot abort a query that spans thirty.
"""
from __future__ import annotations

import asyncio
import logging
import re
import statistics
import time
from datetime import date

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.services import append_store

logger = logging.getLogger(__name__)

SCRAPER_KIND = "prices"
SOURCE_PAGE = "https://www.gov.il/he/pages/cpfta_prices_regulations"

# Resource name → role. The names are the worker's wire contract.
RESOURCES = {
    "prices": "מחירים",
    "products": "מוצרים",
    "promos": "מבצעים",
    "promo_items": "פריטי מבצעים",
    "stores": "סניפים",
    "coverage": "כיסוי יומי",
}

MAX_ROWS = 200
SQL_TIMEOUT_MS = 20000
MAX_ITEMS = 40
_CACHE_TTL = 300

CAVEATS = [
    "המחירים הם כפי שפורסמו על ידי הרשתות לפי חוק קידום התחרות בענף המזון — "
    "לא תוקנו, לא הושלמו ולא אומתו מול הקופה.",
    "'מחיר נוכחי' הוא המחיר בקובץ המלא האחרון שהסניף פרסם; תאריך התמונה (as_of) "
    "מצורף לכל סניף ולכל רשת. רשתות מפרסמות בשעות שונות, ורשת שלא פרסמה היום "
    "מוצגת לפי הקובץ האחרון שלה.",
    "מבצעים אינם מקוזזים מהמחיר בהשוואה — הם מוצגים בנפרד, כי רבים מהם מותנים "
    "(כמות מינימלית, מועדון, קופון).",
    "מוצר שקול (is_weighted=1) מתומחר לפי יחידת מידה (בדרך כלל ק\"ג); השוו "
    "unit_of_measure_price ולא item_price.",
    "העיר של סניף נקבעת מקוד היישוב שהרשת פרסמה, ובהיעדרו — משם הסניף והכתובת.",
]

_DIGITS = re.compile(r"^\d{1,20}$")


class NotCollectedYet(RuntimeError):
    """No chain of this source has published tables yet."""


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

_cache: dict = {"at": 0.0, "chains": None}
_indexed: set[str] = set()


def _qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def chain_name_of(ds) -> str:
    title = (ds.title or "").strip()
    for sep in (" — ", " - "):
        if sep in title:
            return title.split(sep, 1)[1].strip()
    return title or (ds.scraper_config or {}).get("chain", "")


async def chains(db: AsyncSession, *, refresh: bool = False) -> list[dict]:
    """Every chain dataset with at least a prices table, and its physical tables.

    ``[{chain, name, dataset_id, source_url, tables: {role: table}}]``. Tables
    are named deterministically from the resource name
    (``append_store.table_name_for_scraper_resource``) and kept only if they
    exist, so a chain that has not published yet is simply absent."""
    now = time.monotonic()
    if not refresh and _cache["chains"] is not None and now - _cache["at"] < _CACHE_TTL:
        return _cache["chains"]
    rows = (await db.execute(
        select(TrackedDataset).where(
            TrackedDataset.scraper_config["kind"].astext == SCRAPER_KIND)
    )).scalars().all()
    candidates: list[tuple] = []
    for ds in rows:
        key = str((ds.scraper_config or {}).get("chain") or "")
        if not key or key == "index":
            continue
        for role, resource in RESOURCES.items():
            candidates.append((ds, key, role,
                               append_store.table_name_for_scraper_resource(ds, resource)))
    existing: set[str] = set()
    if candidates:
        pool = await append_store.get_readonly_pool()
        async with pool.acquire() as conn:
            found = await conn.fetch(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = ANY($1::text[])",
                [c[3] for c in candidates])
        existing = {r["table_name"] for r in found}
    by_ds: dict = {}
    for ds, key, role, table in candidates:
        entry = by_ds.setdefault(ds.id, {
            "chain": key, "name": chain_name_of(ds), "dataset_id": str(ds.id),
            "source_url": ds.source_url, "tables": {},
            "last_polled_at": ds.last_polled_at.isoformat() if ds.last_polled_at else None,
        })
        if table in existing:
            entry["tables"][role] = table
    out = sorted((e for e in by_ds.values() if "prices" in e["tables"]),
                 key=lambda e: e["name"])
    _cache.update(at=now, chains=out)
    _schedule_indexes(out)
    return out


def _schedule_indexes(entries: list[dict]) -> None:
    """Create the lookup indexes a table needs, once per process, off the
    request path. Without them every item lookup is a sequential scan of
    millions of rows per chain."""
    todo = [e for e in entries
            if any(t not in _indexed for t in e["tables"].values())]
    if not todo:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(_ensure_indexes(todo))


# Item lookups compare the code with its leading zeros stripped (see
# ``bare_codes``), so that is what the item index is built on.
_BARE = "ltrim(\"item_code\", '0')"
_INDEXES = {
    "prices": [("bare", [_BARE]), ("store", ['"store_id"', '"item_code"'])],
    "products": [("bare", [_BARE])],
    "promo_items": [("bare", [_BARE]), ("promo", ['"promotion_id"'])],
    "promos": [("promo", ['"promotion_id"']), ("store", ['"store_id"'])],
}


async def _ensure_indexes(entries: list[dict]) -> None:
    try:
        pool = await append_store.get_pool()
    except Exception as e:  # noqa: BLE001 — an optimisation must never break a read
        logger.warning("prices: no write pool for indexes: %s", e)
        return
    for entry in entries:
        for role, table in entry["tables"].items():
            if table in _indexed:
                continue
            try:
                async with pool.acquire() as conn:
                    for suffix, cols in _INDEXES.get(role, []):
                        name = append_store._index_name(table, f"px_{suffix}")
                        await conn.execute(
                            f"CREATE INDEX IF NOT EXISTS {_qi(name)} ON {_qi(table)} "
                            f"({', '.join(cols)})")
                _indexed.add(table)
            except Exception as e:  # noqa: BLE001
                logger.warning("prices: index on %s failed: %s", table, e)


async def _require(db: AsyncSession, chain_filter: list[str] | None, role: str = "prices"):
    entries = await chains(db)
    if chain_filter:
        wanted = {c.strip().lower() for c in chain_filter if c and c.strip()}
        entries = [e for e in entries
                   if e["chain"].lower() in wanted
                   or e["chain"].split(":", 1)[-1].lower() in wanted
                   or e["name"].lower() in wanted]
    entries = [e for e in entries if role in e["tables"]]
    if not entries:
        raise NotCollectedYet(
            "אין עדיין רשתות שפורסמו עם הטבלה המבוקשת"
            + (" (לפי סינון הרשתות שביקשת)" if chain_filter else "")
            + ". המאגרים נוצרים ב-over.org.il ומתמלאים בגריפה הראשונה — "
              "זהו מצב 'טרם נאסף', לא 'אין נתונים'.")
    return entries


async def _fetch(sql: str, params: list) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {SQL_TIMEOUT_MS}")
            rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


def _f(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def bare_codes(codes) -> list[str]:
    """Barcodes in the one form every query compares: leading zeros stripped.

    Retailers pad the same code differently (``7290000066134``,
    ``0001334277``, ``1334277``), and no list of paddings is complete, so both
    sides are compared bare — ``ltrim(item_code, '0')``, which the item index
    is built on."""
    if isinstance(codes, str):
        codes = [codes]
    out = []
    for code in codes or []:
        code = str(code or "").strip()
        if code:
            bare = code.lstrip("0") if _DIGITS.match(code) else code
            if bare not in out:
                out.append(bare)
    return out


def _item_match(alias: str, param: int) -> str:
    col = f"{alias}.item_code" if alias else "item_code"
    return f"ltrim({col}, '0') = ANY(${param}::text[])"


def _store_city_sql(alias: str = "s") -> tuple[str, str]:
    """(city name, city code) of a store row. A chain writes a CBS settlement
    code (most), a Hebrew name (laibcatalog, Wolt), or nothing (City Market,
    whose address sits in the store name)."""
    c = f"{alias}.city"
    code = (f"(CASE WHEN {c} ~ '^[1-9][0-9]{{0,5}}$' THEN {c}::int "
            f"WHEN {c} <> '' THEN public.over_settlement_code({c}) END)")
    name = (f"COALESCE((SELECT oset.name FROM public.over_settlements oset "
            f"WHERE oset.code = {code} LIMIT 1), NULLIF({c}, ''))")
    return name, code


async def city_code(city: str | None) -> int | None:
    if not city:
        return None
    rows = await _fetch("SELECT public.over_settlement_code($1) AS code", [city])
    return rows[0]["code"] if rows else None


def _as_of(coverage_table: str | None, alias: str = "p") -> str:
    if not coverage_table:
        return "''"
    return (f"(SELECT max(cv.snapshot_date) FROM public.{_qi(coverage_table)} cv "
            f"WHERE cv.store_id = {alias}.store_id)")


# ---------------------------------------------------------------------------
# Chains
# ---------------------------------------------------------------------------

async def list_chains(db: AsyncSession) -> list[dict]:
    entries = await chains(db)
    out = []
    for e in entries:
        info = {"chain": e["chain"], "name": e["name"], "dataset_id": e["dataset_id"],
                "source_url": e["source_url"], "last_polled_at": e["last_polled_at"],
                "stores": None, "latest_snapshot": None}
        cov = e["tables"].get("coverage")
        if cov:
            try:
                rows = await _fetch(
                    f"SELECT count(DISTINCT (sub_chain_id, store_id)) AS stores, "
                    f"max(snapshot_date) AS latest FROM public.{_qi(cov)}", [])
                if rows:
                    info["stores"] = rows[0]["stores"]
                    info["latest_snapshot"] = rows[0]["latest"]
            except Exception as ex:  # noqa: BLE001
                logger.warning("prices: coverage of %s unreadable: %s", e["chain"], ex)
        out.append(info)
    return out


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------

async def search_products(db: AsyncSession, *, q: str, chains_filter: list[str] | None = None,
                          limit: int = 50) -> dict:
    """Items whose code or name matches, across chains, grouped by item code."""
    q = (q or "").strip()
    if len(q) < 2:
        raise ValueError("q חייב להכיל לפחות 2 תווים (שם מוצר או ברקוד)")
    limit = max(1, min(int(limit or 50), MAX_ROWS))
    entries = await _require(db, chains_filter, "products")
    params: list = []
    if _DIGITS.match(q):
        params.append(bare_codes(q))
        where = _item_match("", 1)
    else:
        terms = [t for t in re.split(r"\s+", q) if t][:6]
        conds = []
        for t in terms:
            params.append(f"%{t}%")
            n = len(params)
            conds.append(f"(item_name ILIKE ${n} OR manufacturer_name ILIKE ${n} "
                         f"OR manufacturer_item_description ILIKE ${n})")
        where = " AND ".join(conds)
    per_chain = limit * 2
    selects = [
        f"(SELECT {_lit(e['chain'])} AS chain, {_lit(e['name'])} AS chain_name, "
        f"item_code, item_name, manufacturer_name, quantity, unit_qty, unit_of_measure, "
        f"is_weighted, first_seen_date FROM public.{_qi(e['tables']['products'])} "
        f"WHERE last_seen_date = '' AND {where} LIMIT {per_chain})"
        for e in entries]
    rows = await _fetch("\nUNION ALL\n".join(selects), params)
    grouped: dict[str, dict] = {}
    for r in rows:
        g = grouped.setdefault(r["item_code"], {
            "item_code": r["item_code"], "names": [], "manufacturer": r["manufacturer_name"],
            "quantity": r["quantity"], "unit_qty": r["unit_qty"],
            "unit_of_measure": r["unit_of_measure"], "is_weighted": r["is_weighted"],
            "chains": []})
        if r["item_name"] and r["item_name"] not in g["names"]:
            g["names"].append(r["item_name"])
        if not g["manufacturer"] and r["manufacturer_name"]:
            g["manufacturer"] = r["manufacturer_name"]
        g["chains"].append({"chain": r["chain"], "name": r["chain_name"]})
    items = sorted(grouped.values(), key=lambda g: (-len(g["chains"]), g["names"][:1]))
    # A real barcode (7+ digits, sold by several chains) is the useful kind of
    # hit; an internal code like "1000" is chain-specific and ranks last.
    items.sort(key=lambda g: (len(g["item_code"]) < 7, -len(g["chains"])))
    return {"items": items[:limit], "count": len(items), "truncated": len(items) > limit,
            "chains_searched": [e["chain"] for e in entries]}


# ---------------------------------------------------------------------------
# Current prices across chains
# ---------------------------------------------------------------------------

async def _current_prices(entries: list[dict], codes: list[str], store_where: str = "",
                          extra_params: list | None = None) -> list[dict]:
    """Open price rows for these item codes at every store of these chains,
    with the store's name/city and its snapshot date."""
    params: list = [codes] + list(extra_params or [])
    city_name, city_code_sql = _store_city_sql("s")
    selects = []
    for e in entries:
        t = e["tables"]
        stores = t.get("stores")
        store_join = (
            f"LEFT JOIN LATERAL (SELECT s.store_name, s.address, s.city, "
            f"{city_name} AS city_name, {city_code_sql} AS city_code "
            f"FROM public.{_qi(stores)} s WHERE s.store_id = p.store_id "
            f"AND s.last_seen_date = '' ORDER BY (s.sub_chain_id = p.sub_chain_id) DESC "
            f"LIMIT 1) st ON true") if stores else (
            "LEFT JOIN LATERAL (SELECT NULL::text AS store_name, NULL::text AS address, "
            "NULL::text AS city, NULL::text AS city_name, NULL::int AS city_code) st ON true")
        products = t.get("products")
        name_sql = (f"(SELECT pr.item_name FROM public.{_qi(products)} pr "
                    f"WHERE ltrim(pr.item_code, '0') = ltrim(p.item_code, '0') "
                    f"AND pr.last_seen_date = '' LIMIT 1)"
                    if products else "NULL::text")
        selects.append(
            f"(SELECT {_lit(e['chain'])} AS chain, {_lit(e['name'])} AS chain_name, "
            f"p.item_code, {name_sql} AS item_name, p.sub_chain_id, p.store_id, "
            f"p.item_price, p.unit_of_measure_price, p.first_seen_date, p.price_update_time, "
            f"{_as_of(t.get('coverage'))} AS as_of, "
            f"st.store_name, st.address, st.city_name, st.city_code "
            f"FROM public.{_qi(t['prices'])} p {store_join} "
            f"WHERE {_item_match('p', 1)} AND p.last_seen_date = ''{store_where})")
    return await _fetch("\nUNION ALL\n".join(selects), params)


def _city_filter(rows: list[dict], city: str | None, code: int | None) -> list[dict]:
    if not city:
        return rows
    needle = city.strip()
    out = []
    for r in rows:
        if code is not None and r.get("city_code") == code:
            out.append(r)
        elif r.get("city_code") is None and needle and needle in " ".join(
                filter(None, [r.get("store_name"), r.get("address"), r.get("city_name")])):
            out.append(r)
    return out


def _store_view(r: dict) -> dict:
    return {"chain": r["chain"], "chain_name": r["chain_name"],
            "store_id": r["store_id"], "sub_chain_id": r["sub_chain_id"],
            "store_name": r.get("store_name"), "city": r.get("city_name"),
            "address": r.get("address"), "price": _f(r["item_price"]),
            "unit_price": _f(r["unit_of_measure_price"]),
            "since": r["first_seen_date"], "as_of": r.get("as_of") or None}


async def compare_prices(db: AsyncSession, *, item_codes: list[str], city: str | None = None,
                         chains_filter: list[str] | None = None, top: int = 5) -> dict:
    """Current price of each item at every chain: min / median / max over the
    chain's stores, and the cheapest stores overall."""
    codes = bare_codes(item_codes[:MAX_ITEMS])
    if not codes:
        raise ValueError("נדרש לפחות ברקוד אחד (item_code)")
    top = max(1, min(int(top or 5), 50))
    entries = await _require(db, chains_filter)
    rows = await _current_prices(entries, codes)
    code = await city_code(city) if city else None
    rows = _city_filter(rows, city, code)
    by_item: dict[str, list[dict]] = {}
    for r in rows:
        by_item.setdefault(r["item_code"].lstrip("0") or "0", []).append(r)
    items = []
    for key, group in by_item.items():
        per_chain: dict[str, list[dict]] = {}
        for r in group:
            per_chain.setdefault(r["chain"], []).append(r)
        chain_rows = []
        for chain_key, crs in per_chain.items():
            prices = [p for p in (_f(r["item_price"]) for r in crs) if p is not None]
            if not prices:
                continue
            cheapest = min(crs, key=lambda r: _f(r["item_price"]) or 1e18)
            chain_rows.append({
                "chain": chain_key, "chain_name": crs[0]["chain_name"],
                "stores": len(crs), "min_price": min(prices),
                "median_price": round(statistics.median(prices), 2),
                "max_price": max(prices),
                "as_of": max((r.get("as_of") or "") for r in crs) or None,
                "cheapest_store": _store_view(cheapest)})
        chain_rows.sort(key=lambda c: c["min_price"])
        stores = sorted((r for r in group if _f(r["item_price"]) is not None),
                        key=lambda r: _f(r["item_price"]))[:top]
        names = [r["item_name"] for r in group if r.get("item_name")]
        items.append({
            "item_code": group[0]["item_code"],
            "item_name": max(set(names), key=names.count) if names else None,
            "chains": chain_rows,
            "cheapest_stores": [_store_view(r) for r in stores],
            "store_count": len(group)})
    missing = [c for c in item_codes[:MAX_ITEMS]
               if (c.strip().lstrip("0") or "0") not in by_item]
    return {"items": items, "not_found": missing, "city": city,
            "city_code": code, "chains_searched": [e["chain"] for e in entries]}


async def compare_basket(db: AsyncSession, *, basket: list[dict], city: str | None = None,
                         chains_filter: list[str] | None = None,
                         top: int = 10) -> dict:
    """Total cost of a basket at each store, cheapest first.

    ``basket`` = [{item_code, quantity}]. Stores are ranked by how many of
    the basket's items they carry, then by total: a store that is cheap
    because it lacks half the basket is not the cheapest store."""
    wanted: dict[str, float] = {}
    for line in basket[:MAX_ITEMS]:
        code = str(line.get("item_code") or "").strip()
        if not code:
            continue
        qty = _f(line.get("quantity")) or 1.0
        wanted[code.lstrip("0") or "0"] = wanted.get(code.lstrip("0") or "0", 0) + qty
    if not wanted:
        raise ValueError("הסל ריק — נדרשים item_code (ואופציונלית quantity)")
    if not city and not chains_filter:
        raise ValueError("השוואת סל דורשת עיר (city) או רשימת רשתות (chains), "
                         "כדי לא להשוות אלפי סניפים בכל הארץ")
    top = max(1, min(int(top or 10), 50))
    codes = bare_codes(list(wanted))
    entries = await _require(db, chains_filter)
    rows = await _current_prices(entries, codes)
    code = await city_code(city) if city else None
    rows = _city_filter(rows, city, code)
    stores: dict[tuple, dict] = {}
    for r in rows:
        price = _f(r["item_price"])
        if price is None:
            continue
        key = (r["chain"], r["sub_chain_id"], r["store_id"])
        s = stores.setdefault(key, {**_store_view(r), "total": 0.0, "lines": {},
                                    "price": None, "unit_price": None, "since": None})
        item = r["item_code"].lstrip("0") or "0"
        if item in s["lines"]:
            continue
        qty = wanted.get(item, 1.0)
        s["lines"][item] = {"item_code": r["item_code"], "item_name": r.get("item_name"),
                            "quantity": qty, "price": price,
                            "line_total": round(price * qty, 2)}
        s["total"] += price * qty
    ranked = sorted(stores.values(), key=lambda s: (-len(s["lines"]), s["total"]))
    out = []
    for s in ranked[:top]:
        missing = [c for c in wanted if c not in s["lines"]]
        out.append({k: v for k, v in s.items() if k not in ("price", "unit_price", "since")}
                   | {"total": round(s["total"], 2), "items_found": len(s["lines"]),
                      "items_missing": missing, "lines": list(s["lines"].values())})
    best_per_chain: dict[str, dict] = {}
    for s in ranked:
        best_per_chain.setdefault(s["chain"], {
            "chain": s["chain"], "chain_name": s["chain_name"], "store_id": s["store_id"],
            "store_name": s["store_name"], "total": round(s["total"], 2),
            "items_found": len(s["lines"])})
    return {"basket_size": len(wanted), "stores_compared": len(stores), "city": city,
            "city_code": code, "stores": out,
            "best_per_chain": sorted(best_per_chain.values(),
                                     key=lambda c: (-c["items_found"], c["total"]))}


# ---------------------------------------------------------------------------
# Stores
# ---------------------------------------------------------------------------

async def find_stores(db: AsyncSession, *, city: str | None = None, q: str | None = None,
                      chains_filter: list[str] | None = None, limit: int = 100) -> dict:
    limit = max(1, min(int(limit or 100), MAX_ROWS * 5))
    entries = await _require(db, chains_filter, "stores")
    city_name, city_code_sql = _store_city_sql("s")
    params: list = []
    where = "s.last_seen_date = ''"
    if q:
        params.append(f"%{q.strip()}%")
        where += f" AND (s.store_name ILIKE $1 OR s.address ILIKE $1)"
    selects = [
        f"(SELECT {_lit(e['chain'])} AS chain, {_lit(e['name'])} AS chain_name, "
        f"s.sub_chain_id, s.sub_chain_name, s.store_id, s.store_name, s.address, "
        f"s.zip_code, s.store_type, {city_name} AS city_name, {city_code_sql} AS city_code, "
        f"s.first_seen_date, {_as_of(e['tables'].get('coverage'), 's')} AS as_of "
        f"FROM public.{_qi(e['tables']['stores'])} s WHERE {where})"
        for e in entries]
    rows = await _fetch("\nUNION ALL\n".join(selects), params)
    code = await city_code(city) if city else None
    rows = _city_filter(rows, city, code)
    rows.sort(key=lambda r: (r["chain_name"], r.get("city_name") or "", r["store_name"] or ""))
    return {"stores": rows[:limit], "count": len(rows), "truncated": len(rows) > limit,
            "city": city, "city_code": code}


async def store_prices(db: AsyncSession, *, chain: str, store_id: str, q: str | None = None,
                       limit: int = 100, offset: int = 0) -> dict:
    """What one store charges right now, optionally for items matching ``q``."""
    entries = await _require(db, [chain])
    e = entries[0]
    t = e["tables"]
    limit = max(1, min(int(limit or 100), MAX_ROWS))
    params: list = [str(int(store_id)) if str(store_id).strip().isdigit() else str(store_id)]
    name_join = (f"LEFT JOIN public.{_qi(t['products'])} pr "
                 f"ON ltrim(pr.item_code, '0') = ltrim(p.item_code, '0') "
                 f"AND pr.last_seen_date = ''") if t.get("products") else ""
    name_col = "pr.item_name, pr.manufacturer_name, pr.quantity, pr.unit_qty" if name_join \
        else "NULL AS item_name, NULL AS manufacturer_name, NULL AS quantity, NULL AS unit_qty"
    where = "p.store_id = $1 AND p.last_seen_date = ''"
    if q:
        q = q.strip()
        if _DIGITS.match(q):
            params.append(bare_codes(q))
            where += " AND " + _item_match("p", 2)
        elif name_join:
            params.append(f"%{q}%")
            where += " AND pr.item_name ILIKE $2"
    rows = await _fetch(
        f"SELECT p.item_code, {name_col}, p.item_price, p.unit_of_measure_price, "
        f"p.first_seen_date, p.price_update_time, p.sub_chain_id "
        f"FROM public.{_qi(t['prices'])} p {name_join} WHERE {where} "
        f"ORDER BY {'pr.item_name' if name_join else 'p.item_code'} "
        f"LIMIT {limit} OFFSET {max(0, int(offset or 0))}", params)
    as_of = None
    if t.get("coverage"):
        cov = await _fetch(f"SELECT max(snapshot_date) AS d FROM public.{_qi(t['coverage'])} "
                           f"WHERE store_id = $1", params[:1])
        as_of = cov[0]["d"] if cov else None
    return {"chain": e["chain"], "chain_name": e["name"], "store_id": params[0],
            "as_of": as_of, "items": rows, "limit": limit, "offset": offset}


# ---------------------------------------------------------------------------
# History and promotions
# ---------------------------------------------------------------------------

async def price_history(db: AsyncSession, *, item_code: str,
                        chains_filter: list[str] | None = None,
                        store_id: str | None = None, limit: int = 200) -> dict:
    """Every price state of an item: when each price started and ended, per store.

    A row's ``last_seen_date`` empty = still the price; otherwise the last day
    it was seen. Also a per-chain timeline: the range of prices across the
    chain's stores for each date a state began."""
    codes = bare_codes(item_code)
    if not codes:
        raise ValueError("נדרש item_code")
    limit = max(1, min(int(limit or 200), 2000))
    entries = await _require(db, chains_filter)
    params: list = [codes]
    where = _item_match("", 1)
    if store_id:
        params.append(str(int(store_id)) if str(store_id).strip().isdigit() else str(store_id))
        where += " AND store_id = $2"
    selects = [
        f"(SELECT {_lit(e['chain'])} AS chain, {_lit(e['name'])} AS chain_name, "
        f"sub_chain_id, store_id, item_price, unit_of_measure_price, first_seen_date, "
        f"last_seen_date, price_update_time FROM public.{_qi(e['tables']['prices'])} "
        f"WHERE {where})" for e in entries]
    rows = await _fetch("SELECT * FROM (" + "\nUNION ALL\n".join(selects) + ") h "
                        f"ORDER BY first_seen_date DESC, chain, store_id LIMIT {limit + 1}",
                        params)
    truncated = len(rows) > limit
    rows = rows[:limit]
    timeline: dict[tuple, list[float]] = {}
    for r in rows:
        p = _f(r["item_price"])
        if p is not None:
            timeline.setdefault((r["chain"], r["first_seen_date"]), []).append(p)
    summary = [{"chain": c, "date": d, "states": len(v), "min_price": min(v),
                "max_price": max(v)} for (c, d), v in sorted(timeline.items(),
                                                             key=lambda kv: kv[0][1])]
    return {"item_code": item_code, "states": rows, "truncated": truncated,
            "timeline": summary}


async def item_promotions(db: AsyncSession, *, item_code: str,
                          chains_filter: list[str] | None = None,
                          active_only: bool = True, limit: int = 100) -> dict:
    """The promotions that cover an item, per chain, with how many stores run each."""
    codes = bare_codes(item_code)
    if not codes:
        raise ValueError("נדרש item_code")
    entries = await _require(db, chains_filter, "promo_items")
    entries = [e for e in entries if "promos" in e["tables"]]
    if not entries:
        raise NotCollectedYet("אין עדיין טבלאות מבצעים שפורסמו.")
    today = date.today().isoformat()
    params: list = [codes]
    active = ""
    if active_only:
        params.append(today)
        active = " AND (pm.end_date = '' OR left(pm.end_date, 10) >= $2)"
    selects = [
        f"(SELECT {_lit(e['chain'])} AS chain, {_lit(e['name'])} AS chain_name, "
        f"pi.promotion_id, pi.item_code, pi.discounted_price, pi.discount_rate, "
        f"pi.min_qty, pi.max_qty, pi.reward_type, pi.is_gift_item, "
        f"min(pm.description) AS description, min(pm.start_date) AS start_date, "
        f"max(pm.end_date) AS end_date, min(pm.club_id) AS club_id, "
        f"min(pm.discounted_price) AS promo_discounted_price, "
        f"min(pm.min_qty) AS promo_min_qty, min(pm.additional_restrictions) AS restrictions, "
        f"count(DISTINCT (pm.sub_chain_id, pm.store_id)) AS stores "
        f"FROM public.{_qi(e['tables']['promo_items'])} pi "
        f"JOIN public.{_qi(e['tables']['promos'])} pm ON pm.promotion_id = pi.promotion_id "
        f"AND pm.last_seen_date = ''{active} "
        f"WHERE {_item_match('pi', 1)} AND pi.last_seen_date = '' "
        f"GROUP BY pi.promotion_id, pi.item_code, pi.discounted_price, pi.discount_rate, "
        f"pi.min_qty, pi.max_qty, pi.reward_type, pi.is_gift_item)" for e in entries]
    rows = await _fetch("\nUNION ALL\n".join(selects), params)
    rows.sort(key=lambda r: (-(r["stores"] or 0), r["chain"]))
    limit = max(1, min(int(limit or 100), MAX_ROWS))
    return {"item_code": item_code, "promotions": rows[:limit], "count": len(rows),
            "truncated": len(rows) > limit, "active_only": active_only}
