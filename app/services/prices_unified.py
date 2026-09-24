"""The price-transparency source as SIX UNIFORM TABLES — one per kind of row.

Every chain is its own dataset with its own six physical tables (see
prices_query.py). This module lays SQL VIEWS over them, one per role, that
UNION ALL every chain into a single table with one fixed schema:

    prices_market            every price state at every store, enriched with the
                             product's name/size and the store's name/city
    prices_products          the chains' item descriptions
    prices_stores            the chains' stores, with a resolved CBS city
    prices_promotions        promotions per store
    prices_promotion_items   the items each promotion covers
    prices_coverage          which snapshot each store's data comes from

Views, not a copied table: the chains hold ~16M price states between them, and
a copy would double the storage and go stale between its rebuilds. A view
costs nothing and is always current; a filter on it (``item_code_bare``,
``chain``, ``store_id``, ``is_current``) is pushed down into every chain's
branch, where the indexes prices_query builds serve it.

The same views are the public /api/prices/table/{name} endpoint and are
queryable by name in the /data SQL console and the SQL MCP, so a person, the
API and an agent all read ONE table with ONE set of column names.

A chain's table can be missing a column (a chain that never publishes
``manufacture_country``); its branch then reads NULL for it rather than being
dropped. Prices are cast to numeric only when they look like a number, so one
chain's junk cannot abort a query across thirty.

The views are rebuilt when the set of chain tables (or their columns) changes,
detected by a signature kept in the view's comment. ``append_store.drop_table``
drops with CASCADE so a view never blocks a table reset; the next check
recreates it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession

from app.services import append_store
from app.services import prices_query as pq
from app.services.nadlan_query import _console_url

logger = logging.getLogger(__name__)

_NUM = "^-?[0-9]+(\\.[0-9]+)?$"
_CHECK_TTL = 300
_state: dict = {"checked_at": 0.0}


def _qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# The uniform schema
# ---------------------------------------------------------------------------
# Each view: the role it unions, and its columns in order as
# (name, kind, Hebrew description). kind: "text" = the chain's column as is,
# "num" = numeric when it parses, "date_end" = NULL for an open state.
# Columns every view starts with are added by _branch (chain, chain_name, dataset_id).

COMMON = [
    ("chain", "meta", "מפתח הרשת (למשל cerberus:ramilevi)"),
    ("chain_name", "meta", "שם הרשת כפי שהיא מפרסמת אותו בקובץ הסניפים"),
    ("dataset_id", "meta", "מזהה המאגר של הרשת ב-over.org.il (/versions/<id>)"),
]
_STATE = [
    ("first_seen_date", "text", "היום הראשון שבו נראה המצב הזה (YYYY-MM-DD)"),
    ("last_seen_date", "date_end", "היום האחרון שבו נראה; NULL = עדיין בתוקף"),
    ("is_current", "current", "האם זה המצב שבתוקף בקובץ האחרון של הסניף"),
]

VIEWS: dict[str, dict] = {
    "prices_market": {
        "role": "prices",
        "title": "מחירים — כל הרשתות",
        "description": "כל מצב מחיר של כל מוצר בכל סניף בכל רשת, עם שם המוצר, גודלו, "
                       "שם הסניף והעיר. שורה = מחיר אחד שהיה בתוקף מ-first_seen_date "
                       "ועד last_seen_date (NULL = עדיין בתוקף).",
        "columns": [
            ("sub_chain_id", "text", "מספר תת-הרשת"),
            ("store_id", "text", "מספר הסניף ברשת"),
            ("store_name", "store", "שם הסניף"),
            ("city", "store", "העיר (שם רשמי לפי קוד הלמ\"ס כשהרשת פרסמה קוד)"),
            ("city_code", "store", "סמל היישוב של הלמ\"ס"),
            ("address", "store", "כתובת הסניף"),
            ("item_code", "text", "ברקוד / קוד הפריט כפי שהרשת פרסמה"),
            ("item_code_bare", "bare", "הקוד בלי אפסים מובילים — המפתח להשוואה בין רשתות"),
            ("item_name", "product", "שם המוצר אצל הרשת"),
            ("manufacturer_name", "product", "היצרן"),
            ("quantity", "product", "כמות באריזה"),
            ("unit_qty", "product", "יחידת הכמות (גרם, ליטר…)"),
            ("unit_of_measure", "product", "יחידת המידה למחיר ליחידה"),
            ("is_weighted", "product", "1 = מוצר שקול"),
            ("item_price", "num", "המחיר (₪)"),
            ("unit_of_measure_price", "num", "המחיר ליחידת מידה (₪)"),
            ("allow_discount", "text", "האם מותרת הנחה"),
            ("item_status", "text", "סטטוס הפריט"),
            ("price_update_time", "text", "מועד עדכון המחיר לפי הרשת"),
            *_STATE,
            ("as_of", "as_of", "תאריך הקובץ האחרון שהסניף פרסם"),
        ],
    },
    "prices_products": {
        "role": "products",
        "title": "מוצרים — כל הרשתות",
        "description": "תיאור כל קוד פריט אצל כל רשת: שם, יצרן, ארץ ייצור, גודל ויחידה.",
        "columns": [
            ("item_code", "text", "ברקוד / קוד הפריט"),
            ("item_code_bare", "bare", "הקוד בלי אפסים מובילים"),
            ("item_type", "text", "סוג פריט (1 = ברקוד תקני, 0 = קוד פנימי)"),
            ("item_name", "text", "שם המוצר"),
            ("manufacturer_name", "text", "היצרן"),
            ("manufacture_country", "text", "ארץ הייצור"),
            ("manufacturer_item_description", "text", "תיאור היצרן"),
            ("quantity", "text", "כמות באריזה"),
            ("unit_qty", "text", "יחידת הכמות"),
            ("unit_of_measure", "text", "יחידת המידה"),
            ("is_weighted", "text", "1 = מוצר שקול"),
            ("qty_in_package", "text", "יחידות באריזה"),
            *_STATE,
        ],
    },
    "prices_stores": {
        "role": "stores",
        "title": "סניפים — כל הרשתות",
        "description": "כל סניפי כל הרשתות, עם העיר מנורמלת לסמל יישוב של הלמ\"ס.",
        "columns": [
            ("sub_chain_id", "text", "מספר תת-הרשת"),
            ("sub_chain_name", "text", "שם תת-הרשת"),
            ("store_id", "text", "מספר הסניף"),
            ("store_name", "text", "שם הסניף"),
            ("store_type", "text", "סוג הסניף (1 = פיזי, 2 = מקוון, 3 = שניהם)"),
            ("address", "text", "כתובת"),
            ("city_raw", "raw_city", "העיר כפי שהרשת פרסמה (קוד או שם)"),
            ("city", "city", "העיר — שם רשמי"),
            ("city_code", "city_code", "סמל היישוב של הלמ\"ס"),
            ("zip_code", "text", "מיקוד"),
            *_STATE,
        ],
    },
    "prices_promotions": {
        "role": "promos",
        "title": "מבצעים — כל הרשתות",
        "description": "כל מבצע בכל סניף: תיאור, תאריכים, סוג ההטבה, מחיר המבצע ותנאיו.",
        "columns": [
            ("sub_chain_id", "text", "מספר תת-הרשת"),
            ("store_id", "text", "מספר הסניף"),
            ("promotion_id", "text", "מזהה המבצע"),
            ("description", "text", "תיאור המבצע"),
            ("start_date", "text", "תחילת המבצע"),
            ("end_date", "text", "סוף המבצע"),
            ("reward_type", "text", "סוג ההטבה"),
            ("discount_type", "text", "סוג ההנחה"),
            ("discount_rate", "num", "שיעור ההנחה"),
            ("discounted_price", "num", "מחיר המבצע (₪)"),
            ("min_qty", "num", "כמות מינימלית"),
            ("max_qty", "num", "כמות מקסימלית"),
            ("min_purchase_amount", "num", "סכום קנייה מינימלי"),
            ("club_id", "text", "מועדון (0 = לכל הלקוחות)"),
            ("additional_is_coupon", "text", "האם נדרש קופון"),
            ("additional_restrictions", "text", "הגבלות נוספות"),
            ("remarks", "text", "הערות"),
            *_STATE,
        ],
    },
    "prices_promotion_items": {
        "role": "promo_items",
        "title": "פריטי מבצעים — כל הרשתות",
        "description": "אילו פריטים כל מבצע כולל, ברמת הרשת.",
        "columns": [
            ("promotion_id", "text", "מזהה המבצע (מצטרף ל-prices_promotions)"),
            ("item_code", "text", "ברקוד / קוד הפריט"),
            ("item_code_bare", "bare", "הקוד בלי אפסים מובילים"),
            ("is_gift_item", "text", "1 = פריט מתנה"),
            ("reward_type", "text", "סוג ההטבה"),
            ("discount_type", "text", "סוג ההנחה"),
            ("discount_rate", "num", "שיעור ההנחה"),
            ("discounted_price", "num", "מחיר המבצע (₪)"),
            ("min_qty", "num", "כמות מינימלית"),
            ("max_qty", "num", "כמות מקסימלית"),
            ("is_weighted", "text", "1 = מוצר שקול"),
            *_STATE,
        ],
    },
    "prices_coverage": {
        "role": "coverage",
        "title": "כיסוי יומי — כל הרשתות",
        "description": "שורה לכל סניף בכל יום שבו נקרא הקובץ שלו: מאיזה קובץ, מתי, וכמה פריטים.",
        "columns": [
            ("sub_chain_id", "text", "מספר תת-הרשת"),
            ("store_id", "text", "מספר הסניף"),
            ("snapshot_date", "text", "יום הקריאה"),
            ("price_file", "text", "שם קובץ המחירים"),
            ("price_file_time", "text", "מועד קובץ המחירים"),
            ("price_items", "num", "פריטים בקובץ המחירים"),
            ("promo_file", "text", "שם קובץ המבצעים"),
            ("promo_file_time", "text", "מועד קובץ המבצעים"),
            ("promo_count", "num", "מבצעים בקובץ"),
        ],
    },
}

# Free-text search (q) per view.
_TEXT_COLS = {
    "prices_products": ["item_name", "manufacturer_name", "manufacturer_item_description"],
    "prices_stores": ["store_name", "address", "city"],
    "prices_promotions": ["description"],
}
# prices_market is ~16M rows: a query must name what it is about.
_SELECTIVE = ("item_code", "q", "store_id", "promotion_id")


def column_names(view: str) -> list[str]:
    return [c[0] for c in COMMON + VIEWS[view]["columns"]]


def describe() -> list[dict]:
    return [{"name": name, "title": v["title"], "description": v["description"],
             "columns": [{"name": c[0], "description": c[2],
                          "type": ("numeric" if c[1] == "num" else
                                   "boolean" if c[1] == "current" else
                                   "integer" if c[0] == "city_code" else "text")}
                         for c in COMMON + v["columns"]]}
            for name, v in VIEWS.items()]


# ---------------------------------------------------------------------------
# Building the views
# ---------------------------------------------------------------------------

def _num(expr: str) -> str:
    return f"(CASE WHEN {expr} ~ '{_NUM}' THEN ({expr})::numeric END)"


def _col(cols: set[str], alias: str, name: str) -> str:
    return f"{alias}.{_qi(name)}" if name in cols else "NULL::text"


def _city_exprs(alias: str, cols: set[str]) -> tuple[str, str]:
    if "city" not in cols:
        return "NULL::text", "NULL::int"
    name, code = pq._store_city_sql(alias)
    return name, code


def _branch(view: str, entry: dict, columns: dict[str, set[str]]) -> str | None:
    """One chain's SELECT for ``view``, or None when it lacks the role's table."""
    spec = VIEWS[view]
    t = entry["tables"]
    main = t.get(spec["role"])
    if not main:
        return None
    mc = columns.get(main, set())
    out = [f"{_lit(entry['chain'])}::text AS chain",
           f"{_lit(entry['name'])}::text AS chain_name",
           f"{_lit(entry['dataset_id'])}::text AS dataset_id"]
    joins = []
    alias = "m"
    if view == "prices_market":
        stores, products, cov = t.get("stores"), t.get("products"), t.get("coverage")
        if stores:
            sc = columns.get(stores, set())
            cname, ccode = _city_exprs("s", sc)
            sub_order = ("ORDER BY (s.sub_chain_id = m.sub_chain_id) DESC "
                         if "sub_chain_id" in sc and "sub_chain_id" in mc else "")
            joins.append(
                f"LEFT JOIN LATERAL (SELECT {_col(sc, 's', 'store_name')} AS store_name, "
                f"{_col(sc, 's', 'address')} AS address, {cname} AS city, {ccode} AS city_code "
                f"FROM public.{_qi(stores)} s WHERE s.store_id = m.store_id "
                f"AND s.last_seen_date = '' {sub_order}LIMIT 1) st ON true")
        if products:
            pc = columns.get(products, set())
            joins.append(
                f"LEFT JOIN LATERAL (SELECT "
                + ", ".join(f"{_col(pc, 'x', c)} AS {c}" for c in
                            ("item_name", "manufacturer_name", "quantity", "unit_qty",
                             "unit_of_measure", "is_weighted"))
                + f" FROM public.{_qi(products)} x WHERE ltrim(x.item_code, '0') = "
                  f"ltrim(m.item_code, '0') AND x.last_seen_date = '' LIMIT 1) pr ON true")
        if cov:
            joins.append(
                f"LEFT JOIN LATERAL (SELECT max(c.snapshot_date) AS as_of "
                f"FROM public.{_qi(cov)} c WHERE c.store_id = m.store_id) cv ON true")
    for name, kind, _ in spec["columns"]:
        if kind == "text":
            expr = _col(mc, alias, name)
        elif kind == "num":
            expr = _num(f"{alias}.{_qi(name)}") if name in mc else "NULL::numeric"
        elif kind == "bare":
            expr = f"ltrim({alias}.item_code, '0')"
        elif kind == "date_end":
            expr = f"NULLIF({alias}.last_seen_date, '')" if name in mc else "NULL::text"
        elif kind == "current":
            expr = f"({alias}.last_seen_date = '')" if "last_seen_date" in mc else "true"
        elif kind == "store":
            ok = bool(t.get("stores"))
            expr = f"st.{name}" if ok else ("NULL::int" if name == "city_code" else "NULL::text")
        elif kind == "product":
            expr = f"pr.{name}" if t.get("products") else "NULL::text"
        elif kind == "as_of":
            expr = "cv.as_of" if t.get("coverage") else "NULL::text"
        elif kind == "raw_city":
            expr = _col(mc, alias, "city")
        elif kind == "city":
            expr = _city_exprs(alias, mc)[0]
        elif kind == "city_code":
            expr = _city_exprs(alias, mc)[1]
        else:  # pragma: no cover — a typo in VIEWS
            raise ValueError(kind)
        out.append(f"{expr} AS {_qi(name)}")
    return (f"SELECT {', '.join(out)} FROM public.{_qi(main)} {alias}"
            + ("\n  " + "\n  ".join(joins) if joins else ""))


def view_sql(view: str, entries: list[dict], columns: dict[str, set[str]]) -> str | None:
    branches = [b for b in (_branch(view, e, columns) for e in entries) if b]
    if not branches:
        return None
    return "\nUNION ALL\n".join(branches)


def signature(entries: list[dict], columns: dict[str, set[str]]) -> str:
    payload = [[e["chain"], e["name"], e["dataset_id"],
                sorted((r, t, sorted(columns.get(t, ()))) for r, t in e["tables"].items())]
               for e in entries]
    raw = json.dumps([payload, {k: v["columns"] for k, v in VIEWS.items()}],
                     ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


async def _table_columns(conn, tables: list[str]) -> dict[str, set[str]]:
    rows = await conn.fetch(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = ANY($1::text[])", tables)
    out: dict[str, set[str]] = {}
    for r in rows:
        out.setdefault(r["table_name"], set()).add(r["column_name"])
    return out


async def ensure_views(db: AsyncSession, *, force: bool = False) -> dict:
    """(Re)build the six views when the chain tables changed. Cheap when nothing did."""
    return await build_views(await pq.chains(db, refresh=force), force=force)


async def build_views(chain_entries: list[dict], *, force: bool = False) -> dict:
    now = time.monotonic()
    if not force and now - _state["checked_at"] < _CHECK_TTL:
        return {"skipped": "checked recently"}
    _state["checked_at"] = now
    entries = [dict(e) for e in chain_entries]
    if not entries:
        return {"skipped": "no chains yet"}
    tables = sorted({t for e in entries for t in e["tables"].values()})
    pool = await append_store.get_pool()
    built: list[str] = []
    async with pool.acquire() as conn:
        columns = await _table_columns(conn, tables)
        sig = signature(entries, columns)
        current = await conn.fetch(
            "SELECT c.relname, obj_description(c.oid, 'pg_class') AS comment "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])", list(VIEWS))
        have = {r["relname"]: (r["comment"] or "") for r in current}
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        for view in VIEWS:
            if not force and f"sig={sig}" in have.get(view, ""):
                continue
            body = view_sql(view, entries, columns)
            if not body:
                continue
            async with conn.transaction():
                # Two app processes may notice the change at once.
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))",
                                   f"prices_unified:{view}")
                await conn.execute(f"DROP VIEW IF EXISTS public.{_qi(view)}")
                await conn.execute(f"CREATE VIEW public.{_qi(view)} AS\n{body}")
                await conn.execute(
                    f"COMMENT ON VIEW public.{_qi(view)} IS "
                    f"{_lit(VIEWS[view]['title'] + ' · over.org.il/projects/prices · sig=' + sig)}")
                if role:
                    await conn.execute(f"GRANT SELECT ON public.{_qi(view)} TO {_qi(role)}")
            built.append(view)
    if built:
        logger.info("prices: rebuilt unified views %s (%d chains, sig=%s)",
                    built, len(entries), sig)
    return {"built": built, "chains": len(entries), "signature": sig}


async def ensure_views_safely(db: AsyncSession) -> None:
    try:
        await ensure_views(db)
    except Exception as e:  # noqa: BLE001 — a read must never fail on this
        logger.warning("prices: unified views not rebuilt: %s", e)


async def build_views_safely(chain_entries: list[dict]) -> None:
    """For prices_query.chains(): the views exist for the /data console even
    when nobody has called the API since the last deploy."""
    try:
        await build_views(chain_entries)
    except Exception as e:  # noqa: BLE001
        logger.warning("prices: unified views not rebuilt: %s", e)


# ---------------------------------------------------------------------------
# Querying a uniform table
# ---------------------------------------------------------------------------

MAX_LIMIT = 1000
MAX_OFFSET = 100000
_ORDER = re.compile(r"^-?[a-z_]{1,40}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class Query:
    """Accumulates a WHERE clause with positional parameters, and renders the
    same statement with literals for the /data console."""

    def __init__(self) -> None:
        self.params: list = []
        self.where: list[str] = []

    def p(self, value) -> str:
        self.params.append(value)
        return f"${len(self.params)}"

    def render(self, sql: str) -> str:
        def lit(v):
            if isinstance(v, list):
                return "ARRAY[" + ", ".join(_lit(x) for x in v) + "]"
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return str(v)
            return _lit(v)
        return re.sub(r"\$(\d+)", lambda m: lit(self.params[int(m.group(1)) - 1]), sql)


def _split(value) -> list[str]:
    if value is None:
        return []
    items = value if isinstance(value, list) else str(value).split(",")
    return [str(v).strip() for v in items if str(v).strip()]


async def _chain_keys(db: AsyncSession, wanted: list[str]) -> list[str]:
    entries = await pq.chains(db)
    low = {w.lower() for w in wanted}
    keys = [e["chain"] for e in entries
            if e["chain"].lower() in low or e["chain"].split(":", 1)[-1].lower() in low
            or e["name"].lower() in low or e.get("account", "").lower() in low]
    if not keys:
        raise ValueError(f"לא נמצאה רשת בשם {', '.join(wanted)} — הרשימה ב-/api/prices/chains")
    return keys


async def query(db: AsyncSession, view: str, *, filters: dict[str, str] | None = None,
                q: str | None = None, city: str | None = None,
                current: bool = True, on_date: str | None = None,
                min_price: float | None = None, max_price: float | None = None,
                columns: list[str] | None = None, order: str | None = None,
                limit: int = 100, offset: int = 0) -> dict:
    """Rows of one uniform table, filtered.

    ``filters``: column → value, or several values comma-separated (IN). On
    ``item_code`` the match ignores leading zeros; on ``chain`` a key, a bare
    account or a name is accepted. ``current`` keeps only states in force now;
    ``on_date`` instead keeps the states that were in force on that day."""
    if view not in VIEWS:
        raise ValueError(f"אין טבלה אחידה בשם {view}. הטבלאות: {', '.join(VIEWS)}")
    await ensure_views_safely(db)
    names = column_names(view)
    kinds = {c[0]: c[1] for c in VIEWS[view]["columns"]}
    filters = {k: v for k, v in (filters or {}).items() if v not in (None, "")}
    unknown = [k for k in filters if k not in names]
    if unknown:
        raise ValueError(f"עמודות לא מוכרות ב-{view}: {', '.join(unknown)}")
    limit = max(1, min(int(limit or 100), MAX_LIMIT))
    offset = max(0, min(int(offset or 0), MAX_OFFSET))
    qb = Query()

    if view == "prices_market" and not (set(filters) & set(_SELECTIVE) or q):
        raise ValueError("prices_market מכילה מיליוני מצבי מחיר — ציינו item_code, q (שם "
                         "מוצר), או store_id יחד עם chain.")
    if view == "prices_market" and "store_id" in filters and "chain" not in filters \
            and not ({"item_code", "promotion_id"} & set(filters) or q):
        raise ValueError("store_id הוא מספר סניף בתוך רשת — ציינו גם chain.")

    for col, raw in filters.items():
        values = _split(raw)
        if col == "chain":
            values = await _chain_keys(db, values)
        if col in ("item_code", "item_code_bare"):
            codes = pq.bare_codes(values)
            qb.where.append(f"item_code_bare = ANY({qb.p(codes)}::text[])")
        elif kinds.get(col) == "num" and all(re.match(_NUM, v) for v in values):
            qb.where.append(f"{_qi(col)} = ANY({qb.p([float(v) for v in values])}::numeric[])")
        elif col == "city_code":
            ints = [int(v) for v in values if v.isdigit()]
            qb.where.append(f"city_code = ANY({qb.p(ints)}::int[])")
        elif col == "is_current":
            qb.where.append("is_current" if values[0].lower() in ("1", "true", "yes")
                            else "NOT is_current")
        elif col == "store_id":
            # Chains pad store numbers differently in different files; both
            # spellings, so the (store_id, item_code) index still serves it.
            both = list(dict.fromkeys(values + [v.lstrip("0") or "0" for v in values
                                                if v.isdigit()]))
            qb.where.append(f"store_id = ANY({qb.p(both)}::text[])")
        elif len(values) == 1:
            qb.where.append(f"{_qi(col)} = {qb.p(values[0])}")
        else:
            qb.where.append(f"{_qi(col)} = ANY({qb.p(values)}::text[])")

    q = (q or "").strip()
    resolved_codes: list[str] | None = None
    if q:
        if view == "prices_market" or view in ("prices_promotion_items",):
            # Name → barcodes through the products table, then an indexed match.
            found = await pq.search_products(db, q=q, limit=60)
            resolved_codes = [i["item_code"] for i in found["items"]]
            if not resolved_codes:
                return _empty(view, names, limit, offset, q=q)
            qb.where.append(f"item_code_bare = ANY({qb.p(pq.bare_codes(resolved_codes))}::text[])")
        elif view in _TEXT_COLS:
            for term in [t for t in re.split(r"\s+", q) if t][:6]:
                n = qb.p(f"%{term}%")
                qb.where.append("(" + " OR ".join(f"{_qi(c)} ILIKE {n}"
                                                  for c in _TEXT_COLS[view]) + ")")
        else:
            raise ValueError(f"חיפוש חופשי (q) אינו זמין ב-{view}")

    if city:
        if "city_code" not in names:
            raise ValueError(f"אין עיר ב-{view} — סננו לפי store_id או השתמשו ב-prices_stores")
        code = await pq.city_code(city)
        needle = qb.p(f"%{city.strip()}%")
        text_match = f"(store_name ILIKE {needle} OR address ILIKE {needle})"
        if code is not None:
            qb.where.append(f"(city_code = {qb.p(code)} OR (city_code IS NULL AND {text_match}))")
        else:
            qb.where.append(f"(city_code IS NULL AND {text_match})")

    has_state = "is_current" in names
    if has_state and "is_current" not in filters:
        if on_date:
            if not _DATE.match(on_date):
                raise ValueError("date בפורמט YYYY-MM-DD")
            d = qb.p(on_date)
            qb.where.append(f"first_seen_date <= {d} AND "
                            f"(last_seen_date IS NULL OR last_seen_date >= {d})")
        elif current:
            qb.where.append("is_current")

    price_col = "item_price" if view == "prices_market" else (
        "discounted_price" if "discounted_price" in names else None)
    for bound, op in ((min_price, ">="), (max_price, "<=")):
        if bound is not None:
            if not price_col:
                raise ValueError(f"אין עמודת מחיר ב-{view}")
            qb.where.append(f"{price_col} {op} {qb.p(float(bound))}")

    select_cols = names
    if columns:
        bad = [c for c in columns if c not in names]
        if bad:
            raise ValueError(f"עמודות לא מוכרות: {', '.join(bad)}")
        select_cols = columns
    order_sql = ""
    if order:
        parts = []
        for o in _split(order)[:3]:
            if not _ORDER.match(o) or o.lstrip("-") not in names:
                raise ValueError(f"מיון לא מוכר: {o}")
            parts.append(f"{_qi(o.lstrip('-'))} {'DESC' if o.startswith('-') else 'ASC'} NULLS LAST")
        order_sql = " ORDER BY " + ", ".join(parts)
    elif view == "prices_market":
        order_sql = " ORDER BY item_price ASC NULLS LAST, chain, store_id"

    sql = (f"SELECT {', '.join(_qi(c) for c in select_cols)} FROM public.{_qi(view)}"
           + (" WHERE " + " AND ".join(qb.where) if qb.where else "")
           + order_sql + f" LIMIT {limit + 1} OFFSET {offset}")
    started = time.monotonic()
    try:
        rows = await pq._fetch(sql, qb.params)
    except Exception as e:  # noqa: BLE001
        if "statement timeout" in str(e) or "canceling statement" in str(e):
            raise TimeoutError("השאילתה ארכה יותר מדי — צמצמו אותה (item_code, chain, "
                               "store_id, city) או הסירו את המיון") from e
        raise
    more = len(rows) > limit
    rows = rows[:limit]
    for r in rows:
        for k, v in list(r.items()):
            if isinstance(v, Decimal):
                r[k] = float(v)
    console_sql = qb.render(sql.replace(f" LIMIT {limit + 1} OFFSET {offset}",
                                        f" LIMIT {limit}" + (f" OFFSET {offset}" if offset else "")))
    return {"table": view, "columns": select_cols, "rows": rows, "count": len(rows),
            "has_more": more, "limit": limit, "offset": offset,
            "resolved_item_codes": resolved_codes,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "console_sql": console_sql, "row_url": _console_url(console_sql)}


def _empty(view, names, limit, offset, **extra) -> dict:
    return {"table": view, "columns": names, "rows": [], "count": 0, "has_more": False,
            "limit": limit, "offset": offset, "resolved_item_codes": [], "elapsed_ms": 0,
            "console_sql": None, "row_url": None, **extra}

