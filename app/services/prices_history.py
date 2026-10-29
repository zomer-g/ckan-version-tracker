"""The unified price views with the states that have left SQL.

``prices_unified`` lays six SQL views over every chain's tables
(``prices_market`` and the rest). Since docs/history-prices-stamp.md an ended
state leaves its chain's table for a Parquet part on R2 once it is past the
dataset's window, so a view read for a day before the window comes back short.

The views cannot run on the history engine: they resolve a store's city with
the site's own SQL functions (``over_settlement_code``) and join the chain's
current products and stores, none of which exists in DuckDB. So the work is
split the other way round:

1. **What to fetch** (``extract_filters``): the conjuncts of the query that
   pin the view down (an item code, a store, a chain, a day), read with a SQL
   parser. Only a predicate that holds for EVERY row the query reads from the
   view is used: one inside an OR, a CASE, a FILTER or an ON is not. When
   there is none, everything is fetched, up to COLD_MAX_ROWS.
2. **The ended states** of each chain's table are read from its parts on the
   history engine (``history_query.query_cold``), narrowed by those filters.
3. **Back into Postgres**: each chain's rows become a VALUES list that goes
   through the very same branch SQL the view is built from
   (``prices_unified._branch``), so they get the same store, city and product
   columns. The view's name is then a CTE, the view UNION ALL those rows, and
   the query runs on Postgres as written: same dialect, same functions, same
   indexes for the part still in SQL (the CTE is NOT MATERIALIZED, so its
   filters reach the view's branches).

A part that is still partly in SQL (the window is deleting it) is fetched
whole; the rows of it still in SQL are dropped by an anti-join on
``(row_hash, last_seen)``, so no state is counted twice.
"""
from __future__ import annotations

import logging
import re
from typing import Awaitable, Callable, Iterable

from app.services import append_store
from app.services import prices_unified as pu

logger = logging.getLogger(__name__)

COLD_MAX_ROWS = 20_000
NOTHING_MATCHES = "\u2205"  # ∅: no item code, store or chain is spelled so
_INTERNAL = {"first_seen", "last_seen", "row_hash", "geom"}
_DAY = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")

NOTE_INCLUDED = ("כולל {rows:,} מצבים שהסתיימו ונמצאים בקבצי ההיסטוריה (Parquet), "
                 "מ-{parts} קבצים; השאילתה רצה על SQL עם השורות האלה.")
NOTE_NONE = ("ל-{views} אין מצבים בקבצי ההיסטוריה שמתאימים לשאילתה, ולכן היא רצה על SQL "
             "כפי שהיא.")
NOTE_UNAVAILABLE = ("מנוע ההיסטוריה אינו זמין ({why}), ולכן התוצאה כוללת רק את מה שב-SQL: "
                    "מצבים שהסתיימו לפני חלון השמירה חסרים בה.")


class TooBroad(ValueError):
    """The query would pull more ended states from R2 than can be injected."""


def views_in(tables: Iterable) -> list[str]:
    """The unified price views among a query's ``(schema, table)`` refs."""
    out = []
    for t in tables:
        schema, name = (t[0], t[1]) if isinstance(t, (tuple, list)) else ("public", t)
        if schema == "public" and name in pu.VIEWS and name not in out:
            out.append(name)
    return out


# ── 1. filters from the query (pure) ─────────────────────────────────────────

def _kinds(view: str) -> dict[str, str]:
    kinds = {c[0]: c[1] for c in pu.COMMON}
    kinds.update({c[0]: c[1] for c in pu.VIEWS[view]["columns"]})
    return kinds


def _conjuncts(node) -> list:
    from sqlglot import exp

    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


def _literal(node) -> str | None:
    from sqlglot import exp

    while isinstance(node, (exp.Paren, exp.Cast)):
        node = node.this
    if isinstance(node, exp.Literal):
        return str(node.this)
    return None


def _column(node, alias: str, single: bool, kinds: dict) -> str | None:
    from sqlglot import exp

    while isinstance(node, exp.Paren):
        node = node.this
    if not isinstance(node, exp.Column):
        return None
    if node.table and node.table != alias:
        return None
    if not node.table and not single:
        return None
    return node.name if node.name in kinds else None


def _reference_filters(table, kinds: dict) -> dict | None:
    """Filters one FROM/JOIN reference of the view guarantees, or None when the
    reference is not a plain source of a SELECT with a WHERE."""
    from sqlglot import exp

    parent = table.parent
    if not isinstance(parent, (exp.From, exp.Join)):
        return None
    select = parent.parent
    if not isinstance(select, exp.Select):
        return None
    if isinstance(parent, exp.Join) and (parent.side or "").upper() in ("LEFT", "RIGHT", "FULL"):
        return None  # an outer join keeps the other side's rows without the view's
    where = select.args.get("where")
    if where is None:
        return {"eq": {}, "last_seen_from": None, "first_seen_to": None}
    sources = [select.args.get("from")] + list(select.args.get("joins") or [])
    single = len([s for s in sources if s is not None]) == 1
    alias = table.alias_or_name
    sets: dict[str, list[set[str]]] = {}
    last_from = first_to = None
    for c in _conjuncts(where.this):
        if isinstance(c, exp.EQ):
            for a, b in ((c.left, c.right), (c.right, c.left)):
                col, val = _column(a, alias, single, kinds), _literal(b)
                if col and val is not None:
                    sets.setdefault(col, []).append({val})
                    break
        elif isinstance(c, exp.In) and not c.args.get("query"):
            col = _column(c.this, alias, single, kinds)
            vals = [_literal(e) for e in c.expressions]
            if col and vals and all(v is not None for v in vals):
                sets.setdefault(col, []).append(set(vals))
        elif isinstance(c, (exp.GTE, exp.GT)):
            col, val = _column(c.left, alias, single, kinds), _literal(c.right)
            if col == "last_seen_date" and val and _DAY.match(val):
                last_from = max(last_from or val, val)
        elif isinstance(c, (exp.LTE, exp.LT)):
            col, val = _column(c.left, alias, single, kinds), _literal(c.right)
            if col == "first_seen_date" and val and _DAY.match(val):
                first_to = min(first_to or val, val)
        elif isinstance(c, exp.Or):
            # (last_seen_date IS NULL OR last_seen_date >= 'D'): every ended
            # state the query keeps has last_seen_date >= D.
            a, b = c.left, c.right
            for x, y in ((a, b), (b, a)):
                if (isinstance(x, exp.Is) and _column(x.this, alias, single, kinds) == "last_seen_date"
                        and isinstance(x.expression, exp.Null)
                        and isinstance(y, (exp.GTE, exp.GT))
                        and _column(y.left, alias, single, kinds) == "last_seen_date"):
                    val = _literal(y.right)
                    if val and _DAY.match(val):
                        last_from = max(last_from or val, val)
    # Several conditions on one column are ANDed: only their common values pass
    # (none in common: NOTHING_MATCHES, a value no row has).
    eq = {col: sorted(set.intersection(*ss)) or [NOTHING_MATCHES] for col, ss in sets.items()}
    return {"eq": eq, "last_seen_from": last_from, "first_seen_to": first_to}


def extract_filters(sql: str, view: str) -> dict:
    """``{eq: {column: [values]}, last_seen_from, first_seen_to}`` that hold for
    every row the query reads from ``view``. Empty (``eq: {}``) when the query
    cannot be parsed or names the view somewhere a filter cannot be proved. Pure."""
    empty = {"eq": {}, "last_seen_from": None, "first_seen_to": None}
    try:
        import sqlglot
        from sqlglot import exp

        tree = sqlglot.parse_one(sql, read="postgres")
    except Exception:  # noqa: BLE001 — no filter is always correct, only slower
        return empty
    kinds = _kinds(view)
    refs = [t for t in tree.find_all(exp.Table)
            if t.name == view and (t.db or "public") == "public"]
    found = [_reference_filters(t, kinds) for t in refs]
    if not found or any(f is None for f in found):
        return empty
    eq: dict[str, list[str]] = {}
    for col in set.intersection(*(set(f["eq"]) for f in found)):
        eq[col] = sorted({v for f in found for v in f["eq"][col]})
    lasts = [f["last_seen_from"] for f in found]
    firsts = [f["first_seen_to"] for f in found]
    return {"eq": eq,
            "last_seen_from": min(lasts) if all(lasts) else None,
            "first_seen_to": max(firsts) if all(firsts) else None}


# ── 2. what to read from R2 (pure) ───────────────────────────────────────────

def _lit(v) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def select_entries(entries: list[dict], view: str, filters: dict) -> list[dict]:
    """The chains the query can reach, by the view's chain columns. Pure."""
    role = pu.VIEWS[view]["role"]
    eq = filters.get("eq") or {}
    out = []
    for e in entries:
        if role not in e.get("tables", {}):
            continue
        if "chain" in eq and e["chain"] not in eq["chain"]:
            continue
        if "chain_name" in eq and e["name"] not in eq["chain_name"]:
            continue
        if "dataset_id" in eq and str(e["dataset_id"]) not in eq["dataset_id"]:
            continue
        out.append(e)
    return out


def cold_where(view: str, filters: dict, cols: set[str]) -> list[str]:
    """DuckDB predicates on one chain table's parts for ``filters``. Pure."""
    from app.services import prices_query as pq

    kinds = _kinds(view)
    out = []
    for col, vals in (filters.get("eq") or {}).items():
        kind = kinds.get(col)
        if col in ("item_code", "item_code_bare") and "item_code" in cols:
            if col == "item_code_bare" or kind == "bare":
                codes = pq.bare_codes(vals) or [NOTHING_MATCHES]
                out.append(f"ltrim(item_code, '0') IN ({', '.join(_lit(c) for c in codes)})")
            else:
                out.append(f"item_code IN ({', '.join(_lit(v) for v in vals)})")
        elif kind == "text" and col in cols:
            out.append(f"{pu._qi(col)} IN ({', '.join(_lit(v) for v in vals)})")
    if filters.get("last_seen_from") and "last_seen_date" in cols:
        out.append(f"last_seen_date >= {_lit(filters['last_seen_from'])}")
    if filters.get("first_seen_to") and "first_seen_date" in cols:
        out.append(f"first_seen_date <= {_lit(filters['first_seen_to'])}")
    return out


def cold_read_sql(tables: dict[str, list[str]]) -> str:
    """One DuckDB statement over the parts of every table: ``{table: where}``. Pure."""
    selects = []
    for table, where in tables.items():
        selects.append(f"SELECT {_lit(table)} AS __table, * FROM public.{pu._qi(table)}"
                       + (" WHERE " + " AND ".join(where) if where else ""))
    return "\nUNION ALL BY NAME\n".join(selects)


# ── 3. back into Postgres (pure) ─────────────────────────────────────────────

def values_source(table: str, cols: list[str], rows: list[dict], *, anti_join: bool) -> str:
    """The rows as a relation shaped like the chain table: its source columns
    (text) plus row_hash and last_seen for the anti-join. Pure."""
    names = cols + ["row_hash", "last_seen"]

    def val(v) -> str:
        return "NULL::text" if v is None else _lit(v) + "::text"

    body = ", ".join("(" + ", ".join(val(r.get(c)) for c in names) + ")" for r in rows)
    src = (f"SELECT * FROM (VALUES {body}) AS c({', '.join(pu._qi(c) for c in names)})")
    if anti_join:
        src += (f" WHERE NOT EXISTS (SELECT 1 FROM public.{pu._qi(table)} h "
                f"WHERE h.\"last_seen\" = c.\"last_seen\"::timestamptz "
                f"AND h.\"row_hash\"::text = c.\"row_hash\")")
    return f"({src})"


def view_cte(view: str, branches: list[str]) -> str:
    """``view AS NOT MATERIALIZED (the view UNION ALL the R2 branches)``. Pure."""
    q = pu._qi(view)
    return (f"{q} AS NOT MATERIALIZED (SELECT * FROM public.{q}\nUNION ALL\n"
            + "\nUNION ALL\n".join(branches) + ")")


_QUALIFIED = r'(?i)"?public"?\s*\.\s*"?({names})"?(?![A-Za-z0-9_])'


def unqualify(sql: str, views: list[str]) -> str:
    """``public.prices_market`` → ``prices_market``, so the CTE is what it reads. Pure."""
    if not views:
        return sql
    return re.sub(_QUALIFIED.format(names="|".join(re.escape(v) for v in views)),
                  lambda m: m.group(1), sql)


def wrap(sql: str, ctes: list[str]) -> str:
    """The query under the CTEs. The query stays a subquery, so a WITH of its
    own still works and still sees the CTEs. Pure."""
    return "WITH " + ",\n".join(ctes) + f"\nSELECT * FROM (\n{sql}\n) _with_history"


# ── the database side ────────────────────────────────────────────────────────

async def _entries() -> list[dict]:
    from app.database import async_session
    from app.services import prices_query as pq

    async with async_session() as db:
        return await pq.chains(db)


async def _columns(tables: list[str]) -> dict[str, set[str]]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        return await pu._table_columns(conn, tables)


async def cold_cte(view: str, filters: dict, *, max_rows: int = COLD_MAX_ROWS) -> dict | None:
    """``{sql, rows, parts}`` — the CTE that adds the R2 states of ``view`` —
    or None when none of its chain tables has any. Raises TooBroad past
    ``max_rows`` and the history engine's own errors when it is down."""
    from app.services import history_query

    role = pu.VIEWS[view]["role"]
    entries = select_entries(await _entries(), view, filters)
    by_table = {e["tables"][role]: e for e in entries}
    parts = await history_query.cold_parts_of(list(by_table))
    if not parts:
        return None
    joined = {t for e in entries for t in e["tables"].values()}
    columns = await _columns(sorted(joined))
    reads = {t: cold_where(view, filters, columns.get(t, set())) for t in parts}
    res = await history_query.query_cold(cold_read_sql(reads), parts_by_table=parts,
                                         max_rows=max_rows)
    if res.get("truncated"):
        raise TooBroad(
            f"השאילתה על {view} מושכת יותר מ-{max_rows:,} מצבים מקבצי ההיסטוריה. צמצמו "
            "אותה בתנאי AND פשוט: item_code_bare = '...', store_id, chain_name, או "
            "last_seen_date >= 'YYYY-MM-DD'.")
    rows = res.get("rows") or []
    if not rows:
        return {"sql": None, "rows": 0, "parts": sum(len(p) for p in parts.values())}
    branches = []
    for table, entry in by_table.items():
        mine = [r for r in rows if r.get("__table") == table]
        if not mine:
            continue
        cols = sorted(c for c in columns.get(table, set()) if c not in _INTERNAL)
        src = values_source(table, cols, mine,
                            anti_join=any(p.get("partial") for p in parts.get(table, [])))
        branch = pu._branch(view, entry, columns, main_from=src)
        if branch:
            branches.append(branch)
    if not branches:
        return {"sql": None, "rows": 0, "parts": sum(len(p) for p in parts.values())}
    return {"sql": view_cte(view, branches), "rows": len(rows),
            "parts": sum(len(p) for p in parts.values())}


async def run_with_history(sql: str, *, views: list[str],
                           run_sql: Callable[[str], Awaitable[dict]]) -> dict:
    """A console / MCP query that names unified price views, with their R2
    states added. The answer carries ``engine``, ``cold_rows``, ``cold_parts``
    and a ``history_note`` saying what was added."""
    from app.services import history_query

    ok, why = history_query.is_available()
    if not ok:
        res = await run_sql(sql)
        res.update(engine="postgres", history_note=NOTE_UNAVAILABLE.format(why=why))
        return res
    ctes, rows, parts = [], 0, 0
    for view in views:
        built = await cold_cte(view, extract_filters(sql, view))
        if built and built["sql"]:
            ctes.append(built["sql"])
            rows += built["rows"]
            parts += built["parts"]
    if not ctes:
        res = await run_sql(sql)
        res.update(engine="postgres", history_note=NOTE_NONE.format(views=", ".join(views)))
        return res
    res = await run_sql(wrap(unqualify(sql, views), ctes))
    res.update(engine="postgres+r2", cold_rows=rows, cold_parts=parts,
               history_note=NOTE_INCLUDED.format(rows=rows, parts=parts))
    return res
