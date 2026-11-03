"""מכרזי רמ"י — the Israel Land Authority's land tenders, as one queryable corpus.

The register is scraped by the worker (``govil-scraper``, source ``michrazim``)
into ONE tracked dataset whose version carries several tables, all in the
append DB:

  ===========  ============================================================
  role         what one row is
  ===========  ============================================================
  tenders      a tender: type, status, region, settlement (+ CBS code),
               ייעוד, dates (published / deadline / committee / lottery)
  lots         a lot (מתחם) of a tender: area, units, development costs,
               threshold and appraisal, winner and winning amount, bids count,
               the raw "גוש/חלקה" list, and a polygon for ~20% of them
  lot_bids     every bid on every lot, winning or not
  tender_bids  tender-level bids of registration / lottery tenders
  parcels      (tender, lot, גוש, חלקה) — one row per parcel a lot sits on
  plans        (tender, lot, תוכנית, מגרש)
  documents    the tender's documents, each with a link
  ===========  ============================================================

Nothing here names a physical table. Each role is FOUND by its column
signature (the same rule ``find_deals_table`` follows): the table names carry
the dataset id and a hash of the resource name, and a re-registered dataset or
a renamed resource would otherwise break every surface at once. A role that is
not on this deployment is simply absent — the tenders table, notably, did not
reach SQL in the first version (2026-11-03) — and everything that would use it
degrades to what the lots alone can say, and says so in ``notes``.

Four facts about the data that every surface states, because each returns a
plausible wrong number to a caller who was not told (all measured 2026-11-03):

* **A lot id is not unique.** A lot that drew no bids is re-marketed in a later
  tender under the same מזהה מתחם (15,327 of 51,752 lot rows share an id), so a
  lot is (מזהה מכרז, מזהה מתחם), and the earlier tenders' rows say "אין הצעות".
* **סכום זכייה is not always a land price.** In the price-per-sqm tenders
  (מחיר למשתכן, מחיר מטרה …) the bid is the price of a sqm of FLAT: 2,330 won
  lots carry an amount under 30,000 ₪, median 9,073, beside a threshold whose
  median is 7,600. ``amount_basis`` separates them (see AMOUNT_BASIS_SQL), and
  ``amount_per_unit`` is computed only for the land-price ones.
* **The amount excludes development costs.** הוצאות פיתוח is a separate
  column, paid on top; the per-unit land cost a developer actually carries is
  the two together, and both are returned per unit.
* **Many of the parcels were renumbered after marketing.** 24,192 of the
  32,914 distinct גוש/חלקה pairs exist in today's cadastre; the rest were
  re-parcelled (פרצלציה) and their flats are registered — and sold, in the
  deals register — under the new numbers. A parcel that matches nothing is not
  a parcel that was never sold.
"""
from __future__ import annotations

import asyncio
import logging
import time

from app.services import append_store
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

SITE = "https://www.over.org.il"
#: The tracked dataset, for links only — the tables are found by signature.
DATASET_ID = "b252fc31-29cb-4a17-ac2e-2f22f8fa0878"
VERSIONS_URL = f"{SITE}/versions/{DATASET_ID}"
SOURCE_URL = "https://apps.land.gov.il/MichrazimSite/#/search"
TENDER_URL = "https://apps.land.gov.il/MichrazimSite/#/michraz/{id}"

_TIMEOUT_MS = 15_000
MAX_LIMIT = 200
COUNT_CAP = 10_000

T, L = "מזהה מכרז", "מזהה מתחם"
UNITS = 'קיבולת (יח"ד)'

#: role -> columns that identify its table. Each set is disjoint enough that no
#: table of the register matches two roles (pinned in tests).
SIGNATURES: dict[str, frozenset[str]] = {
    "tenders": frozenset({T, "סוג מכרז", "ייעוד", "תאריך ועדת מכרזים"}),
    "lots": frozenset({T, L, "סכום זכייה", 'קיבולת (יח"ד)', "שם זוכה"}),
    "lot_bids": frozenset({T, L, "מזהה הצעה", "תוספת לתמורה"}),
    "tender_bids": frozenset({T, "מזהה הצעה", "מספר קדימות", "מספר המתנה"}),
    "parcels": frozenset({T, L, "גוש", "חלקה", "מזהה רשומה"}),
    "plans": frozenset({T, L, "תוכנית", "מגרש", "מזהה רשומה"}),
    "documents": frozenset({"מפתח מסמך", T, "סוג פרסום", "קישור"}),
}

#: Tender-level columns carried onto every lot row when the tenders table is
#: here: (source column, output key). Only the ones present are selected.
TENDER_FIELDS: list[tuple[str, str]] = [
    ("סוג מכרז", "tender_type"),
    ("ייעוד", "purpose"),
    ("מרחב", "region"),
    ("שכונה", "neighborhood"),
    ("סמל יישוב", "settlement_code_rmi"),
    ("תאריך פרסום", "published"),
    ("מועד אחרון להגשת הצעות", "deadline"),
    ("תאריך ועדת מכרזים", "committee_date"),
    ("תאריך הגרלה", "lottery_date"),
    ("אוכלוסיות זכאיות", "eligible_populations"),
]

#: Personal identifiers the register publishes on lottery tenders. They stay in
#: the SQL console, where the dataset owner chose to publish them; the API and
#: the MCP have no question that needs them, so they never leave through here.
PERSONAL_COLUMNS = frozenset({"מספרי זהות של השותפים", "ShutafimMezaheIshiyut", "מזהה מציע"})

#: Below this a won amount is a sqm price, not a lot price — when the lot is
#: for several flats or carries a capped price. Measured: the won amounts are
#: bimodal, 2,330 lots under 30k (median 9,073 ₪, threshold median 7,600) and
#: 25,670 above (median 585,976 ₪). A one-flat lot under 30k stays a lot price:
#: the periphery sells those (the 30k–100k band is 3,215 one-unit lots).
PER_SQM_MAX = 30_000

CAVEATS = [
    "הנתונים הם שורות המקור כפי שפורסמו באתר המכרזים של רמ\"י — לא עובדו ולא תוקנו.",
    "מתחם מזוהה לפי (מזהה מכרז, מזהה מתחם): מתחם שלא נמכר משווק שוב במכרז מאוחר "
    "יותר באותו מזהה, והשורות הקודמות שלו אומרות 'אין הצעות'.",
    "סכום הזכייה אינו כולל הוצאות פיתוח, שמשולמות בנפרד; העלות ליחידה היא שני "
    "הסכומים יחד (amount_per_unit + dev_costs_per_unit).",
    "במכרזי מחיר למ\"ר (מחיר למשתכן, מחיר מטרה ודומיהם) סכום הזכייה הוא מחיר "
    "מ\"ר דירה ולא מחיר הקרקע. amount_basis='per_sqm_suspected' מסמן אותם לפי "
    "כלל מדוד (סכום מתחת ל-30,000 ₪ במתחם של כמה יח\"ד או עם מחיר מקסימום), "
    "ו-amount_per_unit לא מחושב להם. כשטבלת המכרזים זמינה, tender_type הוא "
    "המקור הקובע.",
    "גוש וחלקה הם כפי שהיו בעת השיווק. כרבע מהזוגות כבר אינם בשכבת החלקות של "
    "היום (פרצלציה), ועסקאות הדירות שנבנו עליהם רשומות תחת המספרים החדשים.",
    "year הוא שנת המכרז כפי שהיא במספרו (804/2026 → 2026), כלומר שנת השיווק, "
    "ולא שנת ועדת המכרזים.",
]


def _num(expr: str) -> str:
    """A text column → numeric, NULL when it is not a plain number. The
    register has the odd '1,2' and '[12'; ``::numeric`` on those would fail
    the whole query."""
    return f"(CASE WHEN btrim({expr}) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN btrim({expr})::numeric END)"


def _c(alias: str, col: str) -> str:
    """``alias."col"`` with the identifier quoted (columns like שטח (מ"ר) carry
    a double quote of their own)."""
    return f"{alias}.{_qi(col)}"


def _lit(v) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def _console_url(sql: str) -> str:
    from app.services.nadlan_query import _console_url as cu
    return cu(sql)


# ── finding the tables ────────────────────────────────────────────────────────
_TTL = 300.0
_cache: tuple[float, dict[str, tuple[str, str, list[str]]]] | None = None
_lock = asyncio.Lock()


def match_roles(cols_by_table: dict[str, list], sizes: dict[str, int] | None = None
                ) -> dict[str, tuple[str, list[str]]]:
    """Pure: ``{role: (table, [columns])}`` for the tables whose columns carry a
    role's signature. Several candidates for one role (a stale copy beside the
    live table) → the largest, the fact that tells them apart."""
    sizes = sizes or {}
    out: dict[str, tuple[str, list[str]]] = {}
    for table, cols in sorted(cols_by_table.items()):
        names = [c["name"] if isinstance(c, dict) else c for c in cols]
        have = set(names)
        for role, sig in SIGNATURES.items():
            if not sig <= have:
                continue
            cur = out.get(role)
            if cur is None or sizes.get(table, 0) > sizes.get(cur[0], 0):
                out[role] = (table, names)
    return out


async def tables(*, refresh: bool = False) -> dict[str, tuple[str, str, list[str]]]:
    """``{role: (schema, table, [columns])}`` on this deployment, cached."""
    global _cache
    if not refresh and _cache and time.monotonic() - _cache[0] < _TTL:
        return _cache[1]
    async with _lock:
        if not refresh and _cache and time.monotonic() - _cache[0] < _TTL:
            return _cache[1]
        try:
            raw = await append_store.schema_table_columns("public")
        except Exception:  # noqa: BLE001 — no append DB here
            logger.warning("michrazim: public schema unavailable", exc_info=True)
            raw = {}
        cand = {t: c for t, c in raw.items() if t.startswith("append_")
                and any(sig <= {x["name"] if isinstance(x, dict) else x for x in c}
                        for sig in SIGNATURES.values())}
        sizes: dict[str, int] = {}
        for t in cand:
            try:
                sizes[t] = await append_store.table_count_estimate(t)
            except Exception:  # noqa: BLE001
                sizes[t] = 0
        found = {role: ("public", t, cols) for role, (t, cols) in match_roles(cand, sizes).items()}
        _cache = (time.monotonic(), found)
        if found:
            logger.info("michrazim: tables %s", {r: v[1] for r, v in found.items()})
        return found


def invalidate_cache() -> None:
    global _cache
    _cache = None


async def is_ready() -> bool:
    return "lots" in await tables()


def _t(entry: tuple[str, str, list[str]]) -> str:
    return f"{_qi(entry[0])}.{_qi(entry[1])}"


def _current(alias: str, cols: list[str]) -> str:
    """Rows still at the source. The archive keeps every version of a row and
    closes the superseded one with last_seen."""
    return f"{alias}.last_seen IS NULL" if "last_seen" in cols else "TRUE"


async def _fetch(sql: str) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {_TIMEOUT_MS}")
            await conn.execute("SET LOCAL search_path = public")
            rows = await conn.fetch(sql)
    return [dict(r) for r in rows]


# ── the lot row ───────────────────────────────────────────────────────────────
# A lot query is three layers, so the console SQL handed back reads as what it
# does: ``x`` parses each published text column ONCE (and carries the tender's
# fields), ``y`` decides whether the lot was won and what its amount means, and
# the outer SELECT derives the per-unit figures and filters on them.

#: (output key, source column) parsed to numeric in ``x``.
NUMERIC_COLS: list[tuple[str, str]] = [
    ("area_sqm", 'שטח (מ"ר)'),
    ("building_area", "שטח בנייה"),
    ("units", UNITS),
    ("dev_costs", "הוצאות פיתוח"),
    ("threshold_price", "מחיר סף"),
    ("appraisal_price", "מחיר שומה"),
    ("winning_amount", "סכום זכייה"),
    ("bids", "מספר הצעות"),
]
#: (output key, source column) carried as trimmed text.
TEXT_COLS: list[tuple[str, str]] = [
    ("tender_id", T), ("tender_number", "מספר מכרז"),
    ("settlement", "יישוב"), ("status", "סטטוס מכרז"),
    ("lot_id", L), ("lot_name", "שם מתחם"),
    ("winner", "שם זוכה"), ("reduced_consideration", "תמורה מופחתת"),
    ("max_price", "מחיר מקסימום"), ("gush_helka", "גוש/חלקה"),
    ("plans", "תוכניות ומגרשים"), ("tender_url", "קישור למכרז"),
]

AMOUNT_BASIS_SQL = (
    "CASE WHEN NOT (winning_amount > 0 AND coalesce(winner, '') NOT LIKE 'אין הצעות%') "
    "THEN NULL "
    f"WHEN winning_amount < {PER_SQM_MAX} AND (coalesce(units, 0) >= 2 "
    "OR max_price IS NOT NULL) THEN 'per_sqm_suspected' "
    "ELSE 'lot_total' END"
)

DERIVED_SQL = (
    "CASE WHEN amount_basis = 'lot_total' AND units > 0 "
    "THEN round(winning_amount / units) END AS amount_per_unit,\n"
    "       CASE WHEN units > 0 AND dev_costs > 0 "
    "THEN round(dev_costs / units) END AS dev_costs_per_unit"
)


def _x_select(tcols: list[str] | None) -> str:
    parts = [f"nullif(btrim({_c('l', col)}), '') AS {key}" for key, col in TEXT_COLS]
    parts.insert(2, f"left({_c('l', T)}, 4) AS year")
    parts.insert(4, f"public.over_settlement_code({_c('l', 'יישוב')}) AS settlement_code")
    parts += [f"{_num(_c('l', col))} AS {key}" for key, col in NUMERIC_COLS]
    for src, key in TENDER_FIELDS:
        if tcols is not None and src in tcols:
            parts.append(f"nullif(btrim({_c('t', src)}), '') AS {key}")
    return ",\n         ".join(parts)


def _tender_join(tb: dict) -> tuple[str, list[str] | None]:
    ten = tb.get("tenders")
    if not ten:
        return "", None
    return (f"\n  LEFT JOIN {_t(ten)} t ON {_c('t', T)} = {_c('l', T)} "
            f"AND {_current('t', ten[2])}", ten[2])


def _shape(r: dict) -> dict:
    """Numbers to int where whole; None for the empty."""
    out = {}
    for k, v in r.items():
        if v is None or v == "":
            out[k] = None
        elif hasattr(v, "to_integral_value"):  # Decimal
            out[k] = int(v) if v == v.to_integral_value() else float(v)
        else:
            out[k] = v
    return out


# ── filters ───────────────────────────────────────────────────────────────────
FILTERS = ("settlement", "year_from", "year_to", "tender_id", "status", "won_only",
           "min_units", "gush", "helka", "tender_type", "purpose", "amount_basis")


def where_clause(f: dict, tb: dict) -> tuple[str, str, list[str]]:
    """Pure SQL over already-validated values: ``(raw_where, derived_where, notes)``.

    ``raw_where`` filters the lot table (layer x); ``derived_where`` filters on
    what layer y decides (won, amount basis). Every string goes through ``_lit``
    and every number through ``int``, so the text is safe to execute AND to hand
    out as console SQL — the same text, so a caller can rerun what answered."""
    lots = tb["lots"]
    raw = [_current("l", lots[2])]
    derived = ["TRUE"]
    notes: list[str] = []
    if f.get("settlement"):
        s = _lit(str(f["settlement"]).strip())
        # Either spelling: RMI's own ("שהם") or any the gazetteer knows. The
        # code side needs both resolved, never one side's raw text.
        raw.append(f"({_c('l', 'יישוב')} = {s} OR public.over_settlement_code("
                   f"{_c('l', 'יישוב')}) = public.over_settlement_code({s}))")
    for key, op in (("year_from", ">="), ("year_to", "<=")):
        if f.get(key) is not None:
            raw.append(f"left({_c('l', T)}, 4) {op} {_lit(str(int(f[key])))}")
    if f.get("tender_id"):
        raw.append(f"{_c('l', T)} = {_lit(str(int(f['tender_id'])))}")
    if f.get("status"):
        raw.append(f"{_c('l', 'סטטוס מכרז')} = {_lit(f['status'])}")
    if f.get("min_units") is not None:
        raw.append(f"{_num(_c('l', UNITS))} >= {int(f['min_units'])}")
    if f.get("gush") is not None:
        par = tb.get("parcels")
        if not par:
            raise ValueError('טבלת הגושים והחלקות של מכרזי רמ"י אינה זמינה כאן')
        cond = f"{_c('p', 'גוש')} = {_lit(str(int(f['gush'])))}"
        if f.get("helka") is not None:
            cond += f" AND {_c('p', 'חלקה')} = {_lit(str(int(f['helka'])))}"
        raw.append(f"EXISTS (SELECT 1 FROM {_t(par)} p WHERE {_c('p', T)} = {_c('l', T)} "
                   f"AND {_c('p', L)} = {_c('l', L)} AND {cond} AND {_current('p', par[2])})")
    for key, col in (("tender_type", "סוג מכרז"), ("purpose", "ייעוד")):
        if not f.get(key):
            continue
        ten = tb.get("tenders")
        if not ten or col not in ten[2]:
            raise ValueError(
                f"הסינון לפי {col} דורש את טבלת המכרזים, שעדיין לא נטענה ל-SQL. "
                'בינתיים סננו לפי amount_basis, שמבדיל בין מחיר קרקע למחיר מ"ר דירה.')
        raw.append(f"{_c('t', col)} = {_lit(f[key])}")
    if f.get("won_only"):
        derived.append("amount_basis IS NOT NULL")
    if f.get("amount_basis"):
        if f["amount_basis"] not in ("lot_total", "per_sqm_suspected"):
            raise ValueError("amount_basis הוא lot_total או per_sqm_suspected")
        derived.append(f"amount_basis = {_lit(f['amount_basis'])}")
    if "tenders" not in tb:
        notes.append("טבלת המכרזים (סוג מכרז, ייעוד, תאריכים) עדיין לא נטענה ל-SQL; "
                     "השדות האלה ריקים, ו-year הוא שנת מספר המכרז.")
    return " AND ".join(raw), " AND ".join(derived), notes


SORTS = {
    "year_desc": "tender_id DESC, lot_id",
    "year_asc": "tender_id ASC, lot_id",
    "amount_desc": "winning_amount DESC NULLS LAST",
    "per_unit_desc": "amount_per_unit DESC NULLS LAST",
    "per_unit_asc": "amount_per_unit ASC NULLS LAST",
    "bids_desc": "bids DESC NULLS LAST",
}


def _layers(f: dict, tb: dict) -> tuple[str, str, list[str]]:
    """``(with_sql, derived_where, notes)`` — the x/y CTEs every lot query
    starts from, and the filter on what y decides."""
    raw, derived, notes = where_clause(f, tb)
    join, tcols = _tender_join(tb)
    with_sql = (f"WITH x AS (\n  SELECT {_x_select(tcols)}\n"
                f"  FROM {_t(tb['lots'])} l{join}\n  WHERE {raw}\n),\n"
                f"y AS (\n  SELECT x.*, {AMOUNT_BASIS_SQL} AS amount_basis FROM x\n)")
    return with_sql, derived, notes


def lots_sql(f: dict, tb: dict, *, limit: int, offset: int, sort: str) -> tuple[str, list[str]]:
    with_sql, derived, notes = _layers(f, tb)
    order = SORTS.get(sort, SORTS["year_desc"])
    sql = (f"{with_sql}\nSELECT y.*,\n       {DERIVED_SQL}\nFROM y\nWHERE {derived}\n"
           f"ORDER BY {order}\nLIMIT {int(limit)} OFFSET {int(offset)}")
    return sql, notes


def count_sql(f: dict, tb: dict) -> str:
    with_sql, derived, _ = _layers(f, tb)
    return (f"{with_sql}\nSELECT count(*) AS n FROM "
            f"(SELECT 1 FROM y WHERE {derived} LIMIT {COUNT_CAP + 1}) c")


async def _require() -> dict:
    tb = await tables()
    if "lots" not in tb:
        raise LookupError("מאגר מכרזי רמ\"י עדיין לא נטען בפריסה הזו")
    return tb


async def search_lots(f: dict, *, limit: int = 50, offset: int = 0,
                      sort: str = "year_desc") -> dict:
    """Lots matching the filters, one row per (tender, lot), with the derived
    per-unit figures and, when present, the tender's own fields."""
    tb = await _require()
    limit = max(1, min(int(limit), MAX_LIMIT))
    offset = max(0, int(offset))
    sql, notes = lots_sql(f, tb, limit=limit, offset=offset, sort=sort)
    rows, counted = await asyncio.gather(_fetch(sql), _fetch(count_sql(f, tb)))
    total = counted[0]["n"] if counted else 0
    return {"data": [_shape(r) for r in rows], "total": min(total, COUNT_CAP),
            "total_capped": total > COUNT_CAP, "limit": limit, "offset": offset,
            "console_sql": sql, "row_url": _console_url(sql), "notes": notes}


def series_sql(f: dict, tb: dict) -> tuple[str, list[str]]:
    """Per year: lots marketed, lots won, units, and the medians that compare —
    land price per unit (lot_total lots only), development costs per unit, the
    sqm price of the per-sqm tenders, bids per lot. Medians, never means: one
    lot can be one villa plot or a 600-unit compound."""
    with_sql, derived, notes = _layers(f, tb)
    sql = f"""{with_sql},
z AS (
  SELECT y.*, {DERIVED_SQL} FROM y WHERE {derived}
)
SELECT year,
       count(*) AS lots,
       count(*) FILTER (WHERE amount_basis IS NOT NULL) AS lots_won,
       count(*) FILTER (WHERE bids = 0) AS lots_no_bids,
       sum(units) FILTER (WHERE amount_basis IS NOT NULL) AS units_won,
       count(amount_per_unit) AS lots_priced_per_unit,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY amount_per_unit) AS median_amount_per_unit,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY dev_costs_per_unit)
         FILTER (WHERE amount_per_unit IS NOT NULL) AS median_dev_costs_per_unit,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY amount_per_unit + coalesce(dev_costs_per_unit, 0))
         AS median_land_cost_per_unit,
       count(*) FILTER (WHERE amount_basis = 'per_sqm_suspected') AS lots_per_sqm,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY winning_amount)
         FILTER (WHERE amount_basis = 'per_sqm_suspected') AS median_sqm_price,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY bids) AS median_bids
FROM z
GROUP BY year
ORDER BY year"""
    return sql, notes


async def series(f: dict) -> dict:
    tb = await _require()
    sql, notes = series_sql(f, tb)
    rows = await _fetch(sql)
    return {"years": [_shape(r) for r in rows], "console_sql": sql,
            "row_url": _console_url(sql), "notes": notes}


async def parcel_lots(gush: int, helka: int, *, limit: int = 50) -> dict:
    """Every tender lot that sat on this גוש/חלקה, newest tender first."""
    tb = await tables()
    if "lots" not in tb or "parcels" not in tb:
        return {"available": False, "lots": [], "total": 0}
    res = await search_lots({"gush": gush, "helka": helka}, limit=limit, sort="year_desc")
    return {"available": True, "lots": res["data"], "total": res["total"],
            "console_sql": res["console_sql"], "row_url": res["row_url"],
            "notes": res["notes"]}


async def parcel_counts(pairs: list[tuple[int, int]]) -> dict[tuple[int, int], int]:
    """How many tender lots sat on each (gush, helka) — one query for many."""
    tb = await tables()
    par = tb.get("parcels")
    if not par or not pairs:
        return {}
    vals = ", ".join(f"({_lit(str(int(g)))}, {_lit(str(int(h)))})" for g, h in pairs[:500])
    rows = await _fetch(f"""
        SELECT p."גוש" AS g, p."חלקה" AS h, count(DISTINCT (p."{T}", p."{L}")) AS n
        FROM {_t(par)} p JOIN (VALUES {vals}) v(g, h) ON p."גוש" = v.g AND p."חלקה" = v.h
        WHERE {_current('p', par[2])}
        GROUP BY 1, 2""")
    return {(int(r["g"]), int(r["h"])): int(r["n"]) for r in rows}


async def tender(tender_id: int) -> dict | None:
    """One tender in full: its own row (when the table is here), its lots,
    the bids on them, the parcels and plans, its documents."""
    tb = await _require()
    tid = _lit(str(int(tender_id)))
    lots = await search_lots({"tender_id": tender_id}, limit=MAX_LIMIT, sort="year_desc")
    out: dict = {"tender_id": str(int(tender_id)),
                 "tender_url": TENDER_URL.format(id=int(tender_id)),
                 "lots": lots["data"], "notes": lots["notes"]}
    ten = tb.get("tenders")
    if ten:
        rows = await _fetch(f'SELECT * FROM {_t(ten)} t WHERE t."{T}" = {tid} '
                            f"AND {_current('t', ten[2])} LIMIT 1")
        if rows:
            out["tender"] = {k: v for k, v in rows[0].items()
                             if k not in ("first_seen", "last_seen", "geom", "geometry_wkt",
                                          "גאומטריה (רשת ישראל החדשה)") and v not in (None, "")}
    if not lots["data"] and "tender" not in out:
        return None

    async def _rows(role: str, cols: list[str], order: str, cap: int = 2000) -> list[dict]:
        e = tb.get(role)
        if not e:
            return []
        sel = ", ".join(_qi(c) for c in cols if c in e[2] and c not in PERSONAL_COLUMNS)
        if not sel:
            return []
        rows = await _fetch(f"SELECT {sel} FROM {_t(e)} x WHERE x.\"{T}\" = {tid} "
                            f"AND {_current('x', e[2])} ORDER BY {order} LIMIT {cap}")
        return [{k: v for k, v in r.items() if v not in (None, "")} for r in rows]

    out["lot_bids"], out["tender_bids"], out["parcels"], out["plans"], out["documents"] = \
        await asyncio.gather(
            _rows("lot_bids", [L, "שם מתחם", "מזהה הצעה", "סכום הצעה", "תוספת לתמורה",
                               "מצב ההצעה"], f'x."{L}", {_num(_c("x", "סכום הצעה"))} DESC NULLS LAST'),
            _rows("tender_bids", ["מזהה הצעה", "סכום הצעה", "מספר קדימות", "מספר המתנה",
                                  "תיאור קדימות", "תיאור המתנה", "הצעה פסולה", "יחידות דיור",
                                  "מספר שותפים"], 'x."מזהה הצעה"'),
            _rows("parcels", [L, "שם מתחם", "גוש", "חלקה"], 'x."גוש", x."חלקה"'),
            _rows("plans", [L, "שם מתחם", "תוכנית", "מגרש"], 'x."תוכנית", x."מגרש"'),
            _rows("documents", ["תיאור", "סוג פרסום", "מהות מסמך", "שם קובץ", "סוג קובץ",
                                "תאריך עדכון", "קישור"], 'x."תאריך עדכון" DESC'),
        )
    if out["tender_bids"]:
        out["tender_bids_note"] = ("מספרי הזהות של המציעים מפורסמים במקור ובקונסולת "
                                   "ה-SQL, ואינם מוחזרים כאן.")
    return out


async def settlements() -> list[dict]:
    """Settlement names as the register spells them, with lot counts and the
    code OVER resolves each to (null when the name is not in the gazetteer)."""
    tb = await _require()
    lots = tb["lots"]
    rows = await _fetch(f"""
        SELECT l."יישוב" AS settlement, public.over_settlement_code(l."יישוב") AS resolved_code,
               public.over_settlement(l."יישוב") AS resolved_name,
               count(*) AS lots, count(DISTINCT l."{T}") AS tenders,
               max(left(l."{T}", 4)) AS last_year
        FROM {_t(lots)} l WHERE {_current('l', lots[2])}
        GROUP BY 1 ORDER BY lots DESC""")
    return [_shape(r) for r in rows]


async def stats() -> dict:
    tb = await tables()
    out: dict = {"dataset_id": DATASET_ID, "versions_url": VERSIONS_URL,
                 "source_url": SOURCE_URL,
                 "tables": {r: f"{e[0]}.{e[1]}" for r, e in tb.items()},
                 "missing_tables": [r for r in SIGNATURES if r not in tb]}
    if "lots" not in tb:
        return out
    with_sql, _, _ = _layers({}, tb)
    rows = await _fetch(f"""{with_sql}
SELECT count(*) AS lots, count(DISTINCT tender_id) AS tenders,
       min(year) AS year_from, max(year) AS year_to,
       count(*) FILTER (WHERE amount_basis IS NOT NULL) AS lots_won,
       count(*) FILTER (WHERE amount_basis = 'per_sqm_suspected') AS lots_won_per_sqm,
       count(gush_helka) AS lots_with_parcels
FROM y""")
    out.update(_shape(rows[0]))
    return out


# ── indexes ───────────────────────────────────────────────────────────────────
_INDEX_SPECS = {
    "parcels": [("gh", '("גוש", "חלקה")'), ("tl", f'("{T}", "{L}")')],
    "lots": [("t", f'("{T}")')],
    "lot_bids": [("tl", f'("{T}", "{L}")')],
    "tender_bids": [("t", f'("{T}")')],
    "plans": [("tl", f'("{T}", "{L}")')],
    "documents": [("t", f'("{T}")')],
    "tenders": [("t", f'("{T}")')],
}


def index_specs(found: dict) -> list[tuple[str, str, str, str]]:
    """Pure: ``[(schema, table, index_name, cols)]`` for the roles found."""
    out = []
    for role, specs in _INDEX_SPECS.items():
        e = found.get(role)
        if not e:
            continue
        for suffix, cols in specs:
            out.append((e[0], e[1], f"{e[1][:52]}_{suffix}_idx", cols))
    return out


async def ensure_indexes() -> dict:
    """Create whichever lookup indexes are missing. The per-parcel lookup is
    an equality on (גוש, חלקה) and every tender page is an equality on the
    tender id; without these each is a scan, on every cube opened."""
    found = await tables(refresh=True)
    made: list[str] = []
    failed: list[str] = []
    if not found:
        return {"made": made, "failed": failed}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        have = {r["indexname"] for r in await conn.fetch(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")}
        for schema, table, name, cols in index_specs(found):
            if name in have:
                continue
            try:
                await conn.execute(f"CREATE INDEX IF NOT EXISTS {_qi(name)} "
                                   f"ON {_qi(schema)}.{_qi(table)} {cols}", timeout=600)
                made.append(name)
            except Exception as e:  # noqa: BLE001
                failed.append(f"{name}: {e}")
                logger.warning("michrazim: index %s failed: %s", name, e)
    return {"made": made, "failed": failed}

