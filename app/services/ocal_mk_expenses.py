"""Contact-with-the-voter ("קשר עם הבוחר") expenses for יומן לעם (Ocal),
linked to the diary owners of ocal_owners.py.

The data source is not chosen yet, so the layer is OFF
(``settings.ocal_mk_expenses_enabled``) and nothing fetches data on its own.
Once a source is provided, its Excel files are loaded through the admin upload:

  * ``parse_workbook(data, filename)`` — reads one file into expense rows,
    handling two layouts: *wide* (one row per person, one column per expense
    category plus a total) and *long* (one row per expense item, with a category
    / supplier column and an amount);
  * ``import_file`` — stores the rows in the ocal DB (``mk_expense_files`` +
    ``mk_expenses``), replacing an earlier import of the same file;
  * ``link_expenses`` — sets each row's ``owner_key`` to the diary owner with the
    same name (order-insensitive, see ocal_owners.names_match), so a single owner
    can be shown with both their diaries and their expenses.

Rows are stored as published. A total column/row is kept (``is_total``) but
never summed with the category rows.
"""
from __future__ import annotations

import io
import logging
import re
from app.config import settings
from app.services import ocal_db
from app.services.ocal_owners import (
    ensure_tables as ensure_owner_tables, name_key, name_tokens, names_match,
    normalize_name,
)

logger = logging.getLogger(__name__)

MAX_FILE_BYTES = 30 * 1024 * 1024


# ── workbook parsing ─────────────────────────────────────────────────────────

_YEAR_RE = re.compile(r"(?<!\d)(20[0-4]\d|199\d)(?!\d)")
_NAME_HDR_RE = re.compile(r"שם\s*(?:ה?ח[\"״׳']?כ|חבר|חברת|מלא)|^שם$|חבר[ת]?\s*(?:ה)?כנסת|^ח[\"״]כ$|^חה[\"״]כ$|^שם\s*ח")
_FIRST_HDR_RE = re.compile(r"שם\s*פרטי")
_LAST_HDR_RE = re.compile(r"שם\s*משפחה")
_FACTION_HDR_RE = re.compile(r"סיע|מפלג")
_TOTAL_RE = re.compile(r"סה[\"״]?כ|סך\s*ה?כל|סיכום|total", re.IGNORECASE)
_AMOUNT_HDR_RE = re.compile(r"סכום|סה[\"״]?כ|עלות|תשלום|הוצא|ש[\"״]ח|₪|ביצוע|שולם")
_CATEGORY_HDR_RE = re.compile(r"סוג|קטגורי|סעיף|נושא|מהות|תחום|פריט")
_DESC_HDR_RE = re.compile(r"תיאור|פירוט|ספק|שם\s*העסק|הערה|הערות|פרטים")
_DATE_HDR_RE = re.compile(r"תאריך|חודש|תקופה")
_YEAR_HDR_RE = re.compile(r"^שנה$|^שנת")
_BUDGET_HDR_RE = re.compile(r"תקציב|מסגרת|יתרה|אחוז|%")


def _cell_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return " ".join(str(v).split())


def _num(v) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("₪", "").replace("ש\"ח", "").strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()").strip()
    if not s or not re.fullmatch(r"-?\d+(?:\.\d+)?", s):
        return None
    n = float(s)
    return -n if neg else n


def _read_sheets(data: bytes, filename: str) -> list[tuple[str, list[list]]]:
    """[(sheet name, rows)] for an .xlsx or legacy .xls file."""
    head = data[:8]
    if head.startswith(b"PK"):
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        out = []
        for ws in wb.worksheets:
            out.append((ws.title, [list(r) for r in ws.iter_rows(values_only=True)]))
        wb.close()
        return out
    if head.startswith(b"\xd0\xcf\x11\xe0"):
        import xlrd
        book = xlrd.open_workbook(file_contents=data)
        out = []
        for sh in book.sheets():
            rows = []
            for i in range(sh.nrows):
                row = []
                for c in sh.row(i):
                    if c.ctype == xlrd.XL_CELL_DATE:
                        try:
                            row.append(xlrd.xldate.xldate_as_datetime(c.value, book.datemode).date().isoformat())
                        except Exception:  # noqa: BLE001
                            row.append(c.value)
                    elif c.ctype == xlrd.XL_CELL_EMPTY:
                        row.append(None)
                    else:
                        row.append(c.value)
                rows.append(row)
            out.append((sh.name, rows))
        return out
    raise ValueError(f"{filename}: not an Excel file (xls/xlsx)")


def _find_header(rows: list[list]) -> int | None:
    for i, r in enumerate(rows[:40]):
        cells = [_cell_str(c) for c in r]
        if sum(1 for c in cells if c) < 2:
            continue
        if any(_NAME_HDR_RE.search(c) for c in cells) or (
                any(_FIRST_HDR_RE.search(c) for c in cells) and any(_LAST_HDR_RE.search(c) for c in cells)):
            return i
    return None


def _header_names(rows: list[list], hi: int) -> tuple[list[str], int]:
    """Header labels and the number of header rows (1 or 2). A second header row
    is folded in when the first is a group row whose merged cells leave blanks
    ("הוצאות" over "אתר אינטרנט", "פרסום", ...)."""
    window = rows[hi:hi + 50]
    width = max((len(r) for r in window), default=0)
    top = [_cell_str(c) for c in rows[hi]] + [""] * (width - len(rows[hi]))
    nxt = rows[hi + 1] if hi + 1 < len(rows) else []
    sub = [_cell_str(c) for c in nxt] + [""] * (width - len(nxt))
    two_rows = (sum(1 for c in sub if c and _num(c) is None) >= 2
                and not any(_num(c) is not None for c in sub if c))
    if not two_rows:
        return top, 1
    out, group = [], ""
    for t, u in zip(top, sub):
        group = t or group
        if u and group and u != group:
            out.append(f"{group} – {u}")
        else:
            out.append(u or t or "")
    return out, 2


def _context_year(*texts: str) -> int | None:
    for t in texts:
        years = [int(y) for y in _YEAR_RE.findall(t or "")]
        if years:
            return max(years)
    return None


def parse_workbook(data: bytes, filename: str, link_text: str = "") -> dict:
    """Expense rows from one expenses workbook.

    Returns ``{"rows": [...], "sheets": [...], "layout": ..., "year": ...}``;
    each row is ``{mk_name, faction, year, category, description, amount,
    is_total, raw}``.
    """
    rows_out: list[dict] = []
    sheets_meta: list[dict] = []
    for sheet_name, rows in _read_sheets(data, filename):
        rows = [r for r in rows if r is not None]
        hi = _find_header(rows)
        if hi is None:
            sheets_meta.append({"sheet": sheet_name, "skipped": "no MK-name header"})
            continue
        headers, n_hdr = _header_names(rows, hi)
        body = rows[hi + n_hdr:]
        title_text = " ".join(_cell_str(c) for r in rows[:hi] for c in r if c)
        year = _context_year(sheet_name, title_text, link_text, filename)

        def col(rx):
            return next((j for j, h in enumerate(headers) if h and rx.search(h)), None)

        name_c = col(_NAME_HDR_RE)
        first_c, last_c = col(_FIRST_HDR_RE), col(_LAST_HDR_RE)
        faction_c = col(_FACTION_HDR_RE)
        year_c = col(_YEAR_HDR_RE)
        # Numeric columns: most non-empty body cells parse as numbers.
        numeric = []
        for j, h in enumerate(headers):
            if j in (name_c, first_c, last_c, faction_c, year_c) or not h:
                continue
            vals = [r[j] for r in body if j < len(r) and _cell_str(r[j])]
            if vals and sum(1 for v in vals if _num(v) is not None) >= 0.8 * len(vals):
                numeric.append(j)
        text_cols = [j for j, h in enumerate(headers)
                     if h and j not in numeric and j not in (name_c, first_c, last_c, faction_c, year_c)]
        cat_c = next((j for j in text_cols if _CATEGORY_HDR_RE.search(headers[j])), None)
        desc_cols = [j for j in text_cols if j != cat_c and _DESC_HDR_RE.search(headers[j])]
        amount_cols = [j for j in numeric if not _BUDGET_HDR_RE.search(headers[j])]
        long_layout = cat_c is not None or (len(amount_cols) == 1 and desc_cols)
        main_amount = None
        if long_layout:
            main_amount = next((j for j in amount_cols if _AMOUNT_HDR_RE.search(headers[j])), None)
            if main_amount is None and amount_cols:
                main_amount = amount_cols[-1]

        current_name = ""
        current_faction = ""
        n_rows = 0
        for r in body:
            cells = list(r) + [None] * (len(headers) - len(r))
            if first_c is not None and last_c is not None and name_c is None:
                nm = f"{_cell_str(cells[first_c])} {_cell_str(cells[last_c])}".strip()
            else:
                nm = _cell_str(cells[name_c]) if name_c is not None else ""
            fac = _cell_str(cells[faction_c]) if faction_c is not None else ""
            if not nm and long_layout and current_name:
                nm, fac = current_name, fac or current_faction  # merged MK cell
            if not nm:
                continue
            if _TOTAL_RE.search(nm):
                continue  # a sheet total row, not an MK
            current_name, current_faction = nm, fac
            ry = year
            if year_c is not None:
                ry = _context_year(_cell_str(cells[year_c])) or year
            raw = {headers[j]: _cell_str(cells[j]) for j in range(len(headers))
                   if headers[j] and _cell_str(cells[j])}
            mk = normalize_name(nm)
            if long_layout:
                amt = _num(cells[main_amount]) if main_amount is not None else None
                if amt is None:
                    continue
                cat = _cell_str(cells[cat_c]) if cat_c is not None else ""
                desc = " · ".join(_cell_str(cells[j]) for j in desc_cols if _cell_str(cells[j]))
                rows_out.append({"mk_name": mk, "faction": fac or None, "year": ry,
                                 "category": cat or None, "description": desc or None,
                                 "amount": amt, "is_total": False, "raw": raw,
                                 "sheet": sheet_name})
                n_rows += 1
            else:
                for j in amount_cols:
                    amt = _num(cells[j])
                    if amt is None:
                        continue
                    rows_out.append({"mk_name": mk, "faction": fac or None, "year": ry,
                                     "category": headers[j], "description": None,
                                     "amount": amt, "is_total": bool(_TOTAL_RE.search(headers[j])),
                                     "raw": raw, "sheet": sheet_name})
                    n_rows += 1
        sheets_meta.append({"sheet": sheet_name, "header_row": hi + 1, "year": year,
                            "layout": "long" if long_layout else "wide", "rows": n_rows,
                            "columns": [h for h in headers if h]})
    return {"rows": rows_out, "sheets": sheets_meta}


# ── storage ──────────────────────────────────────────────────────────────────

_DDL = [
    """CREATE TABLE IF NOT EXISTS mk_expense_files (
        id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        source_url text NOT NULL UNIQUE,
        file_name text,
        title text,
        year int,
        sheets jsonb,
        row_count int NOT NULL DEFAULT 0,
        imported_at timestamptz NOT NULL DEFAULT now(),
        imported_by text)""",
    """CREATE TABLE IF NOT EXISTS mk_expenses (
        id bigserial PRIMARY KEY,
        file_id uuid NOT NULL REFERENCES mk_expense_files(id) ON DELETE CASCADE,
        year int,
        mk_name text NOT NULL,
        mk_name_key text NOT NULL,
        faction text,
        category text,
        description text,
        amount numeric NOT NULL,
        is_total boolean NOT NULL DEFAULT false,
        sheet text,
        raw jsonb,
        owner_key text,
        person_id uuid)""",
    "CREATE INDEX IF NOT EXISTS mk_expenses_owner_idx ON mk_expenses (owner_key)",
    "CREATE INDEX IF NOT EXISTS mk_expenses_name_idx ON mk_expenses (mk_name_key)",
    "CREATE INDEX IF NOT EXISTS mk_expenses_file_idx ON mk_expenses (file_id)",
]
_ready = False


async def ensure_tables() -> None:
    global _ready
    if _ready or not ocal_db.is_configured():
        return
    await ensure_owner_tables()
    for stmt in _DDL:
        await ocal_db.execute(stmt)
    _ready = True


async def import_file(data: bytes, *, source_url: str, file_name: str, title: str = "",
                      imported_by: str | None = None) -> dict:
    """Parse and store one workbook, replacing an earlier import of ``source_url``."""
    await ensure_tables()
    parsed = parse_workbook(data, file_name, title)
    rows = parsed["rows"]
    year = _context_year(title, file_name) or next((r["year"] for r in rows if r["year"]), None)
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM mk_expense_files WHERE source_url = $1", source_url)
            fid = await conn.fetchval(
                "INSERT INTO mk_expense_files (source_url, file_name, title, year, sheets, "
                "row_count, imported_by) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id",
                source_url, file_name, title or None, year, parsed["sheets"], len(rows), imported_by)
            await conn.executemany(
                "INSERT INTO mk_expenses (file_id, year, mk_name, mk_name_key, faction, category, "
                "description, amount, is_total, sheet, raw) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                [(fid, r["year"] or year, r["mk_name"], name_key(r["mk_name"]), r["faction"],
                  r["category"], r["description"], r["amount"], r["is_total"], r["sheet"],
                  r["raw"]) for r in rows])
    return {"file_id": str(fid), "file_name": file_name, "year": year, "rows": len(rows),
            "mks": len({r["mk_name"] for r in rows}), "sheets": parsed["sheets"]}


# ── linking to diary owners ──────────────────────────────────────────────────

def match_owner(mk_name: str, owners: list[tuple[str, str]]) -> str | None:
    """The owner_key whose label is the same person as ``mk_name``; the closest
    token overlap wins, and a tie between different owners links nobody."""
    mk_t = set(name_tokens(mk_name))
    best, best_score, tie = None, 0.0, False
    for key, label in owners:
        if not names_match(mk_name, label):
            continue
        lt = set(name_tokens(label))
        score = len(mk_t & lt) / len(mk_t | lt)
        if score > best_score:
            best, best_score, tie = key, score, False
        elif score == best_score and key != best:
            tie = True
    return None if tie else best


async def link_expenses() -> dict:
    """Point each expense row at its diary owner (by name); rows with no diary
    owner get their own person key so the MK is still selectable."""
    await ensure_tables()
    owners = await ocal_db.fetch(
        "SELECT owner_key, min(owner_label) AS label, "
        "(array_agg(person_id) FILTER (WHERE person_id IS NOT NULL))[1] AS person_id "
        "FROM diary_source_owners WHERE kind = 'person' GROUP BY owner_key")
    pairs = [(o["owner_key"], o["label"]) for o in owners]
    pid_by_key = {o["owner_key"]: o["person_id"] for o in owners}
    names = await ocal_db.fetch("SELECT DISTINCT mk_name, mk_name_key FROM mk_expenses")
    updates = []
    matched = 0
    # An MK with no diary still gets one key across spellings ("גולן מאי" in
    # one year, "בדרה גולן פלורה מאי" in another): the shortest spelling leads.
    reps: list[str] = []
    for n in sorted(names, key=lambda n: (len(name_tokens(n["mk_name"])), n["mk_name"])):
        key = match_owner(n["mk_name"], pairs)
        if key:
            matched += 1
        else:
            rep = next((r for r in reps if names_match(r, n["mk_name"])), None)
            if rep is None:
                reps.append(n["mk_name"])
                rep = n["mk_name"]
            key = "p:" + name_key(rep)
        updates.append((key, pid_by_key.get(key), n["mk_name"]))
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(
                "UPDATE mk_expenses SET owner_key = $1, person_id = $2 WHERE mk_name = $3", updates)
    return {"mk_names": len(names), "matched_to_diary_owner": matched}


def is_enabled() -> bool:
    return bool(settings.ocal_mk_expenses_enabled)


def upload_url(file_name: str) -> str:
    """Stable key for an uploaded file, so uploading it again replaces it."""
    return f"upload:{file_name}"
