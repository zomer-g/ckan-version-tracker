"""Dedicated SQL MCP server for the whole-site database (``/data/mcp``).

A FIFTH MCP surface (after /mcp, /cbs/mcp, /knesset/mcp and /ocal/mcp). The main
/mcp is a CATALOG server — it answers "which datasets exist and what changed" —
and its only structural tool (get_table_profile) needs a physical table name the
caller must already know. Nothing there lists the tables, hands over the schema,
or runs SQL, so a model connected to it cannot actually query the site.

This server is the MCP twin of the public /data console (app/api/tables.py): the
SAME catalog (data_catalog.build_catalog — every NEON dataset table, the knesset
mirror, the idx collection indexes, odata imports, ocal), the SAME compact DDL,
and the SAME free-SELECT path (append_store.run_readonly_sql over
CONSOLE_SEARCH_PATH) — so whatever a human can do on /data, a model can do here,
with the identical guards:

  * a single SELECT/WITH statement, write/DDL keywords rejected,
  * executed in a Postgres READ ONLY transaction with a statement_timeout,
  * on the least-privilege SELECT-only role (get_readonly_pool),
  * result hard-capped.

Auth is unchanged from the other resources: the SAME OAuth authorization server
(/mcp/oauth/*), the same ``api_users`` allow-list and service token — one Google
login and one invite grant every resource. Usage logged to mcp_usage_events.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage
from app.services import append_store, data_catalog

SERVER_NAME = "over-sql-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

MAX_SQL_ROWS = 1000
SQL_TIMEOUT_MS = 20000

SERVER_INSTRUCTIONS = (
    "מסד הנתונים של גרסאות לעם (OVER, over.org.il) — SQL חופשי מעל כל הטבלאות "
    "השאילתיות באתר: מאגרי data.gov.il ומקורות ממשלתיים נוספים (סכימת public), "
    "מראה נתוני הכנסת (knesset), אינדקסים של אוספים כמו שכבות ממ\"ג (idx), "
    "ייבוא מידע לעם (odata) ויומן לעם (ocal). זהו אותו מנוע שמאחורי הקונסולה "
    "הציבורית בכתובת over.org.il/data.\n\n"
    "סדר עבודה מומלץ: (1) list_tables עם מילת חיפוש כדי לאתר את הטבלה הנכונה — "
    "החיפוש עובר גם על שמות עמודות; (2) describe_schema על אותה טבלה (או על "
    "סכימה שלמה) כדי לקבל DDL מדויק; (3) run_sql. אפשר גם get_table לתצוגת "
    "דוגמה של שורות אמיתיות ו-get_table_profile לטווחי ערכים, פורמט תאריכים "
    "וזיהוי ישויות חוזרות בכל עמודה.\n\n"
    "כללי כתיבה: קריאה בלבד, משפט SELECT/WITH יחיד, ללא ';'. search_path = "
    + data_catalog.CONSOLE_SEARCH_PATH + " ולכן אפשר לכתוב שם טבלה בלי שם "
    "הסכימה, אלא אם השם חוזר ביותר מסכימה אחת. שמות בעברית, באות גדולה או "
    "במילה שמורה חייבים מרכאות כפולות, בדיוק כפי שהם מופיעים ב-describe_schema. "
    "טבלאות knesset הן תמיד באותיות קטנות (KNS_Bill ← kns_bill). כל העמודות "
    "בסכימת idx הן text — המירו לפי הצורך (col::numeric, col::date).\n\n"
    "שלוש מלכודות שמפילות שאילתה שנראית תקינה:\n"
    "(1) ערך חסר הוא מחרוזת ריקה ולא NULL, ולכן ::numeric ישיר נכשל — "
    "NULLIF(col,'')::numeric, ובמכנה NULLIF(NULLIF(col,'')::numeric, 0).\n"
    "(2) תאריכים הם טקסט בפורמט של המפרסם (DD/MM/YYYY, DD.MM.YYYY, YYYY-MM-DD) "
    "ו-to_date נכשל על השורה הראשונה שאינה בפורמט — סננו קודם את הצורה "
    "(WHERE col ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}'), ורק אז המירו.\n"
    "(3) חיבור שני מאגרים לפי שם יישוב או רשות בטקסט חופשי מחזיר תוצאה ומפיל "
    "בשקט כל כתיב שאינו זהה. פתרו את שני הצדדים לקוד עם over_settlement_code() / "
    "over_authority_code() וחברו עליו; over_settlement() מחזירה את השם הרשמי. "
    "רשימת הפונקציות המלאה מופיעה ב-describe_schema.\n\n"
    "(4) נכסים: אל תחברו לפי מחרוזת גוש/חלקה ואל תנתחו כתובת ידנית. "
    "טבלאות over_re_* כבר מצליבות חלקות, גזטיר, מיקוד וכתובות: "
    "over_re_parcels (מפתח parcel_key = גוש-תת גוש-חלקה), over_re_addresses, "
    "over_re_parcel_gazetteer, over_re_streets. פונקציות: over_parcel_key(), "
    "over_gush_helka(גוש,חלקה), over_parcel_at(lat,lon), "
    "over_parcels_near(lat,lon,מטרים), over_street_key(), over_zip(). "
    "שימו לב ל-zip_level (address מול locality) ול-parcel_match "
    "(pip = נקודה בתוך הפוליגון, כלומר שיוך מדויק).\n\n"
    "מגבלת זמן: 20 שניות לשאילתה. אם נגמר הזמן — צמצמו את הטבלה הגדולה ב-CTE "
    "עם GROUP BY לפני ה-JOIN במקום לחבר אליה ישירות, סננו טקסט לפני המרה "
    "(col LIKE '%/2024' לפני to_date), ולפלט ברמת שורה השתמשו ב-"
    "WITH x AS MATERIALIZED (...) כדי שהטבלה הגדולה תיסרק פעם אחת.\n\n"
    "מרחב: עמודת geom היא PostGIS ב-EPSG:4326. ST_AsText(geom) לקריאה, "
    "ST_DWithin(geom::geography, ..., מטרים) למרחק אמיתי — בלי ::geography "
    "המרחק במעלות. שכבות ממ\"ג מפרסמות שמות שדה של המקור (shem_yishuv, "
    "pop_total) ולא כיתוב בעברית; הכיתוב מוחזר כהערה ליד כל עמודה "
    "ב-describe_schema, ו-list_tables מחפש גם בו.\n\n"
    "כשאתה מציג תוצאות למשתמש: ציין שהמקור הוא 'גרסאות לעם', צרף את הקישורים "
    "שמוחזרים בתשובה (page_url / source_url) לאימות מול המקור הממשלתי, וזכור "
    "שהנתונים הם מראה מתוארך — מחיקות ושינויים במקור אינם משתקפים מיד."
)


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "list_schemas",
        "description": (
            "רשימת הסכימות במסד הנתונים ומה יש בכל אחת — כמה טבלאות, כמה שורות "
            "(הערכה) ואילו סוגי מקורות. נקודת הפתיחה כשלא יודעים איפה לחפש."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_tables",
        "description": (
            "חיפוש בקטלוג הטבלאות של האתר. החיפוש (q) עובר על שם הטבלה, כותרת "
            "המאגר, הארגון, התגיות וגם על שמות העמודות והכינויים העבריים שלהן — "
            "כך מוצאים טבלה לפי שדה שמעניין אתכם. מחזיר לכל טבלה את שם ה-SQL, "
            "הסכימה, מספר שורות משוער וקישורי מקור. הפעילו include_columns כדי "
            "לקבל גם את רשימת העמודות."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "טקסט חופשי — שם טבלה, נושא, ארגון או שם עמודה"},
                "schema": {"type": "string", "enum": ["public", "knesset", "idx", "odata", "ocal"],
                           "description": "סינון לסכימה אחת"},
                "source_type": {"type": "string", "description": "סינון לפי סוג מקור, למשל ckan / scraper / govmap"},
                "dataset_id": {"type": "string", "description": "כל הטבלאות של מאגר מסוים (UUID מ-search_datasets ב-MCP הראשי)"},
                "include_columns": {"type": "boolean", "default": False,
                                    "description": "לצרף את רשימת העמודות המלאה לכל טבלה"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "describe_schema",
        "description": (
            "מבנה הטבלאות כטקסט DDL (CREATE TABLE) — בדיוק בצורת הכתיבה שהמסד "
            "מקבל, כולל מרכאות לשמות בעברית. הריצו לפני כתיבת run_sql. "
            "ללא פרמטרים מוחזר הקטלוג המלא בצורה מקוצרת (שורה לטבלה) — הוא גדול; "
            "עדיף לצמצם עם table (טבלה אחת, מפורט) או schema (סכימה אחת)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "table": {"type": "string", "description": "שם טבלה בודדת — מחזיר DDL מפורט שלה"},
                "schema": {"type": "string", "enum": ["public", "knesset", "idx", "odata", "ocal"],
                           "description": "צמצום לסכימה אחת (כשלא נמסר table)"},
            },
        },
    },
    {
        "name": "run_sql",
        "description": (
            "הרצת שאילתת SQL חופשית מעל כל מסד הנתונים של האתר — קריאה בלבד, "
            "משפט SELECT / WITH יחיד, ללא ';'. אפשר לחבר (JOIN) בין סכימות: "
            "מאגרי public, טבלאות knesset, אינדקסי idx, odata ו-ocal. "
            f"עד {MAX_SQL_ROWS} שורות בתשובה — לסיכומים השתמשו ב-GROUP BY / "
            "aggregate במקום לשלוף הכול. הריצו describe_schema קודם כדי לכתוב "
            "שמות עמודות נכונים. שימו לב לשלוש המלכודות שבהוראות השרת: "
            "NULLIF לפני ::numeric, סינון צורת התאריך לפני to_date, וחיבור בין "
            "מאגרים דרך over_settlement_code() ולא לפי שם חופשי. "
            "לשאלות על נכסים/כתובות/גוש-חלקה השתמשו בטבלאות over_re_* "
            "ובפונקציות over_gush_helka() / over_parcel_at()."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "שאילתת SELECT / WITH יחידה"},
                "max_rows": {"type": "integer", "minimum": 1, "maximum": MAX_SQL_ROWS,
                             "default": 200, "description": f"תקרת שורות (עד {MAX_SQL_ROWS})"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_table",
        "description": (
            "כרטיס טבלה בודדת: מספר שורות מדויק, כל העמודות וטיפוסיהן, דוגמת "
            "שורות אמיתיות, קישור למקור הממשלתי ולעמוד הגרסאות, וקבצי המקור של "
            "הגרסה האחרונה. השתמשו כדי להבין מה באמת יושב בטבלה לפני כתיבת SQL."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"table": {"type": "string", "description": "שם הטבלה כפי שמופיע ב-list_tables"}},
            "required": ["table"],
        },
    },
    {
        "name": "get_table_profile",
        "description": (
            "פרופיל סטטיסטי מחושב של טבלה: לכל עמודה — הסוג שזוהה (מספר/תאריך/"
            "טקסט), טווח min/max, פורמט התאריך, שיעור מילוי, מספר ערכים ייחודיים, "
            "הערכים השכיחים, וזיהוי ישות חוזרת (יישוב / רשות מקומית / תאגיד / "
            "אדם). כולל תקציר ומילות מפתח. לא כל הטבלאות עברו פרופיל."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"table": {"type": "string", "description": "שם הטבלה"}},
            "required": ["table"],
        },
    },
]


# ── helpers ─────────────────────────────────────────────────────────────────

def _require_configured() -> None:
    if not append_store.is_configured():
        raise ValueError("מסד הנתונים השאילתי (NEON) אינו מוגדר בשרת")


async def _catalog(db: AsyncSession) -> list[dict]:
    _require_configured()
    return await data_catalog.build_catalog(db)


def _find(catalog: list[dict], table: str) -> dict:
    """Resolve a table name against the catalog — the security gate. Every tool
    that takes a table name resolves it here, so an unknown name can never reach
    a schema-qualified query."""
    name = (table or "").strip()
    rec = next((r for r in catalog if r["table"] == name), None)
    if rec is None:
        # A near-miss list turns "unknown table" from a dead end into a next step.
        low = name.lower()
        near = [r["table"] for r in catalog if low and low in r["table"].lower()][:10]
        msg = f"לא נמצאה טבלה בשם '{name}'."
        if near:
            msg += " אולי התכוונת ל: " + ", ".join(near)
        else:
            msg += " הריצו list_tables כדי לאתר את השם המדויק."
        raise ValueError(msg)
    return rec


def _metadata_hit(rec: dict, q: str) -> bool:
    """Free-text match against the table's identity (name, dataset title,
    organization, source type, tags)."""
    hay = " ".join(str(x) for x in (
        rec.get("table"), rec.get("title"), rec.get("organization"),
        rec.get("source_type"), " ".join(rec.get("tags") or []),
    ) if x).lower()
    return q in hay


def _matching_columns(rec: dict, q: str) -> list[str]:
    """The table's COLUMN names matching the query (name or Hebrew alias).

    Searching by field is how a model finds the right table when it knows what
    it wants to ask but not where that lives — a catalog search that only reads
    table titles answers "nothing found" for a column that exists."""
    return [c["name"] for c in (rec.get("columns") or [])
            if q in (c.get("name") or "").lower() or q in (c.get("alias") or "").lower()]


def _row(rec: dict, base: str, *, columns: bool, matched: list[str]) -> dict:
    out = {
        "table": rec["table"],
        "schema": rec["schema"],
        "kind": rec.get("kind"),
        "title": rec.get("title"),
        "organization": rec.get("organization"),
        "source_type": rec.get("source_type"),
        "est_rows": rec.get("est_rows"),
        "column_count": len(rec.get("columns") or []),
        "tags": rec.get("tags") or [],
        "source_url": rec.get("source_url"),
    }
    if rec.get("dataset_id"):
        out["dataset_id"] = rec["dataset_id"]
        out["page_url"] = f"{base}/versions/{rec['dataset_id']}"
    if matched:
        out["matched_columns"] = matched
    if columns:
        out["columns"] = [{"name": c["name"], "type": c.get("type"), "alias": c.get("alias")}
                          for c in (rec.get("columns") or [])]
    return out


# ── tool implementations ────────────────────────────────────────────────────

async def _tool_list_schemas(request, db, user, a) -> tuple[dict, int]:
    catalog = await _catalog(db)
    by: dict[str, dict] = {}
    for rec in catalog:
        s = by.setdefault(rec["schema"], {"schema": rec["schema"], "tables": 0,
                                          "est_rows": 0, "source_types": set()})
        s["tables"] += 1
        s["est_rows"] += int(rec.get("est_rows") or 0)
        if rec.get("source_type"):
            s["source_types"].add(rec["source_type"])
    items = [{
        "schema": v["schema"],
        "description": data_catalog._SCHEMA_LABEL.get(v["schema"], v["schema"]),
        "tables": v["tables"],
        "est_rows": v["est_rows"],
        "source_types": sorted(v["source_types"]),
    } for v in by.values()]
    items.sort(key=lambda r: -r["tables"])
    return {"schemas": items, "search_path": data_catalog.CONSOLE_SEARCH_PATH,
            "total_tables": len(catalog),
            "console_url": f"{base_url(request)}/data",
            "source": "over.org.il — גרסאות לעם"}, len(items)


async def _tool_list_tables(request, db, user, a) -> tuple[dict, int]:
    catalog = await _catalog(db)
    q = (a.get("q") or "").strip().lower()
    rows = catalog
    if a.get("schema"):
        rows = [r for r in rows if r["schema"] == a["schema"]]
    if a.get("source_type"):
        rows = [r for r in rows if (r.get("source_type") or "") == a["source_type"]]
    if a.get("dataset_id"):
        did = str(a["dataset_id"]).strip()
        rows = [r for r in rows if (r.get("dataset_id") or "") == did]

    matched: dict[str, list[str]] = {}
    if q:
        keep = []
        for r in rows:
            cols = _matching_columns(r, q)
            if cols or _metadata_hit(r, q):
                matched[r["table"]] = cols
                keep.append(r)
        rows = keep

    limit = min(int(a.get("limit") or 50), 200)
    offset = max(int(a.get("offset") or 0), 0)
    total = len(rows)
    # Bigger tables first — with a generic query that is nearly always the one meant.
    page = sorted(rows, key=lambda r: -(r.get("est_rows") or 0))[offset:offset + limit]
    base = base_url(request)
    include_cols = bool(a.get("include_columns"))
    items = [_row(r, base, columns=include_cols, matched=matched.get(r["table"]) or [])
             for r in page]
    return {"total": total, "limit": limit, "offset": offset, "tables": items,
            "console_url": f"{base}/data",
            "note": ("הריצו describe_schema על טבלה לפני כתיבת SQL."
                     if items else "לא נמצאו טבלאות — נסו מילת חיפוש רחבה יותר או list_schemas."),
            "source": "over.org.il — גרסאות לעם"}, len(items)


async def _tool_describe_schema(request, db, user, a) -> tuple[dict, int]:
    table = (a.get("table") or "").strip()
    schema = (a.get("schema") or "").strip() or None
    if table:
        rec = _find(await _catalog(db), table)
        if rec["kind"] == "knesset":
            from app.services import knesset_db
            ddl = await knesset_db.schema_text()
        else:
            ddl = await append_store.schema_text(table, title=rec.get("title"),
                                                 schema=rec["schema"])
        return {"schema_ddl": ddl, "table": table, "schema": rec["schema"],
                "dialect": "postgresql",
                "search_path": data_catalog.CONSOLE_SEARCH_PATH}, 1
    _require_configured()
    if schema and schema not in ("public", "knesset", "idx", "odata", "ocal"):
        raise ValueError(f"סכימה לא מוכרת: {schema}")
    ddl = await data_catalog.schema_text_all(db, schema=schema)
    return {"schema_ddl": ddl, "schema": schema or "all", "dialect": "postgresql",
            "search_path": data_catalog.CONSOLE_SEARCH_PATH}, 1


async def _tool_run_sql(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    sql = a.get("sql") or ""
    max_rows = min(int(a.get("max_rows") or 200), MAX_SQL_ROWS)
    res = await append_store.run_readonly_sql(
        sql, search_path=data_catalog.CONSOLE_SEARCH_PATH,
        max_rows=max_rows, timeout_ms=SQL_TIMEOUT_MS)
    if res.get("truncated"):
        res["note"] = (f"התוצאה נקטעה ב-{max_rows} שורות. השתמשו ב-GROUP BY / "
                       "aggregate, או בהעלאת max_rows / הוספת LIMIT ו-OFFSET.")
    res["source"] = "over.org.il — גרסאות לעם (קונסולת SQL)"
    return res, res.get("row_count") or 0


async def _tool_get_table(request, db, user, a) -> tuple[dict, int]:
    table = (a.get("table") or "").strip()
    _find(await _catalog(db), table)  # resolve + friendly error before the heavy call
    detail = await data_catalog.table_detail(table, db)
    if detail is None:  # catalog changed under us
        raise ValueError(f"לא נמצאה טבלה בשם '{table}'")
    base = base_url(request)
    out = {k: v for k, v in detail.items() if k not in ("field_flags",)}
    for key in ("archive_url", "versions_url", "csv_url"):
        if out.get(key) and str(out[key]).startswith("/"):
            out[key] = f"{base}{out[key]}"
    for f in out.get("files") or []:
        if str(f.get("url", "")).startswith("/"):
            f["url"] = f"{base}{f['url']}"
    out["console_url"] = f"{base}/data?table={table}"
    out["source"] = "over.org.il — גרסאות לעם"
    return out, len(out.get("sample") or [])


async def _tool_get_table_profile(request, db, user, a) -> tuple[dict, int]:
    from app.services import table_profiler
    table = (a.get("table") or "").strip()
    rec = _find(await _catalog(db), table)
    prof = await table_profiler.get_profile(rec["schema"], table)
    if not prof:
        return {"table": table, "schema": rec["schema"], "profiled": False,
                "note": ("הטבלה עדיין לא עברה פרופיל. השתמשו ב-get_table לדוגמת "
                         "שורות ולרשימת העמודות.")}, 0
    return prof, 1


_IMPL = {
    "list_schemas": _tool_list_schemas,
    "list_tables": _tool_list_tables,
    "describe_schema": _tool_describe_schema,
    "run_sql": _tool_run_sql,
    "get_table": _tool_get_table,
    "get_table_profile": _tool_get_table_profile,
}


# ── JSON-RPC dispatch ───────────────────────────────────────────────────────

def _rpc_result(mid, result):
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def _rpc_error(mid, code, message):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


async def _run_tool(request: Request, db: AsyncSession, user: McpUser, session_id: str | None,
                    name: str, args: dict) -> dict:
    impl = _IMPL.get(name)
    started = time.time()
    if not impl:
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=None, result_bytes=None,
                        latency_ms=int((time.time() - started) * 1000), status="error",
                        error_message="unknown tool")
        return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    try:
        data, count = await impl(request, db, user, args or {})
        text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=count,
                        result_bytes=len(text.encode("utf-8")),
                        latency_ms=int((time.time() - started) * 1000), status="ok", error_message=None)
        return {"content": [{"type": "text", "text": text}]}
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors are the user's answer
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=None, result_bytes=None,
                        latency_ms=int((time.time() - started) * 1000), status="error",
                        error_message=str(e)[:1000])
        return {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}


async def handle_message(request: Request, db: AsyncSession, user: McpUser,
                         session_id: str | None, msg: dict):
    """Handle one JSON-RPC message. Returns a response dict, or None for notifications."""
    method = msg.get("method")
    mid = msg.get("id")
    is_notification = "id" not in msg

    if method == "initialize":
        client_proto = (msg.get("params") or {}).get("protocolVersion") or DEFAULT_PROTOCOL
        return _rpc_result(mid, {
            "protocolVersion": client_proto,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            "instructions": SERVER_INSTRUCTIONS,
        })
    if method in ("notifications/initialized", "notifications/cancelled"):
        return None
    if method == "ping":
        return _rpc_result(mid, {})
    if method == "tools/list":
        return _rpc_result(mid, {"tools": TOOLS})
    if method == "tools/call":
        params = msg.get("params") or {}
        result = await _run_tool(request, db, user, session_id,
                                 params.get("name"), params.get("arguments") or {})
        return _rpc_result(mid, result)

    if is_notification:
        return None
    return _rpc_error(mid, -32601, f"Method not found: {method}")


def _uuid(s):
    try:
        return uuid.UUID(str(s)) if s else None
    except (ValueError, TypeError):
        return None
