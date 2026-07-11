"""Dedicated MCP server for Knesset committee protocols (``/knesset/mcp``).

A THIRD MCP surface (after /mcp and /cbs/mcp), exposing the committee-protocols
slice of the Knesset ODATA mirror — the ``knesset`` schema in the append DB
(app/services/knesset_db.py): committees, sessions, discussed items and the
protocol DOCUMENT LINKS. Metadata + links ONLY — the documents' text content is
never fetched or stored here; callers follow the fs.knesset.gov.il FilePath.

Shares the main MCP's OAuth authorization server, ``api_users`` allow-list and
the machine-to-machine service token (app/mcp/auth.py); only the tools and the
resource identity differ. Same hand-rolled Streamable-HTTP JSON-RPC subset as
cbs_server.py. Usage logged to mcp_usage_events.

Data access: parameterized asyncpg queries against the knesset schema (the
append DB), NEVER string-interpolated user input; the generic ``run_sql`` tool
reuses knesset_db.run_sql (single SELECT, READ ONLY tx, statement_timeout).
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
from app.services import append_store, knesset_db

SERVER_NAME = "over-knesset-protocols-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

PROTOCOL_GROUP_TYPE = 23  # GroupTypeID of "פרוטוקול" in KNS_DocumentCommitteeSession

SERVER_INSTRUCTIONS = (
    "פרוטוקולי ועדות הכנסת — גרסאות לעם (OVER, over.org.il). מראה SQL של טבלאות "
    "ה-ODATA הרשמיות של הכנסת (ועדות, ישיבות, נושאי דיון ומסמכים), מתעדכנת כל "
    "כ-12 שעות. הכלים מחזירים מטא-דאטה וקישורים בלבד — תוכן הפרוטוקולים עצמם "
    "אינו נשמר כאן; קישור הקובץ (file_url) מוביל ישירות לשרת הכנסת "
    "(fs.knesset.gov.il). כשאתה מציג תוצאות: (1) ציין שהמקור הוא מראה נתוני "
    "הכנסת של גרסאות לעם; (2) צרף את file_url של הפרוטוקול ו/או את session_url "
    "לאימות; (3) זכור שמחיקות במקור אינן משתקפות במראה. לשאילתות חופשיות השתמש "
    "בכלי run_sql (סכימת knesset, שמות טבלאות באותיות קטנות — kns_committee, "
    "kns_committeesession, kns_cmtsessionitem, kns_documentcommitteesession)."
)

_SESSION_COLS = (
    "s.id, s.number, s.knessetnum, s.typedesc, s.committeeid, s.statusdesc, "
    "s.location, s.sessionurl, s.broadcasturl, s.startdate, s.finishdate, s.note"
)


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search_committees",
        "description": (
            "חיפוש ועדות כנסת לפי שם ו/או מספר כנסת. מחזיר את פרטי הוועדה כולל "
            "מזהה (id) לשימוש בכלים האחרים. אותה ועדה מופיעה כרשומה נפרדת לכל כנסת."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "טקסט חופשי בשם הוועדה, למשל 'כספים'"},
                "knesset_num": {"type": "integer", "description": "מספר כנסת, למשל 25"},
                "only_current": {"type": "boolean", "default": False, "description": "רק ועדות פעילות כעת"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
            },
        },
    },
    {
        "name": "search_sessions",
        "description": (
            "חיפוש ישיבות ועדה לפי ועדה, כנסת, טווח תאריכים ו/או טקסט חופשי בנושאי "
            "הדיון. מחזיר לכל ישיבה את מועדה, מיקומה, קישור לעמוד הישיבה ואת נושאי "
            "הדיון שלה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "committee_id": {"type": "integer", "description": "מזהה ועדה (מ-search_committees)"},
                "knesset_num": {"type": "integer"},
                "q": {"type": "string", "description": "טקסט חופשי בנושאי הדיון של הישיבה"},
                "date_from": {"type": "string", "description": "מתאריך, YYYY-MM-DD"},
                "date_to": {"type": "string", "description": "עד תאריך, YYYY-MM-DD"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "search_protocols",
        "description": (
            "חיפוש פרוטוקולים של ישיבות ועדה — מסמכי ה'פרוטוקול' הרשמיים. סינון לפי "
            "ועדה, כנסת, טווח תאריכים ו/או טקסט חופשי בנושאי הישיבה. מחזיר לכל "
            "פרוטוקול את קישור הקובץ (file_url, בשרת הכנסת) + פרטי הישיבה והוועדה. "
            "התוכן עצמו אינו נשמר כאן — יש לפתוח את הקישור."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "committee_id": {"type": "integer"},
                "knesset_num": {"type": "integer"},
                "q": {"type": "string", "description": "טקסט חופשי בנושאי הישיבה או בשם הוועדה"},
                "date_from": {"type": "string", "description": "YYYY-MM-DD"},
                "date_to": {"type": "string", "description": "YYYY-MM-DD"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "get_session",
        "description": (
            "פרטי ישיבת ועדה בודדת לפי מזהה: הוועדה, המועד, כל נושאי הדיון וכל "
            "המסמכים המקושרים (פרוטוקול, הקלטות וכו') עם קישוריהם."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"session_id": {"type": "integer", "description": "מזהה הישיבה"}},
            "required": ["session_id"],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "שאילתת SQL חופשית (SELECT יחיד, קריאה בלבד) מעל כל 48 טבלאות מראה "
            "הכנסת בסכימת knesset — לא רק ועדות. שמות טבלאות ועמודות באותיות "
            "קטנות (KNS_Bill ← kns_bill). עד 500 שורות. השתמש ב-list_tables "
            "לקטלוג המלא."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "שאילתת SELECT"}},
            "required": ["sql"],
        },
    },
    {
        "name": "list_tables",
        "description": "קטלוג טבלאות מראה הכנסת: שם SQL, תיאור בעברית, מספר שורות ורשימת עמודות.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "group": {"type": "string", "description": "סינון לקבוצת נושא, למשל 'ועדות הכנסת'"},
            },
        },
    },
    {
        "name": "get_stats",
        "description": "סטטיסטיקה וטריות: כמה ועדות/ישיבות/פרוטוקולים במראה ומתי סונכרן לאחרונה.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── helpers ─────────────────────────────────────────────────────────────────

async def _fetch(sql: str, *params) -> list[dict]:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        return [dict(r) for r in await conn.fetch(sql, *params)]


def _require_configured() -> None:
    if not knesset_db.is_configured():
        raise ValueError("מראה נתוני הכנסת אינו מוגדר בשרת זה")


def _clamp(a: dict, key: str, default: int, lo: int = 1, hi: int = 50) -> int:
    try:
        return max(lo, min(int(a.get(key) or default), hi))
    except (TypeError, ValueError):
        return default


def _date_param(a: dict, key: str):
    """Parse a YYYY-MM-DD tool arg to a date (None if absent); raises on junk."""
    v = (a.get(key) or "").strip()
    if not v:
        return None
    from datetime import date
    try:
        return date.fromisoformat(v)
    except ValueError:
        raise ValueError(f"{key} חייב להיות בפורמט YYYY-MM-DD")


def _session_item(r: dict, b: str) -> dict:
    out = dict(r)
    for k in ("startdate", "finishdate"):
        if out.get(k) is not None:
            out[k] = str(out[k])
    out["over_url"] = f"{b}/knesset"
    return out


# ── tool implementations ────────────────────────────────────────────────────

async def _tool_search_committees(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    conds, params = [], []
    if (a.get("q") or "").strip():
        params.append(f"%{a['q'].strip()}%")
        conds.append(f"name ILIKE ${len(params)}")
    if a.get("knesset_num") is not None:
        params.append(int(a["knesset_num"]))
        conds.append(f"knessetnum = ${len(params)}")
    if a.get("only_current"):
        conds.append("iscurrent = true")
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    limit = _clamp(a, "limit", 20)
    rows = await _fetch(
        f"SELECT id, name, knessetnum, categoryid, categorydesc, committeetypedesc, "
        f"parentcommitteeid, committeeparentname, iscurrent "
        f"FROM knesset.kns_committee {where} "
        f"ORDER BY knessetnum DESC NULLS LAST, id LIMIT {limit}", *params)
    return {"items": rows, "count": len(rows),
            "source": "over.org.il — מראה נתוני הכנסת (ODATA)"}, len(rows)


def _session_filters(a: dict, params: list) -> str:
    """Shared WHERE builder for session-scoped searches ($-numbered params)."""
    conds = []
    if a.get("committee_id") is not None:
        params.append(int(a["committee_id"]))
        conds.append(f"s.committeeid = ${len(params)}")
    if a.get("knesset_num") is not None:
        params.append(int(a["knesset_num"]))
        conds.append(f"s.knessetnum = ${len(params)}")
    d_from = _date_param(a, "date_from")
    if d_from:
        params.append(d_from)
        conds.append(f"s.startdate >= ${len(params)}")
    d_to = _date_param(a, "date_to")
    if d_to:
        params.append(d_to)
        conds.append(f"s.startdate < ${len(params)}::date + 1")
    if (a.get("q") or "").strip():
        params.append(f"%{a['q'].strip()}%")
        n = len(params)
        conds.append(
            f"(EXISTS (SELECT 1 FROM knesset.kns_cmtsessionitem it "
            f"         WHERE it.committeesessionid = s.id AND it.name ILIKE ${n}) "
            f" OR c.name ILIKE ${n})"
        )
    return ("WHERE " + " AND ".join(conds)) if conds else ""


async def _tool_search_sessions(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    params: list = []
    where = _session_filters(a, params)
    limit = _clamp(a, "limit", 20)
    offset = max(int(a.get("offset") or 0), 0)
    rows = await _fetch(
        f"SELECT {_SESSION_COLS}, c.name AS committee_name, "
        f"  (SELECT array_agg(it.name ORDER BY it.ordinal) "
        f"     FROM knesset.kns_cmtsessionitem it WHERE it.committeesessionid = s.id) AS topics "
        f"FROM knesset.kns_committeesession s "
        f"LEFT JOIN knesset.kns_committee c ON c.id = s.committeeid "
        f"{where} ORDER BY s.startdate DESC NULLS LAST, s.id DESC "
        f"LIMIT {limit} OFFSET {offset}", *params)
    b = base_url(request)
    return {"items": [_session_item(r, b) for r in rows], "count": len(rows),
            "offset": offset,
            "source": "over.org.il — מראה נתוני הכנסת (ODATA)"}, len(rows)


async def _tool_search_protocols(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    params: list = [PROTOCOL_GROUP_TYPE]
    where = _session_filters(a, params)
    where = (where + " AND " if where else "WHERE ") + "d.grouptypeid = $1"
    limit = _clamp(a, "limit", 20)
    offset = max(int(a.get("offset") or 0), 0)
    rows = await _fetch(
        f"SELECT d.id AS document_id, d.filepath AS file_url, d.applicationdesc AS file_format, "
        f"  s.id AS session_id, s.startdate, s.knessetnum, s.sessionurl AS session_url, "
        f"  c.id AS committee_id, c.name AS committee_name "
        f"FROM knesset.kns_documentcommitteesession d "
        f"JOIN knesset.kns_committeesession s ON s.id = d.committeesessionid "
        f"LEFT JOIN knesset.kns_committee c ON c.id = s.committeeid "
        f"{where} ORDER BY s.startdate DESC NULLS LAST, d.id DESC "
        f"LIMIT {limit} OFFSET {offset}", *params)
    b = base_url(request)
    items = [_session_item(r, b) for r in rows]
    return {"items": items, "count": len(items), "offset": offset,
            "note": "file_url מוביל לקובץ הפרוטוקול בשרת הכנסת; התוכן אינו נשמר בגרסאות לעם.",
            "source": "over.org.il — מראה נתוני הכנסת (ODATA)"}, len(items)


async def _tool_get_session(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    sid = int(a.get("session_id") or 0)
    if not sid:
        raise ValueError("session_id נדרש")
    ses = await _fetch(
        f"SELECT {_SESSION_COLS}, c.name AS committee_name "
        f"FROM knesset.kns_committeesession s "
        f"LEFT JOIN knesset.kns_committee c ON c.id = s.committeeid WHERE s.id = $1", sid)
    if not ses:
        raise ValueError("הישיבה לא נמצאה במראה")
    items = await _fetch(
        "SELECT name, ordinal, itemtypeid FROM knesset.kns_cmtsessionitem "
        "WHERE committeesessionid = $1 ORDER BY ordinal", sid)
    docs = await _fetch(
        "SELECT id AS document_id, grouptypeid, grouptypedesc, filepath AS file_url, "
        "applicationdesc AS file_format FROM knesset.kns_documentcommitteesession "
        "WHERE committeesessionid = $1 ORDER BY grouptypeid, id", sid)
    b = base_url(request)
    out = _session_item(ses[0], b)
    out["topics"] = items
    out["documents"] = docs
    return out, 1


async def _tool_run_sql(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    res = await knesset_db.run_sql(a.get("sql") or "", max_rows=500)
    res["source"] = "over.org.il — מראה נתוני הכנסת (סכימת knesset)"
    return res, res.get("row_count") or 0


async def _tool_list_tables(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    tables = await knesset_db.list_tables()
    group = (a.get("group") or "").strip()
    if group:
        tables = [t for t in tables if t["group"] == group]
    items = [{
        "table": t["table"], "group": t["group"], "description": t["description"],
        "total_rows": t["total_rows"], "full_loaded": t["full_loaded"],
        "columns": [c["name"] for c in t["columns"]],
    } for t in tables]
    return {"tables": items, "count": len(items)}, len(items)


async def _tool_get_stats(request, db, user, a) -> tuple[dict, int]:
    _require_configured()
    summary = await knesset_db.status_summary()
    counts = await _fetch(
        "SELECT (SELECT count(*) FROM knesset.kns_committee) AS committees, "
        "(SELECT count(*) FROM knesset.kns_committeesession) AS sessions, "
        "(SELECT count(*) FROM knesset.kns_documentcommitteesession WHERE grouptypeid = $1) AS protocols",
        PROTOCOL_GROUP_TYPE)
    return {"mirror": summary, **counts[0],
            "source": "over.org.il — מראה נתוני הכנסת (ODATA)"}, 0


_IMPL = {
    "search_committees": _tool_search_committees,
    "search_sessions": _tool_search_sessions,
    "search_protocols": _tool_search_protocols,
    "get_session": _tool_get_session,
    "run_sql": _tool_run_sql,
    "list_tables": _tool_list_tables,
    "get_stats": _tool_get_stats,
}


# ── JSON-RPC dispatch (same shape as cbs_server) ─────────────────────────────

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
        out = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=count,
                        result_bytes=len(out.encode("utf-8")),
                        latency_ms=int((time.time() - started) * 1000), status="ok", error_message=None)
        return {"content": [{"type": "text", "text": out}]}
    except Exception as e:  # noqa: BLE001 — tool errors go back to the model
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=None, result_bytes=None,
                        latency_ms=int((time.time() - started) * 1000), status="error",
                        error_message=str(e)[:1000])
        return {"content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}


async def handle_message(request: Request, db: AsyncSession, user: McpUser, session_id: str | None, msg: dict):
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
