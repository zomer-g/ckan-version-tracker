"""Dedicated MCP server for the CBS (cbs.gov.il) content index.

A SECOND MCP surface, mounted at ``/cbs/mcp``, exposing OVER's index of the
Israeli Central Bureau of Statistics site (the ``cbs_index`` table — see
app/models/cbs_index.py + app/api/cbs.py). It shares the main MCP's OAuth
authorization server and ``api_users`` allow-list (app/mcp/oauth.py); only the
tools and the resource identity differ.

Protocol: the same hand-rolled Streamable-HTTP JSON-RPC subset the main server
speaks (app/mcp/server.py). Kept self-contained so the two servers evolve
independently. Usage is logged to mcp_usage_events via the shared logger.
"""
from __future__ import annotations

import json
import time
import uuid
from urllib.parse import quote

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.api.cbs_search_util import RESULT_COLS, build_search
from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage

SERVER_NAME = "over-cbs-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "אינדקס הלמ\"ס של גרסאות לעם (OVER, over.org.il) — חיפוש מעל תוכן אתר הלשכה "
    "המרכזית לסטטיסטיקה (cbs.gov.il): פרסומים, הודעות לתקשורת, לוחות, סקרים "
    "וקבצים להורדה (xlsx/pdf/csv). זהו אינדקס מעובד שנבנה ע\"י גרסאות לעם — לא "
    "פלט רשמי של הלמ\"ס. כשאתה מציג תוצאות: (1) ציין שהמקור הוא אינדקס הלמ\"ס של "
    "גרסאות לעם; (2) קשר ל-page_url (עמוד המקור באתר cbs.gov.il) וכן ל-file_links "
    "(הקבצים עצמם נשמרים באתר הלמ\"ס ונפתחים ישירות משם); (3) לעיון אנושי אפשר "
    "לצרף את over_url (עמוד החיפוש ב-over.org.il/cbs). הקבצים אינם מאוחסנים "
    "בגרסאות לעם — האינדקס מקטלג מה קיים, לא את הקבצים עצמם."
)

# Columns returned for a page (qualified-free — single table in these queries).
_COLS = (
    "url, lang, section, series, item_type, title, title_en, summary, "
    "subject_tags, year_start, year_end, geo_levels, file_links, file_types, "
    "extra, last_crawled"
)
# Same list aliased to cbs_index (used where cbs_featured is joined — both tables
# carry a ``url`` column so an unqualified SELECT would be ambiguous).
_COLS_I = ", ".join(f"i.{c.strip()}" for c in _COLS.split(","))


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search",
        "description": (
            "חיפוש טקסט חופשי + סינון מעל אינדקס הלמ\"ס. סינון לפי נושא, רזולוציה "
            "גאוגרפית, סוג קובץ, סוג עמוד (section), טווח שנים, ומיון לפי רלוונטיות "
            "או כרונולוגי. מחזיר עמודים עם קישורים וקבצים."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "טקסט חופשי (עברית/אנגלית)"},
                "subject": {"type": "string", "description": "נושא מדויק (מתוך facets.subjects)"},
                "geo": {"type": "string", "description": "רמה גאוגרפית (מתוך facets.geo_levels)"},
                "file_type": {"type": "string", "description": "סיומת קובץ, למשל xlsx / pdf"},
                "section": {"type": "string", "description": "סוג עמוד: mediarelease / publications / subjects / databank ..."},
                "item_type": {"type": "string", "description": "publication / media_release / table / tool / subject / page / intent"},
                "lang": {"type": "string", "description": "שפת העמוד: he / en"},
                "year_from": {"type": "integer"},
                "year_to": {"type": "integer"},
                "sort": {"type": "string", "enum": ["relevance", "chrono"], "default": "relevance",
                          "description": "relevance (ברירת מחדל) או chrono (שנה יורדת)"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "resolve",
        "description": (
            "פתרון שאלה בשפה טבעית למיקום המדויק בלמ\"ס. מקבל שאלה חופשית ומחזיר "
            "תשובה מובנית: answer_type (guidance=הפניה מומלצת / generator=מחולל / "
            "data_file=קובץ להורדה / publication=פרסום / not_available=אין בלמ\"ס / "
            "no_results), הקישור הראשי (primary), הרזולוציה הגאוגרפית הזמינה, "
            "הסתייגויות, ותוצאות מגובות. עדיף על search לשאלות ניסוח-אנושי."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "השאלה בשפה טבעית (עברית/אנגלית)"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 30, "default": 10},
            },
            "required": ["q"],
        },
    },
    {
        "name": "get_page",
        "description": "פרטי עמוד בודד לפי ה-URL שלו באתר הלמ\"ס, כולל תקציר, קבצים, נושאים ומטא-דאטה.",
        "inputSchema": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "ה-URL המלא של העמוד ב-cbs.gov.il"}},
            "required": ["url"],
        },
    },
    {
        "name": "facets",
        "description": "ערכי הסינון הזמינים: נושאים, רמות גאוגרפיות, סוגי קבצים, סוגי עמודים, וטווח השנים.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_featured",
        "description": "העמודים ה'מבוקשים' שהוצמדו ע\"י מנהל באתר over.org.il/cbs — קבצים מרכזיים לגישה מהירה.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_stats",
        "description": "סטטיסטיקת כיסוי: כמה עמודים באינדקס ופילוח לפי סוג עמוד.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── helpers ─────────────────────────────────────────────────────────────────

def _row_to_item(r: dict, b: str) -> dict:
    """Shape one cbs_index row for tool output, adding verification links."""
    url = r.get("url")
    item = {k: r.get(k) for k in (
        "url", "lang", "section", "series", "item_type", "title", "title_en",
        "summary", "subject_tags", "year_start", "year_end", "geo_levels",
        "file_links", "file_types",
    )}
    item["page_url"] = url  # the source page on cbs.gov.il
    item["over_url"] = f"{b}/cbs?q={quote((r.get('title') or '').strip())}" if r.get("title") else f"{b}/cbs"
    return item


# ── tool implementations ────────────────────────────────────────────────────

async def _tool_search(request, db, user, a) -> tuple[dict, int]:
    # Use the shared builder so an MCP client gets the SAME retrieval + ranking
    # as the website: OR-of-words tsquery (was plainto_tsquery — AND-of-words,
    # strictly worse for Hebrew NL), the ``lang`` filter (previously ignored),
    # intent boosting, catch-all demotion and recency tie-breakers.
    where, order, params = build_search(
        {
            "q": a.get("q"), "subject": a.get("subject"), "geo": a.get("geo"),
            "file_type": a.get("file_type"), "section": a.get("section"),
            "item_type": a.get("item_type"), "lang": a.get("lang"),
            "year_from": a.get("year_from"), "year_to": a.get("year_to"),
        },
        sort=("chrono" if a.get("sort") == "chrono" else "relevance"),
    )
    total = (await db.execute(text(f"SELECT count(*) FROM cbs_index{where}"), params)).scalar_one()

    limit = min(int(a.get("limit") or 20), 50)
    offset = max(int(a.get("offset") or 0), 0)
    params["limit"] = limit
    params["offset"] = offset
    rows = (await db.execute(
        text(f"SELECT {RESULT_COLS} FROM cbs_index{where} ORDER BY {order} LIMIT :limit OFFSET :offset"),
        params,
    )).mappings().all()
    b = base_url(request)
    items = [_row_to_item(dict(r), b) for r in rows]
    return {"total": int(total), "limit": limit, "offset": offset, "items": items,
            "source": "over.org.il — אינדקס הלמ\"ס (cbs.gov.il)"}, len(items)


async def _tool_resolve(request, db, user, a) -> tuple[dict, int]:
    # Delegate to the same resolver the REST /api/cbs/resolve uses, so the MCP
    # client gets the identical LLM-parse + intent-aware ranking + answer_type
    # classification. Needs an LLM key (DeepSeek/Anthropic) — errors surface as a
    # normal tool error if unconfigured.
    from app.api.cbs_ask import resolve_question

    q = (a.get("q") or "").strip()
    if not q:
        raise ValueError("q נדרש")
    limit = min(int(a.get("limit") or 10), 30)
    data = await resolve_question(db, q, limit)
    return data, len(data.get("results") or [])


async def _tool_get_page(request, db, user, a) -> tuple[dict, int]:
    url = (a.get("url") or "").strip()
    if not url:
        raise ValueError("url נדרש")
    r = (await db.execute(
        text(f"SELECT {_COLS} FROM cbs_index WHERE url = :url"), {"url": url}
    )).mappings().first()
    if not r:
        raise ValueError("העמוד לא נמצא באינדקס")
    b = base_url(request)
    item = _row_to_item(dict(r), b)
    item["extra"] = r.get("extra")
    return item, 1


async def _tool_facets(request, db, user, a) -> tuple[dict, int]:
    async def distinct_jsonb(col: str) -> list[str]:
        r = await db.execute(text(
            f"SELECT DISTINCT elem AS v FROM cbs_index "
            f"CROSS JOIN LATERAL jsonb_array_elements_text({col}) AS elem "
            f"WHERE {col} IS NOT NULL AND jsonb_typeof({col}) = 'array' ORDER BY v"
        ))
        return [row[0] for row in r]

    async def distinct_col(col: str) -> list[str]:
        r = await db.execute(text(
            f"SELECT DISTINCT {col} AS v FROM cbs_index WHERE {col} IS NOT NULL ORDER BY v"
        ))
        return [row[0] for row in r]

    years = (await db.execute(text("SELECT min(year_start), max(year_end) FROM cbs_index"))).one()
    return {
        "subjects": await distinct_jsonb("subject_tags"),
        "geo_levels": await distinct_jsonb("geo_levels"),
        "file_types": await distinct_jsonb("file_types"),
        "sections": await distinct_col("section"),
        "item_types": await distinct_col("item_type"),
        "year_min": years[0], "year_max": years[1],
    }, 0


async def _tool_list_featured(request, db, user, a) -> tuple[dict, int]:
    rows = (await db.execute(text(
        f"SELECT {_COLS_I} FROM cbs_index i JOIN cbs_featured f ON f.url = i.url "
        f"ORDER BY f.sort_order, f.id"
    ))).mappings().all()
    b = base_url(request)
    items = [_row_to_item(dict(r), b) for r in rows]
    return {"items": items, "source": "over.org.il/cbs — עמודים מבוקשים"}, len(items)


async def _tool_get_stats(request, db, user, a) -> tuple[dict, int]:
    total = (await db.execute(text("SELECT count(*) FROM cbs_index"))).scalar_one()
    by_section = dict((await db.execute(text(
        "SELECT section, count(*) FROM cbs_index WHERE section IS NOT NULL GROUP BY section ORDER BY 2 DESC"
    ))).all())
    return {"total": int(total), "by_section": {k: int(v) for k, v in by_section.items()},
            "source": "over.org.il — אינדקס הלמ\"ס"}, 0


_IMPL = {
    "search": _tool_search,
    "resolve": _tool_resolve,
    "get_page": _tool_get_page,
    "facets": _tool_facets,
    "list_featured": _tool_list_featured,
    "get_stats": _tool_get_stats,
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
        out = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=count,
                        result_bytes=len(out.encode("utf-8")),
                        latency_ms=int((time.time() - started) * 1000), status="ok", error_message=None)
        return {"content": [{"type": "text", "text": out}]}
    except Exception as e:  # noqa: BLE001
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
        name = params.get("name")
        args = params.get("arguments") or {}
        result = await _run_tool(request, db, user, session_id, name, args)
        return _rpc_result(mid, result)

    if is_notification:
        return None
    return _rpc_error(mid, -32601, f"Method not found: {method}")


def _uuid(s):
    try:
        return uuid.UUID(str(s)) if s else None
    except (ValueError, TypeError):
        return None
