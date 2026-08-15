"""Dedicated MCP server for מידע לעם (odata.org.il).

A fifth MCP surface, mounted at ``/odata/mcp``, over the sibling לעם project's
CKAN catalog: ~11,500 datasets of PROCESSED data — mostly responses to freedom
-of-information requests, published by NGOs, municipalities and individual
requesters.

**Why the server lives here and not on odata.org.il.** The same reason ocal and
ocoi were folded into OVER: one deploy, one OAuth server, one ``api_users``
allow-list, one usage log. It also lets the cross-source deep search dispatch
this corpus IN-PROCESS — no token, no second cold start — which is the whole
reason those five servers share a shape.

**This is a pass-through, and that matters for how results are labelled.** OVER
stores nothing here: every call goes to odata's public CKAN API and the files
stay on odata.org.il. The data is also *processed* — a spreadsheet extracted
from an FOI response is not the authority's own publication — so every tool
result carries the source and a link back, and the instructions below require
the caller to say so. See ``app/services/odata_import.py`` for the separate,
admin-curated path that copies SELECTED resources into queryable NEON tables.

Protocol: the same hand-rolled Streamable-HTTP JSON-RPC subset the other four
speak. Kept self-contained so they evolve independently.
"""
from __future__ import annotations

import json
import time
import uuid

import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.mcp.auth import McpUser
from app.mcp.usage import log_usage
from app.services.odata_import import ODATA_BASE, _fetch_json

SERVER_NAME = "over-odata-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "מידע לעם (odata.org.il) — קטלוג של כ-11,500 מאגרי מידע מעובדים, רובם "
    "תשובות לבקשות חופש מידע שהתקבלו מרשויות ופורסמו ע\"י עמותות, רשויות "
    "מקומיות ומבקשים פרטיים. הגישה כאן היא pass-through: גרסאות לעם אינה "
    "מאחסנת את הקבצים — כל קריאה פונה ל-API הציבורי של odata.org.il והקבצים "
    "נפתחים משם. כשאתה מציג תוצאות: (1) ציין שהמקור הוא מידע לעם (odata.org.il) "
    "ושמדובר במידע מעובד — קובץ שהתקבל בתשובה לבקשת חופש מידע אינו פרסום רשמי "
    "של הרשות; (2) קשר תמיד ל-url של המאגר; (3) ציין את הארגון המפרסם "
    "(organization), כי הוא מי שביקש ופרסם, ולא בהכרח מי שהפיק את הנתונים."
)

# A search must not become a bulk export of the catalog.
MAX_ROWS = 50
_TIMEOUT = httpx.Timeout(25.0)


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search_datasets",
        "description": (
            "חיפוש טקסט חופשי בקטלוג מידע לעם (כותרת, תיאור, תגיות). אפשר לסנן "
            "לפי הארגון המפרסם. מחזיר מאגרים עם קישור לעמוד המאגר ורשימת הקבצים "
            "שבו. שים לב: המידע מעובד — ברובו תשובות לבקשות חופש מידע."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "טקסט חופשי (עברית/אנגלית)"},
                "organization": {
                    "type": "string",
                    "description": "מזהה הארגון המפרסם, למשל hatzlacha / "
                                   "freedom-of-information-israel / jerusalem_muni "
                                   "(מתוך list_organizations)",
                },
                "limit": {"type": "integer", "description": f"מספר תוצאות (1-{MAX_ROWS})"},
                "offset": {"type": "integer", "description": "דילוג לצורך עימוד"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_dataset",
        "description": (
            "פרטי מאגר יחיד ממידע לעם לפי המזהה או ה-slug שלו, כולל כל הקבצים "
            "(שם, פורמט, קישור להורדה) והמטא-דאטה המלאה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string", "description": "id או name (slug) של המאגר"},
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "list_organizations",
        "description": (
            "רשימת הארגונים המפרסמים במידע לעם עם מספר המאגרים של כל אחד. "
            "השתמש כדי לגלות ערכים חוקיים לפרמטר organization."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── helpers ────────────────────────────────────────────────────────────────

def dataset_url(pkg: dict) -> str:
    """The dataset's page on odata.org.il. Prefer the slug — it is the stable,
    human-readable address; the uuid works but tells the reader nothing."""
    ident = pkg.get("name") or pkg.get("id") or ""
    return f"{ODATA_BASE}/dataset/{ident}"


def _resources(pkg: dict) -> list[dict]:
    out = []
    for r in (pkg.get("resources") or []):
        if not isinstance(r, dict):
            continue
        out.append({
            "name": r.get("name"),
            "format": (r.get("format") or "").upper() or None,
            "url": r.get("url"),
            "size": r.get("size"),
        })
    return out


def _pkg_to_item(pkg: dict) -> dict:
    org = pkg.get("organization") or {}
    return {
        "id": pkg.get("id"),
        "name": pkg.get("name"),
        "title": pkg.get("title") or pkg.get("name"),
        "notes": pkg.get("notes"),
        # WHO PUBLISHED, which on this catalog is the requester rather than the
        # authority that produced the data — the instructions make the caller
        # say so, because conflating the two misattributes the document.
        "organization": org.get("title") or org.get("name"),
        "organization_id": org.get("name"),
        "url": dataset_url(pkg),
        "metadata_created": pkg.get("metadata_created"),
        "metadata_modified": pkg.get("metadata_modified"),
        "num_resources": len(pkg.get("resources") or []),
        "resources": _resources(pkg),
        "source": "מידע לעם (odata.org.il) — מידע מעובד",
    }


def _clamp(v, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(int(v), hi))
    except (TypeError, ValueError):
        return default


async def _ckan(action: str, **params) -> dict:
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as c:
        return await _fetch_json(c, action, **params)


# ── tools ──────────────────────────────────────────────────────────────────

async def _tool_search(request, db, user, a) -> tuple[dict, int]:
    q = (a.get("query") or "").strip()
    if not q:
        raise ValueError("חסר טקסט לחיפוש")
    rows = _clamp(a.get("limit"), 1, MAX_ROWS, 20)
    start = _clamp(a.get("offset"), 0, 10_000, 0)
    params = {"q": q, "rows": rows, "start": start}
    org = (a.get("organization") or "").strip()
    if org:
        # fq is CKAN's filter query; quoting keeps a slug with a dash from being
        # read as two terms.
        params["fq"] = f'organization:"{org}"'
    res = await _ckan("package_search", **params)
    items = [_pkg_to_item(p) for p in (res.get("results") or []) if isinstance(p, dict)]
    return {
        "items": items,
        "total": res.get("count"),
        "offset": start,
        "source": "מידע לעם (odata.org.il)",
        "note": "מידע מעובד — ברובו תשובות לבקשות חופש מידע. הקבצים מאוחסנים "
                "ב-odata.org.il ולא בגרסאות לעם.",
    }, len(items)


async def _tool_get_dataset(request, db, user, a) -> tuple[dict, int]:
    ident = (a.get("dataset_id") or "").strip()
    if not ident:
        raise ValueError("חסר מזהה מאגר")
    pkg = await _ckan("package_show", id=ident)
    return {"dataset": _pkg_to_item(pkg), "source": "מידע לעם (odata.org.il)"}, 1


async def _tool_list_organizations(request, db, user, a) -> tuple[dict, int]:
    # Read the counts off a facet rather than organization_list: one call, and
    # the number is the count of datasets actually searchable.
    res = await _ckan("package_search", q="*:*", rows=0,
                      **{"facet.field": '["organization"]', "facet.limit": 200})
    facet = ((res.get("search_facets") or {}).get("organization") or {})
    orgs = [{"id": it.get("name"), "title": it.get("display_name") or it.get("name"),
             "datasets": it.get("count")}
            for it in (facet.get("items") or []) if isinstance(it, dict)]
    orgs.sort(key=lambda o: o.get("datasets") or 0, reverse=True)
    return {"organizations": orgs, "total_datasets": res.get("count"),
            "source": "מידע לעם (odata.org.il)"}, len(orgs)


_IMPL = {
    "search_datasets": _tool_search,
    "get_dataset": _tool_get_dataset,
    "list_organizations": _tool_list_organizations,
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
