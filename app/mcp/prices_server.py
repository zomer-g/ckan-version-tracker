"""Dedicated MCP server for שקיפות מחירים, mounted at ``/prices/mcp``.

Every large Israeli food retailer must publish its stores, the price of every
item at every store and every promotion (Food Act, price-transparency
regulations). OVER collects each retailer as its own dataset through the
``prices`` scraper source. This server is where those ~30 datasets become one
market: every tool queries across all the chains at once (a UNION over their
tables, built at query time — see app/services/prices_query.py, which the
public REST API at /api/prices shares).

Shares the main MCP's OAuth authorization server and ``api_users`` allow-list;
only the tools and the resource identity differ. Mirrors elections_server.py.
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
from app.services import prices_query as pq

SERVER_NAME = "over-prices-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "שקיפות מחירים לעם (over.org.il) — מחירי המזון בכל הרשתות הגדולות בישראל, כפי "
    "שהן מחויבות לפרסם לפי חוק קידום התחרות בענף המזון: רשימת הסניפים, מחיר כל מוצר "
    "בכל סניף, והמבצעים. כל רשת נאספת כמאגר נפרד (אחת ליום), והכלים כאן מתשאלים את "
    "כולן יחד.\n\n"
    "כללים: (1) ציין שהמקור הוא הרשתות עצמן דרך 'גרסאות לעם' (over.org.il) וצרף את "
    "הקישורים שבשדה links. (2) לכל מחיר יש as_of — תאריך הקובץ האחרון של הסניף; "
    "רשתות מתעדכנות בשעות שונות, ציין את התאריך כשהוא משנה. (3) מבצעים לא מקוזזים "
    "מהמחיר בהשוואות — הם מותנים (כמות, מועדון, קופון) ומוצגים ב-item_promotions. "
    "(4) במוצר שקול השווה unit_price (למחיר ליחידת מידה), לא price. (5) שם מוצר "
    "שונה בין רשתות — הזיהוי האמין הוא הברקוד (item_code); חפש קודם ב-search_products "
    "ורק אז השווה לפי item_code.\n\n"
    "סדר עבודה: search_products (שם → ברקוד) → compare_prices / compare_basket "
    "(עם city) → item_promotions / price_history. find_stores לאיתור סניפים לפי עיר, "
    "store_prices למחירון של סניף אחד."
)

_CHAINS_PROP = {"type": "string",
                "description": "סינון רשתות, מופרדות בפסיק: מפתח (shufersal:shufersal), "
                               "חשבון (ramilevi) או שם (רמי לוי). ריק = כל הרשתות"}

TOOLS: list[dict] = [
    {
        "name": "list_chains",
        "description": "כל הרשתות שנאספות: שם, מפתח, מספר סניפים ותאריך התמונה האחרונה.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "search_products",
        "description": (
            "חיפוש מוצר לפי שם (מילים, חיפוש חלקי) או לפי ברקוד, בכל הרשתות. מחזיר את "
            "הברקוד (item_code), השמות שהרשתות נותנות לו, היצרן, הגודל ובאילו רשתות הוא "
            "נמכר. זה הצעד הראשון לפני השוואת מחירים."),
        "inputSchema": {"type": "object", "properties": {
            "q": {"type": "string", "description": "שם מוצר או ברקוד"},
            "chains": _CHAINS_PROP,
            "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 30},
        }, "required": ["q"]},
    },
    {
        "name": "compare_prices",
        "description": (
            "המחיר הנוכחי של מוצר (או כמה) בכל רשת: מינימום, חציון ומקסימום על פני "
            "הסניפים, הסניף הזול ביותר בכל רשת, והסניפים הזולים ביותר בסך הכול. אפשר "
            "להגביל לעיר."),
        "inputSchema": {"type": "object", "properties": {
            "item_codes": {"type": "array", "items": {"type": "string"},
                           "description": "ברקודים (עד 40)"},
            "city": {"type": "string", "description": "שם עיר/יישוב (לא חובה)"},
            "chains": _CHAINS_PROP,
            "top": {"type": "integer", "minimum": 1, "maximum": 50, "default": 5},
        }, "required": ["item_codes"]},
    },
    {
        "name": "compare_basket",
        "description": (
            "עלות סל קניות בכל סניף, מהזול ליקר, בעיר מסוימת או ברשתות מסוימות. סניפים "
            "מדורגים קודם לפי כמה מפריטי הסל יש בהם ורק אחר כך לפי הסכום — סניף זול "
            "כי חסרים בו חצי מהפריטים אינו הזול. מחזיר גם את הסניף הזול בכל רשת."),
        "inputSchema": {"type": "object", "properties": {
            "items": {"type": "array", "items": {"type": "object", "properties": {
                "item_code": {"type": "string"},
                "quantity": {"type": "number", "default": 1}},
                "required": ["item_code"]}, "description": "פריטי הסל (עד 40)"},
            "city": {"type": "string", "description": "עיר — נדרש אם לא צוינו רשתות"},
            "chains": _CHAINS_PROP,
            "top": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
        }, "required": ["items"]},
    },
    {
        "name": "find_stores",
        "description": "סניפים לפי עיר, שם או כתובת, בכל הרשתות או ברשתות מסוימות.",
        "inputSchema": {"type": "object", "properties": {
            "city": {"type": "string"},
            "q": {"type": "string", "description": "חלק משם הסניף או הכתובת"},
            "chains": _CHAINS_PROP,
            "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
        }},
    },
    {
        "name": "store_prices",
        "description": "המחירון הנוכחי של סניף אחד, עם סינון אופציונלי לפי שם מוצר או ברקוד.",
        "inputSchema": {"type": "object", "properties": {
            "chain": {"type": "string", "description": "מפתח, חשבון או שם הרשת"},
            "store_id": {"type": "string", "description": "מספר הסניף (מ-find_stores)"},
            "q": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 100},
            "offset": {"type": "integer", "minimum": 0, "default": 0},
        }, "required": ["chain", "store_id"]},
    },
    {
        "name": "price_history",
        "description": (
            "היסטוריית המחיר של מוצר: כל מצב מחיר בכל סניף, מתי התחיל (first_seen_date) "
            "ומתי נראה לאחרונה (last_seen_date; ריק = עדיין בתוקף), וציר זמן לפי רשת."),
        "inputSchema": {"type": "object", "properties": {
            "item_code": {"type": "string"},
            "chains": _CHAINS_PROP,
            "store_id": {"type": "string"},
            "limit": {"type": "integer", "minimum": 1, "maximum": 2000, "default": 200},
        }, "required": ["item_code"]},
    },
    {
        "name": "item_promotions",
        "description": (
            "המבצעים שחלים על מוצר בכל הרשתות: תיאור, תאריכים, מחיר המבצע, כמות "
            "מינימלית, מועדון, ובכמה סניפים הוא רץ."),
        "inputSchema": {"type": "object", "properties": {
            "item_code": {"type": "string"},
            "chains": _CHAINS_PROP,
            "active_only": {"type": "boolean", "default": True},
            "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 100},
        }, "required": ["item_code"]},
    },
]


def _chains(value) -> list[str] | None:
    if value in (None, "", []):
        return None
    items = value if isinstance(value, list) else str(value).split(",")
    return [str(c).strip() for c in items if str(c).strip()] or None


def _codes(value) -> list[str]:
    items = value if isinstance(value, list) else str(value or "").split(",")
    return [str(c).strip() for c in items if str(c).strip()]


async def _wrap(request: Request, db: AsyncSession, data: dict, count: int) -> tuple[dict, int]:
    base = base_url(request)
    entries = await pq.chains(db)
    data["links"] = {
        "source_regulations": pq.SOURCE_PAGE,
        "api": f"{base}/api/prices",
        "over_datasets": [{"chain": e["chain"], "name": e["name"],
                           "versions_url": f"{base}/versions/{e['dataset_id']}"}
                          for e in entries],
    }
    data["caveats"] = pq.CAVEATS
    data["_provenance"] = {"is_processed": False,
                           "description": "Published by the retailers themselves under the "
                                          "Food Act price-transparency regulations; collected "
                                          "daily and stored as a change log. Values unchanged."}
    return data, count


async def _tool_list_chains(request, db, user, a):
    rows = await pq.list_chains(db)
    return await _wrap(request, db, {"chains": rows}, len(rows))


async def _tool_search_products(request, db, user, a):
    data = await pq.search_products(db, q=str(a.get("q") or ""),
                                    chains_filter=_chains(a.get("chains")),
                                    limit=int(a.get("limit") or 30))
    return await _wrap(request, db, data, len(data["items"]))


async def _tool_compare_prices(request, db, user, a):
    data = await pq.compare_prices(db, item_codes=_codes(a.get("item_codes")),
                                   city=a.get("city") or None,
                                   chains_filter=_chains(a.get("chains")),
                                   top=int(a.get("top") or 5))
    return await _wrap(request, db, data, len(data["items"]))


async def _tool_compare_basket(request, db, user, a):
    items = a.get("items") or []
    if isinstance(items, str):
        items = [{"item_code": c} for c in _codes(items)]
    data = await pq.compare_basket(db, basket=[i if isinstance(i, dict) else {"item_code": i}
                                               for i in items],
                                   city=a.get("city") or None,
                                   chains_filter=_chains(a.get("chains")),
                                   top=int(a.get("top") or 10))
    return await _wrap(request, db, data, len(data["stores"]))


async def _tool_find_stores(request, db, user, a):
    data = await pq.find_stores(db, city=a.get("city") or None, q=a.get("q") or None,
                                chains_filter=_chains(a.get("chains")),
                                limit=int(a.get("limit") or 100))
    return await _wrap(request, db, data, len(data["stores"]))


async def _tool_store_prices(request, db, user, a):
    data = await pq.store_prices(db, chain=str(a.get("chain") or ""),
                                 store_id=str(a.get("store_id") or ""), q=a.get("q") or None,
                                 limit=int(a.get("limit") or 100),
                                 offset=int(a.get("offset") or 0))
    return await _wrap(request, db, data, len(data["items"]))


async def _tool_price_history(request, db, user, a):
    data = await pq.price_history(db, item_code=str(a.get("item_code") or ""),
                                  chains_filter=_chains(a.get("chains")),
                                  store_id=a.get("store_id") or None,
                                  limit=int(a.get("limit") or 200))
    return await _wrap(request, db, data, len(data["states"]))


async def _tool_item_promotions(request, db, user, a):
    active = a.get("active_only")
    data = await pq.item_promotions(db, item_code=str(a.get("item_code") or ""),
                                    chains_filter=_chains(a.get("chains")),
                                    active_only=True if active is None else bool(active),
                                    limit=int(a.get("limit") or 100))
    return await _wrap(request, db, data, len(data["promotions"]))


_IMPL = {
    "list_chains": _tool_list_chains,
    "search_products": _tool_search_products,
    "compare_prices": _tool_compare_prices,
    "compare_basket": _tool_compare_basket,
    "find_stores": _tool_find_stores,
    "store_prices": _tool_store_prices,
    "price_history": _tool_price_history,
    "item_promotions": _tool_item_promotions,
}


# ---------------------------------------------------------------------------
# JSON-RPC plumbing (mirrors app/mcp/elections_server.py)
# ---------------------------------------------------------------------------

def _rpc_result(mid, result):
    return {"jsonrpc": "2.0", "id": mid, "result": result}


def _rpc_error(mid, code, message):
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": code, "message": message}}


def _uuid(s):
    try:
        return uuid.UUID(str(s)) if s else None
    except (ValueError, TypeError):
        return None


async def _run_tool(request: Request, db: AsyncSession, user: McpUser, session_id: str | None,
                    name: str, args: dict) -> dict:
    impl = _IMPL.get(name)
    started = time.time()

    async def log(status, count=None, size=None, error=None):
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id),
                        session_id=session_id, tool_name=name, request_params=args,
                        result_count=count, result_bytes=size,
                        latency_ms=int((time.time() - started) * 1000), status=status,
                        error_message=error)

    if not impl:
        await log("error", error="unknown tool")
        return {"content": [{"type": "text", "text": f"Unknown tool: {name}"}], "isError": True}
    try:
        data, count = await impl(request, db, user, args or {})
        out = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log("ok", count, len(out.encode("utf-8")))
        return {"content": [{"type": "text", "text": out}]}
    except (pq.NotCollectedYet, ValueError) as e:
        # A caller mistake or "not collected yet" — said plainly, not as a crash.
        await log("error", error=str(e)[:1000])
        return {"content": [{"type": "text", "text": str(e)}], "isError": True}
    except Exception as e:  # noqa: BLE001
        await log("error", error=str(e)[:1000])
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
