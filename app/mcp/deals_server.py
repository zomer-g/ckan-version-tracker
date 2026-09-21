"""Dedicated MCP server for עסקאות נדל"ן — the מיסוי מקרקעין deal register.

Mounted at ``/deals/mcp``, over 3.84 M reported real-estate deals from 1998,
scraped from nadlan.taxes.gov.il into the append DB.

**What this server is for.** The register is published one גוש at a time behind
a form, which makes the questions people actually have about it unanswerable at
the source: "how much did prices move in my town", "what sold on this street",
"where did the market cool". Those are free-text questions, and this server's
job is to turn them into the right query over the whole corpus — which is a job
a model does well ONLY if it has been told the four things below.

**Why not the generic SQL MCP.** Every column in the register is text; the date
is DD/MM/YYYY and ``to_date`` is not IMMUTABLE, so ordering through it silently
sorts by day-of-month; 17.5% of rows carry no settlement code, so joining on it
drops a sixth of the country; and one row can be a flat or a whole building, so
an average is meaningless. A model handed a bare SQL console gets all four
wrong and returns a confident, wrong number. Here they are not reachable: the
tools take the filter, not the SQL.

Protocol: the same hand-rolled Streamable-HTTP JSON-RPC subset as the sibling
servers. Kept self-contained so they evolve independently.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.mcp.auth import McpUser
from app.mcp.usage import log_usage
from app.services import deals_query, nadlan_query

SERVER_NAME = "over-deals-mcp"
SERVER_VERSION = "0.2.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "עסקאות נדל\"ן (over.org.il/projects/deals) — מאגר עסקאות המקרקעין "
    "המדווחות לרשות המסים: 3.84 מיליון עסקאות מ-1998 ועד היום, לפי יישוב, "
    "גוש, חלקה ותת-חלקה, עם תאריך, שווי מדווח, מהות העסקה, שטח, חדרים ושנת "
    "בנייה.\n\n"
    "אלה שורות המקור כפי שפורסמו — לא עובדו, לא תוקנו ולא הושלמו. אמרו זאת "
    "בתשובה, וקשרו ל-over.org.il/projects/deals.\n\n"
    "ארבעה כללים שבלעדיהם התשובה תהיה בטוחה ושגויה:\n"
    "(1) חציון, לא ממוצע. שורה אחת יכולה להיות דירה אחת או בניין שלם, ושורה "
    "כזו מזיזה ממוצע במיליונים. price_series ו-compare_settlements כבר "
    "מחזירים חציון; אל תחשבו ממוצע מתוך search_deals.\n"
    "(2) סננו לפי nature. השוואה בין יישובים או בין שנים בלי מהות עסקה מערבבת "
    "דירה עם מגרש, ותמהיל שהשתנה נראה כמו מחיר שהשתנה. המהות הנפוצה למגורים "
    "היא 'דירה בבית קומות'; list_deal_types מחזיר את הרשימה עם המספרים.\n"
    "(3) יישוב מזוהה בשם ולא בקוד. ל-17.5% מהשורות אין קוד יישוב במקור. "
    "קחו את השם המדויק מ-list_settlements — זו המחרוזת שהסינון עובד עליה.\n"
    "(4) שים לב למספר העסקאות שמאחורי כל חציון. יישוב עם עשר מכירות בשנה "
    "יראה תנודה של עשרות אחוזים שהיא רעש. ציינו את deals לצד המחיר, "
    "ו-compare_settlements ממילא דורש מינימום עסקאות בשתי השנים.\n"
    "(5) בכל תשובה שמציגה מחיר ושטח, הציגו לצדם גם את החלק הנמכר "
    "(portion_fraction) ואת המחיר המנורמל למ\"ר (price_per_sqm_normalized, "
    "ובסיכומים median_ppsqm_normalized / ppsqm_from / ppsqm_to). השטח במאגר "
    "הוא שטח הנכס כולו, והשווי משולם רק על החלק שנמכר, ולכן 'שווי חלקי שטח' "
    "של מכירת חצי דירה נראה כחצי ממחיר השוק. המדד המנורמל הוא שווי חלקי "
    "(שטח × חלק נמכר), כלומר המחיר למ\"ר שנקנה בפועל, והוא המדד שמשווה בין "
    "עסקאות. חלק נמכר קטן מאוד (למשל 0.001) מעוגל לשלוש ספרות ולכן המחיר "
    "המנורמל שלו לא מדויק; ב-0.000 אין מחיר מנורמל.\n\n"
    "מה מתאים למה: שאלה על מגמה לאורך זמן — price_series. שאלה על 'איפה עלה "
    "הכי הרבה' או השוואה בין מקומות — compare_settlements. שאלה על נכס או "
    "רחוב מסוים — search_deals עם gush/helka, ולזיהוי הגוש והחלקה מכתובת "
    "השתמשו בשרת נדל\"ן לעם ב-over.org.il/nadlan/mcp. שאלה על היקף המאגר "
    "עצמו — register_stats.\n\n"
    "total בחיפוש נספר עד 10,000 ואז מדווח total_capped=true; זה לא המספר "
    "האמיתי אלא 'יותר מ-'. אל תציגו אותו כסך הכול."
)

SOURCE_NOTE = {
    "source": "עסקאות נדל\"ן — רשות המסים (מיסוי מקרקעין), דרך גרסאות לעם",
    "page_url": "https://www.over.org.il/projects/deals",
    "processed": False,
}

_FILTER_PROPERTIES = {
    "settlement": {"type": "string",
                   "description": "שם היישוב כפי שמופיע ב-list_settlements"},
    "gush": {"type": "integer", "description": "מספר גוש"},
    "helka": {"type": "integer", "description": "מספר חלקה"},
    "sub_parcel": {"type": "string", "description": "תת-חלקה, למשל 7 או 007"},
    "nature": {"type": "string",
               "description": "מהות העסקה, מתוך list_deal_types. למגורים בדרך "
                              "כלל 'דירה בבית קומות'"},
    "date_from": {"type": "string", "description": "YYYY-MM-DD"},
    "date_to": {"type": "string", "description": "YYYY-MM-DD"},
    "min_amount": {"type": "integer", "description": "שווי מדווח מינימלי בשקלים"},
    "max_amount": {"type": "integer", "description": "שווי מדווח מקסימלי בשקלים"},
    "min_rooms": {"type": "number"},
    "max_rooms": {"type": "number"},
}


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search_deals",
        "description": (
            "העסקאות עצמן, מסוננות וממוינות. לכל עסקה: שווי, שטח, החלק הנמכר "
            "(portion_fraction), מחיר גולמי למ\"ר ומחיר מנורמל למ\"ר "
            "(price_per_sqm_normalized = שווי / (שטח × חלק)). כל תשובה נושאת גם console_sql "
            "ו-row_url — אותה שאילתה להרצה ישירה בקונסולת /data, כדי שאפשר "
            "יהיה לאמת כל מספר."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                **_FILTER_PROPERTIES,
                "sort": {"type": "string",
                         "description": "date_desc (ברירת מחדל) | date_asc | "
                                        "amount_desc | amount_asc | area_desc"},
                "limit": {"type": "integer",
                          "description": f"מספר עסקאות (1-{deals_query.MAX_LIMIT})"},
                "offset": {"type": "integer"},
            },
        },
    },
    {
        "name": "price_series",
        "description": (
            "מספר העסקאות, חציון השווי, חציון השטח וחציון המחיר המנורמל למ\"ר "
            "(median_ppsqm_normalized) לכל שנה, תחת אותם מסננים בדיוק כמו "
            "search_deals. זה הכלי לשאלות על מגמה: 'איך השתנו המחירים ב...'. "
            "סננו לפי nature, אחרת תמהיל שהשתנה ייראה כמו מחיר שהשתנה."
        ),
        "inputSchema": {"type": "object", "properties": dict(_FILTER_PROPERTIES)},
    },
    {
        "name": "compare_settlements",
        "description": (
            "שתי שנים, כל היישובים, זה לצד זה: מספר העסקאות וחציון המחיר בכל "
            "אחת, והשינוי באחוזים, וגם חציון המחיר המנורמל למ\"ר בכל שנה "
            "(ppsqm_from / ppsqm_to) והשינוי בו (ppsqm_change_pct). "
            "זה הכלי לשאלות 'איפה עלה הכי הרבה' או "
            "'איפה ירד'. min_deals חל על שתי השנים בנפרד כדי שיישוב עם קומץ "
            "מכירות לא יראה קפיצה שהיא עסקה חריגה אחת."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "year_from": {"type": "integer", "description": "שנת הבסיס, למשל 2019"},
                "year_to": {"type": "integer", "description": "שנת ההשוואה, למשל 2025"},
                "nature": {"type": "string",
                           "description": "מהות העסקה — מומלץ מאוד, אחרת התמהיל "
                                          "מזייף את ההשוואה"},
                "min_deals": {"type": "integer",
                              "description": "מינימום עסקאות בכל אחת מהשנים "
                                             "(ברירת מחדל 30)"},
                "order": {"type": "string",
                          "description": "change_desc (ברירת מחדל) | change_asc | "
                                         "median_desc | deals_desc"},
                "limit": {"type": "integer", "description": "מספר יישובים (1-200)"},
            },
            "required": ["year_from", "year_to"],
        },
    },
    {
        "name": "list_settlements",
        "description": (
            "כל היישובים במאגר עם מספר העסקאות ותאריך העסקה האחרונה. השמות "
            "מוחזרים כלשונם במקור — הם המחרוזות המדויקות שהסינון עובד עליהן, "
            "ולכן זו התחנה הראשונה בכל שאלה שנוקבת בשם מקום."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string",
                          "description": "סינון לפי תת-מחרוזת בשם, כדי לצמצם "
                                         "את הרשימה"},
                "limit": {"type": "integer", "description": "מספר יישובים (1-400)"},
            },
        },
    },
    {
        "name": "list_deal_types",
        "description": (
            "47 מהויות העסקה שבמאגר, עם מספר העסקאות, חציון השווי וחציון "
            "המחיר המנורמל למ\"ר בכל אחת. "
            "אפשר לצמצם לפי מסננים כדי לראות את התמהיל של יישוב או שנה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {**_FILTER_PROPERTIES,
                           "limit": {"type": "integer", "description": "מספר סוגים (1-60)"}},
        },
    },
    {
        "name": "parcel_deals",
        "description": (
            "כל העסקאות בגוש וחלקה אחת, מהחדשה לישנה. sub_parcel מצמצם "
            "לתת-חלקה — במגדל מגורים זו הדירה הבודדת, והרמה היחידה שבה סדרת "
            "מחירים של אותו נכס אומרת משהו."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "gush": {"type": "integer"},
                "helka": {"type": "integer"},
                "sub_parcel": {"type": "string"},
                "limit": {"type": "integer", "description": "מספר עסקאות (1-200)"},
                "offset": {"type": "integer"},
            },
            "required": ["gush", "helka"],
        },
    },
    {
        "name": "register_stats",
        "description": (
            "היקף המאגר: כמה עסקאות, מאיזה תאריך עד איזה, כמה יישובים וכמה "
            "חלקות, ומזהה המאגר במעקב כדי שאפשר יהיה לראות את היסטוריית "
            "הגרסאות שלו."
        ),
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── helpers ────────────────────────────────────────────────────────────────

def _clamp(v, lo: int, hi: int, default: int) -> int:
    try:
        return max(lo, min(int(v), hi))
    except (TypeError, ValueError):
        return default


def _filters(a: dict) -> dict:
    """Only the keys deals_query knows, and only the ones actually supplied."""
    return {k: a[k] for k in _FILTER_PROPERTIES
            if a.get(k) is not None and a.get(k) != ""}


async def _require_ready() -> None:
    if not await deals_query.is_ready():
        raise ValueError("מאגר עסקאות הנדל\"ן עדיין לא נטען")


def _caveats() -> list[str]:
    # Imported from the REST router so the two surfaces state the same limits.
    from app.api.deals import CAVEATS
    return list(CAVEATS)


# ── tools ──────────────────────────────────────────────────────────────────

async def _tool_search(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    f = _filters(a)
    res = await deals_query.search(
        f,
        limit=_clamp(a.get("limit"), 1, deals_query.MAX_LIMIT, 50),
        offset=_clamp(a.get("offset"), 0, 100_000, 0),
        sort=a.get("sort") or "date_desc")
    return {**SOURCE_NOTE, "query": f, **res, "caveats": _caveats()}, len(res["data"])


async def _tool_series(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    f = _filters(a)
    data = await deals_query.series(f)
    return {**SOURCE_NOTE, "query": f, "years": data,
            "note": "median_amount הוא חציון השווי המדווח באותה שנה. השוו אותו "
                    "תמיד לצד deals — מספר קטן של עסקאות מייצר תנודה שהיא רעש. "
                    "median_ppsqm_normalized הוא חציון השווי חלקי (שטח × חלק "
                    "נמכר), המחיר למ\"ר שנקנה בפועל, ומשווה בין מכירת נכס שלם "
                    "למכירת חלק ממנו.",
            "caveats": _caveats()}, len(data)


async def _tool_compare(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    nature = (a.get("nature") or "").strip() or None
    rows = await deals_query.compare_settlements(
        int(a["year_from"]), int(a["year_to"]), nature=nature,
        min_deals=_clamp(a.get("min_deals"), 1, 10_000, 30),
        limit=_clamp(a.get("limit"), 1, 200, 30),
        order=a.get("order") or "change_desc")
    out = {**SOURCE_NOTE, "year_from": int(a["year_from"]), "year_to": int(a["year_to"]),
           "nature": nature, "settlements": rows, "caveats": _caveats()}
    if not nature:
        # Said here and not only in the instructions, because this is the one
        # tool whose output looks authoritative enough to quote unqualified.
        out["warning"] = ("ההשוואה רצה על כל מהויות העסקה יחד. תמהיל שהשתנה "
                          "בין השנים (יותר מגרשים, פחות דירות) ייראה כמו שינוי "
                          "מחיר. העבירו nature, למשל 'דירה בבית קומות'.")
    return out, len(rows)


async def _tool_settlements(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    rows = await deals_query.settlements()
    q = (a.get("query") or "").strip()
    if q:
        rows = [r for r in rows if q in (r.get("settlement") or "")]
    rows = rows[:_clamp(a.get("limit"), 1, 400, 100)]
    return {**SOURCE_NOTE, "settlements": rows,
            "note": "השמות כלשונם במקור — זו המחרוזת המדויקת שהסינון עובד "
                    "עליה. ל-17.5% מהשורות אין קוד יישוב, ולכן השם הוא המפתח."
            }, len(rows)


async def _tool_deal_types(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    f = _filters(a)
    rows = (await deals_query.natures() if not f
            else await deals_query.breakdown(f, limit=_clamp(a.get("limit"), 1, 60, 20)))
    return {**SOURCE_NOTE, "query": f, "deal_types": rows}, len(rows)


async def _tool_parcel_deals(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    gush, helka = int(a["gush"]), int(a["helka"])
    sub = str(a.get("sub_parcel") or "").strip()
    rows, total = await nadlan_query.parcel_deals(
        gush, helka,
        limit=_clamp(a.get("limit"), 1, nadlan_query.MAX_DEALS, 50),
        offset=_clamp(a.get("offset"), 0, 100_000, 0),
        sub_parcel=sub.zfill(3) if sub else None)
    return {**SOURCE_NOTE, "gush": gush, "helka": helka, "sub_parcel": sub or None,
            "deals": rows, "total": total, "caveats": _caveats(),
            "property_page": "https://www.over.org.il/projects/nadlan"
                             f"?tab=gush&g={gush}&h={helka}"}, len(rows)


async def _tool_stats(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    s = await deals_query.stats()
    return {**SOURCE_NOTE, "stats": s,
            "versions_url": f"https://www.over.org.il/versions/{s.get('dataset_id')}",
            "caveats": _caveats()}, 1


_IMPL = {
    "search_deals": _tool_search,
    "price_series": _tool_series,
    "compare_settlements": _tool_compare,
    "list_settlements": _tool_settlements,
    "list_deal_types": _tool_deal_types,
    "parcel_deals": _tool_parcel_deals,
    "register_stats": _tool_stats,
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
