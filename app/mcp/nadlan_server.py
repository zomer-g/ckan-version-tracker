"""Dedicated MCP server for נדל"ן לעם — one property, every identity.

Mounted at ``/nadlan/mcp``, over the ``over_re_*`` crosswalk: 1.1 M parcels,
622 k addresses, the gazetteer, the postal file, the CBS statistical areas and
the מיסוי מקרקעין deal register, joined on identifiers only.

**Why this is not a corner of the generic SQL MCP.** The same reason the
elections resource exists: the question a person asks is "what is at גוש 6319
חלקה 225" or "what is at this address", and answering it from SQL means knowing
that gush/parcel pairs are ambiguous 0.63% of the time, that address→parcel is
point-in-polygon and not a string join, that the statistical area is a spatial
lookup against a layer whose socio-economic index lives on a DIFFERENT division,
and that the deal register publishes no gush suffix. Every one of those returns
a plausible wrong answer to a model that has not been told. That knowledge is
the product; a caller should not have to rediscover it.

The tools call the same functions the REST API and the web page call, so the
three surfaces cannot disagree about one property.

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
from app.services import nadlan_query, nadlan_text

SERVER_NAME = "over-nadlan-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "נדל\"ן לעם (over.org.il/projects/nadlan) — הצלבה ברמת הנכס בין שכבת "
    "החלקות של המרכז למיפוי ישראל, גזטיר הנכסים, קובץ המיקוד של דואר ישראל, "
    "רשימת הכתובות, שכבת האזורים הסטטיסטיים של הלמ\"ס ומאגר עסקאות מיסוי "
    "מקרקעין.\n\n"
    "העיקרון: נכנסים עם צורת הזיהוי שיש למשתמש — גוש וחלקה, כתובת, מיקוד או "
    "נקודה על המפה — ומקבלים את כל היתר באותה עטיפה. lookup_property הוא הכלי "
    "המרכזי; כל השאר עוזרים לו.\n\n"
    "חמישה דברים שחובה לדעת לפני שמציגים תשובה, כל אחד מהם מדוד:\n"
    "(1) המידע הזה מעובד — הוא הצלבה שנגזרה מארבעה מקורות ואינו פרסום ממשלתי "
    "ראשוני. ציינו זאת, וקשרו ל-over.org.il/projects/nadlan.\n"
    "(2) match.confidence — כשהוא 'approximate', הגזטיר אינו מפרסם תת-גוש "
    "ולגוש-חלקה הזה יש יותר מחלקה אחת. אל תציגו את הנתונים כוודאיים; הסבירו.\n"
    "(3) האזור הסטטיסטי נקבע לפי מרכז החלקה בתוך שכבת 2022. המדד החברתי-כלכלי "
    "(socio.eshkol) מתפרסם רק על חלוקת 2011, שהיא גיאומטריה אחרת — הוא מגיע "
    "בבלוק נפרד ועם שנת החלוקה שלו, ואסור להציג אותו כמאפיין של אזור 2022.\n"
    "(4) עסקאות מיסוי מקרקעין מקושרות לפי גוש וחלקה בלבד (המאגר אינו מפרסם "
    "תת-גוש), ולכן בחלקות שחולקות מספר ייתכן שאותן עסקאות מוצגות ביותר מחלקה "
    "אחת. השווי הוא הסכום המדווח לרשות המסים, לא מחיר שוק מאומת, ושורה אחת "
    "יכולה להיות דירה או בניין שלם — השוו בחציון ולא בממוצע.\n"
    "(5) caveats חוזר בכל תשובה ומכיל את מגבלות הכיסוי בפועל. אל תשמיטו אותן "
    "כשהן רלוונטיות לשאלה שנשאלה.\n\n"
    "כשלא מוצאים כתובת: רוב הכישלונות הם כתיב של שם רחוב. השתמשו ב-"
    "suggest_streets כדי לקבל את השם כפי שהוא רשום, ורק אז חזרו ל-"
    "lookup_property. לשאלות על מאגר העסקאות עצמו (מגמות, השוואות בין יישובים, "
    "כמה נמכר) יש שרת נפרד: עסקאות נדל\"ן, ב-over.org.il/deals/mcp."
)

MAX_LIMIT = nadlan_query.MAX_LIMIT


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "lookup_property",
        "description": (
            "נכס אחד, כל צורות הזיהוי. הזינו אחד מהצירופים: gush+helka, "
            "city+street(+number), zip, או lat+lon — ותקבלו את כל היתר: גוש, "
            "חלקה ותת-גוש, יישוב ומחוז, הכתובות המקושרות, המיקודים, הנקודה, "
            "האזור הסטטיסטי (א\"ס) עם אוכלוסייה ומדד חברתי-כלכלי, וסיכום "
            "עסקאות מיסוי מקרקעין. כל בלוק מלווה בקישור לשורת המקור שלו "
            "בקונסולת /data."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "gush": {"type": "integer", "description": "מספר גוש"},
                "helka": {"type": "integer", "description": "מספר חלקה"},
                "suffix": {"type": "integer", "description": "תת-גוש (ברירת מחדל: כולם)"},
                "city": {"type": "string", "description": "שם יישוב"},
                "street": {"type": "string", "description": "שם רחוב (ראו suggest_streets)"},
                "number": {"type": "string", "description": "מספר בית, אפשר עם אות: 12א"},
                "zip": {"type": "string", "description": "מיקוד, 5 או 7 ספרות"},
                "lat": {"type": "number", "description": "קו רוחב WGS84"},
                "lon": {"type": "number", "description": "קו אורך WGS84"},
                "radius_m": {
                    "type": "number",
                    "description": "עם lat+lon: 0 (ברירת מחדל) מחזיר את החלקה "
                                   "שמתחת לנקודה; ערך גדול יותר מחזיר את החלקות "
                                   "שמרכזן במרחק הזה, עד 2000 מ׳",
                },
                "q": {
                    "type": "string",
                    "description": "טקסט חופשי כשאין שדות מפורשים: \"גוש 6319 חלקה 225\", "
                                   "מיקוד, או \"32.08,34.88\"",
                },
                "include_deals": {
                    "type": "boolean",
                    "description": "לצרף סיכום עסקאות (ברירת מחדל: כן)",
                },
                "include_stat_area": {
                    "type": "boolean",
                    "description": "לצרף את האזור הסטטיסטי (ברירת מחדל: כן)",
                },
                "include_addresses": {
                    "type": "boolean",
                    "description": "לצרף את רשימת הכתובות המקושרות (ברירת מחדל: כן)",
                },
                "limit": {"type": "integer", "description": f"מספר חלקות בתשובה (1-{MAX_LIMIT})"},
            },
        },
    },
    {
        "name": "parcel_deals",
        "description": (
            "כל עסקאות מיסוי מקרקעין שדווחו על גוש וחלקה, מהחדשה לישנה. "
            "בתשובת lookup_property יש רק סיכום, כי חלקה של מגדל מגורים מחזיקה "
            "מאות עסקאות. sub_parcel מצמצם לתת-חלקה אחת — במגדל זו הדירה "
            "הבודדת, והרמה היחידה שבה סדרת מחירים אומרת משהו."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "gush": {"type": "integer"},
                "helka": {"type": "integer"},
                "sub_parcel": {"type": "string", "description": "תת-חלקה, למשל 7 או 007"},
                "limit": {"type": "integer", "description": "מספר עסקאות (1-200)"},
                "offset": {"type": "integer"},
            },
            "required": ["gush", "helka"],
        },
    },
    {
        "name": "suggest_streets",
        "description": (
            "השלמה אוטומטית לשם רחוב, עם אפשרות לצמצם ליישוב אחד (קוד למ\"ס). "
            "השתמשו בזה כשחיפוש לפי כתובת לא החזיר דבר: הכתיב הרשמי הוא "
            "המחרוזת ש-lookup_property מצפה לה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "תחילת שם הרחוב"},
                "settlement_code": {"type": "integer", "description": "קוד יישוב של הלמ\"ס"},
                "limit": {"type": "integer", "description": "מספר הצעות (1-50)"},
            },
            "required": ["q"],
        },
    },
    {
        "name": "parcel_geometry",
        "description": (
            "גבולות החלקה כ-GeoJSON. הקריאה היחידה שנוגעת בטבלת החלקות "
            "(4.58 GB), ולכן היא נפרדת ולא חלק מכל תשובה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "gush": {"type": "integer"},
                "helka": {"type": "integer"},
                "suffix": {"type": "integer", "description": "תת-גוש (ברירת מחדל 0)"},
            },
            "required": ["gush", "helka"],
        },
    },
    {
        "name": "coverage_stats",
        "description": (
            "היקף ההצלבה ואחוזי הכיסוי בפועל: כמה חלקות, כמה כתובות, כמה מהן "
            "עם נקודה, כמה משויכות לחלקה וכמה עם מיקוד ברמת הכתובת. השתמשו בזה "
            "כשנשאלתם עד כמה אפשר לסמוך על תשובה."
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


def _flag(v, default: bool = True) -> bool:
    return default if v is None else bool(v)


async def _require_ready() -> None:
    if not await nadlan_query.is_ready():
        raise ValueError("האינדקס המוצלב של נדל\"ן לעם עדיין לא נבנה")


SOURCE_NOTE = {
    "source": "נדל\"ן לעם (over.org.il) — מידע מעובד",
    "page_url": "https://www.over.org.il/projects/nadlan",
}


# ── tools ──────────────────────────────────────────────────────────────────

async def _tool_lookup(request, db, user, a) -> tuple[dict, int]:
    """Resolve whichever identity was supplied, then return the one envelope.

    The precedence is the same as /api/nadlan/lookup, and for the same reason:
    a caller that named a גוש and a חלקה has said what it means, so sniffing
    free text over that could only get it wrong."""
    await _require_ready()
    limit = _clamp(a.get("limit"), 1, MAX_LIMIT, 50)
    radius = max(0.0, min(float(a.get("radius_m") or 0), nadlan_query.MAX_RADIUS_M))
    city, street = (a.get("city") or "").strip(), (a.get("street") or "").strip()
    zip_code = str(a.get("zip") or "").strip()
    addrs = None

    if city and street:
        mode = {"mode": "address", "city": city, "street": street,
                "number": a.get("number")}
        addrs, parcels = await nadlan_query.by_address(city, street, a.get("number"))
    elif a.get("gush") is not None and a.get("helka") is not None:
        mode = {"mode": "gush_helka", "gush": int(a["gush"]), "helka": int(a["helka"]),
                "suffix": a.get("suffix")}
        parcels = await nadlan_query.by_gush_helka(
            int(a["gush"]), int(a["helka"]),
            int(a["suffix"]) if a.get("suffix") is not None else None)
    elif zip_code:
        if not zip_code.isdigit() or len(zip_code) not in (5, 7):
            raise ValueError("מיקוד חייב להיות 5 או 7 ספרות")
        mode = {"mode": "zip", "zip": zip_code}
        addrs, parcels = await nadlan_query.by_zip(zip_code)
    elif a.get("lat") is not None and a.get("lon") is not None:
        mode = {"mode": "point", "lat": float(a["lat"]), "lon": float(a["lon"]),
                "radius_m": radius}
        parcels = await nadlan_query.by_point(float(a["lat"]), float(a["lon"]), radius, limit)
    elif (a.get("q") or "").strip():
        sniff = nadlan_text.sniff_mode(a["q"])
        m, parsed = sniff["mode"], sniff["parsed"]
        if m == "point":
            parcels = await nadlan_query.by_point(parsed["lat"], parsed["lon"], radius, limit)
        elif m in ("gush_helka", "gush"):
            parcels = await nadlan_query.by_gush_helka(parsed["gush"], parsed.get("helka", 0))
        elif m == "zip":
            addrs, parcels = await nadlan_query.by_zip(parsed["zip"])
        else:
            return {**SOURCE_NOTE, "properties": [],
                    "hint": "הטקסט נראה ככתובת. קראו שוב עם city ו-street "
                            "(ובמידת הצורך number); suggest_streets יעזור בכתיב.",
                    "alternatives": sniff["alternatives"]}, 0
        mode = {"mode": m, **parsed}
        if sniff["alternatives"]:
            mode["alternatives"] = sniff["alternatives"]
    else:
        raise ValueError("נדרש מזהה אחד לפחות: gush+helka, city+street, zip, "
                         "lat+lon או q")

    data = await nadlan_query.property_envelope(
        parcels[:limit], addresses=addrs,
        include_addresses=_flag(a.get("include_addresses")),
        with_stat_area=_flag(a.get("include_stat_area")),
        with_deals=_flag(a.get("include_deals")))
    return {**SOURCE_NOTE, "query": mode, "properties": data,
            "count": len(data), "caveats": _caveats()}, len(data)


def _caveats() -> list[str]:
    # Imported from the REST router so the two surfaces state the same limits;
    # a second copy would drift the first time one is corrected.
    from app.api.nadlan import CAVEATS
    return list(CAVEATS)


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
            "deals": rows, "total": total,
            "note": "מקור העסקאות: רשות המסים (מיסוי מקרקעין), שורות כפי "
                    "שפורסמו. השווי הוא הסכום המדווח, לא מחיר שוק מאומת.",
            "deals_page": "https://www.over.org.il/projects/deals"
                          f"?gush={gush}&helka={helka}"}, len(rows)


async def _tool_suggest_streets(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    rows = await nadlan_query.suggest_streets(
        (a.get("q") or "").strip(),
        int(a["settlement_code"]) if a.get("settlement_code") is not None else None,
        _clamp(a.get("limit"), 1, 50, 20))
    return {**SOURCE_NOTE, "streets": rows}, len(rows)


async def _tool_parcel_geometry(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    gush, helka = int(a["gush"]), int(a["helka"])
    suffix = _clamp(a.get("suffix"), 0, 999, 0)
    row = await nadlan_query.parcel_geometry(gush, suffix, helka)
    if not row:
        return {**SOURCE_NOTE, "found": False,
                "note": "לא נמצאה חלקה כזו בשכבת החלקות."}, 0
    return {**SOURCE_NOTE, "found": True, "gush": gush, "gush_suffix": suffix,
            "helka": helka, "geojson": row["geojson"],
            "legal_area": row.get("legal_area"),
            "status_text": row.get("status_text")}, 1


async def _tool_coverage(request, db, user, a) -> tuple[dict, int]:
    await _require_ready()
    return {**SOURCE_NOTE, "stats": await nadlan_query.stats(),
            "caveats": _caveats()}, 1


_IMPL = {
    "lookup_property": _tool_lookup,
    "parcel_deals": _tool_parcel_deals,
    "suggest_streets": _tool_suggest_streets,
    "parcel_geometry": _tool_parcel_geometry,
    "coverage_stats": _tool_coverage,
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
