"""Dedicated MCP server for יומן לעם (Ocal), mounted at ``/ocal/mcp``.

A FOURTH MCP surface (alongside /mcp, /cbs/mcp, /knesset/mcp) exposing OVER's
migrated Ocal database — officials' public work-calendars, enriched with
AI-extracted entities and cross-diary references. Shares the main MCP's OAuth
authorization server and ``api_users`` allow-list (app/mcp/oauth.py); only the
tools + resource identity differ.

Queries go to the dedicated Ocal Neon DB via app/services/ocal_db.py (NOT the
main operational DB). Tool logic reuses the /api/ocal helpers (app/api/ocal.py)
so an MCP client gets the same retrieval as the website. Every response carries
a ``_provenance`` block + per-record verification links — Ocal data is
processed/enriched, never raw government output, and must be cited.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.api import ocal as ocal_api
from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage
from app.services import ocal_db

SERVER_NAME = "over-ocal-mcp"
SERVER_VERSION = "0.2.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "יומן לעם (over.org.il/projects/ocal) חושף נתונים מעובדים ומועשרים על יומני "
    "העבודה הציבוריים של נבחרי ציבור ובכירים בישראל. אלה אינם נתוני ממשל גולמיים "
    "— כל תשובה כוללת בלוק \"_provenance\" וקישורי אימות (\"links\"). כשאתה מציג "
    "מידע מ-יומן לעם עליך: (1) לציין במפורש שזהו מידע מעובד של יומן לעם (over.org.il), "
    "לא מקור ראשוני — נסח משהו כמו \"לפי הנתונים המעובדים של יומן לעם\"; (2) לכל אירוע/"
    "ישות/יומן/סטטיסטיקה לצרף את קישורי המקור (\"ocal_view\" לעיון אנושי, \"ckan_resource\" "
    "לרשומה הממשלתית המקורית); (3) לציין שחילוץ הישויות מבוסס-בינה וההצלבות הן היוריסטיות "
    "ועלולות לשגות. לעולם אל תסיר את קישורי המקור — המשתמש אמור לעקוב אחריהם לאימות."
)

PROVENANCE = {
    "is_processed": True,
    "description": (
        "Processed / enriched data. Raw calendar entries are ingested from the "
        "Israeli government open-data portal (data.gov.il / odata.org.il), then "
        "deduplicated, joined across diaries, and run through AI entity-extraction "
        "(NER) to identify people, organizations and places mentioned in event "
        "text. Confidence scores and cross-references reflect those automated "
        "steps and may contain errors."
    ),
    "upstream_source": "https://www.odata.org.il",
    "presentation_url": "https://www.over.org.il/projects/ocal",
}

_EVENT_KEEP = (
    "id", "source_id", "title", "start_time", "end_time", "location",
    "participants", "event_date", "dataset_name", "dataset_link",
    "source_name", "source_color", "match_count", "top_entities",
    "cross_ref_summary",
)


def _event_links(r: dict, b: str) -> dict:
    return {
        "ocal_view": f"{b}/projects/ocal",
        "ckan_resource": r.get("source_resource_url") or r.get("dataset_link"),
        "ckan_dataset": r.get("source_dataset_url"),
    }


def _event_item(r: dict, b: str) -> dict:
    item = {k: r.get(k) for k in _EVENT_KEEP if k in r}
    item["links"] = _event_links(r, b)
    return item


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search_events",
        "description": (
            "חיפוש אירועים ביומני נבחרי הציבור. טקסט חופשי (תומך AND/OR/NOT), סינון "
            "לפי טווח תאריכים, יומנים (source_ids), מיקום, משתתפים וסטטוס הצלבה. "
            "כל אירוע כולל קישורי מקור לאימות."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "טקסט חופשי (עברית/אנגלית), תומך AND/OR/NOT"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD"},
                "to_date": {"type": "string", "description": "YYYY-MM-DD"},
                "source_ids": {"type": "string", "description": "מזהי יומנים (UUID) מופרדים בפסיק"},
                "location": {"type": "string"},
                "participants": {"type": "string"},
                "cross_ref_status": {"type": "string", "enum": ["confirmed", "unconfirmed"]},
                "sort": {"type": "string", "enum": ["date_asc", "date_desc", "relevance"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "get_event",
        "description": "פרטי אירוע בודד לפי מזהה (UUID), כולל הישויות שחולצו ממנו וקישורי מקור.",
        "inputSchema": {
            "type": "object",
            "properties": {"event_id": {"type": "string", "description": "מזהה האירוע (UUID)"}},
            "required": ["event_id"],
        },
    },
    {
        "name": "list_entities",
        "description": (
            "הישויות הנפוצות ביותר (אנשים/ארגונים/מקומות) שחולצו מהאירועים, עד 200, "
            "לפי מספר אירועים. ניתן לסנן לפי סוג, יומנים וטווח תאריכים."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["person", "organization", "place"]},
                "source_ids": {"type": "string", "description": "מזהי יומנים מופרדים בפסיק"},
                "from_date": {"type": "string", "description": "YYYY-MM-DD"},
                "to_date": {"type": "string", "description": "YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "list_sources",
        "description": "רשימת היומנים (מקורות) הפעילים, עם בעלים, מספר אירועים, טווח תאריכים וקישורי מקור. אפשר לסנן בטקסט חופשי.",
        "inputSchema": {
            "type": "object",
            "properties": {"q": {"type": "string", "description": "סינון לפי שם יומן / בעלים"}},
        },
    },
    {
        "name": "find_meetings_between",
        "description": (
            "איתור אירועים שבהם שתי הישויות (person_a, person_b) מוזכרות יחד — "
            "מעקב 'מי נפגש עם מי' לרוחב הקורפוס. ההתאמה מבוססת על ישויות שחולצו "
            "אוטומטית (אזכור באותו אירוע, לא בהכרח פגישה מאושרת) — יש לאמת דרך "
            "קישור ה-ocal_view של כל התאמה."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "person_a": {"type": "string", "description": "שם ראשון (התאמת תת-מחרוזת חסרת רישיות)"},
                "person_b": {"type": "string", "description": "שם שני"},
                "date_from": {"type": "string", "description": "YYYY-MM-DD"},
                "date_to": {"type": "string", "description": "YYYY-MM-DD"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 50},
            },
            "required": ["person_a", "person_b"],
        },
    },
    {
        "name": "get_stats",
        "description": "סטטיסטיקת כיסוי: מספר האירועים, היומנים, הגופים והישויות שחולצו.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── tool implementations ────────────────────────────────────────────────────

def _split_ids(v) -> list[str] | None:
    if not v:
        return None
    if isinstance(v, list):
        return [str(x) for x in v if x]
    return [s for s in str(v).split(",") if s]


async def _tool_search_events(request, db, user, a) -> tuple[dict, int]:
    q = a.get("q") or None
    limit = min(int(a.get("limit") or 20), 100)
    offset = max(int(a.get("offset") or 0), 0)
    sort = a.get("sort") or ("relevance" if q else "date_desc")
    rows, total = await ocal_api._search_events(
        q=q, from_date=a.get("from_date"), to_date=a.get("to_date"),
        source_ids=_split_ids(a.get("source_ids")), location=a.get("location"),
        participants=a.get("participants"), entity_names=None,
        cross_ref_status=a.get("cross_ref_status"), sort=sort,
        offset=offset, limit=limit,
    )
    b = base_url(request)
    items = [_event_item(r, b) for r in rows]
    return {"_provenance": PROVENANCE, "total": int(total), "limit": limit,
            "offset": offset, "events": items}, len(items)


async def _tool_get_event(request, db, user, a) -> tuple[dict, int]:
    eid = str(a.get("event_id") or "").strip()
    if not eid:
        raise ValueError("event_id נדרש")
    ev = await ocal_db.fetchrow(
        "SELECT e.*, s.name AS source_name, s.color AS source_color, "
        "s.dataset_url AS source_dataset_url, s.resource_url AS source_resource_url, "
        "(s.reviewed_at IS NOT NULL) AS source_reviewed "
        "FROM diary_events e JOIN diary_sources s ON e.source_id = s.id WHERE e.id = $1",
        ocal_api._as_uuid(eid),
    )
    if not ev:
        raise ValueError("האירוע לא נמצא")
    ents = await ocal_db.fetch(
        "SELECT entity_type, entity_name, role, confidence, extraction_method "
        "FROM event_entities WHERE event_id = $1 ORDER BY confidence DESC LIMIT 60",
        ocal_api._as_uuid(eid),
    )
    b = base_url(request)
    item = _event_item(dict(ev), b)
    item["entities"] = [dict(x) for x in ents]
    return {"_provenance": PROVENANCE, "event": item}, 1


async def _tool_list_entities(request, db, user, a) -> tuple[dict, int]:
    src = _split_ids(a.get("source_ids"))
    tf, fd, td = a.get("type"), a.get("from_date"), a.get("to_date")
    if not src and not fd and not td:
        try:
            ents = await ocal_api._query_entities_matview(tf)
        except Exception:  # noqa: BLE001 — matview may be absent
            ents = await ocal_api._query_entities_live(src, tf, fd, td)
    else:
        ents = await ocal_api._query_entities_live(src, tf, fd, td)
    return {"_provenance": PROVENANCE, "entities": ents,
            "note": "top 200 by event_count"}, len(ents)


async def _tool_list_sources(request, db, user, a) -> tuple[dict, int]:
    qf = (a.get("q") or "").strip().lower()
    rows = await ocal_db.fetch(
        ocal_api._SOURCE_SELECT + "WHERE diary_sources.is_enabled = true ORDER BY diary_sources.name"
    )
    b = base_url(request)
    out = []
    for r in rows:
        d = dict(r)
        if qf and qf not in (d.get("name") or "").lower() \
                and qf not in (d.get("person_name") or "").lower() \
                and qf not in (d.get("organization_name") or "").lower():
            continue
        out.append({
            "id": d["id"], "name": d.get("name"), "color": d.get("color"),
            "total_events": d.get("total_events"),
            "first_event_date": d.get("first_event_date"),
            "last_event_date": d.get("last_event_date"),
            "person_name": d.get("person_name"),
            "organization_name": d.get("organization_name"),
            "links": {
                "ocal_source_view": f"{b}/projects/ocal",
                "ckan_dataset": d.get("dataset_url"),
                "ckan_resource": d.get("resource_url"),
            },
        })
    return {"_provenance": PROVENANCE, "sources": out}, len(out)


async def _tool_find_meetings_between(request, db, user, a) -> tuple[dict, int]:
    pa = (a.get("person_a") or "").strip().lower()
    pb = (a.get("person_b") or "").strip().lower()
    if not pa or not pb:
        raise ValueError("person_a ו-person_b נדרשים")
    limit = min(int(a.get("limit") or 50), 100)
    params: list = [f"%{pa}%", f"%{pb}%"]
    where = [
        "e.is_active = true", "s.is_enabled = true",
        "EXISTS (SELECT 1 FROM event_entities ee WHERE ee.event_id = e.id AND LOWER(ee.entity_name) LIKE $1)",
        "EXISTS (SELECT 1 FROM event_entities ee WHERE ee.event_id = e.id AND LOWER(ee.entity_name) LIKE $2)",
    ]
    n = 2
    if a.get("date_from"):
        n += 1; where.append(f"e.event_date >= ${n}::date"); params.append(a["date_from"])
    if a.get("date_to"):
        n += 1; where.append(f"e.event_date <= ${n}::date"); params.append(a["date_to"])
    n += 1; limit_ph = f"${n}"; params.append(limit)
    rows = await ocal_db.fetch(
        "SELECT e.id, e.title, e.start_time, e.end_time, e.location, e.event_date, "
        "e.source_id, e.dataset_link, s.name AS source_name, "
        "s.dataset_url AS source_dataset_url, s.resource_url AS source_resource_url "
        f"FROM diary_events e JOIN diary_sources s ON e.source_id = s.id "
        f"WHERE {' AND '.join(where)} ORDER BY e.start_time DESC LIMIT {limit_ph}",
        *params,
    )
    b = base_url(request)
    matches = [_event_item(dict(r), b) for r in rows]
    prov = dict(PROVENANCE)
    prov["note"] = (
        "Matches are based on AI-extracted entities in both event records — they "
        "indicate the mention of both people in the same event text, not "
        "necessarily a confirmed meeting. Verify via each match's ocal_view link."
    )
    return {"_provenance": prov, "matches": matches}, len(matches)


async def _tool_get_stats(request, db, user, a) -> tuple[dict, int]:
    te = await ocal_db.fetchval("SELECT count(*) FROM diary_events WHERE is_active = true")
    ts = await ocal_db.fetchval("SELECT count(*) FROM diary_sources WHERE is_enabled = true")
    to = await ocal_db.fetchval(
        "SELECT count(DISTINCT organization_id) FROM diary_sources "
        "WHERE is_enabled = true AND organization_id IS NOT NULL")
    tent = await ocal_db.fetchval("SELECT count(*) FROM event_entities")
    return {"_provenance": PROVENANCE, "total_events": int(te or 0),
            "total_sources": int(ts or 0), "total_organizations": int(to or 0),
            "total_entities": int(tent or 0),
            "source": "over.org.il — יומן לעם"}, 0


_IMPL = {
    "search_events": _tool_search_events,
    "get_event": _tool_get_event,
    "list_entities": _tool_list_entities,
    "list_sources": _tool_list_sources,
    "find_meetings_between": _tool_find_meetings_between,
    "get_stats": _tool_get_stats,
}


# ── JSON-RPC dispatch (mirrors app/mcp/cbs_server.py) ───────────────────────

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
