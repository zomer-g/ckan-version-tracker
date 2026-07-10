"""MCP server: JSON-RPC dispatch + the OVER data tools.

Implements the Streamable-HTTP MCP protocol subset Claude.ai uses (initialize,
tools/list, tools/call, ping) directly over FastAPI — no SDK needed. Each tool
queries OVER's existing models and returns processed data plus verification
links. Every call is timed + logged to mcp_usage_events.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage
from app.models.organization import Organization
from app.models.tag import Tag, dataset_tags
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex

SERVER_NAME = "over-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "גרסאות לעם (OVER, over.org.il) עוקב אחר היסטוריית הגרסאות של מאגרי מידע "
    "ממשלתיים בישראל. הכלים מחזירים מטא-דאטה מעובדת + קישורי אימות. כשאתה מציג "
    "נתונים למשתמש, ציין שהמקור הוא 'גרסאות לעם' וכלול את קישור ה-page_url / "
    "versions_url כדי שניתן יהיה לאמת. הנתונים הטבלאיים (query_dataset_rows) "
    "מגיעים ממאגרי data.gov.il שנשמרים ב-OVER."
)


# ── tool registry ──────────────────────────────────────────────────────────

TOOLS: list[dict] = [
    {
        "name": "search_datasets",
        "description": "חיפוש מאגרים שבמעקב גרסאות לעם לפי טקסט חופשי / ארגון / סוג מקור / תגית. מחזיר רשימה עם קישורים ותגיות. לסינון קבוצתי (למשל כל ועדות הכנסת ה-25, או כל מופעי ועדת הכספים) השתמש ב-tag; הרץ list_tags כדי לגלות שמות תגיות.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "טקסט חופשי לחיפוש בכותרת המאגר"},
                "source_type": {"type": "string", "enum": ["ckan", "scraper", "govmap"], "description": "סינון לפי סוג מקור"},
                "tag": {"type": "string", "description": "סינון למאגרים הנושאים תגית בשם זה (התאמה מדויקת, ללא תלות ברישיות). למשל 'כנסת 25', 'ועדות כנסת', או שם ועדה כמו 'ועדת הכספים'."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
        },
    },
    {
        "name": "list_tags",
        "description": "רשימת כל התגיות (קטגוריות רוחביות של מאגרים) עם מספר המאגרים לכל תגית. השתמש כדי לגלות תגיות זמינות (למשל 'כנסת 25', 'ועדות כנסת', שמות ועדות) ואז סנן דרך search_datasets עם הפרמטר tag.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "סינון שמות תגיות המכילים מחרוזת זו (אופציונלי)"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 200},
            },
        },
    },
    {
        "name": "get_dataset",
        "description": "פרטי מאגר בודד לפי id, כולל סיכום הגרסאות האחרונות וקישורים לעמוד ולמקור.",
        "inputSchema": {
            "type": "object",
            "properties": {"dataset_id": {"type": "string", "description": "UUID של המאגר"}},
            "required": ["dataset_id"],
        },
    },
    {
        "name": "query_dataset_rows",
        "description": "תשאול תוכן (שורות) של מאגר טבלאי שנשמר ב-NEON (append). פילטרים/חיפוש/עימוד. רק למאגרים עם נתונים טבלאיים.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string", "description": "UUID של המאגר"},
                "q": {"type": "string", "description": "חיפוש מחרוזת בכל העמודות"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 25},
                "offset": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["dataset_id"],
        },
    },
    {
        "name": "list_organizations",
        "description": "רשימת הארגונים הציבוריים שמתחזקים מאגרים, כולל מספר המאגרים לכל ארגון.",
        "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 100}}},
    },
    {
        "name": "get_stats",
        "description": "סטטיסטיקה כללית: מספר מאגרים פעילים, ארגונים, וסך גרסאות.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── tool implementations ────────────────────────────────────────────────────

async def _tool_search_datasets(request, db, user, a) -> tuple[dict, int]:
    limit = min(int(a.get("limit") or 20), 50)
    offset = max(int(a.get("offset") or 0), 0)
    stmt = select(TrackedDataset).where(TrackedDataset.status == "active")
    if a.get("source_type"):
        stmt = stmt.where(TrackedDataset.source_type == a["source_type"])
    if a.get("query"):
        stmt = stmt.where(TrackedDataset.title.ilike(f"%{a['query'].strip()}%"))
    if a.get("tag"):
        # Exact (case-insensitive) tag-name match via the dataset_tags join —
        # this is how a user pulls a whole group (e.g. "כנסת 25", "ועדות כנסת",
        # or a committee name to see all its instances across Knessets).
        tag_name = a["tag"].strip()
        stmt = (
            stmt.join(dataset_tags, dataset_tags.c.dataset_id == TrackedDataset.id)
            .join(Tag, Tag.id == dataset_tags.c.tag_id)
            .where(func.lower(Tag.name) == tag_name.lower())
        )
    total = (await db.execute(select(func.count()).select_from(stmt.subquery()))).scalar() or 0
    rows = (await db.execute(stmt.order_by(TrackedDataset.title).limit(limit).offset(offset))).scalars().all()
    b = base_url(request)
    items = [{
        "id": str(d.id), "title": d.title, "organization": d.organization,
        "source_type": d.source_type, "source_url": d.source_url,
        "version_count": 0,  # filled below
        "tags": [],          # filled below
        "page_url": f"{b}/versions/{d.id}",
        "versions_url": f"{b}/api/v1/datasets/{d.id}/versions",
    } for d in rows]
    if rows:
        ids = [d.id for d in rows]
        counts = dict((await db.execute(
            select(VersionIndex.tracked_dataset_id, func.count(VersionIndex.id))
            .where(VersionIndex.tracked_dataset_id.in_(ids))
            .group_by(VersionIndex.tracked_dataset_id)
        )).all())
        # tag names per dataset (so the client sees the groups a dataset belongs to)
        tag_rows = (await db.execute(
            select(dataset_tags.c.dataset_id, Tag.name)
            .join(Tag, Tag.id == dataset_tags.c.tag_id)
            .where(dataset_tags.c.dataset_id.in_(ids))
            .order_by(Tag.name)
        )).all()
        tags_by_ds: dict = {}
        for ds_id, tname in tag_rows:
            tags_by_ds.setdefault(ds_id, []).append(tname)
        for it, d in zip(items, rows):
            it["version_count"] = int(counts.get(d.id, 0))
            it["tags"] = tags_by_ds.get(d.id, [])
    return {"total": int(total), "limit": limit, "offset": offset, "items": items}, len(items)


async def _tool_get_dataset(request, db, user, a) -> tuple[dict, int]:
    did = _uuid(a.get("dataset_id"))
    if not did:
        raise ValueError("dataset_id חייב להיות UUID תקין")
    d = (await db.execute(select(TrackedDataset).where(TrackedDataset.id == did))).scalar_one_or_none()
    if not d:
        raise ValueError("מאגר לא נמצא")
    versions = (await db.execute(
        select(VersionIndex).where(VersionIndex.tracked_dataset_id == did)
        .order_by(VersionIndex.version_number.desc()).limit(10)
    )).scalars().all()
    b = base_url(request)
    return {
        "id": str(d.id), "title": d.title, "organization": d.organization,
        "source_type": d.source_type, "source_url": d.source_url,
        "storage_mode": d.storage_mode, "version_count": len(versions),
        "recent_versions": [{
            "version_number": v.version_number,
            "detected_at": v.detected_at.isoformat() if v.detected_at else None,
            "change_summary": v.change_summary,
        } for v in versions],
        "page_url": f"{b}/versions/{d.id}",
        "versions_url": f"{b}/api/v1/datasets/{d.id}/versions",
    }, len(versions)


async def _tool_query_dataset_rows(request, db, user, a) -> tuple[dict, int]:
    from app.services import append_store
    did = _uuid(a.get("dataset_id"))
    if not did:
        raise ValueError("dataset_id חייב להיות UUID תקין")
    d = (await db.execute(select(TrackedDataset).where(TrackedDataset.id == did))).scalar_one_or_none()
    if not d:
        raise ValueError("מאגר לא נמצא")
    if not append_store.is_configured():
        raise ValueError("אחסון NEON לא מוגדר בשרת")
    table = append_store.table_name(d)
    limit = min(int(a.get("limit") or 25), 200)
    res = await append_store.query(table, limit=limit, offset=max(int(a.get("offset") or 0), 0),
                                   q=a.get("q"), filters={})
    rows = res.get("rows") if isinstance(res, dict) else res
    b = base_url(request)
    return {
        "dataset_id": str(d.id), "title": d.title,
        "rows": rows, "total": res.get("total") if isinstance(res, dict) else None,
        "query_url": f"{b}/api/append/{d.id}/datastore_search",
        "page_url": f"{b}/versions/{d.id}",
    }, len(rows or [])


async def _tool_list_organizations(request, db, user, a) -> tuple[dict, int]:
    limit = min(int(a.get("limit") or 100), 200)
    counts = dict((await db.execute(
        select(TrackedDataset.organization_id, func.count(TrackedDataset.id))
        .where(TrackedDataset.status == "active").group_by(TrackedDataset.organization_id)
    )).all())
    orgs = (await db.execute(select(Organization).order_by(Organization.title).limit(limit))).scalars().all()
    b = base_url(request)
    return {"organizations": [{
        "id": str(o.id), "title": o.title, "name": o.name,
        "dataset_count": int(counts.get(o.id, 0)),
        "page_url": f"{b}/organizations",
    } for o in orgs]}, len(orgs)


async def _tool_list_tags(request, db, user, a) -> tuple[dict, int]:
    limit = min(int(a.get("limit") or 200), 500)
    # dataset count per tag (active/pending only — mirrors the public /api/tags).
    count_subq = (
        select(
            dataset_tags.c.tag_id.label("tag_id"),
            func.count(dataset_tags.c.dataset_id).label("cnt"),
        )
        .join(TrackedDataset, TrackedDataset.id == dataset_tags.c.dataset_id)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .group_by(dataset_tags.c.tag_id)
        .subquery()
    )
    stmt = (
        select(Tag, count_subq.c.cnt)
        .outerjoin(count_subq, Tag.id == count_subq.c.tag_id)
        .order_by(func.coalesce(count_subq.c.cnt, 0).desc(), Tag.name.asc())
    )
    if a.get("query"):
        stmt = stmt.where(Tag.name.ilike(f"%{a['query'].strip()}%"))
    rows = (await db.execute(stmt.limit(limit))).all()
    tags = [{"name": t.name, "description": t.description, "dataset_count": int(cnt or 0)}
            for t, cnt in rows]
    return {"total": len(tags), "tags": tags}, len(tags)


async def _tool_get_stats(request, db, user, a) -> tuple[dict, int]:
    datasets = (await db.execute(select(func.count()).select_from(TrackedDataset).where(TrackedDataset.status == "active"))).scalar() or 0
    orgs = (await db.execute(select(func.count()).select_from(Organization))).scalar() or 0
    versions = (await db.execute(select(func.count()).select_from(VersionIndex))).scalar() or 0
    return {"active_datasets": int(datasets), "organizations": int(orgs), "total_versions": int(versions),
            "source": "over.org.il"}, 0


_IMPL = {
    "search_datasets": _tool_search_datasets,
    "get_dataset": _tool_get_dataset,
    "query_dataset_rows": _tool_query_dataset_rows,
    "list_organizations": _tool_list_organizations,
    "list_tags": _tool_list_tags,
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
        text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        await log_usage(api_user_id=user.id, client_id=_uuid(user.client_id), session_id=session_id,
                        tool_name=name, request_params=args, result_count=count,
                        result_bytes=len(text.encode("utf-8")),
                        latency_ms=int((time.time() - started) * 1000), status="ok", error_message=None)
        return {"content": [{"type": "text", "text": text}]}
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
        return None  # notification — no response
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
