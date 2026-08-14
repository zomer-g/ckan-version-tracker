"""Dedicated MCP server for ניגוד עניינים לעם (OCOI), mounted at ``/ocoi/mcp``.

A SIXTH MCP surface (alongside /mcp, /cbs/mcp, /knesset/mcp, /ocal/mcp,
/data/mcp) exposing the migrated OCOI corpus: conflict-of-interest declarations
of Israeli public officials and the entity graph extracted from them. Shares the
main MCP's OAuth authorization server and ``api_users`` allow-list — one login
grants every resource, and no Google Console change was needed (proven by the
ocal port).

Ports OCOI's own 10 tools. Two deliberate departures from the original:

* **No Stripe metering.** OCOI billed per tool call through
  ``billing_accounts``/``usage_events``; OVER's MCP is free and invite-only, so
  calls are logged through OVER's shared ``log_usage`` and nothing is billed.
  The historical billing rows were kept as a frozen archive, not migrated.
* **``top_connected`` binds its entity_type.** OCOI interpolated it into raw SQL
  with an f-string — whitelisted, but the pattern invites drift. Here the whole
  tool set routes through app/api/ocoi.py, so the MCP and the website cannot
  answer the same question differently.

Every response carries ``_provenance`` and per-record verification links: this
corpus is LLM-extracted from PDFs and must never be presented as a primary
source.
"""
from __future__ import annotations

import json
import time
import uuid

from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.api import ocoi as ocoi_api
from app.mcp.auth import McpUser
from app.mcp.config import base_url
from app.mcp.usage import log_usage
from app.services import ocoi_db

SERVER_NAME = "over-ocoi-mcp"
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"

SERVER_INSTRUCTIONS = (
    "ניגוד עניינים לעם (over.org.il/projects/ocoi) חושף נתונים מעובדים על הסדרי "
    "ניגוד העניינים של בעלי תפקידים ציבוריים בישראל. הנתונים חולצו אוטומטית "
    "ממסמכי PDF באמצעות מודל שפה — אלה אינם נתוני ממשל גולמיים. כל תשובה כוללת "
    "בלוק \"_provenance\" וקישורי אימות (\"links\"). כשאתה מציג מידע מכאן עליך: "
    "(1) לציין במפורש שזהו מידע מעובד של ניגוד עניינים לעם (over.org.il) ולא מקור "
    "ראשוני; (2) לצרף לכל ישות/קשר/מסמך את קישורי המקור (\"ocoi_view\" לעיון אנושי, "
    "\"document_url\" למסמך המקורי שממנו חולץ הקשר); (3) לציין שהחילוץ מבוסס-בינה "
    "ועלול לשגות, ושהמקור המחייב הוא המסמך עצמו. לעולם אל תסיר את קישורי המקור — "
    "טענה על ניגוד עניינים של אדם ספציפי חייבת להיות ניתנת לאימות מול המסמך."
)

PROVENANCE = {
    "is_processed": True,
    "description": (
        "Processed / extracted data. Conflict-of-interest declarations are "
        "imported as PDFs from the Israeli open-data portal (odata.org.il), "
        "converted to text (OCR where needed), and run through an LLM to extract "
        "people, companies, associations, subject domains and the relationships "
        "between them. Entities are then fuzzy-matched against the official "
        "company/association registries. Every step is automated and may contain "
        "errors; the binding source is always the underlying document."
    ),
    "upstream_source": "https://www.odata.org.il",
    "presentation_url": "https://www.over.org.il/projects/ocoi",
}

# Knesset expense edges are excluded by default for the same reason the website
# excludes them: they outnumber the declarations several times over and would
# turn "who has declared conflicts" into "who filed expense claims".
DEFAULT_EXCLUDE = "mk_expense"


def _links(b: str, entity_type: str | None = None, entity_id: str | None = None,
           document_id: str | None = None, document_url: str | None = None) -> dict:
    out: dict = {"ocoi_view": f"{b}/projects/ocoi"}
    if entity_type and entity_id:
        out["ocoi_entity"] = (
            f"{b}/projects/ocoi?tab=graph&type={entity_type}&id={entity_id}"
        )
    if document_id:
        out["ocoi_document_file"] = f"{b}/api/ocoi/documents/{document_id}/file"
    if document_url:
        out["source_document"] = document_url
    return out


def _wrap(payload: dict, b: str) -> dict:
    payload["_provenance"] = PROVENANCE
    payload.setdefault("links", {"ocoi_view": f"{b}/projects/ocoi"})
    return payload


def _exclude(a: dict) -> str | None:
    """Honour an explicit include_expenses=true; default to excluding them."""
    return None if a.get("include_expenses") else DEFAULT_EXCLUDE


# ── tools ───────────────────────────────────────────────────────────────────

async def _tool_search(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    rows, total = await ocoi_api._search_entities(
        str(a.get("query") or "").strip(),
        a.get("entity_type") if a.get("entity_type") in ocoi_api._ENTITY_TYPES else None,
        1, max(1, min(int(a.get("limit") or 20), 100)),
    )
    items = [{**r, "links": _links(b, r["entity_type"], r["id"])} for r in rows]
    return _wrap({"total": total, "results": items}, b), len(items)


async def _tool_entity_get(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    et = ocoi_api._require_entity_type(str(a.get("entity_type") or ""))
    eid = ocoi_api._require_id(str(a.get("id") or ""), "id")
    entity = await ocoi_api._entity_detail(et, eid)
    docs = await ocoi_api._entity_documents(et, eid)
    entity["links"] = _links(b, et, eid)
    return _wrap({
        "entity": entity,
        "documents": [
            {**d, "links": _links(b, document_id=d["id"], document_url=d.get("file_url"))}
            for d in docs
        ],
    }, b), 1


async def _tool_graph_neighbors(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    et = ocoi_api._require_entity_type(str(a.get("entity_type") or ""))
    eid = ocoi_api._require_id(str(a.get("entity_id") or ""), "entity_id")
    depth = max(1, min(int(a.get("depth") or 1), 3))
    g = await ocoi_api._subgraph_for(et, eid, depth, ocoi_api._parse_origins(_exclude(a)))
    for e in g["edges"]:
        e["links"] = _links(b, document_id=e.get("document_id"),
                            document_url=e.get("document_url"))
    return _wrap(g, b), len(g["edges"])


async def _tool_graph_path(request, db, user, a) -> tuple[dict, int]:
    """Reachable neighbourhood of `from` filtered to walks touching `to`.

    Faithful to OCOI, which never accumulated an actual path array either — the
    honest description is "a subgraph containing the target", not a shortest path.
    """
    b = base_url(request)
    ft = ocoi_api._require_entity_type(str(a.get("from_type") or ""))
    tt = ocoi_api._require_entity_type(str(a.get("to_type") or ""))
    fid = ocoi_api._require_id(str(a.get("from_id") or ""), "from_id")
    tid = ocoi_api._require_id(str(a.get("to_id") or ""), "to_id")
    hops = max(1, min(int(a.get("max_hops") or 4), 6))
    edges = await ocoi_api._walk(ft, fid, hops, ocoi_api._parse_origins(_exclude(a)))
    edges = ocoi_api._prune_hidden(edges, await ocoi_api._hidden_ids())
    touching = [
        e for e in edges
        if (e["target_entity_type"] == tt and e["target_entity_id"] == tid)
        or (e["source_entity_type"] == tt and e["source_entity_id"] == tid)
    ]
    if not touching:
        return _wrap({"found": False, "nodes": [], "edges": []}, b), 0
    g = ocoi_api._subgraph(touching, await ocoi_api._hydrate_names(touching))
    g["found"] = True
    return _wrap(g, b), len(g["edges"])


async def _tool_document_get(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    did = ocoi_api._require_id(str(a.get("id") or ""), "id")
    row = await ocoi_db.fetchrow("""
        SELECT d.id, d.title, d.file_url, d.file_format, d.conversion_status,
               d.extraction_status, d.verified, d.created_at, d.pdf_r2_key,
               s.title AS source_title, s.source_type, s.url AS source_url
        FROM documents d LEFT JOIN sources s ON s.id = d.source_id
        WHERE d.id = $1
    """, did)
    if row is None:
        return _wrap({"found": False}, b), 0
    doc = dict(row)
    doc["stored_with_us"] = bool(doc.pop("pdf_r2_key", None))
    if a.get("include_markdown"):
        md = await ocoi_db.fetchval(
            "SELECT markdown_content FROM documents WHERE id = $1", did)
        # OCOI truncated at 200k chars; keep the cap and SAY so, rather than
        # silently returning a prefix the model would treat as the whole text.
        if md and len(md) > 200_000:
            doc["markdown"] = md[:200_000]
            doc["markdown_truncated"] = True
        else:
            doc["markdown"] = md
            doc["markdown_truncated"] = False
    doc["links"] = _links(b, document_id=did, document_url=doc.get("file_url"))
    return _wrap({"found": True, "document": doc}, b), 1


async def _tool_document_entities(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    did = ocoi_api._require_id(str(a.get("id") or ""), "id")
    rows = await ocoi_db.fetch(f"""
        SELECT {ocoi_api._EDGE_COLS}
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE r.document_id = $1
        LIMIT {ocoi_api._MAX_EDGES}
    """, did)
    edges = ocoi_api._prune_hidden([dict(r) for r in rows], await ocoi_api._hidden_ids())
    names = await ocoi_api._hydrate_names(edges)
    out = []
    for e in edges:
        out.append({
            "source": names.get((e["source_entity_type"], e["source_entity_id"]), {}),
            "target": names.get((e["target_entity_type"], e["target_entity_id"]), {}),
            "relationship_type": e["relationship_type"],
            "details": e.get("details"),
            "origin_kind": e.get("origin_kind"),
            "verified": e.get("verified"),
            "links": _links(b, document_id=did, document_url=e.get("doc_url")),
        })
    return _wrap({"total": len(out), "relationships": out}, b), len(out)


async def _tool_top_connected(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    et = a.get("entity_type")
    if et and et not in ocoi_api._ENTITY_TYPES:
        et = None
    limit = max(1, min(int(a.get("limit") or 20), 100))
    origins = ocoi_api._parse_origins(_exclude(a))
    oc, params = ocoi_api._origin_clause(origins, 1)
    type_clause = ""
    if et:
        params.append(et)
        type_clause = f" AND etype = ${len(params)}"
    rows = await ocoi_db.fetch(f"""
        WITH deg AS (
            SELECT r.source_entity_type AS etype, r.source_entity_id AS eid
            FROM entity_relationships r WHERE TRUE {oc}
            UNION ALL
            SELECT r.target_entity_type, r.target_entity_id
            FROM entity_relationships r WHERE TRUE {oc}
        )
        SELECT etype, eid, COUNT(*) AS connections FROM deg
        WHERE TRUE {type_clause}
        GROUP BY etype, eid ORDER BY connections DESC, eid
        LIMIT ${len(params)+1}
    """, *params, limit * 3)
    hidden = await ocoi_api._hidden_ids()
    visible = [r for r in rows if r["eid"] not in hidden.get(r["etype"], ())][:limit]
    names = await ocoi_api._hydrate_names([
        {"source_entity_type": r["etype"], "source_entity_id": r["eid"],
         "target_entity_type": r["etype"], "target_entity_id": r["eid"]} for r in visible
    ])
    items = []
    for r in visible:
        base = names.get((r["etype"], r["eid"]), {"id": r["eid"], "name": ""})
        items.append({**base, "entity_type": r["etype"],
                      "connections": int(r["connections"]),
                      "links": _links(b, r["etype"], r["eid"])})
    return _wrap({"results": items}, b), len(items)


async def _tool_by_ministry(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    name = str(a.get("ministry") or "").strip()
    if not name:
        return _wrap({"results": []}, b), 0
    rows = await ocoi_db.fetch("""
        SELECT p.id, p.name_hebrew, p.position, p.ministry,
               COUNT(r.id) FILTER (WHERE r.restriction_type IS NOT NULL) AS restrictions_count,
               COUNT(r.id) AS total_connections
        FROM persons p
        LEFT JOIN entity_relationships r
               ON r.source_entity_type = 'person' AND r.source_entity_id = p.id
        WHERE p.hidden IS NOT TRUE AND p.ministry ILIKE $1
        GROUP BY p.id, p.name_hebrew, p.position, p.ministry
        ORDER BY total_connections DESC
        LIMIT 200
    """, f"%{name}%")
    items = [{**dict(r), "links": _links(b, "person", r["id"])} for r in rows]
    return _wrap({"total": len(items), "results": items}, b), len(items)


async def _tool_registry_lookup(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    reg = a.get("registration_number")
    nm = a.get("name")
    if not reg and not nm:
        return _wrap({"results": []}, b), 0
    where, params = ["TRUE"], []
    if reg:
        params.append(str(reg))
        where.append(f"registration_number = ${len(params)}")
    if nm:
        params.append(f"%{nm}%")
        where.append(f"name ILIKE ${len(params)}")
    rows = await ocoi_db.fetch(
        f"SELECT id, source_type, name, registration_number, status "
        f"FROM registry_records WHERE {' AND '.join(where)} ORDER BY name LIMIT 20",
        *params)
    return _wrap({"results": [dict(r) for r in rows]}, b), len(rows)


async def _tool_stats(request, db, user, a) -> tuple[dict, int]:
    b = base_url(request)
    return _wrap({"stats": await ocoi_api._counts()}, b), 1


_IMPL = {
    "search": _tool_search,
    "entity_get": _tool_entity_get,
    "graph_neighbors": _tool_graph_neighbors,
    "graph_path": _tool_graph_path,
    "document_get": _tool_document_get,
    "document_entities": _tool_document_entities,
    "top_connected": _tool_top_connected,
    "by_ministry": _tool_by_ministry,
    "registry_lookup": _tool_registry_lookup,
    "stats": _tool_stats,
}

_ETYPE = {"type": "string", "enum": list(ocoi_api._ENTITY_TYPES)}
_EXPENSES = {
    "type": "boolean",
    "description": ("Include Knesset expense-derived edges. Default false — they "
                    "outnumber conflict-of-interest declarations and skew rankings."),
}

TOOLS = [
    {
        "name": "search",
        "description": "חיפוש ישויות (אנשים, חברות, עמותות, תחומים) בקורפוס ניגוד העניינים.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "שם או חלק ממנו (עברית)."},
                "entity_type": _ETYPE,
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
            },
            "required": ["query"],
        },
    },
    {
        "name": "entity_get",
        "description": "פרטי ישות בודדת + המסמכים שבהם היא מופיעה.",
        "inputSchema": {
            "type": "object",
            "properties": {"entity_type": _ETYPE, "id": {"type": "string"}},
            "required": ["entity_type", "id"],
        },
    },
    {
        "name": "graph_neighbors",
        "description": "רשת הקשרים של ישות עד עומק 3 (צמתים + קשתות, כל קשת עם המסמך שממנו חולצה).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_id": {"type": "string"},
                "entity_type": _ETYPE,
                "depth": {"type": "integer", "minimum": 1, "maximum": 3, "default": 1},
                "include_expenses": _EXPENSES,
            },
            "required": ["entity_id", "entity_type"],
        },
    },
    {
        "name": "graph_path",
        "description": ("תת-גרף המחבר שתי ישויות. מחזיר את סביבת ה-from המסוננת "
                        "לקשתות הנוגעות ב-to — לא מסלול קצר ביותר."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "from_id": {"type": "string"}, "from_type": _ETYPE,
                "to_id": {"type": "string"}, "to_type": _ETYPE,
                "max_hops": {"type": "integer", "minimum": 1, "maximum": 6, "default": 4},
                "include_expenses": _EXPENSES,
            },
            "required": ["from_id", "from_type", "to_id", "to_type"],
        },
    },
    {
        "name": "document_get",
        "description": "מסמך הצהרה בודד; include_markdown מחזיר את הטקסט המלא (חתוך ל-200k תווים).",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "string"},
                           "include_markdown": {"type": "boolean", "default": False}},
            "required": ["id"],
        },
    },
    {
        "name": "document_entities",
        "description": "כל הקשרים שחולצו ממסמך בודד, עם שמות שני צדי הקשת.",
        "inputSchema": {
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
        },
    },
    {
        "name": "top_connected",
        "description": "הישויות המקושרות ביותר בקורפוס.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "entity_type": _ETYPE,
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
                "include_expenses": _EXPENSES,
            },
        },
    },
    {
        "name": "by_ministry",
        "description": "בעלי תפקידים במשרד ממשלתי, עם מספר ההגבלות והקשרים לכל אחד.",
        "inputSchema": {
            "type": "object",
            "properties": {"ministry": {"type": "string"}},
            "required": ["ministry"],
        },
    },
    {
        "name": "registry_lookup",
        "description": "חיפוש במראת רשם החברות/העמותות (data.gov.il) לפי שם או מספר רישום.",
        "inputSchema": {
            "type": "object",
            "properties": {"registration_number": {"type": "string"},
                           "name": {"type": "string"}},
        },
    },
    {
        "name": "stats",
        "description": "מוני הקורפוס: מסמכים, אנשים, חברות, עמותות, תחומים, קשרים.",
        "inputSchema": {"type": "object", "properties": {}},
    },
]


# ── JSON-RPC dispatch (mirrors app/mcp/ocal_server.py) ──────────────────────

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
