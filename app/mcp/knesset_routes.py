"""FastAPI wiring for the Knesset committee-protocols MCP (``/knesset/mcp``).

Mirrors app/mcp/cbs_routes.py: the MCP JSON-RPC endpoint plus this resource's
RFC 9728 protected-resource metadata. NO OAuth endpoints here — the resource
reuses the main authorization server (/mcp/oauth/*) and the shared
``authenticate`` (JWT audience + api_users allow-list + service token), passing
this resource's own metadata URL into the 401 challenge. Registered in main.py
BEFORE the SPA fallback so /knesset/mcp isn't swallowed by the React /knesset
route (same caveat as /cbs/mcp).
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import get_db
from app.mcp import oauth
from app.mcp.auth import McpUser, authenticate
from app.mcp.config import knesset_resource_metadata_url
from app.mcp.knesset_server import handle_message
from app.mcp.routes import _cors, _preflight

knesset_mcp_router = APIRouter(prefix="/knesset/mcp", tags=["knesset-mcp"])
knesset_mcp_wellknown_router = APIRouter(tags=["knesset-mcp"])


# ── metadata (root path — RFC 9728) ────────────────────────────────────────

@knesset_mcp_wellknown_router.get("/.well-known/oauth-protected-resource/knesset/mcp")
async def wk_knesset_protected_resource(request: Request):
    return _cors(oauth.knesset_protected_resource_metadata(request))


# ── metadata (also under /knesset/mcp, for clients that append to the resource) ─

@knesset_mcp_router.get("/.well-known/oauth-protected-resource")
async def knesset_protected_resource(request: Request):
    return _cors(oauth.knesset_protected_resource_metadata(request))


# ── the MCP endpoint ────────────────────────────────────────────────────────

async def _handle(request: Request, db: AsyncSession) -> Response:
    auth = await authenticate(request, db, resource_metadata=knesset_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)  # 401 challenge → discover THIS resource's metadata
    session_id = request.headers.get("mcp-session-id")
    try:
        payload = await request.json()
    except Exception:
        return _cors(JSONResponse(status_code=400, content={
            "jsonrpc": "2.0", "id": None,
            "error": {"code": -32700, "message": "Parse error"},
        }))

    if isinstance(payload, dict):
        resp = await handle_message(request, db, auth, session_id, payload)
        if resp is None:
            return _cors(Response(status_code=202))
        return _cors(JSONResponse(resp))

    if isinstance(payload, list):
        out = []
        for m in payload:
            if isinstance(m, dict):
                r = await handle_message(request, db, auth, session_id, m)
                if r is not None:
                    out.append(r)
        if not out:
            return _cors(Response(status_code=202))
        return _cors(JSONResponse(out))

    return _cors(JSONResponse(status_code=400, content={
        "jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"},
    }))


@knesset_mcp_router.post("")
async def knesset_mcp_post(request: Request, db: AsyncSession = Depends(get_db)):
    return await _handle(request, db)


@knesset_mcp_router.get("")
async def knesset_mcp_get(request: Request, db: AsyncSession = Depends(get_db)):
    # Stateless server: no server-initiated SSE stream. Auth-gate then 405.
    auth = await authenticate(request, db, resource_metadata=knesset_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=405))


@knesset_mcp_router.delete("")
async def knesset_mcp_delete(request: Request, db: AsyncSession = Depends(get_db)):
    auth = await authenticate(request, db, resource_metadata=knesset_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=200))


# ── CORS preflight ──────────────────────────────────────────────────────────

@knesset_mcp_router.options("")
@knesset_mcp_router.options("/{rest:path}")
async def knesset_mcp_options(request: Request, rest: str = ""):
    return _preflight()
