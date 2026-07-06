"""FastAPI wiring for the dedicated CBS index MCP (``/cbs/mcp``).

Mirrors app/mcp/routes.py but for the CBS resource: the MCP JSON-RPC endpoint
plus this resource's RFC 9728 protected-resource metadata. There are NO OAuth
endpoints here — the CBS resource reuses the main authorization server
(/mcp/oauth/*); its metadata simply advertises that server. Auth is the shared
``authenticate`` (same JWT audience + api_users allow-list), passing this
resource's own metadata URL into the 401 challenge.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import get_db
from app.mcp import oauth
from app.mcp.auth import McpUser, authenticate
from app.mcp.cbs_server import handle_message
from app.mcp.config import cbs_resource_metadata_url
from app.mcp.routes import _cors, _preflight

cbs_mcp_router = APIRouter(prefix="/cbs/mcp", tags=["cbs-mcp"])
cbs_mcp_wellknown_router = APIRouter(tags=["cbs-mcp"])


# ── metadata (root path — RFC 9728) ────────────────────────────────────────

@cbs_mcp_wellknown_router.get("/.well-known/oauth-protected-resource/cbs/mcp")
async def wk_cbs_protected_resource(request: Request):
    return _cors(oauth.cbs_protected_resource_metadata(request))


# ── metadata (also under /cbs/mcp, for clients that append to the resource) ─

@cbs_mcp_router.get("/.well-known/oauth-protected-resource")
async def cbs_protected_resource(request: Request):
    return _cors(oauth.cbs_protected_resource_metadata(request))


# ── the MCP endpoint ────────────────────────────────────────────────────────

async def _handle(request: Request, db: AsyncSession) -> Response:
    auth = await authenticate(request, db, resource_metadata=cbs_resource_metadata_url(request))
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


@cbs_mcp_router.post("")
async def cbs_mcp_post(request: Request, db: AsyncSession = Depends(get_db)):
    return await _handle(request, db)


@cbs_mcp_router.get("")
async def cbs_mcp_get(request: Request, db: AsyncSession = Depends(get_db)):
    # Stateless server: no server-initiated SSE stream. Auth-gate then 405.
    auth = await authenticate(request, db, resource_metadata=cbs_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=405))


@cbs_mcp_router.delete("")
async def cbs_mcp_delete(request: Request, db: AsyncSession = Depends(get_db)):
    auth = await authenticate(request, db, resource_metadata=cbs_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=200))


# ── CORS preflight ──────────────────────────────────────────────────────────

@cbs_mcp_router.options("")
@cbs_mcp_router.options("/{rest:path}")
async def cbs_mcp_options(request: Request, rest: str = ""):
    return _preflight()
