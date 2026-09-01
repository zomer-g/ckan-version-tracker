"""FastAPI wiring for the dedicated מימון בחירות לעם MCP (``/elections/mcp``).

Mirrors app/mcp/ocal_routes.py: the MCP JSON-RPC endpoint plus this resource's
RFC 9728 protected-resource metadata. No OAuth endpoints here — the ELECTIONS
resource reuses the main authorization server (/mcp/oauth/*); its metadata
advertises that server. Auth is the shared ``authenticate`` (same JWT audience +
api_users allow-list), passing this resource's own metadata URL into the 401.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import get_db
from app.mcp import oauth
from app.mcp.auth import McpUser, authenticate
from app.mcp.config import elections_resource_metadata_url
from app.mcp.elections_server import handle_message
from app.mcp.routes import _cors, _preflight

elections_mcp_router = APIRouter(prefix="/elections/mcp", tags=["elections-mcp"])
elections_mcp_wellknown_router = APIRouter(tags=["elections-mcp"])


# ── metadata (root path — RFC 9728) ────────────────────────────────────────

@elections_mcp_wellknown_router.get("/.well-known/oauth-protected-resource/elections/mcp")
async def wk_elections_protected_resource(request: Request):
    return _cors(oauth.elections_protected_resource_metadata(request))


# ── metadata (also under /elections/mcp) ─────────────────────────────────────────

@elections_mcp_router.get("/.well-known/oauth-protected-resource")
async def elections_protected_resource(request: Request):
    return _cors(oauth.elections_protected_resource_metadata(request))


# ── the MCP endpoint ────────────────────────────────────────────────────────

async def _handle(request: Request, db: AsyncSession) -> Response:
    auth = await authenticate(request, db, resource_metadata=elections_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
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


@elections_mcp_router.post("")
async def elections_mcp_post(request: Request, db: AsyncSession = Depends(get_db)):
    return await _handle(request, db)


@elections_mcp_router.get("")
async def elections_mcp_get(request: Request, db: AsyncSession = Depends(get_db)):
    auth = await authenticate(request, db, resource_metadata=elections_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=405))


@elections_mcp_router.delete("")
async def elections_mcp_delete(request: Request, db: AsyncSession = Depends(get_db)):
    auth = await authenticate(request, db, resource_metadata=elections_resource_metadata_url(request))
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=200))


# ── CORS preflight ──────────────────────────────────────────────────────────

@elections_mcp_router.options("")
@elections_mcp_router.options("/{rest:path}")
async def elections_mcp_options(request: Request, rest: str = ""):
    return _preflight()
