"""FastAPI wiring for the MCP server + OAuth.

Two routers:
  • ``mcp_router`` (prefix /mcp): OAuth endpoints + the MCP JSON-RPC endpoint.
  • ``mcp_wellknown_router`` (no prefix): the RFC-mandated metadata at the ROOT
    host path ``/.well-known/<type>/mcp`` (Trap #2 — clients look here, NOT
    under /mcp). Must be included BEFORE the SPA catch-all so it returns JSON.

CORS: MCP clients are cross-origin (claude.ai, etc.). Every response carries
permissive CORS headers and OPTIONS preflight is answered 204 — independent of
the app's global CORS policy. Safe because /mcp is Bearer-gated and the OAuth
flow requires Google login + the api_users allow-list.
"""
from __future__ import annotations

import json

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.database import get_db
from app.mcp import config, oauth
from app.mcp.auth import McpUser, authenticate
from app.mcp.server import handle_message

mcp_router = APIRouter(prefix="/mcp", tags=["mcp"])
mcp_wellknown_router = APIRouter(tags=["mcp"])

_CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type, Accept, Mcp-Session-Id, Last-Event-Id",
    "Access-Control-Expose-Headers": "Mcp-Session-Id, WWW-Authenticate",
    "Access-Control-Max-Age": "86400",
}


def _cors(resp: Response) -> Response:
    for k, v in _CORS.items():
        resp.headers[k] = v
    return resp


def _preflight() -> Response:
    return _cors(Response(status_code=204))


# ── metadata (root path — RFC 8414/9728) ───────────────────────────────────

@mcp_wellknown_router.get("/.well-known/oauth-protected-resource/mcp")
async def wk_protected_resource(request: Request):
    return _cors(oauth.protected_resource_metadata(request))


@mcp_wellknown_router.get("/.well-known/oauth-authorization-server/mcp")
async def wk_authorization_server(request: Request):
    return _cors(oauth.authorization_server_metadata(request))


# ── metadata (also under /mcp, for clients that follow resource_metadata) ──

@mcp_router.get("/.well-known/oauth-protected-resource")
async def protected_resource(request: Request):
    return _cors(oauth.protected_resource_metadata(request))


@mcp_router.get("/.well-known/oauth-authorization-server")
async def authorization_server(request: Request):
    return _cors(oauth.authorization_server_metadata(request))


# ── OAuth endpoints ─────────────────────────────────────────────────────────

@mcp_router.post("/oauth/register")
async def oauth_register(request: Request, db: AsyncSession = Depends(get_db)):
    return _cors(await oauth.register_client(request, db))


@mcp_router.get("/oauth/authorize")
async def oauth_authorize(request: Request, db: AsyncSession = Depends(get_db)):
    return await oauth.authorize(request, db)  # browser redirect — no CORS needed


@mcp_router.get("/oauth/google/callback")
async def oauth_google_callback(request: Request, db: AsyncSession = Depends(get_db)):
    return await oauth.google_callback(request, db)


@mcp_router.post("/oauth/token")
async def oauth_token(request: Request, db: AsyncSession = Depends(get_db)):
    return _cors(await oauth.token(request, db))


# ── the MCP endpoint ────────────────────────────────────────────────────────

async def _handle_mcp(request: Request, db: AsyncSession) -> Response:
    auth = await authenticate(request, db)
    if not isinstance(auth, McpUser):
        return _cors(auth)  # 401 challenge
    session_id = request.headers.get("mcp-session-id")
    try:
        payload = await request.json()
    except Exception:
        return _cors(JSONResponse(status_code=400, content={
            "jsonrpc": "2.0", "id": None,
            "error": {"code": -32700, "message": "Parse error"},
        }))

    # Single message (Claude.ai sends one object per POST).
    if isinstance(payload, dict):
        resp = await handle_message(request, db, auth, session_id, payload)
        if resp is None:
            return _cors(Response(status_code=202))  # notification
        return _cors(JSONResponse(resp))

    # Batch (array) — process each, drop notification (None) responses.
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


@mcp_router.post("")
async def mcp_post(request: Request, db: AsyncSession = Depends(get_db)):
    return await _handle_mcp(request, db)


@mcp_router.get("")
async def mcp_get(request: Request, db: AsyncSession = Depends(get_db)):
    # Stateless server: no server-initiated SSE stream. Auth-gate then 405.
    auth = await authenticate(request, db)
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=405))


@mcp_router.delete("")
async def mcp_delete(request: Request, db: AsyncSession = Depends(get_db)):
    auth = await authenticate(request, db)
    if not isinstance(auth, McpUser):
        return _cors(auth)
    return _cors(Response(status_code=200))


# ── CORS preflight ──────────────────────────────────────────────────────────

@mcp_router.options("")
@mcp_router.options("/{rest:path}")
async def mcp_options(request: Request, rest: str = ""):
    return _preflight()


# Every MCP resource on this app. Each dedicated server is a separate protected
# resource under the SAME authorization server, and they all need the permissive
# cross-origin treatment — a resource missing from this tuple silently falls
# back to the app's restrictive global CORS, which fails a browser client's
# preflight before any of our routes run.
#
# Derived from the prefix constants rather than retyped, because retyping is how
# it drifted: /ocoi/mcp and /odata/mcp were both shipped as resources and both
# left out of the hand-written list. Importing the constants means declaring a
# new prefix in config.py is enough.
_MCP_PREFIXES = (
    config.MCP_PREFIX,
    config.CBS_MCP_PREFIX,
    config.KNESSET_MCP_PREFIX,
    config.OCAL_MCP_PREFIX,
    config.OCOI_MCP_PREFIX,
    config.ODATA_MCP_PREFIX,
    config.SQL_MCP_PREFIX,
    config.ELECTIONS_MCP_PREFIX,
)


def _is_mcp_path(path: str) -> bool:
    if any(path == p or path.startswith(p + "/") for p in _MCP_PREFIXES):
        return True
    for wk in ("/.well-known/oauth-protected-resource",
               "/.well-known/oauth-authorization-server"):
        if path.startswith(wk):
            rest = path[len(wk):]
            if any(rest == p or rest.startswith(p + "/") for p in _MCP_PREFIXES):
                return True
    return False


class MCPCorsMiddleware(BaseHTTPMiddleware):
    """Permissive CORS for the cross-origin MCP surface, applied OUTSIDE the
    app's global (restrictive) CORS so claude.ai's preflight to /mcp isn't
    rejected before our routes run (Trap #1). No-op for every other path."""

    async def dispatch(self, request: Request, call_next):
        if not _is_mcp_path(request.url.path):
            return await call_next(request)
        if request.method == "OPTIONS":
            return _preflight()
        resp = await call_next(request)
        for k, v in _CORS.items():
            resp.headers[k] = v
        return resp
