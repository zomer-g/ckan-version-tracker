"""MCP server constants + URL helpers.

The issuer/metadata URLs MUST reflect the host the client actually called
(over.org.il, a render.com URL, localhost) and survive Render's reverse proxy,
so they're derived from the request headers — never hardcoded. The issuer is
``<base>/mcp`` (path component), which dictates the spec metadata location:
``/.well-known/oauth-authorization-server/mcp`` at the ROOT host (RFC 8414).
"""
from __future__ import annotations

from starlette.requests import Request

from app.config import settings

MCP_PREFIX = "/mcp"
# Dedicated CBS index MCP — a SECOND protected resource that reuses the SAME
# authorization server (the /mcp OAuth endpoints + api_users allow-list). Only
# the resource identity differs; tokens (aud=over-mcp) authenticate on both.
CBS_MCP_PREFIX = "/cbs/mcp"
MCP_JWT_AUDIENCE = "over-mcp"
MCP_ACCESS_TOKEN_TTL_SECONDS = 60 * 60          # 1 hour
MCP_REFRESH_TOKEN_TTL_SECONDS = 30 * 24 * 60 * 60  # 30 days
MCP_AUTH_CODE_TTL_SECONDS = 10 * 60             # 10 minutes
MCP_STATE_TTL_SECONDS = 15 * 60                 # signed Google-roundtrip state

GOOGLE_CALLBACK_PATH = "/mcp/oauth/google/callback"


def base_url(request: Request) -> str:
    """scheme://host from the request, honoring Render's X-Forwarded-* headers."""
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{proto}://{host}"


def mcp_url(request: Request, path: str = "") -> str:
    return f"{base_url(request)}{MCP_PREFIX}{path}"


def cbs_mcp_url(request: Request, path: str = "") -> str:
    """The CBS MCP resource URL, e.g. https://www.over.org.il/cbs/mcp."""
    return f"{base_url(request)}{CBS_MCP_PREFIX}{path}"


def cbs_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the CBS resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/cbs/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{CBS_MCP_PREFIX}"


def google_callback_url(request: Request) -> str:
    return f"{base_url(request)}{GOOGLE_CALLBACK_PATH}"


def mcp_jwt_secret() -> str:
    return settings.get_jwt_secret()


def mcp_service_token() -> str:
    """Shared machine-to-machine secret for the discovery gateway (or "" if the
    service-token bypass is disabled). See app/mcp/auth.py."""
    return (settings.mcp_service_token or "").strip()
