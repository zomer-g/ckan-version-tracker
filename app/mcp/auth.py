"""Bearer-token auth for the /mcp endpoint.

Verifies the JWT access token (aud=over-mcp), loads the still-active
``api_users`` row, and returns the authenticated context. On any failure it
returns the RFC-9728 ``WWW-Authenticate`` challenge that tells the MCP client
where to find the OAuth metadata — without it, clients can't discover the
auth server.
"""
from __future__ import annotations

import hmac
import uuid
from dataclasses import dataclass

import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.mcp.config import (
    MCP_JWT_AUDIENCE, mcp_jwt_secret, mcp_service_token, mcp_url,
)
from app.models.mcp import ApiUser

# Fixed identity of the machine-to-machine "service gateway" principal. This
# UUID is SEEDED as a real api_users row by migration 024 so tool-call usage
# events (mcp_usage_events.api_user_id → api_users.id, NOT NULL FK) still log
# for service traffic. MUST stay in lockstep with that migration's insert.
SERVICE_USER_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")


@dataclass
class McpUser:
    id: uuid.UUID
    email: str
    name: str | None
    tier: str
    client_id: str | None


def _service_user() -> "McpUser":
    return McpUser(
        id=SERVICE_USER_ID,
        email="service-gateway@over.org.il",
        name="Discovery Gateway (service)",
        tier="service",
        client_id=None,
    )


def challenge(request: Request, error: str, description: str) -> JSONResponse:
    resource_metadata = f"{mcp_url(request)}/.well-known/oauth-protected-resource"
    resp = JSONResponse(status_code=401, content={"error": error, "error_description": description})
    resp.headers["WWW-Authenticate"] = (
        f'Bearer realm="over-mcp", error="{error}", '
        f'error_description="{description}", resource_metadata="{resource_metadata}"'
    )
    return resp


async def authenticate(request: Request, db: AsyncSession) -> McpUser | Response:
    """Return a McpUser, or a 401 challenge Response to send back."""
    header = request.headers.get("authorization") or ""
    if not header.lower().startswith("bearer "):
        return challenge(request, "invalid_token", "Missing Bearer token")
    token = header[7:].strip()

    # ── service-token path (machine-to-machine; bypasses OAuth) ──
    # A trusted gateway presents a static shared secret instead of a per-user
    # JWT. Checked BEFORE jwt.decode / the api_users lookup so the gateway never
    # needs a Google login. Timing-safe compare (hmac.compare_digest also
    # tolerates unequal lengths). OFF entirely unless MCP_SERVICE_TOKEN is set —
    # an empty configured secret can never match a presented token here.
    svc = mcp_service_token()
    if svc and hmac.compare_digest(token, svc):
        return _service_user()

    # ── otherwise: the normal per-user OAuth flow (JWT + api_users) ──
    try:
        claims = jwt.decode(token, mcp_jwt_secret(), algorithms=["HS256"], audience=MCP_JWT_AUDIENCE)
    except Exception:
        return challenge(request, "invalid_token", "Token invalid or expired")
    if claims.get("typ") == "refresh":
        return challenge(request, "invalid_token", "Refresh tokens are not accepted here")
    try:
        uid = uuid.UUID(str(claims.get("sub")))
    except (ValueError, TypeError):
        return challenge(request, "invalid_token", "Malformed token subject")
    user = (await db.execute(
        select(ApiUser).where(ApiUser.id == uid, ApiUser.is_active.is_(True))
    )).scalar_one_or_none()
    if not user:
        return challenge(request, "invalid_token", "User no longer active")
    return McpUser(id=user.id, email=user.email, name=user.name, tier=user.tier, client_id=claims.get("cid"))
