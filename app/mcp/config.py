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
# Dedicated Knesset committee-protocols MCP — a THIRD protected resource on the
# same authorization server (SQL mirror of the Knesset ODATA tables; metadata +
# protocol links only, no document content).
KNESSET_MCP_PREFIX = "/knesset/mcp"
# Dedicated יומן לעם (Ocal) MCP — a FOURTH protected resource on the same
# authorization server (officials' public work-calendars, migrated into OVER).
OCAL_MCP_PREFIX = "/ocal/mcp"
# Dedicated ניגוד עניינים לעם (OCOI) MCP — conflict-of-interest declarations
# and the entity graph extracted from them, migrated into OVER.
OCOI_MCP_PREFIX = "/ocoi/mcp"
# Dedicated whole-site SQL MCP — a FIFTH protected resource on the same
# authorization server (the MCP twin of the public /data console: the full table
# catalog, its DDL, and free read-only SELECT across every schema).
ODATA_MCP_PREFIX = "/odata/mcp"
SQL_MCP_PREFIX = "/data/mcp"
# Dedicated election-finance MCP — the State Comptroller's register of who
# funded Israeli election campaigns (statements-p.mevaker.gov.il, collected by
# the mevaker_statements source). A resource of its own rather than a corner of
# the generic SQL MCP because the questions it answers are about PEOPLE — "what
# has this person given", "who funded this candidate" — and answering those
# means resolving a name across six tables whose recipient columns deliberately
# differ. That join is the product; a caller should not have to rediscover it.
ELECTIONS_MCP_PREFIX = "/elections/mcp"
# Dedicated נדל"ן לעם MCP — one property, every identity. A resource of its own
# rather than a corner of the SQL MCP because answering "what is at גוש 6319
# חלקה 225" means knowing that the pair is ambiguous 0.63% of the time, that
# address→parcel is point-in-polygon, and that the statistical area's
# socio-economic index lives on a DIFFERENT CBS division. Each of those returns
# a plausible wrong answer to a caller who was not told.
NADLAN_MCP_PREFIX = "/nadlan/mcp"
# Dedicated עסקאות נדל"ן MCP — the מיסוי מקרקעין register as free-text
# questions. Separate from the property lookup because the questions are about
# the MARKET ("where did prices move") rather than about one address, and they
# are answered by aggregates that must be medians over a stated deal type.
DEALS_MCP_PREFIX = "/deals/mcp"
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


def knesset_mcp_url(request: Request, path: str = "") -> str:
    """The Knesset protocols MCP resource URL, e.g. https://www.over.org.il/knesset/mcp."""
    return f"{base_url(request)}{KNESSET_MCP_PREFIX}{path}"


def knesset_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the Knesset resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/knesset/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{KNESSET_MCP_PREFIX}"


def ocal_mcp_url(request: Request, path: str = "") -> str:
    """The Ocal MCP resource URL, e.g. https://www.over.org.il/ocal/mcp."""
    return f"{base_url(request)}{OCAL_MCP_PREFIX}{path}"


def ocal_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the Ocal resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/ocal/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{OCAL_MCP_PREFIX}"


def ocoi_mcp_url(request: Request, path: str = "") -> str:
    """The OCOI MCP resource URL, e.g. https://www.over.org.il/ocoi/mcp."""
    return f"{base_url(request)}{OCOI_MCP_PREFIX}{path}"


def ocoi_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the OCOI resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/ocoi/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{OCOI_MCP_PREFIX}"


def odata_mcp_url(request: Request, path: str = "") -> str:
    """The מידע לעם MCP resource URL, e.g. https://www.over.org.il/odata/mcp."""
    return f"{base_url(request)}{ODATA_MCP_PREFIX}{path}"


def odata_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the odata resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/odata/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{ODATA_MCP_PREFIX}"


def sql_mcp_url(request: Request, path: str = "") -> str:
    """The whole-site SQL MCP resource URL, e.g. https://www.over.org.il/data/mcp."""
    return f"{base_url(request)}{SQL_MCP_PREFIX}{path}"


def sql_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the SQL resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/data/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{SQL_MCP_PREFIX}"


def elections_mcp_url(request: Request, path: str = "") -> str:
    """The election-finance MCP resource URL, e.g. https://www.over.org.il/elections/mcp."""
    return f"{base_url(request)}{ELECTIONS_MCP_PREFIX}{path}"


def elections_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the elections resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/elections/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{ELECTIONS_MCP_PREFIX}"


def nadlan_mcp_url(request: Request, path: str = "") -> str:
    """The נדל"ן לעם MCP resource URL, e.g. https://www.over.org.il/nadlan/mcp."""
    return f"{base_url(request)}{NADLAN_MCP_PREFIX}{path}"


def nadlan_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the nadlan resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/nadlan/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{NADLAN_MCP_PREFIX}"


def deals_mcp_url(request: Request, path: str = "") -> str:
    """The עסקאות נדל"ן MCP resource URL, e.g. https://www.over.org.il/deals/mcp."""
    return f"{base_url(request)}{DEALS_MCP_PREFIX}{path}"


def deals_resource_metadata_url(request: Request) -> str:
    """RFC 9728 location of the deals resource's protected-resource metadata:
    /.well-known/oauth-protected-resource/deals/mcp at the ROOT host."""
    return f"{base_url(request)}/.well-known/oauth-protected-resource{DEALS_MCP_PREFIX}"


def google_callback_url(request: Request) -> str:
    """Pinned to APP_BASE_URL, NOT the request host: Google only accepts
    redirect URIs registered in the Cloud Console, and the site answers on
    several hosts (over.org.il, www.over.org.il, the platform URL). A client
    that connected via an unregistered host got ``redirect_uri_mismatch``.
    The callback is stateless (signed JWT state), so landing on a different
    host than the one the flow started on is harmless."""
    base = (settings.app_base_url or "").rstrip("/")
    if not base or "localhost" in base:
        base = base_url(request)
    return f"{base}{GOOGLE_CALLBACK_PATH}"


def mcp_jwt_secret() -> str:
    return settings.get_jwt_secret()


def mcp_service_token() -> str:
    """Shared machine-to-machine secret for the discovery gateway (or "" if the
    service-token bypass is disabled). See app/mcp/auth.py."""
    return (settings.mcp_service_token or "").strip()
