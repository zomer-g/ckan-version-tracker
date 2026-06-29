"""OAuth 2.1 + PKCE authorization server for the MCP endpoint.

Google is the upstream identity provider (reusing OVER's existing Google
OAuth client); the ``api_users`` table is the closed-beta access gate. Ported
from the Ocal project. Flow: client DCR-registers → /authorize redirects to
Google → /google/callback verifies the email against api_users and mints our
own PKCE auth code → client exchanges it at /token for a JWT access token.
"""
from __future__ import annotations

import base64
import hashlib
import logging
import secrets
import time
from datetime import datetime, timedelta, timezone

import httpx
import jwt
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from app.config import settings
from app.mcp.config import (
    MCP_ACCESS_TOKEN_TTL_SECONDS,
    MCP_AUTH_CODE_TTL_SECONDS,
    MCP_JWT_AUDIENCE,
    MCP_REFRESH_TOKEN_TTL_SECONDS,
    MCP_STATE_TTL_SECONDS,
    base_url,
    google_callback_url,
    mcp_jwt_secret,
    mcp_url,
)
from app.models.mcp import ApiUser, McpOauthClient, McpOauthCode

logger = logging.getLogger(__name__)

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def _s256(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def _err_html(status: int, title: str, message: str) -> HTMLResponse:
    return HTMLResponse(
        status_code=status,
        content=(
            f"<!doctype html><html dir='rtl' lang='he'><head><meta charset='utf-8'>"
            f"<title>{title}</title></head>"
            f"<body style=\"font-family:sans-serif;max-width:560px;margin:80px auto;"
            f"padding:0 20px;text-align:center;\"><h1>{title}</h1><p>{message}</p></body></html>"
        ),
    )


def _oauth_error(status: int, code: str, description: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "error_description": description})


# ── RFC 9728 / RFC 8414 metadata ───────────────────────────────────────────

def protected_resource_metadata(request: Request) -> JSONResponse:
    return JSONResponse({
        "resource": mcp_url(request),
        "authorization_servers": [mcp_url(request)],
        "bearer_methods_supported": ["header"],
        "resource_documentation": f"{base_url(request)}/api",
        "scopes_supported": ["mcp"],
    })


def authorization_server_metadata(request: Request) -> JSONResponse:
    base = mcp_url(request)
    return JSONResponse({
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none", "client_secret_post"],
        "scopes_supported": ["mcp"],
    })


# ── RFC 7591 Dynamic Client Registration ───────────────────────────────────

async def register_client(request: Request, db: AsyncSession) -> Response:
    try:
        body = await request.json()
    except Exception:
        return _oauth_error(400, "invalid_client_metadata", "body must be JSON")
    name = (body or {}).get("client_name")
    redirect_uris = (body or {}).get("redirect_uris")
    if not isinstance(name, str) or not name.strip():
        return _oauth_error(400, "invalid_client_metadata", "client_name required")
    if not isinstance(redirect_uris, list) or not redirect_uris or not all(isinstance(u, str) for u in redirect_uris):
        return _oauth_error(400, "invalid_client_metadata", "redirect_uris must be a non-empty string list")

    auth_method = body.get("token_endpoint_auth_method") or "none"
    plain_secret = secret_hash = None
    if auth_method != "none":
        plain_secret = secrets.token_urlsafe(32)
        secret_hash = hashlib.sha256(plain_secret.encode()).hexdigest()

    client = McpOauthClient(
        client_name=name.strip()[:200],
        redirect_uris=redirect_uris[:10],
        grant_types=body.get("grant_types") or ["authorization_code", "refresh_token"],
        response_types=body.get("response_types") or ["code"],
        token_endpoint_auth_method=auth_method,
        scope=body.get("scope") or "mcp",
        client_secret_hash=secret_hash,
    )
    db.add(client)
    await db.commit()
    await db.refresh(client)
    logger.info("MCP OAuth client registered: %s (%s)", client.client_id, client.client_name)

    out = {
        "client_id": str(client.client_id),
        "client_name": client.client_name,
        "redirect_uris": client.redirect_uris,
        "grant_types": client.grant_types,
        "response_types": client.response_types,
        "token_endpoint_auth_method": client.token_endpoint_auth_method,
        "scope": client.scope,
        "client_id_issued_at": int(client.created_at.timestamp()),
    }
    if plain_secret:
        out["client_secret"] = plain_secret
    return JSONResponse(status_code=201, content=out)


# ── Authorization endpoint (wraps Google) ──────────────────────────────────

async def authorize(request: Request, db: AsyncSession) -> Response:
    q = request.query_params
    if q.get("response_type") != "code":
        return _err_html(400, "בקשה לא תקינה", "response_type חייב להיות 'code'.")
    client_id = q.get("client_id") or ""
    redirect_uri = q.get("redirect_uri") or ""
    code_challenge = q.get("code_challenge") or ""
    if not client_id or not redirect_uri or len(code_challenge) < 43:
        return _err_html(400, "בקשה לא תקינה", "פרמטרי OAuth חסרים או שגויים (client_id / redirect_uri / code_challenge).")

    client = (await db.execute(
        select(McpOauthClient).where(McpOauthClient.client_id == _as_uuid(client_id))
    )).scalar_one_or_none() if _is_uuid(client_id) else None
    if not client:
        return _err_html(400, "לקוח לא רשום", "ה-MCP client לא רשום במערכת.")
    if redirect_uri not in (client.redirect_uris or []):
        return _err_html(400, "redirect_uri לא מאושר", "ה-redirect URI לא תואם למה שנרשם.")
    if not settings.google_client_id or not settings.google_client_secret:
        return _err_html(503, "הזדהות לא זמינה", "Google OAuth לא מוגדר בשרת.")

    state = jwt.encode({
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": q.get("code_challenge_method") or "S256",
        "client_state": q.get("state"),
        "scope": q.get("scope") or "mcp",
        "exp": int(time.time()) + MCP_STATE_TTL_SECONDS,
    }, mcp_jwt_secret(), algorithm="HS256")

    from urllib.parse import urlencode
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": google_callback_url(request),
        "response_type": "code",
        "scope": "openid email profile",
        "prompt": "select_account",
        "access_type": "online",
        "state": state,
    }
    return RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


async def google_callback(request: Request, db: AsyncSession) -> Response:
    code = request.query_params.get("code")
    state = request.query_params.get("state")
    if not code or not state:
        return _err_html(400, "תגובה חלקית", "חסר code או state בתגובה מ-Google.")
    try:
        st = jwt.decode(state, mcp_jwt_secret(), algorithms=["HS256"])
    except Exception:
        return _err_html(400, "state לא תקין", "ה-state פג תוקף או לא תקף. נסה להתחבר מחדש.")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            tok = await client.post(GOOGLE_TOKEN_URL, data={
                "code": code,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "redirect_uri": google_callback_url(request),
                "grant_type": "authorization_code",
            })
            tok.raise_for_status()
            access = tok.json().get("access_token")
            ui = await client.get(GOOGLE_USERINFO_URL, headers={"Authorization": f"Bearer {access}"})
            ui.raise_for_status()
            info = ui.json()
    except Exception:
        logger.exception("MCP Google token/userinfo exchange failed")
        return _err_html(502, "שגיאת הזדהות", "כשל בחילופי הטוקן מול Google. נסה שוב.")

    email = (info.get("email") or "").lower()
    if not email:
        return _err_html(400, "חסר אימייל", "Google לא החזיר כתובת אימייל.")

    api_user = (await db.execute(select(ApiUser).where(ApiUser.email == email))).scalar_one_or_none()
    if not api_user:
        logger.warning("MCP: email %s not in api_users — invite required", email)
        return _err_html(403, "אין הרשאה ל-MCP",
                         f"הכתובת <strong>{email}</strong> אינה מוזמנת ל-MCP של גרסאות לעם. "
                         f"לקבלת הזמנה יש לפנות למנהל המערכת.")
    if not api_user.is_active:
        return _err_html(403, "חשבון מושבת", "החשבון שלך מושבת. פנה למנהל המערכת.")

    api_user.google_id = info.get("id") or api_user.google_id
    api_user.name = info.get("name") or api_user.name
    api_user.last_seen_at = datetime.now(timezone.utc)

    auth_code = secrets.token_urlsafe(32)
    db.add(McpOauthCode(
        code=auth_code,
        client_id=_as_uuid(st["client_id"]),
        api_user_id=api_user.id,
        redirect_uri=st["redirect_uri"],
        code_challenge=st["code_challenge"],
        code_challenge_method=st.get("code_challenge_method", "S256"),
        scope=st.get("scope", "mcp"),
        expires_at=datetime.now(timezone.utc) + timedelta(seconds=MCP_AUTH_CODE_TTL_SECONDS),
    ))
    await db.commit()

    from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
    parts = urlsplit(st["redirect_uri"])
    query = dict(parse_qsl(parts.query))
    query["code"] = auth_code
    if st.get("client_state"):
        query["state"] = st["client_state"]
    dest = urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
    logger.info("MCP authorization code issued for %s (client %s)", email, st["client_id"])
    return RedirectResponse(url=dest)


# ── Token endpoint ─────────────────────────────────────────────────────────

def _sign_access(request: Request, api_user_id: str, client_id: str) -> str:
    return jwt.encode({
        "sub": str(api_user_id), "cid": str(client_id), "scope": "mcp",
        "aud": MCP_JWT_AUDIENCE, "iss": mcp_url(request),
        "exp": int(time.time()) + MCP_ACCESS_TOKEN_TTL_SECONDS,
    }, mcp_jwt_secret(), algorithm="HS256")


def _sign_refresh(request: Request, api_user_id: str, client_id: str) -> str:
    return jwt.encode({
        "sub": str(api_user_id), "cid": str(client_id), "typ": "refresh",
        "aud": MCP_JWT_AUDIENCE, "iss": mcp_url(request),
        "exp": int(time.time()) + MCP_REFRESH_TOKEN_TTL_SECONDS,
    }, mcp_jwt_secret(), algorithm="HS256")


async def token(request: Request, db: AsyncSession) -> Response:
    # RFC 6749 §4.1.3: the token endpoint accepts application/x-www-form-urlencoded.
    body: dict = {}
    try:
        form = await request.form()
        body = {k: v for k, v in form.items()}
    except Exception:
        body = {}
    if not body:
        try:
            body = await request.json()
        except Exception:
            body = {}

    grant_type = body.get("grant_type")
    client_id = body.get("client_id") or ""
    if not _is_uuid(client_id):
        return _oauth_error(400, "invalid_request", "client_id missing or invalid")

    # client auth (public clients use 'none' → PKCE only)
    client = (await db.execute(
        select(McpOauthClient).where(McpOauthClient.client_id == _as_uuid(client_id))
    )).scalar_one_or_none()
    if not client:
        return _oauth_error(401, "invalid_client", "unknown client_id")
    if client.token_endpoint_auth_method != "none":
        provided = body.get("client_secret") or ""
        if not provided or hashlib.sha256(provided.encode()).hexdigest() != client.client_secret_hash:
            return _oauth_error(401, "invalid_client", "invalid client_secret")

    if grant_type == "authorization_code":
        code = body.get("code") or ""
        verifier = body.get("code_verifier") or ""
        redirect_uri = body.get("redirect_uri") or ""
        row = (await db.execute(select(McpOauthCode).where(McpOauthCode.code == code))).scalar_one_or_none()
        if not row:
            return _oauth_error(400, "invalid_grant", "authorization code not found")
        # single-use: delete regardless of outcome
        await db.delete(row)
        await db.commit()
        if row.expires_at < datetime.now(timezone.utc):
            return _oauth_error(400, "invalid_grant", "authorization code expired")
        if str(row.client_id) != client_id:
            return _oauth_error(400, "invalid_grant", "code issued to a different client")
        if row.redirect_uri != redirect_uri:
            return _oauth_error(400, "invalid_grant", "redirect_uri mismatch")
        if len(verifier) < 43 or _s256(verifier) != row.code_challenge:
            return _oauth_error(400, "invalid_grant", "PKCE verification failed")
        return JSONResponse({
            "access_token": _sign_access(request, row.api_user_id, row.client_id),
            "token_type": "Bearer",
            "expires_in": MCP_ACCESS_TOKEN_TTL_SECONDS,
            "refresh_token": _sign_refresh(request, row.api_user_id, row.client_id),
            "scope": row.scope or "mcp",
        })

    if grant_type == "refresh_token":
        try:
            payload = jwt.decode(body.get("refresh_token") or "", mcp_jwt_secret(),
                                 algorithms=["HS256"], audience=MCP_JWT_AUDIENCE)
        except Exception:
            return _oauth_error(400, "invalid_grant", "refresh token invalid or expired")
        if payload.get("typ") != "refresh":
            return _oauth_error(400, "invalid_grant", "not a refresh token")
        if payload.get("cid") != client_id:
            return _oauth_error(400, "invalid_grant", "refresh token bound to a different client")
        user = (await db.execute(
            select(ApiUser).where(ApiUser.id == _as_uuid(payload["sub"]), ApiUser.is_active.is_(True))
        )).scalar_one_or_none()
        if not user:
            return _oauth_error(400, "invalid_grant", "user no longer active")
        return JSONResponse({
            "access_token": _sign_access(request, payload["sub"], client_id),
            "token_type": "Bearer",
            "expires_in": MCP_ACCESS_TOKEN_TTL_SECONDS,
            "refresh_token": _sign_refresh(request, payload["sub"], client_id),
            "scope": "mcp",
        })

    return _oauth_error(400, "unsupported_grant_type", f"unsupported grant_type: {grant_type}")


# ── small uuid helpers ─────────────────────────────────────────────────────

import uuid as _uuid


def _is_uuid(s: str) -> bool:
    try:
        _uuid.UUID(str(s)); return True
    except (ValueError, TypeError, AttributeError):
        return False


def _as_uuid(s) -> _uuid.UUID:
    return s if isinstance(s, _uuid.UUID) else _uuid.UUID(str(s))
