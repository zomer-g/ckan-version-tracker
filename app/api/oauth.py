"""OAuth2 SSO endpoints for Google."""

import base64
import logging
import secrets
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import create_access_token, decode_access_token
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.rate_limit import limiter
from app.services.drive_client import DRIVE_SCOPE

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/auth/sso", tags=["sso"])

# -- Google OAuth2 ------------------------------------------------------------

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


@router.get("/google")
@limiter.limit("20/minute")
async def google_login(request: Request):
    """Redirect user to Google's OAuth2 consent screen."""
    if not settings.google_client_id:
        raise HTTPException(status_code=501, detail="Google SSO not configured")

    state = secrets.token_urlsafe(32)
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": f"{settings.app_base_url}/api/auth/sso/google/callback",
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account",
    }
    return RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


@router.get("/google/callback")
@limiter.limit("20/minute")
async def google_callback(
    request: Request,
    code: str = "",
    error: str = "",
    db: AsyncSession = Depends(get_db),
):
    """Handle Google OAuth2 callback."""
    if error or not code:
        return RedirectResponse(url="/admin/login?error=google_denied")

    try:
        # Exchange code for tokens
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "redirect_uri": f"{settings.app_base_url}/api/auth/sso/google/callback",
                    "grant_type": "authorization_code",
                },
            )
            token_resp.raise_for_status()
            tokens = token_resp.json()

            # Get user info
            userinfo_resp = await client.get(
                GOOGLE_USERINFO_URL,
                headers={"Authorization": f"Bearer {tokens['access_token']}"},
            )
            userinfo_resp.raise_for_status()
            userinfo = userinfo_resp.json()

        email = userinfo.get("email", "")
        name = userinfo.get("name", "") or email.split("@")[0]

        if not email:
            return RedirectResponse(url="/admin/login?error=no_email")

        # Find or create user
        jwt_token = await _find_or_create_user(db, email, name, "google")
        return RedirectResponse(url=f"/admin/login?sso_token={jwt_token}")

    except Exception:
        logger.exception("Google OAuth callback failed")
        return RedirectResponse(url="/admin/login?error=google_failed")


# -- Google Drive connect (admin) ---------------------------------------------
# Separate from the SSO login above so a normal sign-in never asks for Drive
# access. An admin opts in once here; we request offline access + force a
# consent prompt so Google returns a refresh_token, which we store on the user
# for the "export a version's files to Drive" runner.


def _b64u(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode("utf-8")).decode("ascii").rstrip("=")


def _b64u_dec(s: str) -> str:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii")).decode("utf-8")


@router.get("/google/drive/connect")
@limiter.limit("20/minute")
async def google_drive_connect(
    request: Request,
    token: str = "",
    next: str = "/",
    db: AsyncSession = Depends(get_db),
):
    """Start the Drive-authorization flow for the logged-in admin. ``token``
    is the caller's JWT (passed by the SPA, which can't set an auth header on a
    top-level navigation); ``next`` is the path to return to afterwards."""
    if not settings.google_client_id:
        raise HTTPException(status_code=501, detail="Google SSO not configured")

    uid = decode_access_token(token)
    user = None
    if uid:
        user = (await db.execute(select(User).where(User.id == uid))).scalar_one_or_none()
    if not user or not user.is_admin:
        return RedirectResponse(url="/admin/login?error=drive_unauthorized")

    # state carries identity + return path so the callback can attribute the
    # refresh token and bounce back. The JWT inside is self-validating.
    state = f"{token}.{_b64u(next or '/')}"
    params = {
        "client_id": settings.google_client_id,
        "redirect_uri": f"{settings.app_base_url}/api/auth/sso/google/drive/callback",
        "response_type": "code",
        "scope": f"openid email profile {DRIVE_SCOPE}",
        "state": state,
        "access_type": "offline",
        "prompt": "consent",  # force a refresh_token even on re-auth
        "include_granted_scopes": "true",
    }
    return RedirectResponse(url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


@router.get("/google/drive/callback")
@limiter.limit("20/minute")
async def google_drive_callback(
    request: Request,
    code: str = "",
    error: str = "",
    state: str = "",
    db: AsyncSession = Depends(get_db),
):
    """Handle the Drive consent callback: store the refresh token on the
    admin and redirect back to ``next``."""
    # state = "<JWT>.<base64url(next)>". The JWT itself contains two dots
    # (header.payload.signature), and base64url has no dots — so split on the
    # LAST dot to recover the full JWT and the encoded return path.
    token, _, next_b64 = state.rpartition(".")
    try:
        next_path = _b64u_dec(next_b64) if next_b64 else "/"
    except Exception:
        next_path = "/"
    sep = "&" if "?" in next_path else "?"

    if error or not code:
        return RedirectResponse(url=f"{next_path}{sep}drive=denied")

    uid = decode_access_token(token)
    user = None
    if uid:
        user = (await db.execute(select(User).where(User.id == uid))).scalar_one_or_none()
    if not user or not user.is_admin:
        return RedirectResponse(url="/admin/login?error=drive_unauthorized")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            token_resp = await client.post(
                GOOGLE_TOKEN_URL,
                data={
                    "code": code,
                    "client_id": settings.google_client_id,
                    "client_secret": settings.google_client_secret,
                    "redirect_uri": f"{settings.app_base_url}/api/auth/sso/google/drive/callback",
                    "grant_type": "authorization_code",
                },
            )
            token_resp.raise_for_status()
            tokens = token_resp.json()
    except Exception:
        logger.exception("Google Drive token exchange failed")
        return RedirectResponse(url=f"{next_path}{sep}drive=error")

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        # Google omits refresh_token when one was already granted and we
        # didn't force consent. We do force it, so this is unexpected.
        logger.warning("Drive callback returned no refresh_token for %s", user.email)
        return RedirectResponse(url=f"{next_path}{sep}drive=norefresh")

    user.google_refresh_token = refresh_token
    await db.commit()
    return RedirectResponse(url=f"{next_path}{sep}drive=connected")


# -- Shared helpers ------------------------------------------------------------


async def _find_or_create_user(db: AsyncSession, email: str, name: str, provider: str) -> str:
    """Find existing user by email, or create a new one. Returns JWT token."""
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    if user:
        if not user.is_active:
            raise HTTPException(status_code=403, detail="Account disabled")
        # Update provider if not set
        changed = False
        if not user.oauth_provider:
            user.oauth_provider = provider
            changed = True
        # Ensure the designated admin has is_admin set
        if email.lower() == "zomerg@gmail.com" and not user.is_admin:
            user.is_admin = True
            changed = True
        if changed:
            await db.commit()
    else:
        # Create new user (SSO-only, no password auth)
        user = User(
            email=email,
            hashed_password="!sso-only-no-password-auth",
            display_name=name,
            oauth_provider=provider,
            is_admin=(email.lower() == "zomerg@gmail.com"),
        )
        db.add(user)
        await db.commit()
        await db.refresh(user)

    return create_access_token(str(user.id))


# -- SSO availability check ---------------------------------------------------


@router.get("/providers")
async def sso_providers():
    """Return which SSO providers are configured (public endpoint)."""
    return {
        "google": bool(settings.google_client_id),
    }
