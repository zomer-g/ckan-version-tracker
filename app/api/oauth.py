"""OAuth2 SSO endpoints for Google."""

import logging
import secrets
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import create_access_token
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.rate_limit import limiter

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
