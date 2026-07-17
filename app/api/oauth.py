"""OAuth2 SSO endpoints for Google."""

import logging
import secrets
import uuid
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.auth.security import create_access_token
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.rate_limit import limiter
from app.services import auth_codes
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

        # Find or create user, then hand the SPA a ONE-TIME CODE (not the JWT)
        # in the redirect URL. The JWT never touches the query string / Referer /
        # logs; the SPA swaps the code for a fresh token via POST /exchange.
        user = await _find_or_create_user(db, email, name, "google")
        login_code = await auth_codes.issue_code(
            db, user.id, auth_codes.PURPOSE_LOGIN, ttl_seconds=auth_codes.LOGIN_TTL_SECONDS
        )
        return RedirectResponse(url=f"/admin/login?code={login_code}")

    except Exception:
        logger.exception("Google OAuth callback failed")
        return RedirectResponse(url="/admin/login?error=google_failed")


class ExchangeRequest(BaseModel):
    code: str


class TokenResponse(BaseModel):
    token: str


@router.post("/exchange", response_model=TokenResponse)
@limiter.limit("20/minute")
async def exchange_code(
    request: Request,
    body: ExchangeRequest,
    db: AsyncSession = Depends(get_db),
):
    """Swap a one-time login code (minted by the SSO callback) for a JWT.

    This is the only place the login JWT is handed to the browser, and it
    travels in a POST response body — never a URL. The code is single-use and
    expires within seconds."""
    consumed = await auth_codes.consume_code(db, body.code, auth_codes.PURPOSE_LOGIN)
    if consumed is None:
        raise HTTPException(status_code=400, detail="Invalid or expired login code")

    # Confirm the account is still valid before issuing a session.
    try:
        uid = uuid.UUID(consumed.user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid login code")
    user = (
        await db.execute(select(User).where(User.id == uid, User.is_active.is_(True)))
    ).scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=400, detail="Account not found or disabled")

    return TokenResponse(token=create_access_token(str(user.id)))


# -- Google Drive connect (admin) ---------------------------------------------
# Separate from the SSO login above so a normal sign-in never asks for Drive
# access. An admin opts in once here; we request offline access + force a
# consent prompt so Google returns a refresh_token, which we store on the user
# for the "export a version's files to Drive" runner.


class DriveConnectRequest(BaseModel):
    next: str = "/"


class DriveConnectResponse(BaseModel):
    authorize_url: str


@router.post("/google/drive/connect", response_model=DriveConnectResponse)
@limiter.limit("20/minute")
async def google_drive_connect(
    request: Request,
    body: DriveConnectRequest,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_admin_user),
):
    """Begin the Drive-authorization flow for the logged-in admin.

    The SPA calls this as an authenticated POST (JWT in the Authorization
    header, not the URL). We mint a one-time DRIVE code that maps to this admin
    + the return path, put ONLY that opaque code in Google's ``state``, and hand
    back the authorize URL for the SPA to navigate to. No JWT ever rides in a
    query string or through Google."""
    if not settings.google_client_id:
        raise HTTPException(status_code=501, detail="Google SSO not configured")

    state = await auth_codes.issue_code(
        db,
        admin.id,
        auth_codes.PURPOSE_DRIVE,
        next_path=body.next or "/",
        ttl_seconds=auth_codes.DRIVE_TTL_SECONDS,
    )
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
    return DriveConnectResponse(authorize_url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


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
    admin and redirect back to ``next``.

    ``state`` is the one-time DRIVE code minted by /connect — it carries no
    secret, just an opaque handle to the admin + return path stored server-side.
    """
    consumed = await auth_codes.consume_code(db, state, auth_codes.PURPOSE_DRIVE)
    if consumed is None:
        # Unknown / expired / replayed code — we can't trust the return path.
        return RedirectResponse(url="/admin/login?error=drive_unauthorized")

    next_path = consumed.next_path or "/"
    sep = "&" if "?" in next_path else "?"

    if error or not code:
        return RedirectResponse(url=f"{next_path}{sep}drive=denied")

    try:
        uid = uuid.UUID(consumed.user_id)
    except ValueError:
        return RedirectResponse(url="/admin/login?error=drive_unauthorized")
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


async def _find_or_create_user(db: AsyncSession, email: str, name: str, provider: str) -> User:
    """Find existing user by email, or create a new one. Returns the User."""
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

    return user


# -- SSO availability check ---------------------------------------------------


@router.get("/providers")
async def sso_providers():
    """Return which SSO providers are configured (public endpoint)."""
    return {
        "google": bool(settings.google_client_id),
    }
