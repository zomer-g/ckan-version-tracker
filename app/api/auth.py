from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app.auth.dependencies import bearer_scheme, get_current_user, is_admin
from app.auth.security import create_access_token, decode_access_claims, session_too_old
from app.models.user import User

router = APIRouter(prefix="/api/auth", tags=["auth"])


class UserResponse(BaseModel):
    id: str
    email: str
    display_name: str
    is_admin: bool

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    token: str


@router.get("/me", response_model=UserResponse)
async def me(user: User = Depends(get_current_user)):
    return UserResponse(
        id=str(user.id),
        email=user.email,
        display_name=user.display_name,
        # Derived from the ADMIN_EMAILS secret, never from the row.
        is_admin=is_admin(user),
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    user: User = Depends(get_current_user),
):
    """Slide the session forward: a caller holding a still-valid (short-lived)
    JWT gets a fresh one. Admin sessions now expire in ~2h (was 24h) to bound
    the blast radius of any residual token leak; the SPA calls this on load and
    on a timer so an active user is never logged out mid-work.

    The new token keeps the original sign-in time, and a session older than
    ``jwt_max_session_hours`` is refused: sliding alone let one stolen token
    live forever."""
    claims = decode_access_claims(credentials.credentials) or {}
    if session_too_old(claims):
        raise HTTPException(status_code=401, detail="Session expired; sign in again")
    return TokenResponse(token=create_access_token(str(user.id), auth_time=claims["auth_time"]))
