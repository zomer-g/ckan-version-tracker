from pydantic import BaseModel
from fastapi import APIRouter, Depends

from app.auth.dependencies import get_current_user
from app.auth.security import create_access_token
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
        is_admin=user.is_admin,
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh(user: User = Depends(get_current_user)):
    """Slide the session forward: a caller holding a still-valid (short-lived)
    JWT gets a fresh one. Admin sessions now expire in ~2h (was 24h) to bound
    the blast radius of any residual token leak; the SPA calls this on load and
    on a timer so an active user is never logged out mid-work."""
    return TokenResponse(token=create_access_token(str(user.id)))
