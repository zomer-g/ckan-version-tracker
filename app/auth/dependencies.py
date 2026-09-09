"""Who is signed in, and who is an admin.

**Admin is not stored.** It is re-derived on every request from
``settings.admin_emails`` — a platform secret — and never read from
``users.is_admin``. That column still exists in the schema and is still written
by nothing; leaving it in place keeps the migration small, but it authorizes
nothing, so a row edited by hand (or by a future bug, or by anyone who reaches
the database) does not grant anything.

The reason is the migration onto xhostd: the public SQL console and the
application's own tables are moving into one database, and the separation that
used to make privilege escalation *structurally* impossible becomes a matter of
which schema a table lives in. In that world an admin flag sitting in a row is a
target. A flag that only exists in an environment variable is not.

There is deliberately no code path anywhere in the application that adds an
admin. Granting admin means editing the secret on the platform and redeploying.
"""
import uuid

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.security import decode_access_token
from app.config import settings
from app.database import get_db
from app.models.user import User

bearer_scheme = HTTPBearer()
# The same scheme, but a missing Authorization header yields None instead of a
# 403 from FastAPI's own handler. Used by surfaces that are readable to anyone
# and only want to know who is asking.
optional_bearer_scheme = HTTPBearer(auto_error=False)


def is_admin(user: User | None) -> bool:
    """True when this user's email is in the platform's admin secret."""
    return user is not None and settings.is_admin_email(user.email)


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    user_id = decode_access_token(credentials.credentials)
    if user_id is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    result = await db.execute(select(User).where(User.id == uid, User.is_active.is_(True)))
    user = result.scalar_one_or_none()
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


async def get_optional_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(optional_bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> User | None:
    """The signed-in user, or None. Never raises for an anonymous caller."""
    if credentials is None:
        return None
    user_id = decode_access_token(credentials.credentials)
    if user_id is None:
        return None
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        return None
    result = await db.execute(select(User).where(User.id == uid, User.is_active.is_(True)))
    return result.scalar_one_or_none()


async def require_signed_in_user(
    user: User = Depends(get_current_user),
) -> User:
    """Any signed-in account. This is an ATTRIBUTION gate, not an authorization
    one: anyone with a Google account can pass it. It exists so a query on the
    public SQL console has a name attached and a per-user budget, not to decide
    who may read public data — everything those consoles reach is public by
    design. Do not use it to protect anything.
    """
    return user


async def get_admin_user(
    user: User = Depends(get_current_user),
) -> User:
    if not is_admin(user):
        raise HTTPException(status_code=403, detail="Admin access required")
    return user
