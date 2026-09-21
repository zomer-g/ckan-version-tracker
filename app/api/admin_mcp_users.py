"""Admin management of the MCP user list (api_users).

Anyone completing Google SSO on the MCP self-registers at tier beta
(app/mcp/oauth.py); this is the list they land in, with each user's call
counts per MCP server, and the admin's levers over it.

  GET    /api/admin/mcp-users       — list users (+ usage per MCP server)
  POST   /api/admin/mcp-users       — add by email ahead of their first login
  PATCH  /api/admin/mcp-users/{id}  — tier / is_active
  DELETE /api/admin/mcp-users/{id}  — soft-disable (is_active=false)
"""
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.mcp import ApiUser, McpUsageEvent
from app.models.user import User
from app.rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/mcp-users", tags=["admin-mcp"])


class McpUserOut(BaseModel):
    id: str
    email: str
    name: str | None
    tier: str
    is_active: bool
    monthly_quota: int | None
    last_seen_at: str | None
    created_at: str
    calls_30d: int = 0
    calls_total: int = 0
    # One entry per MCP server the user called, most-used first. server is
    # null for old rows whose tool name did not identify it (migration 067).
    usage_by_server: list[dict] = []
    self_registered: bool = False


class InviteRequest(BaseModel):
    email: str
    name: str | None = None
    tier: str = "beta"


class UpdateMcpUser(BaseModel):
    tier: str | None = None
    is_active: bool | None = None
    monthly_quota: int | None = None


def _validate_tier(tier: str) -> str:
    if tier not in ("beta", "free", "pro"):
        raise HTTPException(status_code=400, detail="tier must be beta | free | pro")
    return tier


@router.get("", response_model=list[McpUserOut])
@limiter.limit("60/minute")
async def list_mcp_users(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    rows = (await db.execute(select(ApiUser).order_by(ApiUser.created_at.desc()))).scalars().all()
    # 30-day call counts per user (one grouped query).
    since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    from datetime import timedelta
    since = since - timedelta(days=30)
    per_server = (await db.execute(
        select(McpUsageEvent.api_user_id, McpUsageEvent.mcp_server,
               func.count(McpUsageEvent.id),
               func.count(McpUsageEvent.id).filter(McpUsageEvent.created_at >= since),
               func.max(McpUsageEvent.created_at))
        .group_by(McpUsageEvent.api_user_id, McpUsageEvent.mcp_server)
    )).all()
    by_user: dict = {}
    for uid, server, total, recent, last in per_server:
        by_user.setdefault(uid, []).append({
            "server": server, "calls": int(total), "calls_30d": int(recent),
            "last_at": last.isoformat() if last else None})
    for entries in by_user.values():
        entries.sort(key=lambda e: -e["calls"])
    return [McpUserOut(
        id=str(u.id), email=u.email, name=u.name, tier=u.tier, is_active=u.is_active,
        monthly_quota=u.monthly_quota,
        last_seen_at=u.last_seen_at.isoformat() if u.last_seen_at else None,
        created_at=u.created_at.isoformat() if u.created_at else "",
        calls_30d=sum(e["calls_30d"] for e in by_user.get(u.id, [])),
        calls_total=sum(e["calls"] for e in by_user.get(u.id, [])),
        usage_by_server=by_user.get(u.id, []),
        self_registered=u.invited_by is None,
    ) for u in rows]


@router.post("", response_model=McpUserOut, status_code=201)
@limiter.limit("30/minute")
async def invite_mcp_user(
    request: Request,
    body: InviteRequest,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    email = (body.email or "").strip().lower()
    if "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="כתובת אימייל לא תקינה")
    existing = (await db.execute(select(ApiUser).where(ApiUser.email == email))).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail="משתמש MCP עם הכתובת הזו כבר קיים")
    u = ApiUser(email=email, name=body.name, tier=_validate_tier(body.tier), invited_by=user.id)
    db.add(u)
    await db.commit()
    await db.refresh(u)
    logger.info("MCP user invited: %s by %s", email, user.email)
    return McpUserOut(
        id=str(u.id), email=u.email, name=u.name, tier=u.tier, is_active=u.is_active,
        monthly_quota=u.monthly_quota, last_seen_at=None,
        created_at=u.created_at.isoformat() if u.created_at else "",
    )


@router.patch("/{user_id}", response_model=McpUserOut)
@limiter.limit("30/minute")
async def update_mcp_user(
    request: Request,
    user_id: str,
    body: UpdateMcpUser,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    uid = parse_uuid(user_id, "user_id")
    u = (await db.execute(select(ApiUser).where(ApiUser.id == uid))).scalar_one_or_none()
    if not u:
        raise HTTPException(status_code=404, detail="MCP user not found")
    if body.tier is not None:
        u.tier = _validate_tier(body.tier)
    if body.is_active is not None:
        u.is_active = body.is_active
    if body.monthly_quota is not None:
        u.monthly_quota = body.monthly_quota or None
    u.updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(u)
    return McpUserOut(
        id=str(u.id), email=u.email, name=u.name, tier=u.tier, is_active=u.is_active,
        monthly_quota=u.monthly_quota,
        last_seen_at=u.last_seen_at.isoformat() if u.last_seen_at else None,
        created_at=u.created_at.isoformat() if u.created_at else "",
    )


@router.delete("/{user_id}", status_code=204)
@limiter.limit("30/minute")
async def disable_mcp_user(
    request: Request,
    user_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Soft-disable: keep the row (and usage history) but revoke access."""
    uid = parse_uuid(user_id, "user_id")
    u = (await db.execute(select(ApiUser).where(ApiUser.id == uid))).scalar_one_or_none()
    if not u:
        raise HTTPException(status_code=404, detail="MCP user not found")
    u.is_active = False
    u.updated_at = datetime.now(timezone.utc)
    await db.commit()
    logger.info("MCP user disabled: %s by %s", u.email, user.email)
