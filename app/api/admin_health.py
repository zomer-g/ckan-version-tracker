"""Admin API for the system health checks (app/services/health_checks.py).

    GET  /api/admin/health/checks      the registry: what each check does, which tier
    GET  /api/admin/health/overview    latest verdict per check + latest run per tier
    GET  /api/admin/health/runs        the log, newest first (no per-check bodies)
    GET  /api/admin/health/runs/{id}   one run with every result
    POST /api/admin/health/run         start a run now: {tier} or {checks: [...]}
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.health_run import HealthRun
from app.models.user import User
from app.rate_limit import limiter
from app.services import health_checks as hc

router = APIRouter(prefix="/api/admin/health", tags=["admin-health"])


def _run_row(r: HealthRun, *, full: bool = False) -> dict:
    out = {
        "id": str(r.id),
        "tier": r.tier,
        "trigger": r.trigger,
        "status": r.status,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "duration_ms": r.duration_ms,
        "summary": r.summary or {},
    }
    if full:
        out["results"] = r.results or []
    return out


@router.get("/checks")
@limiter.limit("60/minute")
async def list_checks(request: Request, user: User = Depends(get_admin_user)):
    from app.worker.scheduler import scheduler
    nxt = {}
    if scheduler.running:
        for jid in ("health_daily", "health_weekly"):
            j = scheduler.get_job(jid)
            if j and j.next_run_time:
                nxt[jid.split("_", 1)[1]] = j.next_run_time.isoformat()
    return {
        "checks": [
            {"key": c.key, "component": c.component, "title": c.title,
             "description": c.description, "tiers": sorted(c.tiers),
             "timeout": c.timeout}
            for c in hc.REGISTRY.values()
        ],
        "enabled": settings.health_checks_enabled,
        "next_runs": nxt,
        "alerts_configured": bool(settings.health_alert_webhook_url),
        "running": hc.is_running(),
    }


@router.get("/overview")
@limiter.limit("60/minute")
async def overview(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """The newest verdict for every check, from whichever run last ran it, and
    the newest run of each tier — the 'what is broken right now' view."""
    recent = (await db.execute(
        select(HealthRun).order_by(HealthRun.started_at.desc()).limit(15)
    )).scalars().all()
    latest_by_tier: dict[str, dict] = {}
    checks: dict[str, dict] = {}
    running = None
    for r in recent:
        if r.status == "running":
            running = running or _run_row(r)
            continue
        latest_by_tier.setdefault(r.tier, _run_row(r))
        for res in r.results or []:
            if res.get("key") not in checks:
                checks[res["key"]] = {**res, "run_id": str(r.id),
                                      "run_tier": r.tier,
                                      "checked_at": r.started_at.isoformat()}
    order = list(hc.REGISTRY)
    rows = sorted(checks.values(),
                  key=lambda c: order.index(c["key"]) if c["key"] in order else 999)
    return {"latest": latest_by_tier, "checks": rows, "running": running,
            "busy": hc.is_running()}


@router.get("/runs")
@limiter.limit("60/minute")
async def list_runs(
    request: Request,
    tier: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    limit = max(1, min(limit, 200))
    q = select(HealthRun)
    cq = select(func.count()).select_from(HealthRun)
    if tier:
        q, cq = q.where(HealthRun.tier == tier), cq.where(HealthRun.tier == tier)
    if status:
        q, cq = q.where(HealthRun.status == status), cq.where(HealthRun.status == status)
    total = (await db.execute(cq)).scalar() or 0
    # Bodies stay out of the list: a weekly run's results are tens of KB.
    rows = (await db.execute(
        q.with_only_columns(
            HealthRun.id, HealthRun.tier, HealthRun.trigger, HealthRun.status,
            HealthRun.started_at, HealthRun.finished_at, HealthRun.duration_ms,
            HealthRun.summary)
        .order_by(HealthRun.started_at.desc()).limit(limit).offset(offset)
    )).all()
    return {
        "total": total,
        "runs": [{
            "id": str(r.id), "tier": r.tier, "trigger": r.trigger, "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "duration_ms": r.duration_ms, "summary": r.summary or {},
        } for r in rows],
    }


@router.get("/runs/{run_id}")
@limiter.limit("60/minute")
async def get_run(
    request: Request,
    run_id: str,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    r = await db.get(HealthRun, parse_uuid(run_id, "run_id"))
    if not r:
        raise HTTPException(status_code=404, detail="run not found")
    return _run_row(r, full=True)


class RunRequest(BaseModel):
    tier: str = "daily"
    checks: list[str] | None = None


@router.post("/run")
@limiter.limit("6/minute")
async def start_run(
    request: Request,
    payload: RunRequest,
    user: User = Depends(get_admin_user),
):
    """Start a run in the background and return its id; poll /runs/{id}.
    A hand-picked set of checks is recorded as tier ``manual``."""
    tier = "manual" if payload.checks else payload.tier
    if tier not in hc.TIERS:
        raise HTTPException(status_code=400, detail=f"tier must be one of {hc.TIERS}")
    try:
        rid = hc.start_in_background(tier, payload.checks or None, user.email)
    except hc.HealthBusy as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"id": rid, "tier": tier, "status": "started"}
