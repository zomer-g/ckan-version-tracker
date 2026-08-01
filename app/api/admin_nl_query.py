"""Admin surface for the free-text query feature — log, cost, and kill switches.

  GET   /api/admin/nl/log      the questions people asked + which stage answered
  GET   /api/admin/nl/stats    per-stage counts and token spend over a window
  GET   /api/admin/nl/config   current runtime switches
  POST  /api/admin/nl/config   change them (takes effect within ~30s)
  POST  /api/admin/nl/prune    delete log rows older than N days

WHY THE LOG EXISTS: the two numbers that decide whether this feature is worth
its bill — the cache hit rate and the escalation rate — are invisible from the
outside. Without them the only feedback signal is the invoice, which arrives a
month late and does not say which questions caused it.

WHY THE SWITCHES ARE IN THE DB: the moment an admin wants to stop paying for a
tier is the moment they do not want to wait for a redeploy. Env vars are the
wrong shape for a cost control.

PRIVACY: the log holds raw user questions. It is admin-only, it lives in the app
DB (NOT the append DB, which is publicly queryable through /data), and it is
prunable. Do not move it, and do not add it to any public catalog.

NOTE: no ``from __future__ import annotations`` — see app/api/cbs_ask.py for the
slowapi/FastAPI interaction that makes stringified hints break body parsing.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.services import nl_query
from app.services.llm_budget import usage_today

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin/nl", tags=["admin"])


class ConfigBody(BaseModel):
    enabled: bool | None = None
    allow_deepseek: bool | None = None
    allow_anthropic: bool | None = None
    escalate_on_unanswerable: bool | None = None
    # None means "clear the override and fall back to config.py". Distinguished
    # from 0, which is a real value meaning "disabled".
    daily_call_budget: int | None = None
    daily_output_token_budget: int | None = None
    clear_call_budget: bool = False
    clear_token_budget: bool = False


@router.get("/log")
async def query_log(
    request: Request,
    limit: int = 100,
    offset: int = 0,
    stage: str | None = None,
    answered: bool | None = None,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """The question log, newest first. ``stage`` / ``answered`` narrow it —
    filtering to stage=anthropic shows exactly what the expensive tier was spent
    on, which is the question an admin actually has."""
    where, params = [], {"limit": max(1, min(limit, 500)), "offset": max(0, offset)}
    if stage:
        where.append("stage = :stage")
        params["stage"] = stage
    if answered is not None:
        where.append("answered = :answered")
        params["answered"] = answered
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    rows = (await db.execute(text(
        "SELECT id, created_at, question, answered, stage, attempts, model, escalated, "
        "entity, sql, reason, input_tokens, output_tokens, duration_ms "
        f"FROM nl_query_log{clause} ORDER BY id DESC LIMIT :limit OFFSET :offset"),
        params)).mappings().all()
    total = (await db.execute(text(f"SELECT count(*) FROM nl_query_log{clause}"),
                              params)).scalar_one()
    return {"total": int(total), "rows": [dict(r) for r in rows]}


@router.get("/stats")
async def query_stats(
    request: Request,
    days: int = 7,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-stage counts and token spend over a window, plus today's budget use.

    The per-stage split IS the cost story: `cache` and `template` rows are free,
    `deepseek` is cheap, `anthropic` is not. A shifting ratio between them is
    the earliest signal that something changed."""
    days = max(1, min(days, 90))
    rows = (await db.execute(text(
        "SELECT stage, count(*) AS n, "
        "       count(*) FILTER (WHERE answered) AS answered, "
        "       count(*) FILTER (WHERE escalated) AS escalated, "
        "       coalesce(sum(input_tokens), 0) AS input_tokens, "
        "       coalesce(sum(output_tokens), 0) AS output_tokens, "
        "       percentile_disc(0.5) WITHIN GROUP (ORDER BY duration_ms) AS median_ms "
        "FROM nl_query_log "
        "WHERE created_at >= now() - make_interval(days => :days) "
        "GROUP BY stage ORDER BY n DESC"), {"days": days})).mappings().all()
    by_stage = [dict(r) for r in rows]
    total = sum(r["n"] for r in by_stage) or 0
    free = sum(r["n"] for r in by_stage if r["stage"] in ("cache", "template"))
    return {
        "days": days,
        "total": total,
        # The headline: what fraction of questions cost nothing. Every point
        # here is a point off the bill.
        "free_share": round(free / total, 3) if total else None,
        "by_stage": by_stage,
        "budget_today": await usage_today(db),
    }


@router.get("/config")
async def get_cfg(
    request: Request,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    cfg = await nl_query.get_config(db)
    return {
        "config": cfg,
        # What the switches resolve to right now, given which API keys the
        # deployment actually has. A tier allowed in config but with no key is
        # off, and the UI must not claim otherwise.
        "active_tiers": [{"provider": p, "model": m} for p, m in nl_query.tiers(cfg)],
        "keys": {"deepseek": bool(settings.deepseek_api_key),
                 "anthropic": bool(settings.anthropic_api_key)},
        "defaults": {"daily_call_budget": settings.cbs_ask_daily_budget,
                     "daily_output_token_budget": settings.llm_daily_output_token_budget},
    }


@router.post("/config")
async def set_cfg(
    request: Request,
    body: ConfigBody,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Update the switches. Only the fields present in the body change."""
    sets, params = [], {}
    for field in ("enabled", "allow_deepseek", "allow_anthropic",
                  "escalate_on_unanswerable"):
        val = getattr(body, field)
        if val is not None:
            sets.append(f"{field} = :{field}")
            params[field] = val
    # Budgets are tri-state: set a number, clear back to the deployed default,
    # or leave alone. A plain None has to mean "leave alone" or every partial
    # update would wipe them.
    if body.clear_call_budget:
        sets.append("daily_call_budget = NULL")
    elif body.daily_call_budget is not None:
        sets.append("daily_call_budget = :dcb")
        params["dcb"] = max(0, int(body.daily_call_budget))
    if body.clear_token_budget:
        sets.append("daily_output_token_budget = NULL")
    elif body.daily_output_token_budget is not None:
        sets.append("daily_output_token_budget = :dtb")
        params["dtb"] = max(0, int(body.daily_output_token_budget))

    if not sets:
        raise HTTPException(status_code=400, detail="no fields to update")
    await db.execute(text(
        f"UPDATE nl_query_config SET {', '.join(sets)}, updated_at = now() WHERE id = 1"),
        params)
    await db.commit()
    # Drop the read cache so the change is live now, not in up to 30 seconds.
    nl_query.invalidate_config_cache()
    logger.info("admin %s updated nl_query config: %s", user.email, sorted(params) + [
        s for s in sets if s.endswith("NULL")])
    return await get_cfg(request, user=user, db=db)


class TryBody(BaseModel):
    q: str
    # Default False: a benchmark that reads the cache measures the cache, not
    # the models — the second run would score a broken model perfectly.
    use_cache: bool = False
    run_sql: bool = False


@router.post("/try")
async def try_question(
    request: Request,
    body: TryBody,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Run one question with full detail, bypassing the cache. Admin-only.

    This is the benchmark's entry point (scripts/nl_benchmark.py). It exists
    separately from the public endpoint for two reasons: cache bypass would be
    an unauthenticated way to force paid calls, and a benchmark needs the stage,
    the attempt chain and the token counts that the public response omits.

    Budget is still charged — a benchmark run costs real money and should show
    up in the same ceilings as real traffic. Benchmark runs are deliberately NOT
    written to nl_query_log, so the admin log stays a record of what actual
    users asked."""
    import time as _t

    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")
    cfg = await nl_query.get_config(db)
    started = _t.monotonic()

    async def _reserve() -> bool:
        from app.services.llm_budget import reserve_llm_call
        return await reserve_llm_call(
            db, call_budget=cfg.get("daily_call_budget"),
            token_budget_override=cfg.get("daily_output_token_budget"))

    async def _usage(i: int, o: int) -> None:
        from app.services.llm_budget import record_llm_tokens
        await record_llm_tokens(db, i, o)

    out: dict = {"question": q}
    try:
        res = await nl_query.answer(db, q, use_cache=body.use_cache,
                                    reserve=_reserve, on_usage=_usage)
    except nl_query.OutOfScope as e:
        out.update({"answered": False, "stage": "refused", "reason": e.message,
                    "attempts": e.attempts, "input_tokens": e.usage[0],
                    "output_tokens": e.usage[1],
                    "candidates": [c["key"] for c in e.candidates[:5]]})
    except Exception as e:  # noqa: BLE001 — a benchmark must record failures, not raise
        out.update({"answered": False, "stage": type(e).__name__, "reason": str(e),
                    "attempts": [], "input_tokens": 0, "output_tokens": 0})
    else:
        usage = res.get("usage") or (0, 0)
        out.update({
            "answered": True, "stage": res.get("stage") or res["source"],
            "attempts": res.get("attempts") or [], "model": res.get("model"),
            "escalated": bool(res.get("escalated")), "entity": res["entity"],
            "query": res["query"], "sql": res["sql"],
            "explanation": res["explanation"],
            "input_tokens": usage[0], "output_tokens": usage[1],
        })
        if body.run_sql:
            # Compiling is not the same as returning rows: a query can validate,
            # compile, and still error or return nothing against real data.
            from app.services import append_store, data_catalog
            try:
                r = await append_store.run_readonly_sql(
                    res["sql"], search_path=data_catalog.CONSOLE_SEARCH_PATH)
                out["rows"] = r["row_count"]
            except Exception as e:  # noqa: BLE001
                out["sql_error"] = str(e)
    out["duration_ms"] = int((_t.monotonic() - started) * 1000)
    return out


@router.post("/prune")
async def prune_log(
    request: Request,
    days: int = 90,
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete log rows older than ``days``. The log holds user-authored text, so
    it should not be kept forever; this is the manual control."""
    days = max(1, min(days, 3650))
    res = await db.execute(text(
        "DELETE FROM nl_query_log WHERE created_at < now() - make_interval(days => :days)"),
        {"days": days})
    await db.commit()
    return {"deleted": res.rowcount or 0, "older_than_days": days}
