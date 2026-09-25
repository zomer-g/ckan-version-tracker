"""Admin statistics over the API access log (app.api_access_log).

  GET /api/admin/api-access/stats   — totals, time series and breakdowns
  GET /api/admin/api-access/recent  — the raw rows, newest first

Both take the same filters. ``exclude_site`` (on by default) drops calls our own
frontend makes, which otherwise outnumber everything external by far.
"""

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.user import User
from app.rate_limit import limiter

router = APIRouter(prefix="/api/admin/api-access", tags=["admin-api-access"])

TZ = "Asia/Jerusalem"
# users lives in schema `auth` (065), outside the app search_path: name it in full.
_USERS = User.__table__.fullname


def _where(
    days: int, exclude_site: bool, area: str | None, channel: str | None,
    actor_kind: str | None, ip: str | None, actor_id: str | None, client: str | None,
    status: str | None, route: str | None, target: str | None,
) -> tuple[str, dict]:
    clauses = ["l.ts >= now() - make_interval(days => :days)"]
    params: dict = {"days": days}
    if exclude_site and not channel:
        clauses.append("l.channel <> 'site'")
    for col, val in (("area", area), ("channel", channel), ("actor_kind", actor_kind),
                     ("ip", ip), ("actor_id", actor_id), ("client", client),
                     ("route", route), ("target", target)):
        if val:
            clauses.append(f"l.{col} = :{col}")
            params[col] = val
    if status == "ok":
        clauses.append("l.status < 400")
    elif status == "4xx":
        clauses.append("l.status BETWEEN 400 AND 499")
    elif status == "5xx":
        clauses.append("l.status >= 500")
    elif status == "429":
        clauses.append("l.status = 429")
    return " AND ".join(clauses), params


def _filters(
    days: int = Query(7, ge=1, le=366),
    exclude_site: bool = True,
    area: str | None = None,
    channel: str | None = None,
    actor_kind: str | None = None,
    ip: str | None = None,
    actor_id: str | None = None,
    client: str | None = None,
    status: str | None = None,
    route: str | None = None,
    target: str | None = None,
) -> dict:
    return dict(days=days, exclude_site=exclude_site, area=area, channel=channel,
                actor_kind=actor_kind, ip=ip, actor_id=actor_id, client=client,
                status=status, route=route, target=target)


async def _rows(db: AsyncSession, sql: str, params: dict) -> list[dict]:
    res = await db.execute(text(sql), params)
    return [dict(r._mapping) for r in res]


def _iso(rows: list[dict], *keys: str) -> list[dict]:
    for r in rows:
        for k in keys:
            if r.get(k) is not None and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
    return rows


@router.get("/stats")
@limiter.limit("30/minute")
async def api_access_stats(
    request: Request,
    f: dict = Depends(_filters),
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    where, p = _where(**f)
    frm = f"FROM api_access_log l WHERE {where}"

    totals = (await _rows(db, f"""
        SELECT count(*) AS requests,
               count(DISTINCT l.ip) AS unique_ips,
               count(DISTINCT l.actor_id) AS unique_actors,
               coalesce(sum(l.bytes_out), 0) AS bytes,
               count(*) FILTER (WHERE l.status >= 400) AS errors,
               count(*) FILTER (WHERE l.status = 429) AS throttled,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY l.duration_ms) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY l.duration_ms) AS p95_ms,
               min(l.ts) AS first_ts, max(l.ts) AS last_ts
        {frm}""", p))[0]
    _iso([totals], "first_ts", "last_ts")

    bucket = "hour" if f["days"] <= 3 else "day"
    series = _iso(await _rows(db, f"""
        SELECT date_trunc('{bucket}', l.ts AT TIME ZONE '{TZ}') AS t,
               count(*) AS requests, count(DISTINCT l.ip) AS ips,
               coalesce(sum(l.bytes_out), 0) AS bytes,
               count(*) FILTER (WHERE l.status >= 400) AS errors
        {frm} GROUP BY 1 ORDER BY 1""", p), "t")

    async def by(col: str, limit: int = 25, where_sql: str = where, params: dict = p) -> list[dict]:
        return await _rows(db, f"""
            SELECT {col} AS key, count(*) AS requests, count(DISTINCT l.ip) AS ips,
                   coalesce(sum(l.bytes_out), 0) AS bytes,
                   count(*) FILTER (WHERE l.status >= 400) AS errors,
                   round(avg(l.duration_ms)) AS avg_ms
            FROM api_access_log l WHERE {where_sql}
            GROUP BY 1 ORDER BY requests DESC LIMIT {int(limit)}""", params)

    # The channel split ignores exclude_site, so the site's share stays visible.
    ch_where, ch_p = _where(**{**f, "exclude_site": False})

    routes = await _rows(db, f"""
        SELECT l.area, l.method, coalesce(l.route, l.path) AS route,
               count(*) AS requests, count(DISTINCT l.ip) AS ips,
               coalesce(sum(l.bytes_out), 0) AS bytes,
               count(*) FILTER (WHERE l.status >= 400) AS errors,
               round(avg(l.duration_ms)) AS avg_ms
        {frm} GROUP BY 1, 2, 3 ORDER BY requests DESC LIMIT 40""", p)

    top_ips = _iso(await _rows(db, f"""
        SELECT l.ip, count(*) AS requests, coalesce(sum(l.bytes_out), 0) AS bytes,
               count(*) FILTER (WHERE l.status >= 400) AS errors,
               count(DISTINCT coalesce(l.route, l.path)) AS routes,
               mode() WITHIN GROUP (ORDER BY l.client) AS client,
               mode() WITHIN GROUP (ORDER BY l.channel) AS channel,
               mode() WITHIN GROUP (ORDER BY l.country) AS country,
               mode() WITHIN GROUP (ORDER BY l.area) AS top_area,
               max(l.actor_label) AS actor_label,
               min(l.ts) AS first_ts, max(l.ts) AS last_ts
        {frm} AND l.ip IS NOT NULL GROUP BY l.ip ORDER BY requests DESC LIMIT 30""", p),
        "first_ts", "last_ts")

    top_actors = _iso(await _rows(db, f"""
        SELECT l.actor_kind, l.actor_id,
               coalesce(max(u.email), max(l.actor_label)) AS label,
               count(*) AS requests, coalesce(sum(l.bytes_out), 0) AS bytes,
               count(DISTINCT l.ip) AS ips,
               mode() WITHIN GROUP (ORDER BY l.area) AS top_area,
               max(l.ts) AS last_ts
        FROM api_access_log l
        LEFT JOIN {_USERS} u ON l.actor_kind = 'user' AND u.id::text = l.actor_id
        WHERE {where} AND l.actor_kind <> 'anonymous'
        GROUP BY 1, 2 ORDER BY requests DESC LIMIT 30""", p), "last_ts")

    heatmap = await _rows(db, f"""
        SELECT extract(isodow FROM l.ts AT TIME ZONE '{TZ}')::int AS dow,
               extract(hour FROM l.ts AT TIME ZONE '{TZ}')::int AS hour,
               count(*) AS requests
        {frm} GROUP BY 1, 2""", p)

    return {
        "filters": f,
        "bucket": bucket,
        "retention_days": settings.api_access_log_retention_days,
        "totals": totals,
        "series": series,
        "by_area": await by("l.area"),
        "by_channel": await by("l.channel", where_sql=ch_where, params=ch_p),
        "by_client": await by("coalesce(l.client, '(ריק)')"),
        "by_actor_kind": await by("l.actor_kind"),
        "by_country": await by("coalesce(l.country, '?')"),
        "by_status": await by("l.status::text", limit=20),
        "by_target": await by("l.area || ' · ' || l.target", where_sql=f"{where} AND l.target IS NOT NULL"),
        "by_referer": await by("l.referer_host", where_sql=f"{where} AND l.referer_host IS NOT NULL"),
        "routes": routes,
        "top_ips": top_ips,
        "top_actors": top_actors,
        "heatmap": heatmap,
    }


@router.get("/recent")
@limiter.limit("60/minute")
async def api_access_recent(
    request: Request,
    f: dict = Depends(_filters),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    where, p = _where(**f)
    rows = await _rows(db, f"""
        SELECT l.id, l.ts, l.method, l.path, l.route, l.area, l.query, l.target, l.status,
               l.duration_ms, l.bytes_out, l.ip, l.country, l.user_agent, l.client,
               l.channel, l.actor_kind, l.actor_id,
               coalesce(u.email, l.actor_label) AS actor_label, l.referer_host
        FROM api_access_log l
        LEFT JOIN {_USERS} u ON l.actor_kind = 'user' AND u.id::text = l.actor_id
        WHERE {where}
        ORDER BY l.ts DESC LIMIT :limit OFFSET :offset""", {**p, "limit": limit, "offset": offset})
    for r in rows:
        r["ts"] = r["ts"].isoformat()
    return {"rows": rows, "limit": limit, "offset": offset}
