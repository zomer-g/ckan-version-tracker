"""Public API of the Knesset ODATA mirror — the /knesset page's backend.

All read endpoints are public (the data is parliamentary public record); the
SQL path has the same defense-in-depth as /api/append/{id}/sql (READ ONLY tx,
single SELECT, statement_timeout, row cap) and /api/knesset-db is metered by
the per-IP data budget middleware. Sync management is admin-only.

Endpoints:
  GET  /api/knesset-db/status      → compact sync stats (page header)
  GET  /api/knesset-db/tables      → all tables + schema + Hebrew descriptions
  POST /api/knesset-db/sql         → {sql} run read-only, ≤1000 rows
  GET  /api/knesset-db/export.csv  → ?sql=… streamed CSV (≤200k rows)
  POST /api/knesset-db/sync        → admin: kick a sync pass now (optional table/reset)
"""
import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.dependencies import get_admin_user
from app.config import settings
from app.rate_limit import limiter
from app.services import knesset_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knesset-db", tags=["knesset-db"])


class SqlBody(BaseModel):
    sql: str


class SyncBody(BaseModel):
    table: str | None = None   # limit the pass to one table
    reset: bool = False        # force a fresh full walk of that table


def _require_enabled() -> None:
    if not knesset_db.is_configured():
        raise HTTPException(status_code=409, detail="Knesset DB mirror is not configured")


@router.get("/status")
async def status():
    return await knesset_db.status_summary()


@router.get("/tables")
@limiter.limit("30/minute")
async def tables(request: Request):
    _require_enabled()
    try:
        return {"tables": await knesset_db.list_tables()}
    except Exception as e:  # noqa: BLE001 — surface init errors readably
        logger.exception("knesset-db /tables failed")
        raise HTTPException(status_code=503, detail=f"{type(e).__name__}: {e}")


@router.post("/sql")
@limiter.limit("20/minute")
async def sql(request: Request, body: SqlBody):
    _require_enabled()
    try:
        return await knesset_db.run_sql(body.sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors go to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


@router.get("/export.csv")
@limiter.limit("6/minute")
async def export_csv(request: Request, sql: str):
    _require_enabled()
    try:
        stream = knesset_db.iter_sql_csv(sql)
        # Pull the first chunk eagerly so validation/SQL errors become a clean
        # 400 instead of a broken download.
        first = await anext(stream)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StopAsyncIteration:
        first = "﻿\r\n"
        async def _empty():
            return
            yield  # pragma: no cover
        stream = _empty()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")

    async def _chain():
        yield first
        async for chunk in stream:
            yield chunk

    return StreamingResponse(
        _chain(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="knesset_query.csv"'},
    )


@router.post("/sync")
async def sync(body: SyncBody, admin=Depends(get_admin_user)):
    """Kick a sync pass in the background right now (admin). With reset=true the
    named table is re-walked from scratch (rows re-upsert; nothing is lost)."""
    _require_enabled()
    if body.reset:
        if not body.table:
            raise HTTPException(status_code=400, detail="reset requires a table name")
        await knesset_db.reset_table(body.table)

    async def _run():
        try:
            res = await knesset_db.sync_tick(
                budget_seconds=settings.knesset_db_tick_budget_seconds,
                sync_interval_hours=settings.knesset_db_sync_interval_hours,
                only_table=body.table,
            )
            logger.info("knesset-db manual sync: %s", res)
        except Exception:  # noqa: BLE001
            logger.exception("knesset-db manual sync failed")

    asyncio.create_task(_run())
    return {"started": True, "table": body.table, "reset": body.reset}
