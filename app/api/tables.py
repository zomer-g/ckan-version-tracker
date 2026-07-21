"""Central /data SQL console API — the whole-site table catalog + free SQL.

Public + read-only. Backs the frontend DataSqlPage:
  GET  /api/tables                 → the unified table catalog (browser + autocomplete)
  GET  /api/tables/{table}/detail  → one table's cube (sample rows, source, files, count)
  POST /api/tables/sql             → run a read-only SELECT over public + knesset
  GET  /api/tables/schema.txt      → DDL text for copy-to-AI (optional ?table=)
  GET  /api/tables/export.csv      → ?sql=… streamed full CSV (≤200k rows)

The SQL/CSV paths reuse append_store's least-privilege read-only role and its
defense-in-depth guards (single SELECT/WITH, denylist, READ ONLY tx,
statement_timeout, row cap); the only addition here is a fixed
``search_path = public, knesset`` so tables of both schemas resolve unqualified.
Table names in /detail are validated against the live catalog (the security gate).
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import append_store, data_catalog

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tables", tags=["tables"])


class SqlBody(BaseModel):
    sql: str


def _require_enabled() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")


@router.get("")
@limiter.limit("30/minute")
async def catalog(request: Request, db: AsyncSession = Depends(get_db)):
    _require_enabled()
    try:
        return {"tables": await data_catalog.build_catalog(db)}
    except Exception as e:  # noqa: BLE001 — surface init errors readably
        logger.exception("/api/tables catalog failed")
        raise HTTPException(status_code=503, detail=f"{type(e).__name__}: {e}")


@router.get("/{table}/detail")
@limiter.limit("30/minute")
async def table_detail(table: str, request: Request, db: AsyncSession = Depends(get_db)):
    _require_enabled()
    detail = await data_catalog.table_detail(table, db)
    if detail is None:
        raise HTTPException(status_code=404, detail="Unknown table")
    return detail


@router.post("/sql")
@limiter.limit("20/minute")
async def run_sql(request: Request, body: SqlBody):
    """Read-only SELECT over the append DB, spanning the public (dataset) and
    knesset schemas (search_path = public, knesset)."""
    _require_enabled()
    try:
        return await append_store.run_readonly_sql(
            body.sql, search_path=data_catalog.CONSOLE_SEARCH_PATH
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors go to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


@router.get("/schema.txt", response_class=PlainTextResponse)
@limiter.limit("20/minute")
async def schema_txt(request: Request, table: str | None = None,
                     db: AsyncSession = Depends(get_db)):
    """DESCRIBE-style DDL for copy-to-AI. With ?table= → that one table's DDL
    (dataset table via append_store, knesset table via the knesset schema dump);
    without it → the whole knesset schema plus a pointer note (the full public
    catalog can be hundreds of tables — pick one from the browser)."""
    _require_enabled()
    if table:
        rec = next((r for r in await data_catalog.build_catalog(db)
                    if r["table"] == table), None)
        if rec is None:
            raise HTTPException(status_code=404, detail="Unknown table")
        if rec["kind"] == "knesset":
            from app.services import knesset_db
            return await knesset_db.schema_text()
        return await append_store.schema_text(table, title=rec.get("title"))
    from app.services import knesset_db
    if knesset_db.is_configured():
        return await knesset_db.schema_text()
    return "-- בחרו טבלה מהדפדפן כדי להעתיק את הסכימה שלה.\n"


@router.get("/export.csv")
@limiter.limit("6/minute")
async def export_csv(request: Request, sql: str):
    """Run the SQL on the server and stream the full result (≤200k rows) as CSV
    over both schemas. First chunk is pulled eagerly so validation/SQL errors
    become a clean 400 instead of a broken download."""
    _require_enabled()
    try:
        stream = append_store.iter_sql_csv(
            sql, search_path=data_catalog.CONSOLE_SEARCH_PATH
        )
        first = await anext(stream)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StopAsyncIteration:
        first = "﻿\r\n".encode("utf-8")

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
        headers={"Content-Disposition": 'attachment; filename="over_query.csv"'},
    )
