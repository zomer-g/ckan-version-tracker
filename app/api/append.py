"""Public read API over the append archive (the per-dataset Postgres tables).

Lets anyone browse and pull the accumulated rows of a data.gov.il datastore
dataset that OVER archives append-only. Read-only and public — the whole point
is open access — but every column name is validated against the live schema and
every filter value is parameterized (see app/services/append_store.py).

Endpoints (all under /api/append):
  GET /{dataset_id}/schema           → {dataset_title, table, total, columns, key}
  GET /{dataset_id}/rows?…           → {columns, rows, total, limit, offset, sort, order}
  GET /{dataset_id}/download.csv?…   → streaming CSV of the (filtered) table

Filtering on rows/download: ``q`` does a free-text ILIKE across all columns;
any query param whose name is a real column does a per-column ILIKE. Reserved
params: limit, offset, sort, order, q.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import parse_uuid
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services import append_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/append", tags=["append"])


class SqlBody(BaseModel):
    sql: str

_RESERVED = {"limit", "offset", "sort", "order", "q"}


async def _resolve(dataset_id: str, db: AsyncSession) -> tuple[TrackedDataset, str]:
    """Return (dataset, append_table) or raise 404/409.

    The table name is read from the dataset's most recent ``append_db`` version
    (resource_mappings.append_table); falls back to the deterministic
    table_name(ds). 409 if this dataset isn't an append-DB dataset / the feature
    is off."""
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")
    uid = parse_uuid(dataset_id, "dataset_id")
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    table: str | None = None
    rows = (await db.execute(
        select(VersionIndex.resource_mappings)
        .where(VersionIndex.tracked_dataset_id == uid)
        .order_by(VersionIndex.version_number.desc())
    )).all()
    for (mappings,) in rows:
        if mappings and mappings.get("append_table"):
            table = mappings["append_table"]
            break
    if not table:
        if ds.storage_mode != "append_only":
            raise HTTPException(status_code=409, detail="Dataset is not an append archive")
        table = append_store.table_name(ds)
    return ds, table


def _filters_from(request: Request, exclude: set[str]) -> dict[str, str]:
    """Per-column filters = every non-reserved query param. (append_store.query
    drops any that aren't real columns, so unknown params are simply ignored.)"""
    return {
        k: v for k, v in request.query_params.items()
        if k not in _RESERVED and k not in exclude
    }


@router.get("/{dataset_id}/schema")
async def archive_schema(dataset_id: str, db: AsyncSession = Depends(get_db)):
    ds, table = await _resolve(dataset_id, db)
    cols = await append_store.user_columns(table)
    if not cols:
        raise HTTPException(status_code=404, detail="No archived rows yet for this dataset")
    total = await append_store.table_count(table)
    return {
        "dataset_id": str(ds.id),
        "dataset_title": ds.title,
        "table": table,
        "total": total,
        "columns": cols,
        "key": (ds.scraper_config or {}).get("append_key"),
        "first_seen_column": "first_seen",
    }


@router.get("/{dataset_id}/rows")
async def archive_rows(
    dataset_id: str,
    request: Request,
    limit: int = 50,
    offset: int = 0,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    _, table = await _resolve(dataset_id, db)
    return await append_store.query(
        table,
        limit=limit, offset=offset, sort=sort, order=order, q=q,
        filters=_filters_from(request, exclude=set()),
    )


@router.get("/{dataset_id}/download.csv")
async def archive_download(
    dataset_id: str,
    request: Request,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    ds, table = await _resolve(dataset_id, db)
    filters = _filters_from(request, exclude=set())
    safe = (ds.ckan_name or "archive").replace("/", "_")[:60]
    stream = append_store.iter_csv(table, sort=sort, order=order, q=q, filters=filters)
    return StreamingResponse(
        stream,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{safe}_append.csv"'},
    )


@router.post("/{dataset_id}/sql")
@limiter.limit("20/minute")
async def archive_sql(
    dataset_id: str,
    request: Request,
    body: SqlBody,
    db: AsyncSession = Depends(get_db),
):
    """Run a user-supplied read-only SELECT against the append DB. Guarded by a
    READ ONLY transaction + statement_timeout + row cap (see
    append_store.run_readonly_sql). The dataset's table name is in /schema so
    the client can reference it. Errors (validation, SQL syntax, timeout) come
    back as 400 with the message."""
    await _resolve(dataset_id, db)  # 404/409 if not an append dataset
    try:
        return await append_store.run_readonly_sql(body.sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — surface SQL/timeout errors to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")
