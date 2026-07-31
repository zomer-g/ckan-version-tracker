"""Public read API over the append archive (the per-dataset Postgres tables).

Lets anyone browse and pull the accumulated rows of a data.gov.il datastore
dataset that OVER archives append-only. Read-only and public — the whole point
is open access — but every column name is validated against the live schema and
every filter value is parameterized (see app/services/append_store.py).

Endpoints (all under /api/append):
  GET /{dataset_id}/schema           → {dataset_title, table, tables, total, columns, key}
  GET /{dataset_id}/rows?…           → {columns, rows, total, limit, offset, sort, order}
  GET /{dataset_id}/item?value=…     → every sample of ONE item, newest first
  GET /{dataset_id}/download.csv?…   → streaming CSV of the (filtered) table

Filtering on rows/download: ``q`` does a free-text ILIKE across all columns;
any query param whose name is a real column does a per-column ILIKE. Reserved
params: limit, offset, sort, order, q, latest, table/resource/resource_id.

SAMPLING ARCHIVES. Some sources are sampled repeatedly rather than published
whole: the Jerusalem building-licensing register is read file by file, and each
reading appends a row stamped with when it was taken. Such a table holds SEVERAL
rows per real-world item, which is the point (the history of a plan is the
sequence of its samples) and also a trap for a consumer who reads rows as items.
Two params make both readings available:

  ``?latest=true``     one row per item — the newest sample of each. This is
                       "the register as it stands now", and filters apply to
                       that current state.
  ``/item?value=…``    the opposite end: every sample of one item, in order.

Which column identifies an item and which carries the sample time are declared
by the source and published in /schema (``item_key`` / ``sample_column``), so a
client can discover the shape instead of assuming it.

MULTI-RESOURCE DATASETS. A dataset can hold one NEON table PER RESOURCE — a
CKAN dataset archived as ``append_db_multi`` (one per datastore resource) or a
scraper dataset that publishes several tabular resources per version (one per
resource name; ykpubdata's building register and its documents corpus are
different grains with different columns). These endpoints are single-table by
nature, so each accepts ``?table=`` — a physical table name, a resource id, or a
resource name — and defaults to the dataset's first resource. ``/schema`` lists
them all (and ``/schema.txt`` emits every table's DDL when unselected), which is
the only way a consumer learns there is more than one.
"""
import json as _json
import logging
import re
from urllib.parse import quote

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.utils import MAX_API_OFFSET, parse_uuid
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter
from app.services import append_store, append_tables

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/append", tags=["append"])


class SqlBody(BaseModel):
    sql: str

# `table` and its aliases select ONE table of a multi-resource dataset. Reserved
# like limit/offset/sort/order/q so /rows and /download.csv never mistake the
# selector for a per-column filter.
_TABLE_PARAMS = ("table", "resource", "resource_id")
_RESERVED = {"limit", "offset", "sort", "order", "q", "latest", *_TABLE_PARAMS}


def item_spec(ds) -> tuple[str | None, str | None]:
    """``(item_key, sample_column)`` for a dataset, or (None, None).

    An archive that appends a row per SAMPLING holds several rows per real-world
    item — one per time it was looked at. Two things are then needed to read it
    as items rather than as rows: which column identifies an item, and which
    column says when a row was sampled. Both are declared by the source (see
    app/services/sampling_runs.py); the item key falls back to the keys a
    dataset may already carry for other reasons, and the sample column falls
    back to ``first_seen``, which is when OVER first stored the row."""
    from app.services import sampling_runs

    sc = ds.scraper_config or {}
    # Resolved through sampling_runs so the source's MANIFEST answers for a
    # dataset created before it declared any of this — otherwise ?latest=true
    # would be available on new datasets and quietly missing on the ones that
    # have the history worth reading.
    spec = sampling_runs.sampling_spec(ds) or {}
    key = spec.get("item_key") or sc.get("dedup_key") or sc.get("append_key")
    return key, spec.get("sample_column")


def _latest_key(ds, latest: bool) -> tuple[str | None, str | None]:
    """Resolve ``?latest=`` into (latest_key, latest_order), or 400.

    Refusing when the dataset declares no item key is deliberate: silently
    serving the full history for ``latest=true`` would answer a different
    question than the one asked, and the caller would have no way to tell."""
    key, sample_col = item_spec(ds)
    if not latest:
        return None, None
    if not key:
        raise HTTPException(
            status_code=400,
            detail=("latest=true needs an item key, and this dataset declares "
                    "none (scraper_config.sampling.item_key)"),
        )
    return key, sample_col


def _pick(tables: list[dict], selector: str | None) -> dict:
    """The table a request is addressing.

    No selector ⇒ the first, which for a multi-resource dataset is the first
    resource as registered (see append_store.tables_from_mappings on why the
    order is not the JSONB key order). A selector matches a physical table name,
    a resource id, or a resource name — one param, three ways to name the same
    thing, because callers arrive from /schema, from CKAN habits, or from the UI.

    An unmatched selector is a 404 that LISTS what exists: the alternative is
    silently serving a different table than the one asked for, which is how this
    whole class of bug goes unnoticed."""
    if not selector:
        return tables[0]
    sel = selector.strip()
    for t in tables:
        if sel in (t["table"], t.get("resource_id"), t.get("resource_name")):
            return t
    raise HTTPException(
        status_code=404,
        detail={
            "error": f"No table {sel!r} in this dataset",
            "available": [
                {k: t.get(k) for k in ("table", "resource_id", "resource_name")}
                for t in tables
            ],
        },
    )


async def _resolve(dataset_id: str, db: AsyncSession,
                   selector: str | None = None) -> tuple[TrackedDataset, str, list[dict]]:
    """Return (dataset, chosen_table, all_tables) or raise 404/409.

    Tables come from the dataset's most recent version that carries a NEON
    mapping — ``append_table`` for a single-table dataset, ``_append_tables`` for
    a multi-resource one (``append_db_multi``) — and fall back to the
    deterministic table_name(ds). 409 if this dataset isn't an append-DB dataset
    / the feature is off.

    Reading ONLY ``append_table`` here is what made all 7 multi-resource datasets
    serve as empty across all 7 endpoints; resolution now goes through the shared
    append_store.tables_from_mappings."""
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")
    uid = parse_uuid(dataset_id, "dataset_id")
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if not ds:
        raise HTTPException(status_code=404, detail="Dataset not found")

    has_mapping = any(
        m and (m.get("append_table") or m.get("_append_tables"))
        for (m,) in (await db.execute(
            select(VersionIndex.resource_mappings)
            .where(VersionIndex.tracked_dataset_id == uid)
        )).all()
    )
    # No NEON mapping on any version. Still resolvable when this dataset archives
    # to NEON — either a classic append_only dataset, or a full-snapshot dataset
    # opted into the r2+neon plan (archive_neon) and seeded retroactively before
    # its first forward dual-write version exists.
    if not has_mapping and not append_tables.is_append_archive(ds):
        raise HTTPException(status_code=409, detail="Dataset is not an append archive")
    tables = await append_tables.resolve_tables(ds, db)
    return ds, _pick(tables, selector)["table"], tables


def _selector(request: Request, table: str | None) -> str | None:
    """The table selector, from the documented ``table`` param or its aliases."""
    if table:
        return table
    for k in _TABLE_PARAMS[1:]:
        v = request.query_params.get(k)
        if v:
            return v
    return None


def _content_disposition(name: str) -> str:
    """``Content-Disposition`` for a download whose name may be non-ASCII.

    HTTP header values must be latin-1 encodable, so a Hebrew name in a bare
    ``filename=`` raises UnicodeEncodeError inside the server and the response
    500s. Measured, not theoretical: naming multi-table CSVs after their resource
    ("סניפים - עברית") turned every one of those downloads into a 500, while the
    single-table path was fine because a ckan_name is always a slug.

    So: an ASCII-only ``filename`` that always works, plus RFC 5987
    ``filename*`` carrying the real name for anything from the last decade. The
    ASCII fallback is restricted to a safe character set rather than merely
    transliterated — it lands in a quoted header string, where a stray quote or
    newline would be header injection."""
    ascii_name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "archive"
    return (f"attachment; filename=\"{ascii_name[:80]}\"; "
            f"filename*=UTF-8''{quote(name, safe='')}")


def _filters_from(request: Request, exclude: set[str]) -> dict[str, str]:
    """Per-column filters = every non-reserved query param. (append_store.query
    drops any that aren't real columns, so unknown params are simply ignored.)"""
    return {
        k: v for k, v in request.query_params.items()
        if k not in _RESERVED and k not in exclude
    }


@router.get("/{dataset_id}/schema")
# Public + heavy: table_count is a COUNT(*) over the whole archive table, which
# on the giant append datasets (e.g. the 4.1M-row vehicle registry) is a real
# scan. Matches its /schema.txt sibling's ceiling.
@limiter.limit("20/minute")
async def archive_schema(dataset_id: str, request: Request,
                         table: str | None = None,
                         db: AsyncSession = Depends(get_db)):
    """Schema of one archive table.

    ``tables`` lists EVERY table of the dataset, so a consumer can discover that
    a multi-resource dataset has more than one at all — without it, the only
    honest reading of this response is "the dataset is this one table"."""
    ds, tbl, tables = await _resolve(dataset_id, db, _selector(request, table))
    cols = await append_store.user_columns(tbl)
    if not cols:
        raise HTTPException(status_code=404, detail="No archived rows yet for this dataset")
    total = await append_store.table_count(tbl)
    chosen = next((t for t in tables if t["table"] == tbl), {})
    return {
        "dataset_id": str(ds.id),
        "dataset_title": ds.title,
        "table": tbl,
        "resource_id": chosen.get("resource_id"),
        "resource_name": chosen.get("resource_name"),
        # No per-table COUNT(*) here: this endpoint's single count is already a
        # full scan on the big archives (the 4.1M-row vehicle registry), and
        # multiplying it by the table count is how a discovery call turns into a
        # timeout. Ask /schema?table=… for another table's total.
        "tables": [
            {k: t.get(k) for k in ("table", "resource_id", "resource_name")}
            for t in tables
        ],
        "multi_table": len(tables) > 1,
        "total": total,
        "columns": cols,
        "key": (ds.scraper_config or {}).get("append_key"),
        "capture_changes": bool((ds.scraper_config or {}).get("capture_changes")),
        "first_seen_column": "first_seen",
        # How to read this table as ITEMS rather than rows. Present (non-null
        # item_key) means the archive holds a sampling history: several rows per
        # item, one per time it was sampled. Without publishing these a consumer
        # can't tell that shape apart from an ordinary growing table, and would
        # read N rows for one building file as N buildings.
        "item_key": item_spec(ds)[0],
        "sample_column": item_spec(ds)[1] or "first_seen",
        "supports_latest": bool(item_spec(ds)[0]),
    }


@router.get("/{dataset_id}/schema.txt", response_class=PlainTextResponse)
@limiter.limit("20/minute")
async def archive_schema_txt(dataset_id: str, request: Request,
                             table: str | None = None,
                             db: AsyncSession = Depends(get_db)):
    """DESCRIBE-style DDL of this dataset's archive table(s) as plain text — for
    pasting into an LLM ('copy schema for AI').

    With no ``table`` selector, a multi-resource dataset returns the DDL of ALL
    its tables. Handing an LLM one table out of several is worse than useless:
    it will confidently write queries against the half it was shown."""
    sel = _selector(request, table)
    ds, tbl, tables = await _resolve(dataset_id, db, sel)
    wanted = tables if (sel is None and len(tables) > 1) else [
        next(t for t in tables if t["table"] == tbl)]
    out: list[str] = []
    for t in wanted:
        if await append_store.user_columns(t["table"]):
            label = ds.title
            if t.get("resource_name"):
                label = f"{ds.title} — {t['resource_name']}"
            out.append(await append_store.schema_text(t["table"], title=label))
    if not out:
        raise HTTPException(status_code=404, detail="No archived rows yet for this dataset")
    return "\n\n".join(out)


@router.get("/{dataset_id}/rows")
@limiter.limit("60/minute")
async def archive_rows(
    dataset_id: str,
    request: Request,
    limit: int = 50,
    offset: int = Query(0, ge=0, le=MAX_API_OFFSET),
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    table: str | None = None,
    latest: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """One page of the archive.

    ``latest=true`` collapses it to the NEWEST sample per item — the register as
    it stands now, one row per file — instead of every sample ever taken. Any
    filter then applies to that current state (``?latest=true&סטטוס=…`` is "the
    files at this status now", not "ever"). Without it you get the full sampling
    history, which is what this archive is for."""
    ds, tbl, _tables = await _resolve(dataset_id, db, _selector(request, table))
    latest_key, latest_order = _latest_key(ds, latest)
    return await append_store.query(
        tbl,
        limit=limit, offset=offset, sort=sort, order=order, q=q,
        filters=_filters_from(request, exclude=set()),
        latest_key=latest_key, latest_order=latest_order,
    )


@router.get("/{dataset_id}/item")
@limiter.limit("60/minute")
async def archive_item_history(
    dataset_id: str,
    request: Request,
    value: str,
    limit: int = 200,
    offset: int = Query(0, ge=0, le=MAX_API_OFFSET),
    order: str = "desc",
    table: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Every sample of ONE item, oldest or newest first — the history of a single
    building file, plan or record.

    ``value`` is an EXACT match on the dataset's item key (``מספר תיק`` for the
    Jerusalem register), not the substring match the per-column filters do: a
    file number is an identifier, and ``2024/012`` must not quietly return
    ``2024/0120.00`` as well.

    The response says which column was matched and how many samples exist, so a
    consumer can page without guessing the schema.
    """
    ds, tbl, _tables = await _resolve(dataset_id, db, _selector(request, table))
    key, sample_col = item_spec(ds)
    if not key:
        raise HTTPException(
            status_code=400,
            detail=("This dataset declares no item key "
                    "(scraper_config.sampling.item_key)"),
        )
    cols = await append_store.user_columns(tbl)
    if key not in cols:
        raise HTTPException(
            status_code=404,
            detail={"error": f"No column {key!r} in this table",
                    "table": tbl, "columns": cols},
        )
    sort_col = sample_col if (sample_col and sample_col in cols) else "first_seen"
    res = await append_store.datastore_search(
        tbl, filters={key: value}, limit=limit, offset=offset,
        sort=f"{sort_col} {'desc' if str(order).lower() != 'asc' else 'asc'}",
    )
    if res is None:
        raise HTTPException(status_code=404, detail="No archived rows yet for this dataset")
    return {
        "dataset_id": str(ds.id),
        "dataset_title": ds.title,
        "table": tbl,
        "item_key": key,
        "item": value,
        "sample_column": sort_col,
        "samples": res["total"],
        "rows": res["records"],
        "limit": res["limit"],
        "offset": res["offset"],
    }


@router.get("/{dataset_id}/download.csv")
@limiter.limit("6/minute")
async def archive_download(
    dataset_id: str,
    request: Request,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    table: str | None = None,
    latest: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """The (filtered) archive as CSV. ``latest=true`` exports one row per item —
    the newest sample of each — instead of the whole sampling history."""
    ds, tbl, tables = await _resolve(dataset_id, db, _selector(request, table))
    latest_key, latest_order = _latest_key(ds, latest)
    filters = _filters_from(request, exclude=set())
    safe = (ds.ckan_name or "archive").replace("/", "_")[:60]
    if len(tables) > 1:
        # Name the table in the file, or several downloads of a multi-resource
        # dataset all land as the same filename and become indistinguishable.
        chosen = next((t for t in tables if t["table"] == tbl), {})
        part = (chosen.get("resource_name") or tbl).replace("/", "_")[:40]
        safe = f"{safe}_{part}"
    if latest_key:
        # The file says which of the two it is — a "latest" export and a full
        # history export of the same table are otherwise the same filename.
        safe = f"{safe}_latest"
    stream = append_store.iter_csv(tbl, sort=sort, order=order, q=q, filters=filters,
                                   latest_key=latest_key, latest_order=latest_order)
    return StreamingResponse(
        stream,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": _content_disposition(f"{safe}_append.csv")},
    )


@router.post("/{dataset_id}/sql")
@limiter.limit("20/minute")
async def archive_sql(
    dataset_id: str,
    request: Request,
    body: SqlBody,
    table: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Run a user-supplied read-only SELECT against the append DB. Guarded by a
    READ ONLY transaction + statement_timeout + row cap (see
    append_store.run_readonly_sql). The dataset's table name(s) are in /schema so
    the client can reference them — the SQL may name any table it likes, and
    ``table`` only picks whose column casing gets auto-corrected. Errors
    (validation, SQL syntax, timeout) come back as 400 with the message."""
    # 404/409 if not an append dataset
    _, tbl, _tables = await _resolve(dataset_id, db, _selector(request, table))
    try:
        return await append_store.run_readonly_sql(body.sql, table=tbl)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — surface SQL/timeout errors to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


# ── CKAN datastore-API–inspired content query endpoints ──────────────────────
# These mirror CKAN's datastore_search / datastore_search_sql so the row-level
# CONTENT of NEON-archived datasets is queryable like a CKAN datastore (not just
# the R2 files). Public + read-only.

@router.get("/{dataset_id}/datastore_search")
@limiter.limit("60/minute")
async def datastore_search(
    dataset_id: str,
    request: Request,
    limit: int = 100,
    offset: int = Query(0, ge=0, le=MAX_API_OFFSET),
    q: str | None = None,
    fields: str | None = None,
    sort: str | None = None,
    filters: str | None = None,
    distinct: bool = False,
    include_total: bool = True,
    table: str | None = None,
    latest: bool = False,
    db: AsyncSession = Depends(get_db),
):
    """CKAN ``datastore_search``-style query over the dataset's NEON content.

    Params (CKAN-aligned): ``filters`` (JSON object of exact-match column→value,
    value may be a list for IN), ``q`` (substring across all columns),
    ``fields`` (comma-separated projection), ``sort`` ("col, col2 desc"),
    ``limit``/``offset``, ``distinct``, ``include_total``. Returns the CKAN
    envelope ``{success, result:{resource_id, fields:[{id,type}], records, total,
    limit, offset, _links}}``.

    ``table`` selects one table of a multi-resource dataset (name, resource id or
    resource name); ``tables`` in the result says what else is there.

    OVER-specific: ``latest=true`` queries the newest sample per item instead of
    every sample (see /rows)."""
    ds, tbl, tables = await _resolve(dataset_id, db, _selector(request, table))
    latest_key, latest_order = _latest_key(ds, latest)
    field_list = [c.strip() for c in fields.split(",") if c.strip()] if fields else None
    filt: dict = {}
    if filters:
        try:
            filt = _json.loads(filters)
        except ValueError:
            raise HTTPException(status_code=400, detail="filters must be valid JSON")
        if not isinstance(filt, dict):
            raise HTTPException(status_code=400, detail="filters must be a JSON object")
    res = await append_store.datastore_search(
        tbl, fields=field_list, filters=filt, q=q, sort=sort,
        limit=limit, offset=offset, distinct=distinct, include_total=include_total,
        latest_key=latest_key, latest_order=latest_order,
    )
    if res is None:
        raise HTTPException(status_code=404, detail="No archived rows yet for this dataset")
    res["resource_id"] = str(ds.id)
    res["table"] = tbl
    if len(tables) > 1:
        # Otherwise a caller (MCP's query_dataset_rows included) reads a partial
        # answer as the whole dataset — the same wrong conclusion the resolution
        # bug produced, just with rows in it.
        res["tables"] = [
            {k: t.get(k) for k in ("table", "resource_id", "resource_name")}
            for t in tables
        ]
    base = request.url.remove_query_params("offset")
    res["_links"] = {
        "start": str(base.include_query_params(offset=0)),
        "next": str(base.include_query_params(offset=res["offset"] + res["limit"])),
    }
    return {"success": True, "result": res}


@router.get("/{dataset_id}/datastore_search_sql")
@limiter.limit("20/minute")
async def datastore_search_sql(
    dataset_id: str,
    request: Request,
    sql: str,
    table: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """CKAN ``datastore_search_sql``-style read-only SQL (single SELECT/WITH).
    Reference the dataset's table(s) by the name(s) in /schema. Returns the CKAN
    envelope ``{success, result:{records, fields:[{id,type}]}}``."""
    _, tbl, _tables = await _resolve(dataset_id, db, _selector(request, table))
    try:
        r = await append_store.run_readonly_sql(sql, table=tbl)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")
    return {"success": True, "result": {"records": r["rows"], "fields": r["fields"],
                                        "truncated": r["truncated"]}}
