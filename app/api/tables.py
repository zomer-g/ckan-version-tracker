"""Central /data SQL console API — the whole-site table catalog + free SQL.

Public + read-only. Backs the frontend DataSqlPage:
  GET  /api/tables                 → the unified table catalog (browser + autocomplete)
  GET  /api/tables/{table}/detail  → one table's cube (sample rows, source, files, count)
  GET  /api/tables/{table}/features → GeoJSON FeatureCollection, optional ?bbox=
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
from app.services import append_store, data_catalog, sql_shares

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tables", tags=["tables"])


def _decode_sql(sql: str | None, sql_b64: str | None) -> str:
    """Resolve a query from either the plain ``sql`` or its base64 form.

    The console sends ``sql_b64`` so a Cloudflare/WAF managed rule doesn't match
    the SQL keywords in the request as an injection attack and 403 the request
    before it ever reaches us (legitimate console queries with patterns like
    ``<> ''`` were being blocked). Plain ``sql`` stays supported for API callers."""
    if sql_b64:
        import base64
        try:
            return base64.b64decode(sql_b64).decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f"sql_b64 is not valid base64/UTF-8: {exc}")
    if sql:
        return sql
    raise ValueError("either sql or sql_b64 is required")


class SqlBody(BaseModel):
    sql: str | None = None
    sql_b64: str | None = None


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


@router.get("/{table}/profile")
@limiter.limit("60/minute")
async def table_profile(table: str, request: Request, db: AsyncSession = Depends(get_db)):
    """The stored PROFILE / metadata for one table — min/max ranges, detected
    column kinds & date formats, recurring-entity classification, keywords, and
    the LLM summary. Public + read-only. 404 if the table is unknown or has not
    been profiled yet. Resolves the schema from the catalog (the security gate)."""
    _require_enabled()
    from app.services import table_profiler
    catalog = await data_catalog.build_catalog(db)
    rec = next((r for r in catalog if r["table"] == table), None)
    if rec is None:
        raise HTTPException(status_code=404, detail="Unknown table")
    prof = await table_profiler.get_profile(rec["schema"], table)
    if prof is None:
        raise HTTPException(status_code=404, detail="Table has not been profiled yet")
    return prof


def _parse_bbox(raw: str | None) -> tuple[float, float, float, float] | None:
    """``"min_lon,min_lat,max_lon,max_lat"`` → floats, or None when absent.

    WGS84 degrees, the same order GeoJSON and OGC API - Features use, so a
    caller can hand over a Leaflet/MapLibre ``getBounds().toBBoxString()``
    unchanged. Rejects a reversed or out-of-range box rather than quietly
    returning nothing: an empty FeatureCollection for a typo'd bbox is
    indistinguishable from "the layer has nothing here"."""
    if not raw:
        return None
    parts = [p.strip() for p in str(raw).split(",")]
    if len(parts) != 4:
        raise ValueError("bbox needs 4 comma-separated numbers: "
                         "min_lon,min_lat,max_lon,max_lat (WGS84)")
    try:
        min_lon, min_lat, max_lon, max_lat = (float(p) for p in parts)
    except ValueError:
        raise ValueError("bbox values must be numbers (WGS84 degrees)")
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180
            and -90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("bbox is out of range — lon in [-180,180], lat in [-90,90]")
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError("bbox must be min_lon,min_lat,max_lon,max_lat "
                         "(the minimum corner first)")
    return min_lon, min_lat, max_lon, max_lat


@router.get("/{table}/features")
@limiter.limit("30/minute")
async def table_features(
    table: str,
    request: Request,
    bbox: str | None = None,
    columns: str | None = None,
    limit: int = 500,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    """One table's rows as a **GeoJSON FeatureCollection**, optionally clipped to
    a bounding box.

    The spatial half of the SQL console, without the SQL. Every mirrored mapping
    layer that carries a PostGIS ``geom`` column is served here — 815 of them at
    the time of writing — in WGS84 lon/lat, which is what a web map consumes
    directly. No projection, no ITM conversion, no client-side work: the mirror
    already reprojected everything that arrived in EPSG:6991 (see
    index_mirror._ensure_degrees).

    Why this exists next to /sql: ``bbox`` is the single question a map asks, and
    requiring an ``ST_MakeEnvelope`` incantation for it meant the capability was
    reachable only by someone who already knew the schema had PostGIS in it. It
    is also the answer /api/append gives when asked about a GovMap dataset.

    404 when the table is unknown (the catalog is the security gate) or carries
    no geometry — with the reason, so a caller can tell the two apart.
    """
    _require_enabled()
    try:
        box = _parse_bbox(bbox)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    catalog = await data_catalog.build_catalog(db)
    rec = next((r for r in catalog if r["table"] == table), None)
    if rec is None:
        raise HTTPException(status_code=404, detail="Unknown table")
    names = [c["name"] for c in (rec.get("columns") or [])]
    if "geom" not in names:
        raise HTTPException(
            status_code=404,
            detail=(f"Table {table} has no PostGIS geometry column. "
                    f"Tables that do are flagged with field_flags.has_geometry "
                    f"in /api/tables and carry a `geom` column."),
        )

    # Properties: everything except the geometry itself (raw WKT is a bulk
    # payload nobody wants twice in the same response) and the mirror's
    # bookkeeping. ?columns= narrows further, and is validated against the live
    # column list rather than trusted.
    default = [c for c in names if c not in ("geom", "geometry_wkt", "_row_hash")]
    if columns:
        wanted = [c.strip() for c in columns.split(",") if c.strip()]
        unknown = [c for c in wanted if c not in names]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"unknown column(s): {', '.join(unknown)}")
        picked = [c for c in wanted if c != "geom"]
    else:
        picked = default

    try:
        res = await append_store.geo_features(
            table, schema=rec["schema"], columns=picked, bbox=box,
            limit=limit, offset=offset,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — timeouts/SQL errors go to the caller
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")

    return {
        "type": "FeatureCollection",
        "features": res["features"],
        # OGC API - Features names these two, and they are the only way a caller
        # can tell a full page from a truncated one without counting.
        "numberReturned": res["number_returned"],
        "exceededTransferLimit": res["exceeded_transfer_limit"],
        "crs": {"type": "name",
                "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}},
        "table": table,
        "schema": rec["schema"],
        "title": rec.get("title"),
        "source_url": rec.get("source_url"),
        "limit": limit,
        "offset": offset,
    }


@router.post("/sql")
@limiter.limit("20/minute")
async def run_sql(request: Request, body: SqlBody):
    """Read-only SELECT over the append DB, spanning the public (dataset) and
    knesset schemas (search_path = public, knesset)."""
    _require_enabled()
    try:
        sql = _decode_sql(body.sql, body.sql_b64)
        return await append_store.run_readonly_sql(
            sql, search_path=data_catalog.CONSOLE_SEARCH_PATH
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors go to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


class ShareBody(BaseModel):
    sql: str | None = None
    sql_b64: str | None = None
    # The view around the query (chart type, axes, selected table) as a query
    # string. Filtered server-side to the console's own keys.
    params: str | None = None


@router.post("/share")
@limiter.limit("10/minute")
async def create_share(request: Request, body: ShareBody, db: AsyncSession = Depends(get_db)):
    """Store a console view and return its short slug.

    Anonymous on purpose — the console is public, and a share button that needs
    a login is a share button the audience cannot use. What bounds it is this
    route's rate limit, the size cap and content dedup in the service, and the
    fact that only the console's known view keys survive from ``params``.
    """
    try:
        sql = _decode_sql(body.sql, body.sql_b64)
        slug = await sql_shares.create(db, sql, body.params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"slug": slug, "path": f"/s/{slug}"}


@router.get("/share/{slug}")
@limiter.limit("60/minute")
async def read_share(request: Request, slug: str, db: AsyncSession = Depends(get_db)):
    """Resolve a slug back to the stored query + view settings."""
    share = await sql_shares.resolve(db, slug)
    if share is None:
        raise HTTPException(status_code=404, detail="הקישור לא נמצא")
    return share


@router.get("/schema.txt", response_class=PlainTextResponse)
@limiter.limit("20/minute")
async def schema_txt(request: Request, table: str | None = None,
                     schema: str | None = None,
                     db: AsyncSession = Depends(get_db)):
    """DESCRIBE-style DDL for copy-to-AI.

    ``?table=`` → that one table's full DDL. Otherwise the WHOLE catalog in
    compact form (one CREATE TABLE line per table), optionally narrowed with
    ``?schema=public|knesset|idx``.

    It used to return only the knesset schema, which made the button misleading:
    it is labelled "copy schema to AI" but handed over ~13% of the tables, so an
    assistant given that text would confidently write SQL against tables it could
    not see and miss every dataset and collection index."""
    _require_enabled()
    if table:
        rec = next((r for r in await data_catalog.build_catalog(db)
                    if r["table"] == table), None)
        if rec is None:
            raise HTTPException(status_code=404, detail="Unknown table")
        if rec["kind"] == "knesset":
            from app.services import knesset_db
            return await knesset_db.schema_text()
        return await append_store.schema_text(table, title=rec.get("title"),
                                              schema=rec["schema"])
    if schema and schema not in ("public", "knesset", "idx"):
        raise HTTPException(status_code=400, detail="Unknown schema")
    return await data_catalog.schema_text_all(db, schema=schema)


@router.get("/export.csv")
@limiter.limit("6/minute")
async def export_csv(request: Request, sql: str | None = None, sql_b64: str | None = None):
    """Run the SQL on the server and stream the full result (≤200k rows) as CSV
    over both schemas. First chunk is pulled eagerly so validation/SQL errors
    become a clean 400 instead of a broken download. ``sql_b64`` (base64) is
    accepted so the query in the URL doesn't trip a WAF SQLi rule."""
    _require_enabled()
    try:
        sql = _decode_sql(sql, sql_b64)
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
