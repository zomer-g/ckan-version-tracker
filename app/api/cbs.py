"""CBS (cbs.gov.il) content-index API.

Write side (worker only):
* ``POST /api/cbs/ingest`` — the govil-scraper ``cbs`` engine upserts a batch of
  crawled pages (auth: ``Authorization: Bearer <WORKER_API_KEY>``).

Read side (public, rate-limited):
* ``GET  /api/cbs/search``  — full-text + faceted search over the index.
* ``GET  /api/cbs/facets``  — distinct subjects / geo levels / file types /
  sections + the overall year span, to populate the extension's filter UI.
* ``GET  /api/cbs/stats``   — coverage counters.

The same table backs a future dedicated MCP (app/mcp) — keep the query logic in
helpers so it can be shared. See app/models/cbs_index.py.
"""
import hmac
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.cbs_search_util import RESULT_COLS, build_search
from app.api.utils import MAX_API_OFFSET
from app.auth.dependencies import get_admin_user
from app.config import settings
from app.database import get_db
from app.models.cbs_featured import CbsFeatured
from app.models.cbs_index import CbsIndex
from app.models.user import User
from app.rate_limit import limiter
from app.services import cbs_neon

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cbs", tags=["cbs"])

# Columns the worker may set; everything else (id, first_seen, search_vector) is
# server-managed. Used to build the ON CONFLICT update set.
_INGEST_FIELDS = (
    "lang", "section", "series", "item_type", "title", "title_en", "summary",
    "subject_tags", "year_start", "year_end", "geo_levels", "file_links",
    "file_types", "extra", "full_text", "content_hash", "crawl_status", "crawl_error",
)


def _verify_worker_key(request: Request) -> None:
    """Bearer-token auth for the worker, mirroring app/api/worker.py."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing worker key")
    key = auth[7:].strip()
    # Constant-time compare (see app/api/worker.py). Fail closed when no key is
    # configured — an empty secret can never match a presented token.
    if not settings.worker_api_key or not hmac.compare_digest(key, settings.worker_api_key):
        raise HTTPException(status_code=403, detail="Invalid worker key")


# ── Ingest ────────────────────────────────────────────────────────────────
class FileLink(BaseModel):
    label: str | None = None
    href: str
    ext: str | None = None
    size: int | None = None
    last_modified: str | None = None


class CbsPageIn(BaseModel):
    url: str
    lang: str | None = None
    section: str | None = None
    series: str | None = None
    item_type: str | None = None
    title: str | None = None
    title_en: str | None = None
    summary: str | None = None
    subject_tags: list[str] | None = None
    year_start: int | None = None
    year_end: int | None = None
    geo_levels: list[str] | None = None
    file_links: list[FileLink] | None = None
    file_types: list[str] | None = None
    extra: dict | None = None
    full_text: str | None = None
    content_hash: str | None = None
    crawl_status: str = "ok"
    crawl_error: str | None = None


class IngestRequest(BaseModel):
    pages: list[CbsPageIn] = Field(..., max_length=500)


class IngestResponse(BaseModel):
    upserted: int


@router.post("/ingest", response_model=IngestResponse)
async def ingest(request: Request, body: IngestRequest, db: AsyncSession = Depends(get_db)):
    """Upsert a batch of crawled CBS pages (keyed on ``url``)."""
    _verify_worker_key(request)
    if not body.pages:
        return IngestResponse(upserted=0)

    now = datetime.now(timezone.utc)
    rows = []
    for p in body.pages:
        d = p.model_dump()
        # file_links are nested models → plain dicts for JSONB
        if d.get("file_links") is not None:
            d["file_links"] = [fl for fl in d["file_links"]]
        d["last_crawled"] = now
        rows.append(d)

    stmt = pg_insert(CbsIndex).values(rows)
    update_set = {c: getattr(stmt.excluded, c) for c in _INGEST_FIELDS}
    update_set["last_crawled"] = stmt.excluded.last_crawled
    stmt = stmt.on_conflict_do_update(index_elements=["url"], set_=update_set)
    await db.execute(stmt)
    await db.commit()

    # Dual-write the same batch into the NEON append archive so CBS behaves like
    # every other tabular tracked dataset (card + /archive SQL console +
    # /api/append). Best-effort: a NEON hiccup must never fail the crawl ingest.
    try:
        await cbs_neon.upsert_pages(rows)
    except Exception:  # noqa: BLE001 — mirror is advisory, cbs_index is the source of truth
        logger.warning("cbs_neon dual-write failed for %d rows", len(rows), exc_info=True)

    return IngestResponse(upserted=len(rows))


@router.post("/sync-neon")
async def sync_neon(request: Request, db: AsyncSession = Depends(get_db)):
    """Backfill the NEON append table from cbs_index (worker-key auth).

    One-shot after deploy: seeds the CBS append archive with the rows that
    predate the ingest dual-write. Idempotent (upsert by url)."""
    _verify_worker_key(request)
    if not cbs_neon.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")
    synced = await cbs_neon.backfill(db)
    return {"synced": synced, "table": cbs_neon.CBS_APPEND_TABLE}


# ── Search ───────────────────────────────────────────────────────────────
class CbsResult(BaseModel):
    url: str
    lang: str | None
    section: str | None
    series: str | None
    item_type: str | None
    title: str | None
    title_en: str | None
    summary: str | None
    subject_tags: list | None
    year_start: int | None
    year_end: int | None
    geo_levels: list | None
    file_links: list | None
    file_types: list | None
    extra: dict | None
    last_crawled: datetime | None


class SearchResponse(BaseModel):
    total: int
    results: list[CbsResult]


@router.get("/search", response_model=SearchResponse)
@limiter.limit("60/minute")
async def search(
    request: Request,
    q: str | None = Query(None, description="Free-text query (Hebrew/English)"),
    subject: str | None = Query(None),
    geo: str | None = Query(None),
    file_type: str | None = Query(None),
    section: str | None = Query(None),
    item_type: str | None = Query(None),
    lang: str | None = Query(None),
    year_from: int | None = Query(None),
    year_to: int | None = Query(None),
    sort: str = Query(
        "relevance",
        pattern="^(relevance|chrono)$",
        description="'relevance' (text rank, default) or 'chrono' (newest data year first)",
    ),
    limit: int = Query(30, ge=1, le=100),
    offset: int = Query(0, ge=0, le=MAX_API_OFFSET),
    db: AsyncSession = Depends(get_db),
):
    """Full-text + faceted search over the CBS index."""
    where, order, params = build_search(
        {
            "q": q, "subject": subject, "geo": geo, "file_type": file_type,
            "section": section, "item_type": item_type, "lang": lang,
            "year_from": year_from, "year_to": year_to,
        },
        sort=sort,
    )

    total = (
        await db.execute(text(f"SELECT count(*) FROM cbs_index{where}"), params)
    ).scalar_one()

    params["limit"] = limit
    params["offset"] = offset
    result = await db.execute(
        text(
            f"SELECT {RESULT_COLS} FROM cbs_index{where} "
            f"ORDER BY {order} LIMIT :limit OFFSET :offset"
        ),
        params,
    )
    rows = [CbsResult(**dict(r._mapping)) for r in result]
    return SearchResponse(total=total, results=rows)


# ── Facets ───────────────────────────────────────────────────────────────
class FacetsResponse(BaseModel):
    subjects: list[str]
    geo_levels: list[str]
    file_types: list[str]
    sections: list[str]
    item_types: list[str]
    year_min: int | None
    year_max: int | None


@router.get("/facets", response_model=FacetsResponse)
@limiter.limit("60/minute")
async def facets(request: Request, db: AsyncSession = Depends(get_db)):
    """Distinct filter values for the search UI."""
    async def distinct_jsonb(col: str) -> list[str]:
        # LATERAL + jsonb_typeof guard: a single non-array value (should never
        # happen, but defensive) would otherwise crash the whole SRF query.
        r = await db.execute(
            text(
                f"SELECT DISTINCT elem AS v FROM cbs_index "
                f"CROSS JOIN LATERAL jsonb_array_elements_text({col}) AS elem "
                f"WHERE {col} IS NOT NULL AND jsonb_typeof({col}) = 'array' "
                f"ORDER BY v"
            )
        )
        return [row[0] for row in r]

    async def distinct_col(col: str) -> list[str]:
        r = await db.execute(
            text(
                f"SELECT DISTINCT {col} AS v FROM cbs_index "
                f"WHERE {col} IS NOT NULL ORDER BY v"
            )
        )
        return [row[0] for row in r]

    years = (
        await db.execute(text("SELECT min(year_start), max(year_end) FROM cbs_index"))
    ).one()

    return FacetsResponse(
        subjects=await distinct_jsonb("subject_tags"),
        geo_levels=await distinct_jsonb("geo_levels"),
        file_types=await distinct_jsonb("file_types"),
        sections=await distinct_col("section"),
        item_types=await distinct_col("item_type"),
        year_min=years[0],
        year_max=years[1],
    )


# ── Stats ────────────────────────────────────────────────────────────────
class StatsResponse(BaseModel):
    total: int
    crawled: int
    pending: int
    errored: int
    by_section: dict[str, int]


@router.get("/stats", response_model=StatsResponse)
@limiter.limit("60/minute")
async def stats(request: Request, db: AsyncSession = Depends(get_db)):
    """Coverage counters for the index."""
    total = (await db.execute(select(func.count()).select_from(CbsIndex))).scalar_one()
    by_status_rows = (
        await db.execute(
            select(CbsIndex.crawl_status, func.count()).group_by(CbsIndex.crawl_status)
        )
    ).all()
    by_status = {s: c for s, c in by_status_rows}
    by_section_rows = (
        await db.execute(
            select(CbsIndex.section, func.count())
            .where(CbsIndex.section.isnot(None))
            .group_by(CbsIndex.section)
        )
    ).all()
    return StatsResponse(
        total=total,
        crawled=by_status.get("ok", 0),
        pending=by_status.get("pending", 0),
        errored=by_status.get("error", 0),
        by_section={s: c for s, c in by_section_rows},
    )


# ── Featured (admin-pinned quick-access pages) ─────────────────────────────
# Columns returned by /search — reused verbatim so a featured card renders
# exactly like a search-result card on the frontend.
_RESULT_COLS = (
    "url, lang, section, series, item_type, title, title_en, summary, "
    "subject_tags, year_start, year_end, geo_levels, file_links, file_types, "
    "extra, last_crawled"
)
# Same list qualified with the cbs_index alias — the featured query joins
# cbs_featured, and BOTH tables have a ``url`` column, so an unqualified
# ``SELECT url`` is ambiguous. Every column here lives on cbs_index.
_RESULT_COLS_I = ", ".join(f"i.{c.strip()}" for c in _RESULT_COLS.split(","))


class FeaturedResponse(BaseModel):
    results: list[CbsResult]


class FeaturedPinRequest(BaseModel):
    url: str


async def _featured_rows(db: AsyncSession) -> "FeaturedResponse":
    """Shared reader — the current featured cards in order.

    Inner-joins cbs_featured against cbs_index so a pin whose page has left the
    index (or was never crawled) simply yields no card — no error, no gap."""
    result = await db.execute(
        text(
            f"SELECT {_RESULT_COLS_I} FROM cbs_index i "
            "JOIN cbs_featured f ON f.url = i.url "
            "ORDER BY f.sort_order, f.id"
        )
    )
    rows = [CbsResult(**dict(r._mapping)) for r in result]
    return FeaturedResponse(results=rows)


@router.get("/featured", response_model=FeaturedResponse)
@limiter.limit("60/minute")
async def featured(request: Request, db: AsyncSession = Depends(get_db)):
    """The admin-pinned pages, in card order."""
    return await _featured_rows(db)


@router.post("/featured", response_model=FeaturedResponse)
async def pin_featured(
    body: FeaturedPinRequest,
    _admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Pin a CBS page (admin). Idempotent; new pins sort to the end."""
    url = body.url.strip()
    if not url:
        raise HTTPException(status_code=422, detail="url is required")
    # New pin goes after the current last card.
    next_order = (
        await db.execute(select(func.coalesce(func.max(CbsFeatured.sort_order), 0)))
    ).scalar_one()
    stmt = (
        pg_insert(CbsFeatured)
        .values(url=url, sort_order=next_order + 1)
        .on_conflict_do_nothing(index_elements=["url"])
    )
    await db.execute(stmt)
    await db.commit()
    return await _featured_rows(db)


@router.delete("/featured", response_model=FeaturedResponse)
async def unpin_featured(
    url: str = Query(..., description="The pinned page URL to remove"),
    _admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Unpin a CBS page (admin)."""
    await db.execute(text("DELETE FROM cbs_featured WHERE url = :url"), {"url": url})
    await db.commit()
    return await _featured_rows(db)
