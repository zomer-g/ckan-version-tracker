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
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

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
from app.models.cbs_feedback import CbsFeedback
from app.models.cbs_gazetteer import CbsGazetteer
from app.models.cbs_index import CbsIndex
from app.models.user import User
from app.rate_limit import limiter
from app.services import cbs_neon
from app.services.cbs_enrich import enrich as enrich_row

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cbs", tags=["cbs"])

# Columns the worker may set; everything else (id, first_seen, search_vector) is
# server-managed. Used to build the ON CONFLICT update set.
_INGEST_FIELDS = (
    "lang", "section", "series", "item_type", "title", "title_en", "summary",
    "subject_tags", "year_start", "year_end", "geo_levels", "file_links",
    "file_types", "extra", "full_text", "content_hash", "crawl_status", "crawl_error",
)

# Server-derived columns (app/services/cbs_enrich.py) — computed on ingest so
# fresh crawls arrive enriched, and re-computable in bulk via POST /enrich.
_ENRICH_FIELDS = (
    "product_form", "freq", "source_op", "data_vintage", "geo_vintage",
    "geo_coverage", "series_key", "edition_year", "metrics", "cuts",
)

# Marks the newest edition per series (ties broken by id). Rows outside any
# series keep TRUE. Optionally scoped to a set of series keys (:keys) so the
# per-ingest refresh doesn't rescan the whole table.
_LATEST_SQL = """
UPDATE cbs_index i SET is_latest_edition = t.is_max
FROM (
    SELECT id,
           (rank() OVER (PARTITION BY series_key
                         ORDER BY edition_year DESC NULLS LAST, id DESC) = 1) AS is_max
    FROM cbs_index
    WHERE series_key IS NOT NULL {scope}
) t
WHERE i.id = t.id AND i.is_latest_edition IS DISTINCT FROM t.is_max
"""


async def _refresh_latest(db: AsyncSession, keys: list[str] | None = None) -> None:
    if keys is not None:
        if not keys:
            return
        await db.execute(
            text(_LATEST_SQL.format(scope="AND series_key = ANY(:keys)")),
            {"keys": keys},
        )
    else:
        await db.execute(text(_LATEST_SQL.format(scope="")))


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
    # Deduplicate by url, last-one-wins. ``url`` is the ON CONFLICT target, and
    # Postgres refuses a statement that would update the same row twice ("ON
    # CONFLICT DO UPDATE command cannot affect row a second time") — so a batch
    # that happens to repeat a url used to fail the whole ingest with a 500.
    # A crawler re-listing the same page in one batch is normal, not an error.
    by_url: dict[str, dict] = {}
    for p in body.pages:
        d = p.model_dump()
        # file_links are nested models → plain dicts for JSONB
        if d.get("file_links") is not None:
            d["file_links"] = [fl for fl in d["file_links"]]
        d["last_crawled"] = now
        # Derived metadata (product form, series identity, vintage…) — computed
        # here so a fresh crawl arrives enriched; enrich() also completes
        # geo_levels from title evidence.
        d.update(enrich_row(d))
        by_url[d["url"]] = d
    rows = list(by_url.values())

    stmt = pg_insert(CbsIndex).values(rows)
    update_set = {c: getattr(stmt.excluded, c) for c in _INGEST_FIELDS + _ENRICH_FIELDS}
    update_set["last_crawled"] = stmt.excluded.last_crawled
    stmt = stmt.on_conflict_do_update(index_elements=["url"], set_=update_set)
    await db.execute(stmt)
    # A new edition may dethrone last year's: refresh the flag for the series
    # this batch touched (scoped — not a full-table rescan).
    await _refresh_latest(db, sorted({r["series_key"] for r in rows if r.get("series_key")}))
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
    # Enrichment layer (migration 038 / app/services/cbs_enrich.py).
    product_form: str | None = None
    freq: str | None = None
    source_op: str | None = None
    data_vintage: int | None = None
    geo_vintage: str | None = None
    geo_coverage: str | None = None
    series_key: str | None = None
    edition_year: int | None = None
    is_latest_edition: bool | None = None
    metrics: list | None = None
    cuts: list | None = None


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
    product_form: str | None = Query(None, description="Product taxonomy: data_file/gis_layer/puf/generator/dashboard/api/database/publication/methodology"),
    freq: str | None = Query(None, description="Time-axis unit (שנתי/רבעוני/חודשי…)"),
    source_op: str | None = Query(None, description="Collection operation (מפקד אוכלוסין, סקר כוח אדם…)"),
    latest_only: bool = Query(False, description="Keep only the newest edition of each series"),
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
            "product_form": product_form, "freq": freq, "source_op": source_op,
            "latest_only": latest_only,
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
    # Enrichment facets — empty lists until the enrich backfill has run.
    product_forms: list[str] = []
    freqs: list[str] = []
    source_ops: list[str] = []


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
        product_forms=await distinct_col("product_form"),
        freqs=await distinct_col("freq"),
        source_ops=await distinct_col("source_op"),
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


# ── Enrichment backfill (worker) ───────────────────────────────────────────
# Recomputes the derived columns for existing rows in id-ordered batches, so
# the whole 53K-row index can be enriched without a re-crawl. The caller loops
# on next_after until done=true; the final call refreshes is_latest_edition
# table-wide. Idempotent — safe to re-run after tweaking cbs_enrich rules.

_ENRICH_SRC_COLS = (
    "id, title, summary, section, series, item_type, subject_tags, year_start, "
    "year_end, geo_levels, file_links, file_types, extra"
)
_ENRICH_JSONB = {"metrics", "cuts", "geo_levels"}

_ENRICH_UPDATE_SQL = text(
    "UPDATE cbs_index SET "
    + ", ".join(
        f"{c} = CAST(:{c} AS jsonb)" if c in ("metrics", "cuts", "geo_levels")
        else f"{c} = :{c}"
        for c in _ENRICH_FIELDS + ("geo_levels",)
    )
    + " WHERE id = :b_id"
)


class EnrichRequest(BaseModel):
    start_after: int = 0
    batch_size: int = Field(2000, ge=100, le=5000)
    max_batches: int = Field(5, ge=1, le=20)


@router.post("/enrich")
async def enrich_backfill(request: Request, body: EnrichRequest, db: AsyncSession = Depends(get_db)):
    """Batch-recompute derived columns over existing rows (worker-key auth)."""
    _verify_worker_key(request)
    cursor = body.start_after
    processed = 0
    done = False
    for _ in range(body.max_batches):
        rows = (await db.execute(
            text(f"SELECT {_ENRICH_SRC_COLS} FROM cbs_index "
                 "WHERE id > :cursor ORDER BY id LIMIT :lim"),
            {"cursor": cursor, "lim": body.batch_size},
        )).mappings().all()
        if not rows:
            done = True
            break
        updates = []
        for r in rows:
            e = enrich_row(dict(r))
            u = {"b_id": r["id"]}
            for k, v in e.items():
                u[k] = json.dumps(v, ensure_ascii=False) if k in _ENRICH_JSONB and v is not None else v
            updates.append(u)
        await db.execute(_ENRICH_UPDATE_SQL, updates)
        cursor = rows[-1]["id"]
        processed += len(rows)
        if len(rows) < body.batch_size:
            done = True
            break
    if done:
        await _refresh_latest(db)  # table-wide, once, at the end
    await db.commit()
    return {"processed": processed, "next_after": cursor, "done": done}


# ── Gazetteer (locality registry) ──────────────────────────────────────────

_GAZETTEER_SEED = Path(__file__).resolve().parents[1] / "data" / "cbs_gazetteer.json"
_GAZ_FIELDS = ("name", "name_en", "aliases", "district", "subdistrict",
               "municipal_status", "regional_council", "population", "ses_cluster")


@router.post("/gazetteer/load")
async def gazetteer_load(request: Request, db: AsyncSession = Depends(get_db)):
    """Load/refresh the locality gazetteer from the packaged seed (worker-key).

    The seed is generated from the CBS bycode file by cbs_gazetteer_build.py in
    the govil-scraper repo and committed under app/data/. Upsert by code."""
    _verify_worker_key(request)
    if not _GAZETTEER_SEED.exists():
        raise HTTPException(status_code=409, detail="gazetteer seed not packaged")
    entries = json.loads(_GAZETTEER_SEED.read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc)
    rows = [{"code": e["code"], **{f: e.get(f) for f in _GAZ_FIELDS}, "updated_at": now}
            for e in entries if e.get("code")]
    stmt = pg_insert(CbsGazetteer).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["code"],
        set_={f: getattr(stmt.excluded, f) for f in _GAZ_FIELDS + ("updated_at",)},
    )
    await db.execute(stmt)
    await db.commit()
    return {"loaded": len(rows)}


class GazetteerEntry(BaseModel):
    code: int
    name: str
    name_en: str | None
    district: str | None
    subdistrict: str | None
    municipal_status: str | None
    regional_council: str | None
    population: int | None


@router.get("/gazetteer")
@limiter.limit("120/minute")
async def gazetteer_search(
    request: Request,
    q: str = Query(..., min_length=2, description="Locality name prefix/substring (Hebrew or English)"),
    limit: int = Query(10, ge=1, le=30),
    db: AsyncSession = Depends(get_db),
):
    """Locality autocomplete: matches name, English name and known aliases.

    Prefix matches rank first, then substring; bigger localities first within
    each group — so typing "בית ש" puts בית שמש above the small moshavim."""
    like_prefix = f"{q}%"
    like_sub = f"%{q}%"
    rows = (await db.execute(text(
        "SELECT code, name, name_en, district, subdistrict, municipal_status, "
        "       regional_council, population "
        "FROM cbs_gazetteer "
        "WHERE name ILIKE :sub OR name_en ILIKE :sub OR EXISTS ("
        "  SELECT 1 FROM jsonb_array_elements_text(coalesce(aliases,'[]'::jsonb)) a"
        "  WHERE a ILIKE :sub) "
        # coalesce: NULL name_en would make the OR NULL (not false), and DESC
        # sorts NULLs FIRST — floating non-prefix rows above real prefix hits.
        # (Same three-valued-logic trap as the 2b244ab ranking regression.)
        "ORDER BY (name ILIKE :pfx OR coalesce(name_en,'') ILIKE :pfx) DESC, "
        "         population DESC NULLS LAST, name "
        "LIMIT :lim"),
        {"sub": like_sub, "pfx": like_prefix, "lim": limit},
    )).mappings().all()
    return {"results": [GazetteerEntry(**dict(r)).model_dump() for r in rows]}


# ── Series (edition history of one product) ────────────────────────────────

@router.get("/series")
@limiter.limit("60/minute")
async def series_editions(
    request: Request,
    key: str = Query(..., min_length=4, description="series_key of any edition"),
    db: AsyncSession = Depends(get_db),
):
    """All editions sharing a series_key, newest first — powers the 'מהדורות
    קודמות' timeline on both tabs."""
    rows = (await db.execute(
        text(f"SELECT {RESULT_COLS} FROM cbs_index WHERE series_key = :k "
             "ORDER BY edition_year DESC NULLS LAST, id DESC LIMIT 50"),
        {"k": key},
    )).mappings().all()
    return {"results": [CbsResult(**dict(r)) for r in rows]}


# ── Feedback (like / dislike on a search) ──────────────────────────────────
# A lightweight quality signal: each search can be thumbed up or down. Public
# (unauthenticated, rate-limited) like /search itself. The admin report ranks
# queries by dislikes so improvement effort goes where it hurts most.

class FeedbackRequest(BaseModel):
    query: str
    vote: int = Field(..., description="+1 like, -1 dislike")
    mode: str = "ask"                       # ask | advanced
    answer_type: str | None = None
    top_url: str | None = None
    source: str | None = None               # web | extension


@router.post("/feedback")
@limiter.limit("60/minute")
async def submit_feedback(request: Request, body: FeedbackRequest, db: AsyncSession = Depends(get_db)):
    """Record one like/dislike on a search. Vote is coerced to ±1."""
    query = (body.query or "").strip()
    if not query:
        raise HTTPException(status_code=400, detail="query is required")
    vote = 1 if body.vote > 0 else -1
    mode = body.mode if body.mode in ("ask", "advanced") else "ask"
    db.add(CbsFeedback(
        query=query[:2000], mode=mode, vote=vote,
        answer_type=(body.answer_type or None), top_url=(body.top_url or None),
        source=(body.source or None),
    ))
    await db.commit()
    return {"ok": True}


class FeedbackQueryRow(BaseModel):
    query: str
    likes: int
    dislikes: int
    total: int
    score: int          # likes - dislikes
    last_at: datetime | None


class FeedbackReport(BaseModel):
    total_votes: int
    likes: int
    dislikes: int
    queries: list[FeedbackQueryRow]


@router.get("/feedback/report", response_model=FeedbackReport)
async def feedback_report(
    request: Request,
    order: str = Query("dislikes", pattern="^(dislikes|likes|total|recent)$",
                       description="Ranking: dislikes (improvement targets, default) / likes / total / recent"),
    limit: int = Query(200, ge=1, le=1000),
    _admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
):
    """Aggregated like/dislike report, grouped by query (admin only).

    Default order surfaces the most-disliked queries first — the ones whose
    search results most need fixing."""
    totals = (await db.execute(text(
        "SELECT count(*) AS n, "
        "coalesce(sum(case when vote>0 then 1 else 0 end),0) AS likes, "
        "coalesce(sum(case when vote<0 then 1 else 0 end),0) AS dislikes "
        "FROM cbs_feedback"
    ))).one()

    order_sql = {
        "dislikes": "dislikes DESC, total DESC, last_at DESC",
        "likes": "likes DESC, total DESC, last_at DESC",
        "total": "total DESC, last_at DESC",
        "recent": "last_at DESC",
    }[order]
    rows = (await db.execute(text(
        "SELECT query, "
        "sum(case when vote>0 then 1 else 0 end) AS likes, "
        "sum(case when vote<0 then 1 else 0 end) AS dislikes, "
        "count(*) AS total, "
        "sum(vote) AS score, "
        "max(created_at) AS last_at "
        "FROM cbs_feedback GROUP BY query "
        f"ORDER BY {order_sql} LIMIT :lim"
    ), {"lim": limit})).mappings().all()

    return FeedbackReport(
        total_votes=int(totals.n),
        likes=int(totals.likes),
        dislikes=int(totals.dislikes),
        queries=[FeedbackQueryRow(
            query=r["query"], likes=int(r["likes"]), dislikes=int(r["dislikes"]),
            total=int(r["total"]), score=int(r["score"]), last_at=r["last_at"],
        ) for r in rows],
    )


@router.delete("/feedback")
async def delete_feedback(
    request: Request,
    source: str | None = Query(None, description="Delete only votes from this source (e.g. a test/spam tag)"),
    query: str | None = Query(None, description="Delete only votes for this exact query"),
    db: AsyncSession = Depends(get_db),
):
    """Prune feedback rows (worker-key auth). Scope by ``source`` and/or
    ``query``; at least one is required so this can never wipe the table
    wholesale by accident. Used to clear test/spam votes."""
    _verify_worker_key(request)
    conds, params = [], {}
    if source:
        conds.append("source = :source"); params["source"] = source
    if query:
        conds.append("query = :query"); params["query"] = query
    if not conds:
        raise HTTPException(status_code=400, detail="source or query is required")
    res = await db.execute(
        text(f"DELETE FROM cbs_feedback WHERE {' AND '.join(conds)}"), params
    )
    await db.commit()
    return {"deleted": res.rowcount}


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
