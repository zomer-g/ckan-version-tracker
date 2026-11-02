"""Public home-page list — GET /api/home/datasets.

One page of tracked datasets plus the hero's datasets / versions / organizations
totals, served from an in-process cache. Replaces the home page's download of
the entire public catalog; see app/services/home_catalog.py.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import dataset_facets, home_catalog

router = APIRouter(prefix="/api/home", tags=["home"])


@router.get("/datasets")
@limiter.limit("120/minute")
async def home_datasets(
    request: Request,
    q: str = Query("", max_length=200),
    page: int = Query(1, ge=1, le=10_000),
    field: list[str] | None = Query(None, description="Content field (locality, authority, …); repeatable, AND"),
    file_format: list[str] | None = Query(None, alias="format", description="File format (csv, xlsx, …); repeatable, OR"),
    db: AsyncSession = Depends(get_db),
):
    """``{items, total, page, page_size, committee, stats, facets}`` for the home list."""
    try:
        fields = dataset_facets.parse_content_fields(field)
        formats = dataset_facets.parse_file_formats(file_format)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    catalog = await home_catalog.get_catalog(db)
    searching = q.strip() or fields or formats
    featured = [] if searching else await home_catalog.featured_ids(db)
    return home_catalog.page_of(catalog, featured, q, page, fields, formats)
