"""Public home-page list — GET /api/home/datasets.

One page of tracked datasets plus the hero's datasets / versions / organizations
totals, served from an in-process cache. Replaces the home page's download of
the entire public catalog; see app/services/home_catalog.py.
"""
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import home_catalog

router = APIRouter(prefix="/api/home", tags=["home"])


@router.get("/datasets")
@limiter.limit("120/minute")
async def home_datasets(
    request: Request,
    q: str = Query("", max_length=200),
    page: int = Query(1, ge=1, le=10_000),
    db: AsyncSession = Depends(get_db),
):
    """``{items, total, page, page_size, committee, stats}`` for the home list."""
    catalog = await home_catalog.get_catalog(db)
    featured = [] if q.strip() else await home_catalog.featured_ids(db)
    return home_catalog.page_of(catalog, featured, q, page)
