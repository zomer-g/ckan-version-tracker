"""Public site-wide totals — backs the home page hero stats.

One cached endpoint (GET /api/stats) returning how many SQL tables the site
exposes, how many rows they hold, and how many archived files it stores. See
app/services/site_stats.py for why all three are cached and fail soft.
"""
import logging

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import site_stats

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("")
@limiter.limit("60/minute")
async def site_totals(request: Request, db: AsyncSession = Depends(get_db)):
    """``{tables, rows, files}``; a value is null when that total is unavailable."""
    return await site_stats.get_site_stats(db)
