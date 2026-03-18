from fastapi import APIRouter, Depends, Query, Request

from app.auth.dependencies import get_current_user
from app.models.user import User
from app.rate_limit import limiter
from app.services.ckan_client import ckan_client

router = APIRouter(prefix="/api/ckan", tags=["ckan-proxy"])


@router.get("/search")
@limiter.limit("30/minute")
async def search_datasets(
    request: Request,
    q: str = Query("", description="Search query"),
    rows: int = Query(20, ge=1, le=100),
    start: int = Query(0, ge=0),
    user: User = Depends(get_current_user),
):
    return await ckan_client.package_search(query=q, rows=rows, start=start)


@router.get("/dataset/{id_or_name}")
@limiter.limit("60/minute")
async def get_dataset(
    request: Request,
    id_or_name: str,
    user: User = Depends(get_current_user),
):
    return await ckan_client.package_show(id_or_name)


@router.get("/organizations")
@limiter.limit("10/minute")
async def list_organizations(
    request: Request,
    user: User = Depends(get_current_user),
):
    return await ckan_client.organization_list(all_fields=True)
