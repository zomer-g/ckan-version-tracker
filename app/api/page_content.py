"""Admin-editable text for the static About / Rationale pages.

Read side (public, rate-limited):
* ``GET /api/page-content/{page}`` — every override for a page, grouped by
  language: ``{"he": {"what_text": "..."}, "en": {...}}``. The frontend merges
  this over the bundled i18n defaults at runtime (see
  frontend/src/hooks/usePageContentOverrides.ts), so a key with no row here just
  keeps its bundled default.

Write side (admin only, JWT + is_admin):
* ``PUT    /api/admin/page-content`` — upsert one (page, lang, key) → value.
* ``DELETE /api/admin/page-content?page=&lang=&key=`` — drop the override so the
  string reverts to the bundled default.

The set of editable keys is defined by the frontend bundle, not here — the admin
UI enumerates them from the in-browser i18n resources. This module only
validates ``page`` / ``lang`` and stores whatever value it is given (the value
keeps the ``<1>``/``<2>``/``<strong>`` inline-tag convention the pages render
through <Trans>). See app/models/page_content.py.
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.page_content import PageContent
from app.models.user import User
from app.rate_limit import limiter

logger = logging.getLogger(__name__)

# Pages whose copy is editable. Keep in sync with the frontend page namespaces.
PAGES = {"about", "rationale"}
LANGS = {"he", "en"}

router = APIRouter(prefix="/api/page-content", tags=["page-content"])
admin_router = APIRouter(prefix="/api/admin/page-content", tags=["admin"])


class PageContentUpsert(BaseModel):
    page: str = Field(..., max_length=40)
    lang: str = Field(..., max_length=8)
    key: str = Field(..., max_length=80)
    value: str


def _validate(page: str, lang: str) -> None:
    if page not in PAGES:
        raise HTTPException(status_code=404, detail=f"unknown page '{page}'")
    if lang not in LANGS:
        raise HTTPException(status_code=422, detail=f"unsupported lang '{lang}'")


@router.get("/{page}")
@limiter.limit("120/minute")
async def get_page_content(
    request: Request, page: str, db: AsyncSession = Depends(get_db)
) -> dict[str, dict[str, str]]:
    """All overrides for a page, grouped by language."""
    if page not in PAGES:
        raise HTTPException(status_code=404, detail=f"unknown page '{page}'")
    rows = (
        await db.execute(
            select(PageContent.lang, PageContent.key, PageContent.value).where(
                PageContent.page == page
            )
        )
    ).all()
    out: dict[str, dict[str, str]] = {lang: {} for lang in LANGS}
    for lang, key, value in rows:
        out.setdefault(lang, {})[key] = value
    return out


@admin_router.put("")
async def upsert_page_content(
    body: PageContentUpsert,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Create or update one string override (admin)."""
    _validate(body.page, body.lang)
    key = body.key.strip()
    if not key:
        raise HTTPException(status_code=422, detail="key is required")
    stmt = (
        pg_insert(PageContent)
        .values(
            page=body.page,
            lang=body.lang,
            key=key,
            value=body.value,
            updated_by=admin.email,
            updated_at=datetime.now(timezone.utc),
        )
        .on_conflict_do_update(
            constraint="uq_page_content_plk",
            set_={
                "value": body.value,
                "updated_by": admin.email,
                "updated_at": datetime.now(timezone.utc),
            },
        )
    )
    await db.execute(stmt)
    await db.commit()
    logger.info("page_content upsert %s/%s/%s by %s", body.page, body.lang, key, admin.email)
    return {"ok": True}


@admin_router.delete("")
async def delete_page_content(
    page: str = Query(...),
    lang: str = Query(...),
    key: str = Query(...),
    _admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Drop an override so the string reverts to the bundled default (admin)."""
    _validate(page, lang)
    await db.execute(
        text(
            "DELETE FROM page_content WHERE page = :page AND lang = :lang AND key = :key"
        ),
        {"page": page, "lang": lang, "key": key},
    )
    await db.commit()
    return {"ok": True}
