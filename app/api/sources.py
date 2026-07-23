"""Public endpoints for declaratively-registered sources.

The fifteen built-in sources each have their own ``/api/<source>/validate``.
Sources registered by the worker have no code here, so these two endpoints
serve all of them:

  POST /api/sources/validate  — classify a pasted URL against every manifest
  GET  /api/sources/registry  — badge/label metadata for the frontend

See app/services/source_registry.py for the manifest contract.
"""
import logging

from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import source_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sources", tags=["sources"])


class ValidateRequest(BaseModel):
    url: str


class ValidateResponse(BaseModel):
    """Superset of the built-in sources' ValidateResponse.

    The extra fields let the frontend render a source it has no code for:
    the badge colours, the source-link label, and the poll cadence to
    pre-select in the tracking form.
    """

    valid: bool
    page_type: str | None = None
    collector_name: str | None = None
    title: str | None = None
    url: str | None = None
    error: str | None = None
    source_id: str | None = None
    label_he: str | None = None
    label_en: str | None = None
    badge: dict | None = None
    source_link_he: str | None = None
    source_link_en: str | None = None
    default_poll_interval: int | None = None


@router.post("/validate", response_model=ValidateResponse)
@limiter.limit("20/minute")
async def validate_source_url(
    request: Request,
    body: ValidateRequest,
    db: AsyncSession = Depends(get_db),
):
    """Match a pasted URL against the registered manifests.

    Recognition is by URL shape only — no live fetch, so the answer is
    instant. The title comes from the manifest's template and is replaced
    with the site's real title on the first successful scrape (push_version's
    ``scrape_metadata.dataset_title_he``).
    """
    url = (body.url or "").strip()
    match = await source_registry.classify_url(db, url)
    if not match:
        return ValidateResponse(
            valid=False,
            error="URL does not match any registered source.",
        )

    display = source_registry.display_view(match.manifest)
    return ValidateResponse(
        valid=True,
        page_type=match.page_type,
        collector_name=match.collector_name,
        title=match.title,
        url=url,
        source_id=match.source_id,
        label_he=display["label_he"],
        label_en=display["label_en"],
        badge=display["badge"],
        source_link_he=display["source_link_he"],
        source_link_en=display["source_link_en"],
        default_poll_interval=display["default_poll_interval"],
    )


@router.get("/registry")
@limiter.limit("60/minute")
async def list_registered_sources(
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Display metadata for every enabled registered source.

    Badges only — no URL regexes. Python's ``(?P<name>…)`` named groups are a
    syntax error in a JavaScript RegExp, so the browser never evaluates a
    manifest pattern; it calls /validate instead.
    """
    manifests = await source_registry.load_enabled(db)
    response.headers["Cache-Control"] = "public, max-age=300"
    return {"sources": [source_registry.display_view(m) for m in manifests]}
