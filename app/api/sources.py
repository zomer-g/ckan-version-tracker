"""Public endpoints for declaratively-registered sources.

The fifteen built-in sources each have their own ``/api/<source>/validate``.
Sources registered by the worker have no code here, so these endpoints serve
all of them:

  POST /api/sources/validate  — classify a pasted URL against every manifest
  POST /api/sources/preview   — list the files on a pasted page, for the picker
  GET  /api/sources/registry  — badge/label metadata for the frontend

See app/services/source_registry.py for the manifest contract.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.rate_limit import limiter
from app.services import source_registry

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/sources", tags=["sources"])

# Sources whose pages hold many files and can be previewed before tracking, so
# the person choosing can see what they are choosing from.
#
# A manifest declaring ``file_picker`` is a REQUEST for this; the entry here is
# what makes it possible, because reading a page's file table is site-specific
# work that no manifest can express. A manifest that asks for a picker with no
# previewer registered simply gets none — the dataset is created tracking
# whatever its engine defaults to, which is the pre-picker behaviour and never
# an error.
#
# Each value is an async callable ``(url) -> {"title", "url", "files": [...]}``
# raising an exception whose message is safe to show. Keep the row shape in
# step with the worker's engine for that source, or the picker will offer files
# the scrape then ignores.
def _cbs_pub_previewer():
    from app.services.cbs_pub_preview import preview

    return preview


PREVIEWERS = {"cbs_pub": _cbs_pub_previewer}


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
    # Other datasets this URL will open alongside the one it names — a channel
    # brings its feed. Surfaced so the form can say so before the user submits:
    # one paste creating two datasets is a surprise worth spending a line on.
    companions: list[dict] | None = None
    # True when the tracking form should call POST /preview and offer the page's
    # files to tick. Both halves have to be true — the manifest asking for a
    # picker and this OVER build knowing how to read that source's pages — so
    # the frontend is told the answer rather than deriving it from either.
    file_picker: bool = False


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
        # The MATCHED PATTERN's cadence, not the source-wide default. The
        # request form seeds its frequency picker from this and then always
        # sends a value back, so declaring a per-pattern poll_interval had no
        # effect through the UI at all: telegram's feed (one page read, meant
        # for every 5 minutes) was offered at the whole-channel default of 24
        # hours, which made the feed poll exactly as often as the history it
        # exists to front-run. RegistryMatch.poll_interval already falls back
        # to the manifest default when a pattern declares none.
        default_poll_interval=max(
            match.poll_interval, settings.min_poll_interval,
        ),
        companions=[
            {
                "url": companion_url,
                "title": companion.title,
                "page_type": companion.page_type,
                "poll_interval": max(
                    companion.poll_interval, settings.min_poll_interval,
                ),
            }
            for companion_url in match.companion_urls
            # Resolve each through the registry, exactly as the creation path
            # will, so what is shown is what will actually be opened rather
            # than a template rendered hopefully.
            if (companion := await source_registry.classify_url(db, companion_url))
        ] or None,
        file_picker=bool(display.get("file_picker")) and match.source_id in PREVIEWERS,
    )


class PreviewRequest(BaseModel):
    url: str


@router.post("/preview")
# Tighter than /validate: this one leaves the building. Every call is a handful
# of requests to the source site, made on behalf of an anonymous visitor.
@limiter.limit("10/minute")
async def preview_source_url(
    request: Request,
    body: PreviewRequest,
    db: AsyncSession = Depends(get_db),
):
    """List the files a pasted page publishes, so they can be picked.

    Read-only and live: nothing is stored, no dataset exists yet, and no file
    is downloaded. Returns ``{"title", "url", "source_id", "files": [...]}``.
    A file with ``on_page: false`` sits in the same folder but is not part of
    this page's own table — shown, and left unticked, rather than hidden.

    Each file also carries ``tracked``: whether OVER already has a dataset for
    it, whether active or still awaiting approval. Without it, coming back to a
    page of 27 files of which 23 are already in the queue gives no way to see
    which four are left — the picker offers all 27, the submit reports them all
    as duplicates, and the whole thing reads as a failure.
    """
    url = (body.url or "").strip()
    match = await source_registry.classify_url(db, url)
    if not match:
        raise HTTPException(status_code=400, detail="URL does not match any registered source.")
    factory = PREVIEWERS.get(match.source_id)
    if not factory or not match.manifest.file_picker:
        raise HTTPException(
            status_code=400,
            detail=f"Source '{match.source_id}' does not publish a file list to pick from.",
        )
    try:
        result = await factory()(url)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("source preview failed for %s: %s", url, exc)
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    files = await _mark_tracked(db, result.get("files") or [])
    return {**result, "files": files, "source_id": match.source_id}


async def _mark_tracked(db: AsyncSession, files: list[dict]) -> list[dict]:
    """Stamp ``tracked`` on each file that already has a dataset.

    ONE query over the exact URLs, not a per-file identity lookup: the picker
    is what created these datasets and it stores the URL it shows here, so an
    exact match covers every file it opened. A file tracked under some other
    spelling is reported untracked and the submit reports it as a duplicate —
    the pre-existing behaviour, not a regression.

    Never fatal. The file list is the point of this endpoint; losing the
    annotation is a worse picker, losing the list is a broken one.
    """
    urls = [f.get("url") for f in files if f.get("url")]
    if not urls:
        return files
    try:
        from sqlalchemy import select

        from app.models.tracked_dataset import TrackedDataset

        rows = (await db.execute(
            select(TrackedDataset.source_url, TrackedDataset.id,
                   TrackedDataset.status)
            # A rejected dataset is NOT "already tracked" — nothing will ever
            # scrape it. Showing those files as taken is what left a whole
            # publication un-addable after one batch rejection.
            .where(TrackedDataset.source_url.in_(urls),
                   TrackedDataset.status != "rejected")
        )).all()
    except Exception:  # noqa: BLE001
        logger.warning("preview: could not mark tracked files", exc_info=True)
        return files
    known = {r[0]: {"dataset_id": str(r[1]), "status": r[2]} for r in rows}
    return [
        {**f, "tracked": f.get("url") in known,
         **({"tracked_dataset": known[f["url"]]} if f.get("url") in known else {})}
        for f in files
    ]


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
