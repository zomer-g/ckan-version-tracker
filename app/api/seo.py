"""robots.txt and sitemap.xml.

Neither existed. Both paths fell through the SPA catch-all and answered 200
with the app shell — so a crawler asking for /robots.txt got HTML, and there
was nothing anywhere that named the ~1,250 dataset pages. Combined with a
client-rendered body, that left the deep pages with no discovery path at all:
not linked in any server-rendered HTML, and not listed in a sitemap.

The sitemap is generated from the database rather than kept as a file, because
the catalogue changes on every poll and a stale sitemap is worse than none.
It is cached for an hour: crawlers re-fetch it often, and it costs a full scan
of the tracked table.

Sitemaps cap at 50,000 URLs / 50 MB uncompressed. The catalogue is nowhere near
that today, so this emits a single sitemap; ``_URL_CAP`` keeps it honest if the
corpus ever grows past it, rather than silently emitting an invalid file.
"""
from __future__ import annotations

import asyncio
import html
import logging
import time
from datetime import datetime, timezone

from fastapi import APIRouter, Response
from sqlalchemy import select

from app.services.seo import SITE_URL

logger = logging.getLogger(__name__)

router = APIRouter(tags=["seo"])

_TTL_SECONDS = 3600.0
_URL_CAP = 45_000

_sitemap_cache: tuple[float, str] | None = None
_lock = asyncio.Lock()

# Static routes worth indexing, with the weight they deserve relative to each
# other. Admin, login and feedback are deliberately absent.
_STATIC_ROUTES: list[tuple[str, str, str]] = [
    ("/", "daily", "1.0"),
    ("/data", "daily", "0.9"),
    ("/knesset", "daily", "0.8"),
    ("/projects/questions", "weekly", "0.8"),
    ("/organizations", "weekly", "0.7"),
    ("/tags", "weekly", "0.7"),
    ("/sources", "weekly", "0.7"),
    ("/about", "monthly", "0.6"),
    ("/rationale", "monthly", "0.6"),
    ("/api", "monthly", "0.6"),
    ("/data/explore", "weekly", "0.6"),
    ("/data/guide", "monthly", "0.5"),
    ("/data/normalize", "monthly", "0.5"),
    ("/cbs", "weekly", "0.6"),
    ("/lookup", "monthly", "0.4"),
    ("/projects/ocal", "weekly", "0.6"),
    ("/projects/ocoi", "weekly", "0.6"),
    ("/projects/nadlan", "weekly", "0.6"),
    ("/projects/odata", "weekly", "0.6"),
    ("/growth", "monthly", "0.5"),
]


def _url(loc: str, lastmod: str | None, changefreq: str, priority: str) -> str:
    out = f"  <url>\n    <loc>{html.escape(SITE_URL + loc, quote=True)}</loc>\n"
    if lastmod:
        out += f"    <lastmod>{lastmod}</lastmod>\n"
    out += f"    <changefreq>{changefreq}</changefreq>\n"
    out += f"    <priority>{priority}</priority>\n  </url>\n"
    return out


async def _build_sitemap() -> str:
    from app.database import async_session
    from app.models.organization import Organization
    from app.models.tag import Tag
    from app.models.tracked_dataset import TrackedDataset

    body = "".join(_url(p, None, freq, pri) for p, freq, pri in _STATIC_ROUTES)
    count = len(_STATIC_ROUTES)

    try:
        async with async_session() as db:
            rows = (
                await db.execute(
                    select(
                        TrackedDataset.id,
                        TrackedDataset.last_polled_at,
                        TrackedDataset.updated_at,
                    )
                    .where(
                        TrackedDataset.status.in_(("active", "pending")),
                        # The ~2,900 one-per-meeting Knesset committee rows are
                        # bulk-managed and not browsable pages; /knesset is the
                        # page that represents them. Same exclusion the public
                        # catalogue endpoint applies.
                        TrackedDataset.ckan_name.notlike("knesset-committee-single-%"),
                    )
                    .order_by(TrackedDataset.created_at.desc())
                )
            ).all()

            for ds_id, polled, updated in rows:
                if count >= _URL_CAP:
                    break
                stamp = polled or updated
                lastmod = stamp.date().isoformat() if stamp else None
                body += _url(f"/versions/{ds_id}", lastmod, "weekly", "0.8")
                count += 1

            for (org_id,) in (
                await db.execute(select(Organization.id))
            ).all():
                if count >= _URL_CAP:
                    break
                body += _url(f"/organizations/{org_id}", None, "weekly", "0.6")
                count += 1

            for (tag_id,) in (await db.execute(select(Tag.id))).all():
                if count >= _URL_CAP:
                    break
                body += _url(f"/tags/{tag_id}", None, "weekly", "0.5")
                count += 1
    except Exception:  # noqa: BLE001
        # A sitemap of the static routes still beats a 500: the crawler keeps a
        # valid document and the deep pages come back on the next build.
        logger.warning("sitemap: dynamic section failed, serving static only", exc_info=True)

    if count >= _URL_CAP:
        logger.warning("sitemap hit the %d URL cap — split into an index", _URL_CAP)

    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f"{body}</urlset>\n"
    )


@router.get("/sitemap.xml", include_in_schema=False)
async def sitemap() -> Response:
    global _sitemap_cache
    now = time.monotonic()
    if _sitemap_cache and now - _sitemap_cache[0] < _TTL_SECONDS:
        xml = _sitemap_cache[1]
    else:
        async with _lock:
            if _sitemap_cache and time.monotonic() - _sitemap_cache[0] < _TTL_SECONDS:
                xml = _sitemap_cache[1]
            else:
                xml = await _build_sitemap()
                _sitemap_cache = (time.monotonic(), xml)
    return Response(
        content=xml,
        media_type="application/xml",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/robots.txt", include_in_schema=False)
async def robots() -> Response:
    # /api/ is disallowed for crawling but the pages are not: the JSON is not
    # search content, and letting a crawler spend its budget on ~1,250 dataset
    # payloads instead of the pages is the opposite of what we want.
    body = (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /admin\n"
        "Disallow: /api/\n"
        "Disallow: /direct/\n"
        "Disallow: /cbs/feedback\n"
        "\n"
        f"Sitemap: {SITE_URL}/sitemap.xml\n"
    )
    return Response(
        content=body,
        media_type="text/plain; charset=utf-8",
        headers={"Cache-Control": "public, max-age=86400"},
    )
