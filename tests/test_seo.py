"""The server-rendered head, robots.txt and sitemap.xml.

Before this layer every URL on the site — home page and all ~1,250 dataset
pages alike — served one title, no description, no canonical and no structured
data, because the body is client-rendered and the app writes its titles in
JavaScript. These tests pin the parts a crawler actually reads.

Runs against in-memory SQLite, driven by asyncio.run, matching the repo's
dependency-light style (see tests/test_auth_codes.py).
"""
import asyncio
import os
import re
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.services import seo  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


def _strip_comments(html: str) -> str:
    """The template's own comment mentions <title>; drop comments before counting."""
    return re.sub(r"<!--.*?-->", "", html, flags=re.S)


TEMPLATE = (
    "<!DOCTYPE html><html lang=\"he\"><head>\n"
    "    <!--SEO:START-->\n"
    "    <title>גרסאות לעם</title>\n"
    "    <!--SEO:END-->\n"
    "</head><body><div id=\"root\"></div></body></html>"
)


# ── rendering ───────────────────────────────────────────────────────────────

def test_render_replaces_the_fallback_title_rather_than_joining_it():
    out = seo.render(TEMPLATE, seo.PageMeta(title="עמוד בדיקה", canonical_path="/x"))
    body = _strip_comments(out)
    assert len(re.findall(r"<title>", body)) == 1
    assert "עמוד בדיקה — גרסאות לעם" in body
    assert "SEO:START" not in out and "SEO:END" not in out


def test_render_keeps_the_spa_mount_point():
    out = seo.render(TEMPLATE, seo.PageMeta(title="x"))
    assert '<div id="root"></div>' in out


def test_render_falls_back_to_the_title_element_when_markers_are_absent():
    """A dist/ built before the markers existed must still get the tags."""
    old_build = TEMPLATE.replace("<!--SEO:START-->\n", "").replace("<!--SEO:END-->\n", "")
    out = seo.render(old_build, seo.PageMeta(title="ישן", canonical_path="/old"))
    assert "ישן — גרסאות לעם" in out
    assert 'rel="canonical"' in out


def test_render_survives_a_template_with_no_head_hooks_at_all():
    plain = "<html><body><div id=\"root\"></div></body></html>"
    assert seo.render(plain, seo.PageMeta(title="x")) == plain


def test_site_name_page_is_not_doubled_in_the_title():
    out = seo.render(TEMPLATE, seo.PageMeta(title=seo.SITE_NAME))
    assert "גרסאות לעם — גרסאות לעם" not in _strip_comments(out)


def test_head_carries_the_tags_a_crawler_and_a_link_unfurl_need():
    html = seo.head_html(seo.PageMeta(title="כותרת", description="תיאור", canonical_path="/p"))
    assert 'name="description" content="תיאור"' in html
    assert f'rel="canonical" href="{seo.SITE_URL}/p"' in html
    for prop in ("og:title", "og:description", "og:url", "og:site_name", "og:locale"):
        assert f'property="{prop}"' in html
    assert 'name="twitter:card"' in html


def test_quotes_in_a_dataset_title_cannot_break_out_of_an_attribute():
    nasty = 'מאגר "מצוטט" <script>alert(1)</script>'
    html = seo.head_html(seo.PageMeta(title=nasty, description=nasty))
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_jsonld_cannot_close_the_script_element_early():
    node = {"@context": "https://schema.org", "@type": "Dataset", "name": "a</script>b"}
    html = seo.head_html(seo.PageMeta(title="t", jsonld=[node]))
    # The sitewide WebSite node is emitted first, so take the block that
    # actually carries the payload rather than whichever comes first.
    blocks = [b.split("</script>")[0] for b in html.split('application/ld+json">')[1:]]
    payload = next(b for b in blocks if "Dataset" in b)
    assert "</script>" not in payload
    assert "<\\/script>" in payload


def test_every_page_carries_the_sitewide_website_node():
    html = seo.head_html(seo.PageMeta(title="t"))
    assert '"@type":"WebSite"' in html
    assert '"SearchAction"' in html


# ── route metadata ──────────────────────────────────────────────────────────

def test_static_routes_each_get_their_own_title_and_description():
    metas = [_run(seo.meta_for(p)) for p in ("/", "/about", "/data", "/knesset", "/api")]
    titles = [m.title for m in metas]
    descriptions = [m.description for m in metas]
    assert len(set(titles)) == len(titles), "titles must be distinct per route"
    assert len(set(descriptions)) == len(descriptions), "descriptions must be distinct"
    assert all(m.canonical_path.startswith("/") for m in metas)


def test_trailing_slash_resolves_to_the_same_route():
    assert _run(seo.meta_for("/about/")).title == _run(seo.meta_for("/about")).title


def test_admin_is_noindex():
    assert _run(seo.meta_for("/admin")).noindex is True
    assert _run(seo.meta_for("/admin/login")).noindex is True
    assert "noindex" in seo.head_html(_run(seo.meta_for("/admin")))


def test_public_pages_are_indexable():
    html = seo.head_html(_run(seo.meta_for("/")))
    assert "noindex" not in html
    assert 'content="index, follow' in html


def test_an_unknown_route_falls_back_to_the_site_default_without_raising():
    meta = _run(seo.meta_for("/no/such/page"))
    assert meta.title == seo.SITE_NAME
    assert meta.description == seo.DEFAULT_DESCRIPTION


def test_a_lookup_failure_never_propagates():
    """The DB being down must cost the head, not the page."""
    seo._cache.clear()

    async def boom(_path):
        raise RuntimeError("database is down")

    original = seo._lookup_dynamic
    seo._lookup_dynamic = boom
    try:
        meta = _run(seo.meta_for("/versions/" + str(uuid.uuid4())))
    finally:
        seo._lookup_dynamic = original
    assert meta.title == seo.SITE_NAME


# ── dataset pages, against a real database ──────────────────────────────────

async def _seeded_session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        # dataset_tags too: Tag.datasets is a secondary relationship, so
        # loading a Tag touches the association table.
        for table in (
            TrackedDataset.__table__, Organization.__table__, Tag.__table__, dataset_tags,
        ):
            await conn.run_sync(lambda c, t=table: t.create(c, checkfirst=True))
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    ds_id, org_id, tag_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with factory() as db:
        db.add(TrackedDataset(
            id=ds_id, ckan_id="c1", ckan_name="fire-stations", title="תחנות כיבוי",
            organization="כבאות והצלה לישראל", status="active",
            source_url="https://data.gov.il/dataset/fire", poll_interval=3600,
        ))
        db.add(Organization(id=org_id, name="moj", title="משרד המשפטים"))
        db.add(Tag(id=tag_id, name="תחבורה"))
        await db.commit()
    return factory, ds_id, org_id, tag_id


def test_a_dataset_page_gets_its_own_title_description_and_dataset_markup():
    factory, ds_id, _, _ = _run(_seeded_session_factory())
    seo._cache.clear()
    import app.database as database
    original = database.async_session
    database.async_session = factory
    try:
        meta = _run(seo.meta_for(f"/versions/{ds_id}"))
    finally:
        database.async_session = original

    assert meta.title == "תחנות כיבוי"
    assert "תחנות כיבוי" in meta.description
    assert "כבאות והצלה לישראל" in meta.description
    assert meta.canonical_path == f"/versions/{ds_id}"

    html = seo.head_html(meta)
    assert '"@type":"Dataset"' in html
    assert '"DataCatalog"' in html
    assert '"isAccessibleForFree":true' in html
    assert "data.gov.il/dataset/fire" in html


def test_organization_and_tag_pages_resolve_their_names():
    factory, _, org_id, tag_id = _run(_seeded_session_factory())
    seo._cache.clear()
    import app.database as database
    original = database.async_session
    database.async_session = factory
    try:
        org = _run(seo.meta_for(f"/organizations/{org_id}"))
        seo._cache.clear()
        tag = _run(seo.meta_for(f"/tags/{tag_id}"))
    finally:
        database.async_session = original
    assert org.title == "משרד המשפטים"
    assert tag.title == "תחבורה"


def test_a_hidden_dataset_does_not_get_an_indexable_page():
    """status outside active/pending is not public, so it must not resolve."""
    factory, _, _, _ = _run(_seeded_session_factory())
    hidden = uuid.uuid4()

    async def seed():
        async with factory() as db:
            db.add(TrackedDataset(
                id=hidden, ckan_id="c2", ckan_name="secret", title="מוסתר",
                status="hidden", poll_interval=3600,
            ))
            await db.commit()
    _run(seed())

    seo._cache.clear()
    import app.database as database
    original = database.async_session
    database.async_session = factory
    try:
        meta = _run(seo.meta_for(f"/versions/{hidden}"))
    finally:
        database.async_session = original
    assert meta.title == seo.SITE_NAME, "a hidden dataset must fall back to the generic head"


def test_a_malformed_id_does_not_reach_the_database():
    seo._cache.clear()
    assert _run(seo.meta_for("/versions/not-a-uuid")).title == seo.SITE_NAME


# ── robots.txt ──────────────────────────────────────────────────────────────

def test_robots_points_at_the_sitemap_and_keeps_crawlers_off_the_api():
    from app.api import seo as seo_api
    body = _run(seo_api.robots()).body.decode()
    assert f"Sitemap: {seo.SITE_URL}/sitemap.xml" in body
    assert "Disallow: /admin" in body
    assert "Disallow: /api/" in body
    assert "Allow: /" in body


# ── sitemap.xml ─────────────────────────────────────────────────────────────

def test_sitemap_lists_the_static_routes_and_every_public_dataset():
    from app.api import seo as seo_api
    factory, ds_id, org_id, tag_id = _run(_seeded_session_factory())
    import app.database as database
    original = database.async_session
    database.async_session = factory
    seo_api._sitemap_cache = None
    try:
        xml = _run(seo_api._build_sitemap())
    finally:
        database.async_session = original

    assert xml.startswith('<?xml version="1.0" encoding="UTF-8"?>')
    assert "<urlset" in xml and "</urlset>" in xml
    assert f"{seo.SITE_URL}/</loc>" in xml
    assert f"{seo.SITE_URL}/versions/{ds_id}</loc>" in xml
    assert f"{seo.SITE_URL}/organizations/{org_id}</loc>" in xml
    assert f"{seo.SITE_URL}/tags/{tag_id}</loc>" in xml
    # admin must never be advertised
    assert "/admin" not in xml


def test_sitemap_omits_the_bulk_knesset_committee_rows():
    """~2,900 one-per-meeting rows are represented by /knesset, not by a page each."""
    from app.api import seo as seo_api
    factory, _, _, _ = _run(_seeded_session_factory())
    noisy = uuid.uuid4()

    async def seed():
        async with factory() as db:
            db.add(TrackedDataset(
                id=noisy, ckan_id="k1", ckan_name="knesset-committee-single-123",
                title="ועדה", status="active", poll_interval=3600,
            ))
            await db.commit()
    _run(seed())

    import app.database as database
    original = database.async_session
    database.async_session = factory
    seo_api._sitemap_cache = None
    try:
        xml = _run(seo_api._build_sitemap())
    finally:
        database.async_session = original
    assert str(noisy) not in xml


def test_sitemap_still_serves_the_static_routes_when_the_database_fails():
    from app.api import seo as seo_api
    import app.database as database

    class Boom:
        def __call__(self):
            raise RuntimeError("no database")

    original = database.async_session
    database.async_session = Boom()
    seo_api._sitemap_cache = None
    try:
        xml = _run(seo_api._build_sitemap())
    finally:
        database.async_session = original
    assert "</urlset>" in xml
    assert f"{seo.SITE_URL}/</loc>" in xml


# ── the wiring, over real HTTP ──────────────────────────────────────────────
# The tests above prove the pieces. These prove main.py actually calls them:
# that a request for a route comes back with the composed head rather than the
# shell, and that robots/sitemap are reachable at their real paths.
#
# httpx.ASGITransport drives the app without running its lifespan, so no
# scheduler, no worker and no external connection is required.

def _app_client():
    import httpx
    import app.main as main_module
    return httpx.ASGITransport(app=main_module.app)


def test_a_route_is_served_with_its_own_head_not_the_bare_shell():
    import httpx

    async def go():
        async with httpx.AsyncClient(transport=_app_client(), base_url="http://t") as c:
            return await c.get("/about")

    r = _run(go())
    assert r.status_code == 200
    body = _strip_comments(r.text)
    assert "אודות — גרסאות לעם" in body
    assert 'rel="canonical"' in body
    assert 'property="og:title"' in body
    # the shell's own fallback title must be gone, not duplicated
    assert len(re.findall(r"<title>", body)) == 1
    # and the SPA still has somewhere to mount
    assert '<div id="root"></div>' in r.text


def test_two_different_routes_do_not_share_a_title():
    import httpx

    async def go(path):
        async with httpx.AsyncClient(transport=_app_client(), base_url="http://t") as c:
            r = await c.get(path)
        return re.search(r"<title>(.*?)</title>", _strip_comments(r.text)).group(1)

    assert _run(go("/data")) != _run(go("/knesset"))


def test_html_is_cached_briefly_and_revalidated():
    """Long enough to absorb a burst, short enough not to pin a stale bundle."""
    import httpx

    async def go():
        async with httpx.AsyncClient(transport=_app_client(), base_url="http://t") as c:
            return await c.get("/")

    cc = _run(go()).headers.get("cache-control", "")
    assert "max-age=60" in cc
    assert "stale-while-revalidate" in cc


def test_robots_and_sitemap_answer_at_their_real_paths():
    """Both used to fall through the SPA catch-all and return HTML with a 200."""
    import httpx

    async def go(path):
        async with httpx.AsyncClient(transport=_app_client(), base_url="http://t") as c:
            return await c.get(path)

    robots = _run(go("/robots.txt"))
    assert robots.status_code == 200
    assert robots.headers["content-type"].startswith("text/plain")
    assert "Sitemap:" in robots.text

    sitemap = _run(go("/sitemap.xml"))
    assert sitemap.status_code == 200
    assert "xml" in sitemap.headers["content-type"]
    assert sitemap.text.startswith("<?xml")
    assert "<loc>" in sitemap.text


# ── what the search result actually says ────────────────────────────────────

def test_creator_uses_the_organization_name_not_the_source_slug():
    """A result reading "interior_affairs" is a result nobody clicks."""
    factory, ds_id, org_id, _ = _run(_seeded_session_factory())

    async def link():
        async with factory() as db:
            ds = (await db.execute(
                select(TrackedDataset).where(TrackedDataset.id == ds_id)
            )).scalar_one()
            ds.organization = "interior_affairs"
            ds.organization_id = org_id
            await db.commit()
    _run(link())

    seo._cache.clear()
    import app.database as database
    original = database.async_session
    database.async_session = factory
    try:
        meta = _run(seo.meta_for(f"/versions/{ds_id}"))
    finally:
        database.async_session = original

    html = seo.head_html(meta)
    assert "משרד המשפטים" in html
    assert "interior_affairs" not in meta.description


def test_a_ckan_dataset_points_back_at_its_publisher():
    """sameAs is how the markup says this is a mirror, not the origin."""
    class Fake:
        id = uuid.uuid4()
        title = "מאגר"
        organization = "interior_affairs"
        ckan_name = "beaches"
        source_url = None
        source_type = "ckan"
        last_polled_at = None

    node = seo._dataset_jsonld(Fake())
    assert node["sameAs"] == "https://data.gov.il/he/datasets/interior_affairs/beaches"


def test_a_scraped_source_without_a_url_claims_no_origin():
    class Fake:
        id = uuid.uuid4()
        title = "מאגר"
        organization = "somewhere"
        ckan_name = "x"
        source_url = None
        source_type = "scraper"
        last_polled_at = None

    assert "sameAs" not in seo._dataset_jsonld(Fake())
