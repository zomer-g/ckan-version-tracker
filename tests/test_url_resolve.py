"""Tests for the source-URL resolver — "does OVER already track this link?".

This is the machinery behind the /lookup deep link and the fix for the bug it
was born from: pasting a GovMap layer URL into the search box opened a REQUEST
FORM for a layer OVER already had, and only after submitting did the server
say "duplicate". The cause was string equality — GovMap writes the current
viewport into the URL, so the copied ``?c=219143.61,618345.06&lay=11`` never
equalled the stored ``?lay=11``. Migration 035 cleaned up ~20 duplicate layers
created that way.

So the invariants locked here are:

  1. Noise in a copied URL (viewport, ``www.``, trailing slash, param order,
     utm_*) never hides an existing dataset.
  2. Noise is not confused with MEANING — jeden.co.il's ``?category=tenders``
     and ``?category=decisions`` stay two different things.
  3. ``strict=True`` (duplicate checks) accepts identity matches only, so a
     second corpus cut from the same page can still be requested.
  4. A hash-routed SPA's route is MEANING, an anchor is noise — the
     ykpubdata.jerusalem.muni.il register, its document corpus and a single
     building file are three identities, while ``/about#contact`` is still
     ``/about``.

Runs against in-memory SQLite, driven by asyncio.run — matching the repo's
dependency-light test style (see tests/test_auth_codes.py).
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services.dataset_lookup import find_datasets_for_url  # noqa: E402
from app.services.url_identity import canonical_url, path_key, url_identity  # noqa: E402


GOVMAP_STORED = "https://www.govmap.gov.il/?lay=11"
GOVMAP_COPIED = "https://www.govmap.gov.il/?c=219143.61,618345.06&lay=11"

# ykpubdata.jerusalem.muni.il — Jerusalem building licensing. A React SPA whose
# entire route lives in the hash, and the reason identities look at fragments.
YK_REGISTER = "https://ykpubdata.jerusalem.muni.il/#/?SystemCode=26400046"
YK_DOCUMENTS = "https://ykpubdata.jerusalem.muni.il/#/documents?SystemCode=26400046"
YK_FILE = ("https://ykpubdata.jerusalem.muni.il/"
           "#/Details?TikNum=2004/0196.00&SystemCode=26400046")


def _run(coro):
    return asyncio.run(coro)


async def _seed(rows):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
        await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        for row in rows:
            db.add(TrackedDataset(**{
                "id": uuid.uuid4(),
                "ckan_id": row.get("ckan_name", "x"),
                "ckan_name": row.get("ckan_name", "x"),
                "title": row.get("title", "T"),
                "status": row.get("status", "active"),
                "is_active": row.get("is_active", True),
                **{k: v for k, v in row.items() if k not in ("ckan_name", "title", "status", "is_active")},
            }))
        await db.commit()
    return Session


# ── identity: what a URL denotes ─────────────────────────────────────────

def test_govmap_viewport_params_do_not_change_the_layer():
    """The bug, at its root: these two URLs are the same layer."""
    assert url_identity(GOVMAP_COPIED) == ("govmap", "11")
    assert url_identity(GOVMAP_STORED) == ("govmap", "11")
    assert url_identity(GOVMAP_COPIED) == url_identity(GOVMAP_STORED)


def test_govmap_different_layers_are_different():
    assert url_identity("https://www.govmap.gov.il/?lay=12") != url_identity(GOVMAP_STORED)


def test_govmap_config_layer_id_wins_over_the_url():
    """A stored dataset's config is authoritative — its source_url may have
    been written in any shape."""
    assert url_identity(
        "https://www.govmap.gov.il/", "govmap", {"layer_id": "227195"}
    ) == ("govmap", "227195")


def test_www_slash_param_order_and_utm_are_all_noise():
    a = url_identity("https://www.example.gov.il/collectors/x/?b=2&a=1")
    b = url_identity("http://example.gov.il/collectors/x?a=1&b=2&utm_source=twitter")
    assert a == b == ("url", "example.gov.il/collectors/x?a=1&b=2")


def test_a_meaningful_query_param_is_not_noise():
    """jeden.co.il serves both its corpora from one page; ?category= is the
    only thing telling them apart, so it must survive canonicalisation."""
    tenders = url_identity("https://jeden.co.il/?category=tenders")
    decisions = url_identity("https://jeden.co.il/?category=decisions")
    assert tenders != decisions
    assert path_key("https://jeden.co.il/?category=tenders") == "jeden.co.il/"


def test_ckan_permalink_shapes_agree():
    assert url_identity("https://data.gov.il/dataset/my-pkg") == ("ckan", "my-pkg")
    assert url_identity("https://data.gov.il/he/datasets/mot/my-pkg") == ("ckan", "my-pkg")
    assert url_identity(
        "https://data.gov.il/he/datasets/mot/my-pkg/abcdef12-1234-1234-1234-123456789abc"
    ) == ("ckan", "my-pkg")


# ── route fragments: a hash-routed SPA is not one dataset ────────────────

def test_the_three_ykpubdata_routes_are_three_identities():
    """The bug this rule exists for: all three used to canonicalise to the bare
    host, so the site could hold exactly ONE dataset — the second request was
    refused as "already tracked" and a per-building-file dataset was
    impossible."""
    ids = [url_identity(u) for u in (YK_REGISTER, YK_DOCUMENTS, YK_FILE)]
    assert all(i is not None for i in ids)
    assert len(set(ids)) == 3


def test_a_plain_anchor_is_still_the_same_page():
    """An ordinary fragment is a scroll position, not a route. Making
    ``#contact`` meaningful would split ordinary sites into fake datasets."""
    assert url_identity("https://example.gov.il/about#contact") == \
        url_identity("https://example.gov.il/about")
    # Chrome's scroll-to-text fragment is an anchor too — it starts with ":~:".
    assert url_identity("https://example.gov.il/about#:~:text=%D7%A9%D7%9C%D7%95%D7%9D") == \
        url_identity("https://example.gov.il/about")


def test_a_bare_root_route_adds_nothing():
    """``site/#/`` is how a hash router spells the home page — the same page as
    ``site/``, so the empty route must not become a second identity."""
    assert url_identity("https://ykpubdata.jerusalem.muni.il/#/") == \
        url_identity("https://ykpubdata.jerusalem.muni.il/")


def test_hashbang_routes_count_as_routes():
    assert url_identity("https://example.gov.il/#!/reports") == \
        url_identity("https://example.gov.il/#/reports")
    assert url_identity("https://example.gov.il/#!/reports") != \
        url_identity("https://example.gov.il/")


def test_noise_inside_a_route_is_still_noise():
    """The route is canonicalised like a path+query: param order and utm_*
    inside the hash can't mint a second identity for one building file."""
    a = url_identity("https://ykpubdata.jerusalem.muni.il/"
                     "#/Details?SystemCode=26400046&TikNum=2004/0196.00")
    b = url_identity("https://ykpubdata.jerusalem.muni.il/"
                     "#/Details?TikNum=2004/0196.00&SystemCode=26400046&utm_source=wa")
    assert a == b == ("url", "ykpubdata.jerusalem.muni.il/"
                             "#/Details?SystemCode=26400046&TikNum=2004/0196.00")


def test_the_route_does_not_leak_into_the_looser_path_lookup():
    """path_key stays fragment-blind on purpose — every route of an SPA is
    served from the same page, so a bare host link offers them all as a
    "did you mean"."""
    assert path_key(YK_REGISTER) == path_key(YK_DOCUMENTS) == path_key(YK_FILE) \
        == "ykpubdata.jerusalem.muni.il/"


def test_existing_sources_are_untouched_by_the_fragment_rule():
    """The URL shapes already in the catalog carry no fragment, so their
    identities must be byte-identical to what they were before the rule."""
    assert url_identity(GOVMAP_COPIED) == ("govmap", "11")
    assert url_identity(
        "https://www.govmap.gov.il/?c=183604.69,572861.25&lay=228012&z=6"
    ) == ("govmap", "228012")
    assert url_identity(
        "https://data.gov.il/he/datasets/ministry_of_social_affairs/social-workers-registration"
    ) == ("ckan", "social-workers-registration")
    assert url_identity("https://avodata.labor.gov.il/occupations") == \
        ("url", "avodata.labor.gov.il/occupations")
    assert url_identity("https://workagreements.labor.gov.il") == \
        ("url", "workagreements.labor.gov.il/")
    assert url_identity(
        "https://ykpubdata.jerusalem.muni.il/?corpus=documents&SystemCode=26400046"
    ) == ("url", "ykpubdata.jerusalem.muni.il/?SystemCode=26400046&corpus=documents")


def test_non_urls_have_no_identity():
    """A keyword search must never be treated as a URL and match a dataset."""
    assert url_identity("") is None
    assert url_identity(None) is None
    assert url_identity("החלטות ממשלה") is None
    assert canonical_url("just a search phrase") is None


# ── resolution against the catalog ───────────────────────────────────────

def test_copied_govmap_link_resolves_to_the_stored_layer():
    async def go():
        Session = await _seed([
            {"ckan_name": "govmap-11", "title": "תחנות הידרומטריות",
             "source_type": "govmap", "source_url": GOVMAP_STORED,
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
        ])
        async with Session() as db:
            hits = await find_datasets_for_url(db, GOVMAP_COPIED)
        assert len(hits) == 1
        assert hits[0]["title"] == "תחנות הידרומטריות"
        assert hits[0]["match"] == "identity"
        assert hits[0]["version_count"] == 0

    _run(go())


def test_an_untracked_layer_resolves_to_nothing():
    """The other half of the deep link: no match is what sends the user to
    the collection request form."""
    async def go():
        Session = await _seed([
            {"ckan_name": "govmap-11", "source_type": "govmap",
             "source_url": GOVMAP_STORED,
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
        ])
        async with Session() as db:
            hits = await find_datasets_for_url(
                db, "https://www.govmap.gov.il/?c=1,2&lay=99"
            )
        assert hits == []

    _run(go())


def test_demoted_duplicates_are_never_returned():
    async def go():
        Session = await _seed([
            {"ckan_name": "govmap-11-dup", "title": "כפילות",
             "source_type": "govmap", "source_url": GOVMAP_COPIED,
             "status": "duplicate",
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
            {"ckan_name": "govmap-11", "title": "המקורי",
             "source_type": "govmap", "source_url": GOVMAP_STORED,
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
        ])
        async with Session() as db:
            hits = await find_datasets_for_url(db, GOVMAP_COPIED)
        assert [h["title"] for h in hits] == ["המקורי"]

    _run(go())


def test_one_page_two_corpora_is_ambiguous_not_a_duplicate():
    """A bare jeden.co.il link matches both corpora by PATH — the caller must
    show the choice. And because a path match is not an identity match, the
    strict (duplicate-check) form returns nothing, so requesting the second
    corpus is still allowed."""
    async def go():
        Session = await _seed([
            {"ckan_name": "eden-tenders", "title": "מכרזים",
             "source_type": "scraper", "source_url": "https://jeden.co.il/?category=tenders"},
            {"ckan_name": "eden-decisions", "title": "החלטות",
             "source_type": "scraper", "source_url": "https://jeden.co.il/?category=decisions"},
        ])
        async with Session() as db:
            loose = await find_datasets_for_url(db, "https://jeden.co.il/")
            strict = await find_datasets_for_url(db, "https://jeden.co.il/", strict=True)
            exact = await find_datasets_for_url(db, "https://jeden.co.il/?category=decisions")
        assert len(loose) == 2 and all(h["match"] == "path" for h in loose)
        assert strict == []
        assert [h["title"] for h in exact] == ["החלטות"]

    _run(go())


def test_a_second_spa_route_can_be_requested_while_the_first_is_tracked():
    """The production failure: POST /api/datasets/requests answered "400 Already
    tracked" for the document corpus because it shared an identity with the
    register. strict=True is what that check uses, so it is what this asserts —
    and a genuine re-submission of the tracked route must still be refused."""
    async def go():
        Session = await _seed([
            {"ckan_name": "ykpubdata-all-665e9000", "title": "תיקי רישוי ובנייה",
             "source_type": "scraper", "source_url": YK_REGISTER},
        ])
        async with Session() as db:
            second_corpus = await find_datasets_for_url(db, YK_DOCUMENTS, strict=True)
            one_file = await find_datasets_for_url(db, YK_FILE, strict=True)
            resubmit = await find_datasets_for_url(db, YK_REGISTER, strict=True)
            # …including a re-submission written with the params the other way
            # round and a viewport-style tracking param bolted on.
            resubmit_noisy = await find_datasets_for_url(
                db, YK_REGISTER + "&utm_source=telegram", strict=True,
            )
        assert second_corpus == [], "a second corpus must be requestable"
        assert one_file == [], "a single building file must be requestable"
        assert [h["title"] for h in resubmit] == ["תיקי רישוי ובנייה"]
        assert [h["title"] for h in resubmit_noisy] == ["תיקי רישוי ובנייה"]

    _run(go())


def test_every_route_of_one_spa_resolves_to_its_own_dataset():
    """Three datasets, three routes, no crossover — and a bare link to the site
    is ambiguous rather than a guess."""
    async def go():
        Session = await _seed([
            {"ckan_name": "yk-all", "title": "המרשם", "source_type": "scraper",
             "source_url": YK_REGISTER},
            {"ckan_name": "yk-documents", "title": "המסמכים", "source_type": "scraper",
             "source_url": YK_DOCUMENTS},
            {"ckan_name": "yk-file", "title": "תיק 2004/0196.00",
             "source_type": "scraper", "source_url": YK_FILE},
        ])
        async with Session() as db:
            register = await find_datasets_for_url(db, YK_REGISTER)
            docs = await find_datasets_for_url(db, YK_DOCUMENTS)
            one = await find_datasets_for_url(db, YK_FILE)
            bare = await find_datasets_for_url(db, "https://ykpubdata.jerusalem.muni.il/")
        assert [h["title"] for h in register] == ["המרשם"]
        assert [h["title"] for h in docs] == ["המסמכים"]
        assert [h["title"] for h in one] == ["תיק 2004/0196.00"]
        assert len(bare) == 3 and all(h["match"] == "path" for h in bare)

    _run(go())


def test_ckan_package_link_resolves_by_package_name():
    """CKAN datasets are tracked per resource and carry no source_url — the
    package name is the link back, and one package legitimately resolves to
    several tracked resources."""
    async def go():
        Session = await _seed([
            {"ckan_name": "bus-stops", "title": "תחנות — קובץ א", "source_type": "ckan"},
            {"ckan_name": "bus-stops", "title": "תחנות — קובץ ב", "source_type": "ckan"},
            {"ckan_name": "other-pkg", "title": "אחר", "source_type": "ckan"},
        ])
        async with Session() as db:
            hits = await find_datasets_for_url(
                db, "https://data.gov.il/he/datasets/mot/bus-stops"
            )
        assert sorted(h["title"] for h in hits) == ["תחנות — קובץ א", "תחנות — קובץ ב"]

    _run(go())


def test_the_endpoint_answers_both_halves_of_the_deep_link():
    """GET /api/resolve is what /lookup calls. A miss is a normal 200 with
    found=false — that's the signal to open the request form, not an error."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from slowapi import _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded

    from app.api.resolve import router as resolve_router
    from app.database import get_db
    from app.rate_limit import limiter

    async def go():
        return await _seed([
            {"ckan_name": "govmap-11", "title": "תחנות הידרומטריות",
             "source_type": "govmap", "source_url": GOVMAP_STORED,
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
        ])

    Session = _run(go())

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(resolve_router)

    async def _db():
        async with Session() as db:
            yield db

    app.dependency_overrides[get_db] = _db
    client = TestClient(app)

    hit = client.get("/api/resolve", params={"url": GOVMAP_COPIED})
    assert hit.status_code == 200
    body = hit.json()
    assert body["found"] is True
    assert len(body["matches"]) == 1
    assert body["matches"][0]["title"] == "תחנות הידרומטריות"
    assert uuid.UUID(body["matches"][0]["id"])  # a real dataset id to link to

    miss = client.get("/api/resolve", params={"url": "https://www.govmap.gov.il/?lay=99"})
    assert miss.status_code == 200
    assert miss.json() == {"url": "https://www.govmap.gov.il/?lay=99",
                           "found": False, "matches": []}


# ── /direct/<source url> — the shape tools build by concatenation ────────

def test_direct_rebuilds_the_url_from_both_spellings():
    """A caller may hand us the source URL verbatim after the prefix, or
    percent-encode the whole thing. Both must rebuild identically."""
    from app.api.resolve import rebuild_source_url

    verbatim = rebuild_source_url(
        "https://www.govmap.gov.il/", "c=219143.61,618345.06&lay=11"
    )
    encoded = rebuild_source_url(GOVMAP_COPIED, "")
    assert verbatim == encoded == GOVMAP_COPIED


def test_direct_needs_the_encoded_spelling_to_carry_a_route_fragment():
    """A browser never sends anything after "#", so the verbatim spelling loses
    the route before it reaches us — the percent-encoded one keeps it. Both are
    honest answers; only one is exact."""
    from app.api.resolve import rebuild_source_url

    # What the server actually receives for a verbatim hash-routed link.
    assert rebuild_source_url("https://ykpubdata.jerusalem.muni.il/", "") == \
        "https://ykpubdata.jerusalem.muni.il/"
    # The encoded spelling arrives already decoded, in one piece.
    assert rebuild_source_url(YK_DOCUMENTS, "") == YK_DOCUMENTS
    assert url_identity(rebuild_source_url(YK_DOCUMENTS, "")) == url_identity(YK_DOCUMENTS)


def test_direct_survives_a_proxy_collapsing_the_double_slash():
    from app.api.resolve import rebuild_source_url

    assert rebuild_source_url("https:/www.govmap.gov.il/", "lay=11") == GOVMAP_STORED


def test_direct_redirects_to_versions_form_or_chooser():
    """The whole contract of the link, in one place."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from slowapi import _rate_limit_exceeded_handler
    from slowapi.errors import RateLimitExceeded

    from app.api.resolve import direct_router
    from app.database import get_db
    from app.rate_limit import limiter

    async def go():
        return await _seed([
            {"ckan_name": "govmap-11", "title": "תחנות הידרומטריות",
             "source_type": "govmap", "source_url": GOVMAP_STORED,
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
            {"ckan_name": "eden-tenders", "title": "מכרזים", "source_type": "scraper",
             "source_url": "https://jeden.co.il/?category=tenders"},
            {"ckan_name": "eden-decisions", "title": "החלטות", "source_type": "scraper",
             "source_url": "https://jeden.co.il/?category=decisions"},
        ])

    Session = _run(go())

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(direct_router)

    async def _db():
        async with Session() as db:
            yield db

    app.dependency_overrides[get_db] = _db
    client = TestClient(app, follow_redirects=False)

    # Tracked → straight to the versions page.
    tracked = client.get(f"/direct/{GOVMAP_COPIED}")
    assert tracked.status_code == 302
    assert tracked.headers["location"].startswith("/versions/")

    # Not tracked → the search box, which opens the request form for it.
    missing = client.get("/direct/https://www.govmap.gov.il/?lay=99999")
    assert missing.status_code == 302
    assert missing.headers["location"].startswith("/?q=")

    # Ambiguous → the chooser, never a guess.
    ambiguous = client.get("/direct/https://jeden.co.il/")
    assert ambiguous.status_code == 302
    assert ambiguous.headers["location"].startswith("/lookup?url=")

    # A bare prefix is not an error — it lands on the link generator.
    assert client.get("/direct/").headers["location"] == "/lookup"


def test_bulk_committee_rows_are_not_resolution_targets():
    async def go():
        Session = await _seed([
            {"ckan_name": "knesset-committee-single-123", "title": "ישיבה בודדת",
             "source_type": "scraper", "source_url": "https://knesset.gov.il/x?a=1"},
        ])
        async with Session() as db:
            hits = await find_datasets_for_url(db, "https://knesset.gov.il/x?a=1")
        assert hits == []

    _run(go())
