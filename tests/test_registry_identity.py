"""Two URLs that a manifest resolves the same way are the same dataset.

The URL identity (url_identity) answers "is this the same LINK?". For a site
that spells one corpus several ways, that answers no where the truth is yes:

    …/                                          the bare root
    …/#/                                        the SPA's home route
    …/#/?SystemCode=26400046                    the search screen
    …/#/TableData?TikNum=0&SystemCode=26400046  the address-results grid

Four canonical URLs, one corpus — and for ykpubdata one corpus is a ~10-hour
sweep against an edge that bans a client over ~2 req/s. Tracking two spellings
runs it twice. The duplicate check is the guard that should catch that and
couldn't, because it only ever saw the URL.

By the time OVER accepts or refuses, it has already CLASSIFIED the URL:
source_registry hands back (source_id, page_type, resolved scraper_config).
Two URLs with the same triple would be handed to the same engine with the same
instructions, so they are the same dataset by construction. That is sound
because a manifest's config is URL-derived — named groups are substituted into
it — so what the author declared as distinguishing is IN the config.

It is NOT sound for the hardcoded parsers, and this file pins that too: every
gov.il ``/collectors/policies?officeId=…`` dataset shares one page_type and one
config while being a different ministry (15 of them on prod, plus 7
publications and 5 legalinfo). They have no manifest, so they never reach this
code — and the tests below fail if that ever stops being true.
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.models.source_registry import SourceRegistry  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services import source_registry as sr  # noqa: E402
from app.services.dataset_lookup import find_datasets_for_url  # noqa: E402


HOST = "https://ykpubdata.jerusalem.muni.il"
SYS = "26400046"

# The four spellings of the register, as seen in a browser's address bar.
ROOT = f"{HOST}/"
HOME_ROUTE = f"{HOST}/#/"
SEARCH_SCREEN = f"{HOST}/#/?SystemCode={SYS}"
RESULTS_GRID = f"{HOST}/#/TableData?TikNum=0&SystemCode={SYS}"
REGISTER_SPELLINGS = [ROOT, HOME_ROUTE, SEARCH_SCREEN, RESULTS_GRID]

DOCUMENTS = f"{HOST}/#/documents?SystemCode={SYS}"
ONE_FILE = f"{HOST}/#/Details?TikNum=2024/0123.00&SystemCode={SYS}"
ANOTHER_FILE = f"{HOST}/#/Details?TikNum=2004/0196.00&SystemCode={SYS}"

# A cut-down ykpubdata manifest: the three page_types, the specific patterns
# first, and system_code as a NAMED GROUP — which is what makes a different
# SystemCode on the same SPA (פיקוח vs רישוי) a different dataset.
MANIFEST = {
    "manifest_version": 1,
    "id": "ykpubdata",
    "label_he": "עיריית ירושלים — רישוי ובנייה",
    "label_en": "Jerusalem Municipality — Building Licensing",
    "site_url": f"{HOST}/",
    "badge": {"bg": "#fef3c7", "fg": "#78350f", "accent": "#d97706"},
    "default_poll_interval": 2592000,
    "neon_eligible": True,
    "default_config": {"download_files": False, "system_code": SYS},
    "url_patterns": [
        {
            "regex": (r"^https?://ykpubdata\.jerusalem\.muni\.il/?"
                      r"(?:\?[^#]*)?#/Details"
                      r"(?=[^#]*[?&]TikNum=(?P<tik_num>[^&#]+))"
                      r"(?:(?=[^#]*[?&]SystemCode=(?P<system_code>\d+)))?.*$"),
            "page_type": "ykpubdata_tik",
            "title_he": "תיק רישוי בנייה ירושלים — {tik_num}",
            "config": {"tik_num": "{tik_num}", "system_code": "{system_code}",
                       "corpus": "tik"},
        },
        {
            "regex": (r"^https?://ykpubdata\.jerusalem\.muni\.il/?"
                      r"(?:\?[^#]*)?#/documents"
                      r"(?:(?=[^#]*[?&]SystemCode=(?P<system_code>\d+)))?.*$"),
            "page_type": "ykpubdata_documents",
            "title_he": "עיריית ירושלים — מסמכי תיקי רישוי ובנייה",
            "config": {"corpus": "documents", "system_code": "{system_code}"},
        },
        {
            # The SystemCode lookahead scans with ".*", not "[^#]*": on this SPA
            # the param lives INSIDE the hash route, and a "[^#]*" placed before
            # the "#" cannot reach past it — the group never participates, the
            # config keeps default_config's value, and every system on the site
            # collapses into one identity.
            "regex": (r"^https?://ykpubdata\.jerusalem\.muni\.il"
                      r"(?![^#]*#/Details)(?![^#]*#/documents)"
                      r"(?:(?=.*[?&]SystemCode=(?P<system_code>\d+)))?"
                      r"(?:[/?#].*)?$"),
            "page_type": "ykpubdata_all",
            "title_he": "עיריית ירושלים — תיקי רישוי ובנייה",
            # system_code is SUBSTITUTED here, not merely captured. Capturing a
            # group the config never reads leaves default_config's value in
            # place, so every SystemCode on the SPA resolves to one identity —
            # which is how פיקוח (26400056) ends up classified as building
            # licensing. The identity follows what the manifest declares, so the
            # declaration is the lever.
            "config": {"corpus": "all", "system_code": "{system_code}"},
        },
    ],
}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def clean_cache():
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


async def _seed(rows, *, manifests=(MANIFEST,)):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
        await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
        await conn.run_sync(lambda c: SourceRegistry.__table__.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        for man in manifests:
            db.add(SourceRegistry(id=man["id"], manifest=man,
                                  manifest_hash="x" * 8, enabled=True))
        for i, row in enumerate(rows):
            db.add(TrackedDataset(
                id=uuid.uuid4(),
                ckan_id=row.get("ckan_name", f"ds-{i}"),
                ckan_name=row.get("ckan_name", f"ds-{i}"),
                title=row.get("title", "T"),
                status=row.get("status", "active"),
                is_active=True,
                source_type=row.get("source_type", "scraper"),
                source_url=row.get("source_url"),
                scraper_config=row.get("scraper_config"),
            ))
        await db.commit()
    sr.invalidate_cache()
    return Session


# ── the identity itself ──────────────────────────────────────────────────

def _identity(url, manifests=(MANIFEST,)):
    mans = [sr.validate_manifest(m) for m in manifests]
    return sr.identity_of(sr.match_manifests(url, mans))


def test_every_spelling_of_the_register_is_one_identity():
    ids = {_identity(u) for u in REGISTER_SPELLINGS}
    assert len(ids) == 1
    assert next(iter(ids))[:2] == ("ykpubdata", "ykpubdata_all")


def test_the_other_corpora_keep_their_own_identities():
    register, documents, one_file = (
        _identity(SEARCH_SCREEN), _identity(DOCUMENTS), _identity(ONE_FILE))
    assert len({register, documents, one_file}) == 3


def test_two_building_files_are_two_datasets():
    """The config is URL-derived, so tik_num distinguishes them without anyone
    saying so twice."""
    assert _identity(ONE_FILE) != _identity(ANOTHER_FILE)


def test_a_different_system_on_the_same_spa_is_a_different_dataset():
    """SystemCode 26400056 is פיקוח, not building licensing. Substituting the
    named group into the config is what makes the identity follow it."""
    assert _identity(f"{HOST}/#/?SystemCode=26400056") != _identity(SEARCH_SCREEN)


def test_a_spelling_that_omits_the_param_falls_back_to_the_manifest_default():
    """…and the spellings that carry no SystemCode still unify with the ones
    that do, because default_config fills the key in. Both halves are needed:
    without the default, the bare root would be its own dataset."""
    assert _identity(ROOT) == _identity(SEARCH_SCREEN)


def test_an_unclassified_url_has_no_registry_identity():
    assert _identity("https://www.govmap.gov.il/?lay=11") is None
    assert _identity("https://data.gov.il/dataset/my-pkg") is None


# ── resolution against the catalog ───────────────────────────────────────

def test_a_second_spelling_of_a_tracked_corpus_is_a_duplicate():
    """The bug: four tracked datasets, one corpus, four ~10-hour sweeps."""
    async def go():
        Session = await _seed([
            {"ckan_name": "ykpubdata-all-665e9000", "title": "המרשם",
             "source_url": SEARCH_SCREEN},
        ])
        async with Session() as db:
            return [
                await find_datasets_for_url(db, u, strict=True)
                for u in REGISTER_SPELLINGS
            ]

    for hits in _run(go()):
        assert [h["title"] for h in hits] == ["המרשם"]
        assert hits[0]["match"] == "identity"

    _run(go())


def test_the_other_corpora_are_still_requestable_alongside_it():
    """The guard must not become a wall: a genuinely different corpus of the
    same source has to get through."""
    async def go():
        Session = await _seed([
            {"ckan_name": "ykpubdata-all-665e9000", "title": "המרשם",
             "source_url": SEARCH_SCREEN},
        ])
        async with Session() as db:
            return (
                await find_datasets_for_url(db, DOCUMENTS, strict=True),
                await find_datasets_for_url(db, ONE_FILE, strict=True),
            )

    documents, one_file = _run(go())
    assert documents == []
    assert one_file == []


def test_each_corpus_matches_only_itself():
    async def go():
        Session = await _seed([
            {"ckan_name": "yk-all", "title": "המרשם", "source_url": ROOT},
            {"ckan_name": "yk-docs", "title": "המסמכים", "source_url": DOCUMENTS},
            {"ckan_name": "yk-file", "title": "תיק 2024/0123.00",
             "source_url": ONE_FILE},
        ])
        async with Session() as db:
            return {
                "register": await find_datasets_for_url(db, RESULTS_GRID, strict=True),
                "documents": await find_datasets_for_url(db, DOCUMENTS, strict=True),
                "file": await find_datasets_for_url(db, ONE_FILE, strict=True),
                "other_file": await find_datasets_for_url(db, ANOTHER_FILE, strict=True),
            }

    got = _run(go())
    assert [h["title"] for h in got["register"]] == ["המרשם"]
    assert [h["title"] for h in got["documents"]] == ["המסמכים"]
    assert [h["title"] for h in got["file"]] == ["תיק 2024/0123.00"]
    assert got["other_file"] == [], "a different תיק is a different dataset"


def test_the_public_resolver_finds_it_too():
    """Not only the duplicate check: pasting any spelling into the search box
    must open the dataset rather than a request form for a corpus we have."""
    async def go():
        Session = await _seed([
            {"ckan_name": "yk-all", "title": "המרשם", "source_url": SEARCH_SCREEN},
        ])
        async with Session() as db:
            return await find_datasets_for_url(db, RESULTS_GRID)

    hits = _run(go())
    assert [h["title"] for h in hits] == ["המרשם"]
    assert hits[0]["match"] == "identity"


# ── the hardcoded sources must not move ──────────────────────────────────

GOVIL_A = ("https://www.gov.il/he/collectors/policies"
           "?officeId=104cb0f4-d65a-4692-b590-94af928c19c0")
GOVIL_B = ("https://www.gov.il/he/collectors/policies"
           "?officeId=86842de6-987b-42d4-b9c2-cbd7d0619534")


def test_two_ministries_on_one_collector_stay_two_datasets():
    """Measured on prod: 15 /collectors/policies datasets, 7 publications, 5
    legalinfo — one page_type and one config each, all legitimately separate.
    They are parsed by hardcoded code and have no manifest, which is the whole
    reason the registry identity is scoped to registry matches."""
    async def go():
        Session = await _seed([
            {"ckan_name": "policies-aaaaaaaa", "title": "משרד א",
             "source_url": GOVIL_A},
        ])
        async with Session() as db:
            return (
                await find_datasets_for_url(db, GOVIL_B, strict=True),
                await find_datasets_for_url(db, GOVIL_A, strict=True),
            )

    other_ministry, same_ministry = _run(go())
    assert other_ministry == [], "a second ministry must still be trackable"
    assert [h["title"] for h in same_ministry] == ["משרד א"]


def test_govmap_and_ckan_resolution_is_unchanged():
    async def go():
        Session = await _seed([
            {"ckan_name": "govmap-11", "title": "שכבה 11", "source_type": "govmap",
             "source_url": "https://www.govmap.gov.il/?lay=11",
             "scraper_config": {"kind": "govmap", "layer_id": "11"}},
            {"ckan_name": "bus-stops", "title": "תחנות", "source_type": "ckan"},
        ])
        async with Session() as db:
            return (
                await find_datasets_for_url(
                    db, "https://www.govmap.gov.il/?c=1,2&lay=11", strict=True),
                await find_datasets_for_url(
                    db, "https://data.gov.il/dataset/bus-stops", strict=True),
                await find_datasets_for_url(
                    db, "https://www.govmap.gov.il/?lay=99", strict=True),
            )

    layer, package, missing = _run(go())
    assert [h["title"] for h in layer] == ["שכבה 11"]
    assert [h["title"] for h in package] == ["תחנות"]
    assert missing == []


def test_an_unreadable_registry_degrades_instead_of_failing_the_lookup():
    """/api/resolve and /direct are public. This axis only ever ADDS matches, so
    losing it must cost the extra matches, not the answer."""
    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
            await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
            # source_registry deliberately NOT created — every read of it raises.
        Session = async_sessionmaker(engine, class_=AsyncSession,
                                     expire_on_commit=False)
        async with Session() as db:
            db.add(TrackedDataset(
                id=uuid.uuid4(), ckan_id="yk", ckan_name="yk", title="המרשם",
                status="active", is_active=True, source_type="scraper",
                source_url=SEARCH_SCREEN,
            ))
            await db.commit()
        sr.invalidate_cache()
        async with Session() as db:
            return (
                await find_datasets_for_url(db, SEARCH_SCREEN, strict=True),
                await find_datasets_for_url(db, RESULTS_GRID, strict=True),
            )

    same_url, other_spelling = _run(go())
    assert [h["title"] for h in same_url] == ["המרשם"], "the URL identity still works"
    assert other_spelling == [], "the registry axis is simply absent"


def test_a_source_with_no_manifest_registered_behaves_as_before():
    """With the registry empty there is no triple, and resolution falls back to
    the URL identity alone — the pre-change behaviour, spelled out."""
    async def go():
        Session = await _seed(
            [{"ckan_name": "yk-all", "title": "המרשם", "source_url": SEARCH_SCREEN}],
            manifests=(),
        )
        async with Session() as db:
            return (
                await find_datasets_for_url(db, RESULTS_GRID, strict=True),
                await find_datasets_for_url(db, SEARCH_SCREEN, strict=True),
            )

    grid, same = _run(go())
    assert grid == []
    assert [h["title"] for h in same] == ["המרשם"]


# ── synthetic fragment routes ───────────────────────────────────────────────

def test_a_manifest_route_fragment_must_survive_url_identity():
    """A manifest that splits one site into two datasets with a fragment must
    write it as a ROUTE (``#/x``), not an anchor (``#x``).

    Registry identity is only half the duplicate check. find_datasets_for_url
    ALSO compares url_identity, and url_identity keeps a fragment only when it
    starts with "/" — that is what keeps "…/about#contact" and "…/about" one
    dataset. So a manifest using a bare anchor produces two page_types whose
    URLs canonicalise identically: pasting the second route resolves straight
    to the first's dataset, which can therefore never be created.

    The telegram source shipped exactly that bug with "#feed" and was moved to
    "#/feed". ykpubdata's "#/documents" had the slash from the start.
    """
    from app.services.url_identity import url_identity

    assert url_identity("https://t.me/MOHreport#feed") == \
        url_identity("https://t.me/MOHreport")          # the bug: indistinguishable
    assert url_identity("https://t.me/MOHreport#/feed") != \
        url_identity("https://t.me/MOHreport")          # the fix


def test_every_registered_manifest_uses_route_shaped_fragments():
    """Guards the whole registry, not just telegram: a future manifest that
    reaches for "#something" to split a corpus gets caught here."""
    import re as _re

    manifests = _load_repo_manifests()
    assert manifests, "govscraper checkout not readable — the guard would pass vacuously"
    for manifest in manifests:
        for pattern in manifest.url_patterns:
            for anchor in _re.findall(r"#(?!/)([A-Za-z][\w-]*)", pattern.regex):
                raise AssertionError(
                    f"{manifest.id}: pattern fragment '#{anchor}' is an anchor, "
                    "not a route — url_identity drops it, so this route shares "
                    f"an identity with the same URL without it. Use '#/{anchor}'."
                )


def _load_repo_manifests():
    """Every manifest in the sibling govscraper checkout, or nothing.

    Collected in a SUBPROCESS, deliberately. That repo has an ``app.py`` at its
    root while this one has an ``app/`` package, so putting it on sys.path makes
    ``import app`` resolve to the wrong project for every test that runs
    afterwards — which is exactly what happened when this was written in-process
    (nine unrelated worker tests failed). A child process cannot leak sys.path
    or sys.modules back here.
    """
    import json
    import os.path
    import subprocess
    import sys

    root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "GOV scraper")
    )
    if not os.path.isdir(root):
        return []
    code = (
        "import json;"
        "from govscraper.scrapers.manifests import all_manifests;"
        "print(json.dumps(all_manifests()))"
    )
    try:
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=root, capture_output=True,
            text=True, encoding="utf-8", timeout=120,
        )
    except Exception:
        return []
    if out.returncode != 0 or not (out.stdout or "").strip():
        return []
    from app.services import source_registry as _sr

    return [_sr.validate_manifest(m) for m in json.loads(out.stdout)]
