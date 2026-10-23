"""The per-``Info`` split of gov.il's מאגר התשובות must stay six datasets.

``repository-of-answers`` is one collector holding 15,517 items and 23,673
attachments — a 6.5-hour scrape that fails often enough to leave gaps in the
history. It is split by the collector's own "סוג מידע" facet into five filtered
datasets (``?skip=0&Info=1`` … ``&Info=5``) plus the original unfiltered one,
which stays on a rare cadence because 305 items carry no ``Info`` at all and no
filter reaches them.

The whole split rests on one property: ``Info`` is MEANING, not noise. If URL
canonicalisation ever starts folding it away — the way it folds ``www.``,
param order and ``utm_*`` — then all six URLs collapse to one identity and
``find_datasets_for_url(strict=True)`` answers "already tracked" for every
subset. Five datasets would silently become un-creatable, and a re-point or a
re-request would resolve to the wrong one. That failure is invisible from the
outside, which is why it is pinned here rather than left to the general
"a meaningful query param is not noise" case in tests/test_url_resolve.py.

The second property is the slugs: they seed ``ckan_name``/``ckan_id``, which
carry a uniqueness expectation of their own, and they are derived from the URL
rather than from the identity — so they need their own assertion.
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.govil import _parse_govil_url  # noqa: E402
from app.api.utils import scraper_url_slug  # noqa: E402
from app.models.source_registry import SourceRegistry  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services.dataset_lookup import find_datasets_for_url  # noqa: E402
from app.services.url_identity import url_identity  # noqa: E402

BASE = "https://www.gov.il/he/Departments/DynamicCollectors/repository-of-answers?skip=0"
# The five "סוג מידע" values the collector's own facet offers, keyed as gov.il
# keys them (InfoMultiChoiseValues on the collector page).
INFO_VALUES = (1, 2, 3, 4, 5)
SUBSET_URLS = [f"{BASE}&Info={i}" for i in INFO_VALUES]
ALL_URLS = [BASE] + SUBSET_URLS


def test_every_subset_is_its_own_identity():
    identities = [url_identity(u) for u in ALL_URLS]
    assert all(identities), "every URL must resolve to an identity"
    assert len(set(identities)) == len(ALL_URLS), (
        "Info must survive canonicalisation — folding it away collapses the "
        f"split back into one dataset: {identities}"
    )


def test_the_parent_is_not_confused_with_any_subset():
    """The unfiltered register is a sixth thing, not the same as Info=1.

    It is the one that still covers the 305 items with no סוג מידע, so a
    duplicate check that matched it against a subset would block exactly the
    dataset the split exists to keep.
    """
    parent = url_identity(BASE)
    for url in SUBSET_URLS:
        assert url_identity(url) != parent, url


def test_subsets_stay_dynamic_collectors():
    """Adding a query param must not knock the URL out of its page type — the
    filtered URLs go through the same gov.il DynamicCollector engine, which is
    what forwards ``Info`` into the API's QueryFilters."""
    for url in ALL_URLS:
        page_type, collector = _parse_govil_url(url)
        assert page_type == "dynamic_collector", url
        assert collector == "repository-of-answers", url


def test_each_subset_gets_its_own_slug():
    """``scraper_url_slug`` seeds ckan_name/ckan_id, so a shared slug would
    make two datasets collide on a column the catalog treats as unique."""
    slugs = [scraper_url_slug("repository-of-answers", u) for u in ALL_URLS]
    assert len(set(slugs)) == len(ALL_URLS), slugs
    # The parent is live under this slug; pinning it means a change to the
    # hashing scheme shows up here instead of orphaning the existing dataset.
    assert slugs[0] == "repository-of-answers-6c4638db"


async def _seed_parent_only():
    """A DB holding just the unfiltered register — the state before the split."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (TrackedDataset, VersionIndex, SourceRegistry):
            await conn.run_sync(lambda c, t=table: t.__table__.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        db.add(TrackedDataset(
            id=uuid.uuid4(),
            ckan_id="govil-scraper-repository-of-answers-6c4638db",
            ckan_name="repository-of-answers-6c4638db",
            title="Repository Of Answers",
            status="active", is_active=True,
            source_type="scraper", source_url=BASE,
        ))
        await db.commit()
    return Session


def test_the_parent_does_not_block_creating_the_subsets():
    """The gate the split actually has to pass.

    ``track_dataset`` refuses a URL when ``find_datasets_for_url(strict=True)``
    finds anything, so if the already-tracked parent matched a subset URL,
    every one of the five would be rejected as "Dataset already tracked" and
    the split could not be created at all.

    This is worth asserting separately from identity because the LOOSE lookup
    genuinely does match — ``path_key`` ignores the query string, so
    /api/resolve answers the parent for all five (verified against production
    on 2026-09-08). Identity distinctness and duplicate-check behaviour are two
    different questions, and only the strict one gates creation.
    """
    async def go():
        Session = await _seed_parent_only()
        async with Session() as db:
            for url in SUBSET_URLS:
                strict = await find_datasets_for_url(db, url, strict=True)
                assert strict == [], (
                    f"{url} would be rejected as a duplicate of the parent")
            # The parent still recognises itself — a duplicate check that
            # stopped matching anything would be a different, worse bug.
            assert len(await find_datasets_for_url(db, BASE, strict=True)) == 1

    asyncio.run(go())
