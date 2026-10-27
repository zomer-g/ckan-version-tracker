"""A manifest pattern can name the tags and organization its datasets are
created with — and a dataset created from a template title takes the real
title its first scrape reports.

A pattern that opens 48 companions otherwise leaves 48 datasets to be tagged,
filed under an organization and renamed by hand, one at a time.
"""
import asyncio
import os
import uuid

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.models.organization import Organization
from app.models.source_registry import SourceRegistry
from app.models.tag import Tag, dataset_tags
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import source_registry as sr

TOKEN = "eyJrIjoiYWJjIn0abcdefgh"
DASH = r"^https://app\.powerbi\.com/view\?r=(?P<r>[A-Za-z0-9]+)$"
VISUAL = r"^https://app\.powerbi\.com/view\?r=(?P<r>[A-Za-z0-9]+)#/visual/(?P<v>[0-9a-f]+)$"

MANIFEST = {
    "manifest_version": 1,
    "id": "powerbi",
    "label_he": "דשבורד Power BI ציבורי",
    "label_en": "Public Power BI dashboard",
    "site_url": "https://app.powerbi.com/",
    "badge": {"bg": "#ffedd5", "fg": "#9a3412", "accent": "#ea580c"},
    "neon_eligible": True,
    "url_patterns": [
        {"regex": VISUAL, "page_type": "powerbi_visual",
         "title_he": "דשבורד — רכיב {v}",
         "config": {"report": "{r}", "visual": "{v}"},
         "tags": ["דשבורד לכל פקיד", "אין כזו תגית"],
         "organization": "the_electricity_authority"},
        {"regex": DASH, "page_type": "powerbi_report",
         "title_he": "דשבורד",
         "config": {"report": "{r}"},
         "tags": ["דשבורד לכל פקיד"],
         "organization": "the_electricity_authority",
         "companions": ["https://app.powerbi.com/view?r={r}#/visual/aa11",
                        "https://app.powerbi.com/view?r={r}#/visual/bb22"]},
    ],
}


@pytest.fixture(autouse=True)
def _clear_cache():
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


async def _session(org_exists=True):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (Organization.__table__, TrackedDataset.__table__,
                      VersionIndex.__table__, SourceRegistry.__table__,
                      Tag.__table__, dataset_tags):
            await conn.run_sync(lambda c, t=table: t.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    db = Session()
    db.add(SourceRegistry(id="powerbi", manifest=MANIFEST, manifest_hash="z" * 8, enabled=True))
    db.add(Tag(id=uuid.uuid4(), name="דשבורד לכל פקיד"))
    if org_exists:
        db.add(Organization(id=uuid.uuid4(), name="the_electricity_authority",
                            title="רשות החשמל"))
    await db.commit()
    return db


def test_the_match_carries_the_patterns_labels():
    m = sr.match_manifests(f"https://app.powerbi.com/view?r={TOKEN}",
                           [sr.validate_manifest(MANIFEST)])
    assert m.tags == ["דשבורד לכל פקיד"]
    assert m.organization == "the_electricity_authority"


def test_companions_are_created_tagged_and_filed_and_unknown_names_skipped():
    async def run():
        from app.api.datasets import _create_companion_requests

        db = await _session()
        match = await sr.classify_url(db, f"https://app.powerbi.com/view?r={TOKEN}")
        created = await _create_companion_requests(db, match)
        rows = (await db.execute(select(TrackedDataset).options(
            selectinload(TrackedDataset.tags)))).scalars().all()
        org = (await db.execute(select(Organization))).scalar_one()
        await db.close()
        return created, rows, org

    created, rows, org = asyncio.run(run())
    assert len(created) == 2 and len(rows) == 2
    for ds in rows:
        assert ds.status == "pending"
        assert [t.name for t in ds.tags] == ["דשבורד לכל פקיד"]   # the unknown one skipped
        assert ds.organization_id == org.id
        assert ds.organization == "the_electricity_authority"
        assert ds.scraper_config["visual"] in ("aa11", "bb22")


def test_an_admin_opens_companions_active():
    async def run():
        from app.api.datasets import _create_companion_requests

        db = await _session()
        match = await sr.classify_url(db, f"https://app.powerbi.com/view?r={TOKEN}")
        created = await _create_companion_requests(db, match, status="active")
        rows = (await db.execute(select(TrackedDataset))).scalars().all()
        await db.close()
        return created, rows

    created, rows = asyncio.run(run())
    assert all("id" in c for c in created)
    assert {r.status for r in rows} == {"active"}


def test_a_missing_organization_leaves_the_dataset_unfiled_not_failed():
    async def run():
        from app.api.datasets import _create_companion_requests

        db = await _session(org_exists=False)
        match = await sr.classify_url(db, f"https://app.powerbi.com/view?r={TOKEN}")
        created = await _create_companion_requests(db, match)
        rows = (await db.execute(select(TrackedDataset))).scalars().all()
        await db.close()
        return created, rows

    created, rows = asyncio.run(run())
    assert len(created) == 2
    assert all(r.organization_id is None for r in rows)


@pytest.mark.parametrize("current,reported,expected", [
    # still the template → promoted
    ("דשבורד — רכיב aa11", "רשות החשמל — הספק לפי מחוז", "רשות החשמל — הספק לפי מחוז"),
    # edited by hand → kept
    ("שם שמנהל נתן", "רשות החשמל — הספק לפי מחוז", "שם שמנהל נתן"),
    # nothing reported → kept
    ("דשבורד — רכיב aa11", "", "דשבורד — רכיב aa11"),
])
def test_a_template_title_takes_the_scraped_one_and_a_hand_edit_stands(current, reported, expected):
    async def run():
        from app.api.worker import promote_registry_title

        db = await _session()
        ds = TrackedDataset(id=uuid.uuid4(), ckan_id="p-1", ckan_name="p-1", title=current,
                            status="active", source_type="scraper",
                            source_url=f"https://app.powerbi.com/view?r={TOKEN}#/visual/aa11",
                            scraper_config={"kind": "powerbi"}, poll_interval=604800)
        await promote_registry_title(db, ds, {"dataset_title_he": reported})
        await db.close()
        return ds.title

    assert asyncio.run(run()) == expected


def test_a_dataset_of_another_source_is_never_renamed():
    async def run():
        from app.api.worker import promote_registry_title

        db = await _session()
        ds = TrackedDataset(id=uuid.uuid4(), ckan_id="p-2", ckan_name="p-2",
                            title="דשבורד — רכיב aa11", status="active", source_type="scraper",
                            source_url=f"https://app.powerbi.com/view?r={TOKEN}#/visual/aa11",
                            scraper_config={"kind": "something_else"}, poll_interval=604800)
        changed = await promote_registry_title(db, ds, {"dataset_title_he": "שם אחר"})
        await db.close()
        return changed, ds.title

    assert asyncio.run(run()) == (False, "דשבורד — רכיב aa11")
