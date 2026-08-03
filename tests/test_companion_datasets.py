"""Tracking one URL opens the datasets its pattern says belong with it.

A source can hold one corpus that is honestly two datasets. A Telegram channel
is its whole history AND the five-minute feed of what it just posted: different
cadence (daily vs 300s), different cost (~680 page reads vs one), different
question. Before this, tracking a channel properly meant pasting it twice — the
second time in a spelling (``#/feed``) the user had to know existed.

So a url_pattern may declare ``companions``: URL templates rendered from its own
named groups. Each is then classified by the registry exactly like a pasted URL,
so it picks up its own page_type, config, title and cadence with nothing
special-cased at the creation site.
"""
import asyncio
import os
import uuid

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.models.source_registry import SourceRegistry
from app.models.tag import Tag, dataset_tags
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import source_registry as sr


CHANNEL = r"^https?://(www\.)?t\.me/(s/)?(?P<channel>[A-Za-z][A-Za-z0-9_]{4,31})(/\d+)?/?$"
FEED = r"^https?://(www\.)?t\.me/(s/)?(?P<channel>[A-Za-z][A-Za-z0-9_]{4,31})/?#/feed/?$"

MANIFEST = {
    "manifest_version": 1,
    "id": "telegram",
    "label_he": "טלגרם ממשלתי",
    "label_en": "Government Telegram",
    "site_url": "https://t.me/",
    "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
    "default_poll_interval": 86400,
    "neon_eligible": True,
    "default_config": {"download_files": True},
    "url_patterns": [
        {"regex": FEED, "page_type": "telegram_feed", "poll_interval": 300,
         "title_he": "טלגרם — @{channel} (פיד)",
         "config": {"channel": "{channel|lower}", "corpus": "feed"}},
        {"regex": CHANNEL, "page_type": "telegram_channel",
         "title_he": "טלגרם — @{channel}",
         "config": {"channel": "{channel|lower}"},
         "companions": ["https://t.me/{channel}#/feed"]},
    ],
}


@pytest.fixture(autouse=True)
def _clear_cache():
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


async def _session(existing=()):
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
        await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
        await conn.run_sync(lambda c: SourceRegistry.__table__.create(c))
        # log_event touches dataset.tags, so the association must exist.
        await conn.run_sync(lambda c: Tag.__table__.create(c))
        await conn.run_sync(lambda c: dataset_tags.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    db = Session()
    db.add(SourceRegistry(id="telegram", manifest=MANIFEST,
                          manifest_hash="x" * 8, enabled=True))
    for url in existing:
        db.add(TrackedDataset(
            id=uuid.uuid4(), ckan_id=f"t-{uuid.uuid4().hex[:6]}",
            ckan_name=f"t-{uuid.uuid4().hex[:6]}", title="already there",
            status="active", is_active=True, source_type="scraper",
            source_url=url, scraper_config={"kind": "telegram"},
            poll_interval=86400,
        ))
    await db.commit()
    return db


async def _companions_for(url, existing=()):
    from app.api.datasets import _create_companion_requests

    db = await _session(existing)
    match = await sr.classify_url(db, url)
    created = await _create_companion_requests(db, match)
    rows = (await db.execute(select(TrackedDataset))).scalars().all()
    await db.close()
    return created, rows


# ── the templates themselves ────────────────────────────────────────────────

@pytest.mark.parametrize("pasted,expected", [
    ("https://t.me/MOHreport", "https://t.me/MOHreport#/feed"),
    # The preview spelling and a permalink must still yield the CANONICAL
    # channel form: gluing "#/feed" onto "t.me/s/X" or "t.me/X/13513" produces
    # a URL the feed pattern does not match.
    ("https://t.me/s/Israel_Cyber", "https://t.me/Israel_Cyber#/feed"),
    ("https://t.me/MOHreport/13513", "https://t.me/MOHreport#/feed"),
])
def test_the_companion_url_is_built_from_the_group_not_the_pasted_url(pasted, expected):
    manifest = sr.validate_manifest(MANIFEST)
    assert sr.match_manifests(pasted, [manifest]).companion_urls == [expected]


def test_a_companion_does_not_have_companions_of_its_own():
    """One level. Two patterns naming each other must not loop."""
    manifest = sr.validate_manifest(MANIFEST)
    feed = sr.match_manifests("https://t.me/MOHreport#/feed", [manifest])
    assert feed.page_type == "telegram_feed"
    assert feed.companion_urls == []


# ── creating them ───────────────────────────────────────────────────────────

def test_tracking_a_channel_opens_its_feed_too():
    created, rows = asyncio.run(_companions_for("https://t.me/MOHreport"))
    assert [c["url"] for c in created] == ["https://t.me/MOHreport#/feed"]
    [feed] = [r for r in rows if r.source_url.endswith("#/feed")]
    assert feed.title == "טלגרם — @MOHreport (פיד)"
    # Its OWN pattern's cadence and config, not the channel's.
    assert feed.poll_interval == 300
    assert feed.scraper_config["corpus"] == "feed"
    assert feed.scraper_config["channel"] == "mohreport"
    assert feed.scraper_config["kind"] == "telegram"
    # neon_eligible carries into the config, as on the primary path.
    assert feed.scraper_config["archive_neon"] is True
    assert feed.status == "pending"


def test_a_companion_that_is_already_tracked_is_not_opened_twice():
    created, rows = asyncio.run(_companions_for(
        "https://t.me/MOHreport", existing=("https://t.me/MOHreport#/feed",),
    ))
    assert created == []
    assert len([r for r in rows if r.source_url.endswith("#/feed")]) == 1


def test_a_pattern_with_no_companions_creates_nothing():
    created, rows = asyncio.run(_companions_for("https://t.me/MOHreport#/feed"))
    assert created == []
    assert rows == []


def test_an_unrecognised_companion_template_is_skipped_not_raised():
    """The dataset the user asked for is already committed by the time this
    runs, so a bad template must not turn a successful request into a 500."""
    broken = {
        **MANIFEST, "id": "telegram",
        "url_patterns": [
            {"regex": CHANNEL, "page_type": "telegram_channel",
             "companions": ["https://example.invalid/{channel}"]},
        ],
    }

    async def _run():
        from app.api.datasets import _create_companion_requests

        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
            await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
            await conn.run_sync(lambda c: SourceRegistry.__table__.create(c))
            await conn.run_sync(lambda c: Tag.__table__.create(c))
            await conn.run_sync(lambda c: dataset_tags.create(c))
        Session = async_sessionmaker(engine, class_=AsyncSession,
                                     expire_on_commit=False)
        db = Session()
        db.add(SourceRegistry(id="telegram", manifest=broken,
                              manifest_hash="y" * 8, enabled=True))
        await db.commit()
        match = await sr.classify_url(db, "https://t.me/MOHreport")
        created = await _create_companion_requests(db, match)
        rows = (await db.execute(select(TrackedDataset))).scalars().all()
        await db.close()
        return created, rows

    created, rows = asyncio.run(_run())
    assert created == [] and rows == []
