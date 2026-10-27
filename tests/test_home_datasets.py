"""The home page's dataset list is paged, ordered and searched server-side.

GET /api/home/datasets replaces the home page's download of the whole public
catalog (GET /api/datasets, ~2.3MB). These tests pin what the page relied on the
client-side code for: the hero totals over the whole catalog, pinned datasets
first, the Knesset committees folded into one tile that takes a grid slot, the
order-free keyword search, and pages that never overlap or skip a dataset.

Runs on in-memory SQLite (conftest maps JSONB → JSON), driven through a bare
FastAPI app carrying only the home router.
"""
import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.home import router as home_router  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.page_content import PageContent  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import home_catalog  # noqa: E402

_TABLES = [
    Organization.__table__,
    User.__table__,
    TrackedDataset.__table__,
    Tag.__table__,
    dataset_tags,
    VersionIndex.__table__,
    PageContent.__table__,
]

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
# 30 plain datasets, newest first by created_at: "ds-00" is the newest.
_PLAIN = 30
_COMMITTEES = 3


def _ds(title, n, **kw):
    return TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=kw.pop("ckan_id", f"id-{title}"),
        ckan_name=kw.pop("ckan_name", f"name-{title}"),
        title=title,
        poll_interval=3600,
        is_active=True,
        status=kw.pop("status", "active"),
        created_at=_T0 - timedelta(minutes=n),
        **kw,
    )


@pytest.fixture()
def env():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    ids: dict[str, str] = {}

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            org = Organization(id=uuid.uuid4(), name="mot", title="משרד התחבורה")
            committee_tag = Tag(id=uuid.uuid4(), name=home_catalog.KNESSET_COMMITTEE_TAG)
            bus_tag = Tag(id=uuid.uuid4(), name="תחבורה ציבורית")
            db.add_all([org, committee_tag, bus_tag])
            plain = [
                _ds(f"ds-{i:02d}", i, organization="mot" if i % 2 else "other",
                    organization_id=org.id if i % 2 else None)
                for i in range(_PLAIN)
            ]
            committees = [_ds(f"ועדת {i}", 100 + i) for i in range(_COMMITTEES)]
            hidden = [
                _ds("ישיבת ועדה", 200, ckan_name="knesset-committee-single-42"),
                _ds("מאגר מושבת", 201, status="inactive"),
            ]
            db.add_all(plain + committees + hidden)
            await db.flush()
            for c in committees:
                await db.execute(dataset_tags.insert().values(
                    dataset_id=c.id, tag_id=committee_tag.id))
            await db.execute(dataset_tags.insert().values(
                dataset_id=plain[7].id, tag_id=bus_tag.id))
            # 2 versions on ds-00, 1 on each committee.
            for n in (1, 2):
                db.add(VersionIndex(id=uuid.uuid4(), tracked_dataset_id=plain[0].id,
                                    version_number=n, metadata_modified="x"))
            for c in committees:
                db.add(VersionIndex(id=uuid.uuid4(), tracked_dataset_id=c.id,
                                    version_number=1, metadata_modified="x"))
            await db.commit()
            ids.update({ds.title: str(ds.id) for ds in plain + committees})

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    async def pin(*titles):
        async with Session() as db:
            db.add(PageContent(page="home", lang="he", key=home_catalog.FEATURED_KEY,
                               value=json.dumps([ids[t] for t in titles])))
            await db.commit()

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(home_router)
    app.dependency_overrides[get_db] = _db
    limiter.reset()
    home_catalog.cache.clear()
    yield TestClient(app), (lambda *t: asyncio.run(pin(*t)))
    home_catalog.cache.clear()


def _get(client, **params):
    r = client.get("/api/home/datasets", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _titles(body):
    return [i["title"] for i in body["items"]]


def test_hero_totals_cover_the_whole_catalog(env):
    client, _ = env
    body = _get(client)
    # Committees count as datasets (as the old client-side sum did); the
    # per-meeting Knesset rows and inactive rows do not.
    assert body["stats"] == {
        "datasets": _PLAIN + _COMMITTEES,
        "versions": 2 + _COMMITTEES,
        "organizations": 2,  # the org row, and the bare "other" slug
    }


def test_committee_tile_takes_the_first_slot_and_pages_tile_evenly(env):
    client, _ = env
    first = _get(client)
    assert first["committee"] == {"count": _COMMITTEES, "versions": _COMMITTEES}
    assert first["total"] == _PLAIN
    # 23 cards + the committee tile = 24 tiles on page 1.
    assert len(first["items"]) == 23
    assert _titles(first)[:2] == ["ds-00", "ds-01"]

    second = _get(client, page=2)
    assert _titles(second) == [f"ds-{i:02d}" for i in range(23, _PLAIN)]
    # Every dataset exactly once across the pages, no committee among them.
    assert set(_titles(first) + _titles(second)) == {f"ds-{i:02d}" for i in range(_PLAIN)}


def test_out_of_range_page_is_clamped(env):
    client, _ = env
    body = _get(client, page=99)
    assert body["page"] == 2
    assert _titles(body)[0] == "ds-23"


def test_card_carries_only_what_the_card_renders(env):
    client, _ = env
    card = _get(client)["items"][1]  # ds-01: odd → linked organization
    assert card["organization_title"] == "משרד התחבורה"
    assert card["source_url"] == "https://data.gov.il/he/datasets/mot/name-ds-01"
    # The heavy fields of the full catalog stay out of the home payload.
    for heavy in ("field_flags", "new_resources_at_source", "resource_ids",
                  "requester_email", "last_error"):
        assert heavy not in card


def test_pinned_datasets_lead_in_pin_order(env):
    client, pin = env
    pin("ds-12", "ds-05")
    body = _get(client)
    assert _titles(body)[:3] == ["ds-12", "ds-05", "ds-00"]
    # Moved, not duplicated.
    all_titles = _titles(body) + _titles(_get(client, page=2))
    assert len(all_titles) == len(set(all_titles)) == _PLAIN


def test_search_matches_every_word_in_any_order_and_unfolds_committees(env):
    client, _ = env
    body = _get(client, q="ציבורית תחבורה")
    assert _titles(body) == ["ds-07"]  # via its tag, words reversed
    assert body["committee"] is None

    body = _get(client, q="ועדת")
    assert sorted(_titles(body)) == [f"ועדת {i}" for i in range(_COMMITTEES)]
    assert body["total"] == _COMMITTEES

    assert _get(client, q="אין-כזה")["total"] == 0


def test_stale_catalog_is_served_while_it_rebuilds(env, monkeypatch):
    client, _ = env
    first = _get(client)
    started = []

    async def _refresh():
        started.append(True)

    monkeypatch.setattr(home_catalog.cache, "_refresh", _refresh)
    home_catalog.cache.built_at -= home_catalog.cache.ttl_seconds + 1
    # Answered from the stale copy, with one rebuild kicked off behind it.
    assert _get(client) == first
    assert started == [True]
