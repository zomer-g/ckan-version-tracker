"""The admin surface for per-source worker caps.

Sources are DERIVED from dataset columns (app/services/source_load.py), not
stored as entities, so the API has to be strict about which keys exist: a typo
would otherwise create a row that caps an imaginary upstream and is invisible
until someone wonders why a source never gets work.

Bare FastAPI app + in-memory SQLite, as in tests/test_admin_datasets_page.py.
"""
import asyncio
import os
import sys
import uuid

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

from app.api.admin import router as admin_router  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.scrape_task import ScrapeTask  # noqa: E402
from app.models.source_limit import SourceLimit  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

_TABLES = [
    Organization.__table__, User.__table__, TrackedDataset.__table__,
    Tag.__table__, dataset_tags, ScrapeTask.__table__, SourceLimit.__table__,
]


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            for ckan_id, source_type in [
                ("layer-1", "govmap"), ("layer-2", "govmap"),
                ("munidata-scraper-a", "scraper"),
                ("bus-lines", "ckan"),
            ]:
                db.add(TrackedDataset(
                    id=uuid.uuid4(), ckan_id=ckan_id, ckan_name=ckan_id,
                    title=ckan_id, source_type=source_type, is_active=True,
                ))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    yield TestClient(app)


def _by_key(client):
    r = client.get("/api/admin/source-limits")
    assert r.status_code == 200, r.text
    return {s["source_key"]: s for s in r.json()["sources"]}


def test_lists_every_derived_source_uncapped_by_default(client):
    rows = _by_key(client)
    assert set(rows) == {"govmap", "munidata", "ckan"}
    assert rows["govmap"]["datasets"] == 2
    assert all(r["max_workers"] is None for r in rows.values())


def test_set_and_clear_a_cap(client):
    r = client.put("/api/admin/source-limits/govmap", json={"max_workers": 3})
    assert r.status_code == 200, r.text
    assert _by_key(client)["govmap"]["max_workers"] == 3

    # Null is how the UI says "uncapped" — it must delete the row, not store 0,
    # which is a different and much stronger instruction.
    r = client.put("/api/admin/source-limits/govmap", json={"max_workers": None})
    assert r.status_code == 200, r.text
    assert _by_key(client)["govmap"]["max_workers"] is None


def test_zero_is_accepted_as_a_real_setting(client):
    r = client.put("/api/admin/source-limits/munidata", json={"max_workers": 0})
    assert r.status_code == 200, r.text
    assert _by_key(client)["munidata"]["max_workers"] == 0


def test_unknown_source_is_refused(client):
    """Otherwise a typo caps nothing, forever, silently."""
    r = client.put("/api/admin/source-limits/munidatta", json={"max_workers": 2})
    assert r.status_code == 404
    assert "munidata" in r.json()["detail"], "the error should list the real keys"


def test_out_of_range_is_refused(client):
    assert client.put("/api/admin/source-limits/govmap", json={"max_workers": -1}).status_code == 400
    assert client.put("/api/admin/source-limits/govmap", json={"max_workers": 500}).status_code == 400
