"""Approving a data.gov.il request puts its rows in NEON, not just its files.

A CKAN dataset is tracked because its ROWS matter — that is what the /data SQL
console reads. Approval used to derive the plan from the global file backend,
so a request approved without an explicit choice came out as "r2": files
archived, nothing in the append DB, no SQL, and nothing in the UI saying half
the plan was missing.

Scrapers are unchanged: they archive PDFs/ZIPs/catalog indexes, and NEON is not
meaningful for them.
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
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.admin import router as admin_router  # noqa: E402
from app.api.datasets import storage_target_of  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter, rate_limit_exceeded_handler  # noqa: E402

_TABLES = [
    Organization.__table__,
    User.__table__,
    TrackedDataset.__table__,
    Tag.__table__,
    dataset_tags,
    VersionIndex.__table__,
]

_CKAN = uuid.uuid4()
_SCRAPER = uuid.uuid4()


@pytest.fixture()
def stack(monkeypatch):
    monkeypatch.setattr(settings, "odata_api_key", "")   # no mirror round-trip
    monkeypatch.setattr(settings, "storage_backend", "r2")

    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            db.add(TrackedDataset(
                id=_CKAN, ckan_id="pkg-1", ckan_name="pkg-1", resource_id="r-1",
                resource_ids=["r-1"], title="מאגר CKAN", source_type="ckan",
                poll_interval=86400, status="pending",
            ))
            db.add(TrackedDataset(
                id=_SCRAPER, ckan_id="scr-1", ckan_name="scr-1", title="מאגר גירוד",
                source_type="scraper", source_url="https://example.gov.il/x",
                scraper_config={"kind": "mankal"},
                poll_interval=86400, status="pending",
            ))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False), Session


def _plan(Session, ds_id):
    async def _load():
        async with Session() as db:
            ds = (await db.execute(
                select(TrackedDataset).where(TrackedDataset.id == ds_id)
            )).scalar_one()
            return storage_target_of(ds.scraper_config)

    return asyncio.run(_load())


def test_ckan_approval_defaults_to_the_dual_write(stack):
    client, Session = stack
    r = client.post(f"/api/admin/approve/{_CKAN}")
    assert r.status_code == 200, r.text
    assert _plan(Session, _CKAN) == "r2+neon"


def test_an_explicit_choice_still_wins(stack):
    """The admin picking a plan is never overridden by the default."""
    client, Session = stack
    r = client.post(f"/api/admin/approve/{_CKAN}", json={"storage_target": "r2"})
    assert r.status_code == 200, r.text
    assert _plan(Session, _CKAN) == "r2"


def test_a_scraper_is_still_files_only(stack):
    client, Session = stack
    r = client.post(f"/api/admin/approve/{_SCRAPER}")
    assert r.status_code == 200, r.text
    assert _plan(Session, _SCRAPER) == "r2"
