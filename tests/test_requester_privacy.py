"""The public catalog must not say WHO asked for each dataset.

Reported privately on 2026-10-04: ``GET /api/datasets``, which the home page
loads for every anonymous visitor, carried ``requester_name`` and
``requester_email`` for every tracked dataset — 75 of them populated with the
name and email of the person who requested the tracking, readable in devtools.

Both the public list and the admin list go through the same serializer
(``build_dataset_response``) so they cannot drift; the requester is now an
opt-in that only the admin-gated list takes. Pinned here from both ends:
the serializer itself, and the two endpoints over in-memory SQLite in the
same style as tests/test_archive_state.py.
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
from app.api.datasets import build_dataset_response  # noqa: E402
from app.api.datasets import router as datasets_router  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

REQUESTER_EMAIL = "requester@example.org"
REQUESTER_NAME = "פלוני אלמוני"


def _ds(title, **kw):
    return TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=f"id-{title}",
        ckan_name=f"name-{title}",
        title=title,
        poll_interval=3600,
        is_active=True,
        status="active",
        source_type="ckan",
        **kw,
    )


def test_serializer_drops_the_requester_unless_told_otherwise():
    requester = User(id=uuid.uuid4(), email=REQUESTER_EMAIL,
                     display_name=REQUESTER_NAME)
    ds = _ds("x")

    public = build_dataset_response(ds, requester, None, 0)
    assert public.requester_name is None
    assert public.requester_email is None

    admin = build_dataset_response(ds, requester, None, 0, with_requester=True)
    assert admin.requester_name == REQUESTER_NAME
    assert admin.requester_email == REQUESTER_EMAIL


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    tables = [
        Organization.__table__, User.__table__, TrackedDataset.__table__,
        Tag.__table__, dataset_tags, VersionIndex.__table__,
    ]

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=tables)
        async with Session() as db:
            requester = User(id=uuid.uuid4(), email=REQUESTER_EMAIL,
                             display_name=REQUESTER_NAME, hashed_password="x")
            db.add(requester)
            await db.flush()
            db.add(_ds("מאגר שמישהו ביקש", created_by=requester.id))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(datasets_router)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    yield TestClient(app)


def test_public_catalog_never_carries_the_requester(client):
    r = client.get("/api/datasets")
    assert r.status_code == 200, r.text
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["requester_name"] is None
    assert rows[0]["requester_email"] is None
    # Belt and braces: the values must not appear ANYWHERE in the payload.
    assert REQUESTER_EMAIL not in r.text
    assert REQUESTER_NAME not in r.text


def test_admin_list_still_shows_the_requester(client):
    r = client.get("/api/admin/datasets")
    assert r.status_code == 200, r.text
    (row,) = r.json()["items"]
    assert row["requester_name"] == REQUESTER_NAME
    assert row["requester_email"] == REQUESTER_EMAIL
