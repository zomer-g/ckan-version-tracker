"""The admin datasets tab is paginated and searchable server-side.

GET /api/admin/datasets replaces the tab's old "fetch the entire catalog from
the public GET /api/datasets" load (~1,100 rows / 1MB in one shot). These tests
pin the two things that make that safe: the page slice + total are consistent,
and the free-text search really does span title / organization / tag / storage
plan — including the derived storage target, whose SQL twin (_storage_target_expr)
must stay in lockstep with storage_target_of in app/api/datasets.py.

Runs on in-memory SQLite (conftest maps JSONB → JSON), driven through a bare
FastAPI app carrying only the admin router, in the repo's dependency-light style.
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
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

_TABLES = [
    Organization.__table__,
    User.__table__,
    TrackedDataset.__table__,
    Tag.__table__,
    dataset_tags,
    VersionIndex.__table__,
]


def _ds(title, **kw):
    return TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=kw.pop("ckan_id", f"id-{title}"),
        ckan_name=kw.pop("ckan_name", f"name-{title}"),
        title=title,
        poll_interval=3600,
        is_active=True,
        status=kw.pop("status", "active"),
        **kw,
    )


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            org = Organization(id=uuid.uuid4(), name="mot", title="משרד התחבורה")
            db.add(org)
            tag = Tag(id=uuid.uuid4(), name="תחבורה")
            db.add(tag)
            rows = [
                _ds("קווי אוטובוס", organization="mot", organization_id=org.id,
                    source_type="ckan",
                    scraper_config={"storage_backend": "r2", "archive_neon": True}),
                _ds("שכבות מיפוי", organization="govmap", source_type="govmap",
                    scraper_config={"upload_mode": "local_only"}),
                _ds("מרשם רכב", source_type="ckan", storage_mode="append_only",
                    scraper_config={"storage_backend": "neon", "append_key": "x"}),
                _ds("ישיבת ועדה", ckan_name="knesset-committee-single-42"),
                _ds("מאגר שנדחה", status="pending"),
            ]
            db.add_all(rows)
            await db.flush()
            await db.execute(
                dataset_tags.insert().values(dataset_id=rows[1].id, tag_id=tag.id)
            )
            db.add(VersionIndex(
                id=uuid.uuid4(), tracked_dataset_id=rows[0].id, version_number=1,
                metadata_modified="2026-07-01T00:00:00",
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


def _titles(payload):
    return {i["title"] for i in payload["items"]}


def test_lists_only_administrable_active_datasets(client):
    r = client.get("/api/admin/datasets")
    assert r.status_code == 200, r.text
    body = r.json()
    # The pending row and the bulk-managed per-meeting Knesset row are excluded,
    # exactly as they are from the public catalog.
    assert body["total"] == 3
    assert _titles(body) == {"קווי אוטובוס", "שכבות מיפוי", "מרשם רכב"}


def test_page_slice_and_total_are_consistent(client):
    first = client.get("/api/admin/datasets?limit=2&offset=0").json()
    second = client.get("/api/admin/datasets?limit=2&offset=2").json()
    assert first["total"] == second["total"] == 3
    assert len(first["items"]) == 2 and len(second["items"]) == 1
    # No row served twice across the two pages.
    assert not (_titles(first) & _titles(second))


def test_search_matches_title_org_and_tag(client):
    assert _titles(client.get("/api/admin/datasets?q=אוטובוס").json()) == {"קווי אוטובוס"}
    # Organization title, not just the slug.
    assert _titles(client.get("/api/admin/datasets?q=התחבורה").json()) == {"קווי אוטובוס"}
    # Tag name — and the dataset appears exactly once.
    tagged = client.get("/api/admin/datasets?q=תחבורה").json()
    assert tagged["total"] == 2  # the org-title match + the tagged row
    assert _titles(tagged) == {"קווי אוטובוס", "שכבות מיפוי"}


def test_search_matches_storage_plan_keywords(client):
    # "neon" covers the NEON-only plan and the r2+neon combo.
    assert _titles(client.get("/api/admin/datasets?q=neon").json()) == {
        "קווי אוטובוס", "מרשם רכב",
    }
    assert _titles(client.get("/api/admin/datasets?q=מקומי").json()) == {"שכבות מיפוי"}
    assert _titles(client.get("/api/admin/datasets?q=תוספת").json()) == {"מרשם רכב"}


def test_storage_filter_is_exact(client):
    assert _titles(client.get("/api/admin/datasets?storage=r2%2Bneon").json()) == {
        "קווי אוטובוס",
    }
    assert _titles(client.get("/api/admin/datasets?storage=local").json()) == {"שכבות מיפוי"}
    assert _titles(client.get("/api/admin/datasets?storage=append_only").json()) == {
        "מרשם רכב",
    }
    assert client.get("/api/admin/datasets?storage=odata").json()["total"] == 0


def test_source_type_filter_and_version_count(client):
    body = client.get("/api/admin/datasets?source_type=govmap").json()
    assert _titles(body) == {"שכבות מיפוי"}
    counts = {
        i["title"]: i["version_count"]
        for i in client.get("/api/admin/datasets").json()["items"]
    }
    assert counts["קווי אוטובוס"] == 1
    assert counts["שכבות מיפוי"] == 0
