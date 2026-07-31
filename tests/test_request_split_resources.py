"""One picked CSV → one independent dataset.

A CKAN package on data.gov.il is a folder of unrelated tables. Ticking five
CSVs used to create ONE tracked dataset mirroring all five together — one poll
cadence, one versions page, one shared archive. ``split_resources`` gives each
file its own TrackedDataset instead, which is also what the NEON/SQL path wants
(a single tracked resource per dataset).

Also pins the dedup rule: a file already tracked (by any dataset on the same
package) is reported as a duplicate and skipped, not created twice.
"""
import asyncio
import os
import sys

os.environ.setdefault("JWT_SECRET_KEY", "test")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api import datasets as datasets_api  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.rate_limit import limiter, rate_limit_exceeded_handler  # noqa: E402


CKAN_ID = "elections-package"
_R1, _R2, _R3 = "res-aaa", "res-bbb", "res-ccc"

PKG = {
    "name": "elections",
    "title": "תוצאות בחירות",
    "organization": {"name": "knesset"},
    "resources": [
        {"id": _R1, "name": "תוצאות אמת 2022", "format": "CSV"},
        {"id": _R2, "name": "תוצאות אמת 2021", "format": "CSV"},
        {"id": _R3, "name": "קלפיות", "format": "CSV"},
    ],
}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def stack(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "min_poll_interval", 900)
    monkeypatch.setattr(settings, "odata_api_key", "")

    async def _package_show(ckan_id):
        if ckan_id != CKAN_ID:
            raise RuntimeError("not found")
        return PKG

    monkeypatch.setattr(datasets_api.ckan_client, "package_show", _package_show)

    db_path = tmp_path / "split.sqlite"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")

    async def _create():
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Organization.__table__.create(c))
            await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
            await conn.run_sync(lambda c: Tag.__table__.create(c))
            await conn.run_sync(lambda c: dataset_tags.create(c))

    _run(_create())
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(datasets_api.router)

    async def _db():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False), Session


def _rows(Session):
    async def _load():
        async with Session() as db:
            return (await db.execute(
                select(TrackedDataset).order_by(TrackedDataset.title)
            )).scalars().all()

    return _run(_load())


def test_each_picked_file_becomes_its_own_dataset(stack):
    client, Session = stack

    r = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID,
        "resource_ids": [_R1, _R2, _R3],
        "split_resources": True,
        "preferred_interval": 86400,
    })
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 3
    assert {x["status"] for x in r.json()["results"]} == {"pending"}

    rows = _rows(Session)
    assert len(rows) == 3
    # Each dataset pins exactly one resource — that single-resource shape is
    # what routes the poll to the per-table NEON/SQL path.
    assert sorted(d.resource_id for d in rows) == sorted([_R1, _R2, _R3])
    assert all(d.resource_ids and len(d.resource_ids) == 1 for d in rows)
    assert all(d.resource_id == d.resource_ids[0] for d in rows)
    # ...and carries the file name, so the three are distinguishable.
    assert {d.title for d in rows} == {
        "תוצאות בחירות — תוצאות אמת 2022",
        "תוצאות בחירות — תוצאות אמת 2021",
        "תוצאות בחירות — קלפיות",
    }
    # Same requested cadence to start with; the admin can retune each one
    # independently afterwards, which is the point of splitting.
    assert all(d.poll_interval == 86400 for d in rows)
    assert all(d.status == "pending" for d in rows)


def test_combined_mode_still_makes_one_dataset(stack):
    """Without the flag, nothing changes: one dataset mirrors all the files."""
    client, Session = stack

    r = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID, "resource_ids": [_R1, _R2],
    })
    assert r.status_code == 201, r.text

    rows = _rows(Session)
    assert len(rows) == 1
    assert rows[0].resource_ids == [_R1, _R2]
    assert rows[0].title == "תוצאות בחירות"


def test_already_tracked_files_are_skipped_not_duplicated(stack):
    client, Session = stack

    first = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID, "resource_ids": [_R1], "split_resources": True,
    })
    assert first.status_code == 201, first.text

    # Re-submitting the same file alongside a new one adds only the new one.
    second = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID, "resource_ids": [_R1, _R2], "split_resources": True,
    })
    assert second.status_code == 201, second.text
    body = second.json()
    assert body["created"] == 1
    by_rid = {x["resource_id"]: x["status"] for x in body["results"]}
    assert by_rid == {_R1: "duplicate", _R2: "pending"}
    assert len(_rows(Session)) == 2

    # Everything already taken → nothing created, and the caller is told.
    third = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID, "resource_ids": [_R1, _R2], "split_resources": True,
    })
    assert third.status_code == 201, third.text
    assert third.json()["created"] == 0
    assert third.json()["status"] == "noop"
    assert len(_rows(Session)) == 2


def test_unknown_resource_is_rejected(stack):
    client, _ = stack
    r = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID, "resource_ids": ["res-nope"], "split_resources": True,
    })
    assert r.status_code == 400
    assert "not found on source" in r.json()["detail"]


def test_rate_limited_response_speaks_hebrew(stack):
    """A 429 must arrive as `detail` — the frontend reads nothing else, so
    slowapi's default {"error": …} surfaced as a bare 'שגיאת שרת (429)'."""
    client, _ = stack
    last = None
    for i in range(40):
        last = client.post("/api/datasets/requests", json={
            "ckan_id": CKAN_ID, "resource_ids": [_R1, _R2, _R3],
            "split_resources": True,
        })
        if last.status_code == 429:
            break
    assert last.status_code == 429, "the /requests limit never tripped"
    detail = last.json()["detail"]
    assert "יותר מדי בקשות" in detail
    assert "נסו שוב בעוד" in detail
    assert last.headers.get("Retry-After")
