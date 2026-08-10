"""What of a collection is already here — answered BEFORE the user picks.

The picker used to submit blind: the duplicate check ran server-side after the
submit, so ticking a file that was already archived came back as an error. This
endpoint puts the same claim rule in front of the choice.

The rule it must agree with is the split-request handler's (see
test_request_split_resources). The pin that matters most here: a file the
coverage view calls selectable must be one the submit will actually create —
in particular the files of a PENDING combined request, which the split path
supersedes rather than refusing.
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


CKAN_ID = "streets-package"
_LIVE_CSV, _LIVE_XML, _OLD_CSV = "res-csv-live", "res-xml-live", "res-csv-2019"

PKG = {
    "id": CKAN_ID,
    "name": "israel-streets",
    "title": "רשימת רחובות בישראל",
    "organization": {"name": "population_authority"},
    "resources": [
        {"id": _LIVE_CSV, "name": "רחובות - מתעדכן", "format": "csv",
         "last_modified": "2026-08-02T00:31:06", "size": 9301206,
         "datastore_active": True},
        {"id": _LIVE_XML, "name": "רחובות - מתעדכן", "format": "XML",
         "last_modified": "2026-08-09T00:30:46", "size": 47029414},
        {"id": _OLD_CSV, "name": "רחובות 2019", "format": "CSV",
         "last_modified": "2019-05-02T05:20:29", "size": 12984090},
    ],
}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def stack(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "min_poll_interval", 900)
    monkeypatch.setattr(settings, "odata_api_key", "")

    async def _package_show(ckan_id):
        if ckan_id not in (CKAN_ID, PKG["name"]):
            raise RuntimeError("not found")
        return PKG

    monkeypatch.setattr(datasets_api.ckan_client, "package_show", _package_show)

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'cov.sqlite').as_posix()}"
    )

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


def _seed(Session, **kwargs):
    async def _add():
        async with Session() as db:
            ds = TrackedDataset(
                ckan_id=kwargs.pop("ckan_id", CKAN_ID),
                ckan_name=PKG["name"],
                title=kwargs.pop("title", "מאגר"),
                **kwargs,
            )
            db.add(ds)
            await db.commit()
            return str(ds.id)

    return _run(_add())


def _by_id(payload):
    return {r["id"]: r for r in payload["resources"]}


def test_untracked_package_is_all_free(stack):
    client, _ = stack
    r = client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}")
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["total"], body["collected"], body["free"]) == (3, 0, 3)
    assert all(e["state"] == "free" and e["selectable"] for e in body["resources"])


def test_collected_file_names_the_dataset_holding_it(stack):
    """The whole point: "already collected" must come with somewhere to go."""
    client, Session = stack
    held = _seed(Session, resource_id=_LIVE_XML, resource_ids=[_LIVE_XML],
                 status="active", title="רחובות — XML")

    body = client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json()
    entries = _by_id(body)
    assert entries[_LIVE_XML]["state"] == "collected"
    assert entries[_LIVE_XML]["selectable"] is False
    assert entries[_LIVE_XML]["dataset_id"] == held
    assert entries[_LIVE_XML]["dataset_title"] == "רחובות — XML"
    # The rest of the folder stays addable — that's the feature.
    assert entries[_LIVE_CSV]["state"] == "free"
    assert body["collected"] == 1 and body["free"] == 2


def test_pending_single_file_request_is_not_offered_again(stack):
    client, Session = stack
    _seed(Session, resource_id=_LIVE_CSV, resource_ids=[_LIVE_CSV],
          status="pending", title="ממתין")

    entries = _by_id(client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json())
    assert entries[_LIVE_CSV]["state"] == "pending"
    assert entries[_LIVE_CSV]["selectable"] is False


def test_pending_combined_request_keeps_its_files_selectable(stack):
    """A combined request is superseded by a split submit, not honoured.

    Reporting its files as taken is what would lock the package forever: the
    first "select all" submit claims everything, and every later per-file
    request reads back as a duplicate.
    """
    client, Session = stack
    _seed(Session, resource_ids=[_LIVE_CSV, _LIVE_XML, _OLD_CSV],
          status="pending", title="בקשה מאוחדת")

    body = client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json()
    assert all(e["selectable"] for e in body["resources"])
    assert body["free"] == 3

    # ...and the submit agrees: it supersedes the combined row and creates one
    # dataset per file. Coverage promising what the submit refuses is the bug.
    r = client.post("/api/datasets/requests", json={
        "ckan_id": CKAN_ID,
        "resource_ids": [_LIVE_CSV, _LIVE_XML, _OLD_CSV],
        "split_resources": True,
    })
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 3


def test_rejected_row_does_not_block_a_file(stack):
    """A rejection is not a tombstone — see HOLDING_STATES."""
    client, Session = stack
    _seed(Session, resource_id=_LIVE_CSV, resource_ids=[_LIVE_CSV],
          status="rejected", title="נדחה")

    entries = _by_id(client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json())
    assert entries[_LIVE_CSV]["state"] == "free"


def test_legacy_track_all_row_claims_the_whole_package(stack):
    """A row pinning nothing mirrors everything — so nothing is free."""
    client, Session = stack
    _seed(Session, status="active", title="הכול")

    body = client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json()
    assert body["collected"] == 3 and body["free"] == 0


def test_slug_and_uuid_are_the_same_package(stack):
    """Requested under the slug, looked up by uuid (or the reverse)."""
    client, Session = stack
    _seed(Session, ckan_id=PKG["name"], resource_id=_LIVE_CSV,
          resource_ids=[_LIVE_CSV], status="active", title="לפי slug")

    entries = _by_id(client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json())
    assert entries[_LIVE_CSV]["state"] == "collected"


def test_picker_metadata_tells_live_files_from_frozen_ones(stack):
    """20 of 22 files on the real package are dead 2019 copies; the reader
    needs the dates and the datastore flag to pick the one that still moves."""
    client, _ = stack
    entries = _by_id(client.get(f"/api/datasets/ckan-coverage/{CKAN_ID}").json())
    assert entries[_LIVE_CSV]["last_modified"].startswith("2026-08-02")
    assert entries[_OLD_CSV]["last_modified"].startswith("2019-")
    assert entries[_LIVE_CSV]["datastore_active"] is True
    assert entries[_LIVE_XML]["datastore_active"] is False
    # Format is normalised — the source mixes "csv" and "CSV".
    assert entries[_LIVE_CSV]["format"] == "CSV"


def test_unknown_package_is_404(stack):
    client, _ = stack
    assert client.get("/api/datasets/ckan-coverage/nope").status_code == 404
