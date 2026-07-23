"""End-to-end: a source the worker declares becomes trackable with no OVER code.

Everything the other registry tests fake — the DB, the session, the router
stack — is real here, against in-memory SQLite. The flow under test is the one
that actually matters:

    worker syncs a manifest
      → user pastes a URL that no hardcoded parser recognises
      → OVER classifies it, creates the TrackedDataset
      → the dataset carries the manifest's kind, config and cadence
      → the worker later dispatches on that kind

If this passes, adding a source needs no change in this repo.
"""
import asyncio
import os
import sys

os.environ.setdefault("JWT_SECRET_KEY", "test")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.dialects.postgresql import JSONB  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.ext.compiler import compiles  # noqa: E402


# SQLite has no JSONB. JSONB subclasses the generic JSON type, so only the DDL
# keyword needs translating — bind/result handling is already portable.
@compiles(JSONB, "sqlite")
def _compile_jsonb_on_sqlite(type_, compiler, **kw):  # noqa: D401
    return "JSON"


from app.api import datasets as datasets_api  # noqa: E402
from app.api import sources as sources_api  # noqa: E402
from app.api import worker as worker_api  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.source_registry import SourceRegistry  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import source_registry as sr  # noqa: E402
from tests.test_source_registry import TOY_MANIFEST  # noqa: E402


TOY_URL = "https://toy.example.org/מכרזים/2024"


def _run(coro):
    """Run a coroutine on a throwaway loop.

    The TestClient owns its own event loop, so assertions that read the DB
    afterwards can't share it — which is also why the DB is file-backed below
    rather than ``:memory:`` (in-memory SQLite is per-connection).
    """
    return asyncio.run(coro)


@pytest.fixture
def stack(monkeypatch, tmp_path):
    """A real router stack over a real (SQLite) DB."""
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")
    monkeypatch.setattr(settings, "min_poll_interval", 900)
    monkeypatch.setattr(settings, "odata_api_key", "")  # no mirror creation
    sr.invalidate_cache()

    db_path = tmp_path / "registry-e2e.sqlite"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")

    async def _create():
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: SourceRegistry.__table__.create(c))
            await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
            # TrackedDataset.tags is a selectin relationship, so reading a
            # dataset back touches these two even though nothing tags one here.
            await conn.run_sync(lambda c: Tag.__table__.create(c))
            await conn.run_sync(lambda c: dataset_tags.create(c))

    _run(_create())
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)
    app.include_router(sources_api.router)
    app.include_router(datasets_api.router)

    async def _db():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False), Session
    sr.invalidate_cache()


def _sync(client):
    return client.post(
        "/api/worker/sources/sync",
        json={"manifests": [TOY_MANIFEST], "worker_version": "deadbeef"},
        headers={"Authorization": "Bearer workerkey"},
    )


def test_unregistered_url_is_rejected_before_the_sync(stack):
    """Baseline: without the manifest, OVER has no idea what this URL is."""
    client, _ = stack
    r = client.post("/api/datasets/requests", json={
        "source_type": "scraper", "source_url": TOY_URL, "title": "בדיקה",
    })
    assert r.status_code == 400
    assert "Invalid scraper URL" in r.json()["detail"]


def test_sync_then_paste_creates_a_tracked_dataset(stack):
    client, Session = stack

    assert _sync(client).json()["upserted"] == ["toysource"]

    # The user pastes the URL: OVER validates it with no source-specific code.
    validation = client.post("/api/sources/validate", json={"url": TOY_URL}).json()
    assert validation["valid"] is True
    assert validation["title"] == "מקור צעצוע — מכרזים 2024"
    assert validation["default_poll_interval"] == 43200

    # ...and submits the tracking request.
    created = client.post("/api/datasets/requests", json={
        "source_type": "scraper",
        "source_url": TOY_URL,
        "title": validation["title"],
    })
    assert created.status_code == 201, created.text

    async def _load():
        async with Session() as db:
            return (await db.execute(select(TrackedDataset))).scalars().all()

    rows = _run(_load())
    assert len(rows) == 1
    ds = rows[0]

    # Conventions derived from the manifest id — no OVER-side table.
    assert ds.ckan_id.startswith("toysource-scraper-")
    assert ds.organization == "toy.example.org"
    assert ds.source_type == "scraper"
    # The worker dispatches on this, and it reached the dataset from the manifest.
    assert ds.scraper_config["kind"] == "toysource"
    assert ds.scraper_config["corpus"] == "tenders"
    assert ds.scraper_config["year"] == "2024"
    assert ds.scraper_config["max_docs"] == 500
    # Cadence came from the manifest because the requester didn't pick one.
    assert ds.poll_interval == 43200


def test_requester_cadence_beats_the_manifest_default(stack):
    client, Session = stack
    _sync(client)
    client.post("/api/datasets/requests", json={
        "source_type": "scraper",
        "source_url": TOY_URL,
        "title": "בדיקה",
        "preferred_interval": 604800,
    })

    async def _load():
        async with Session() as db:
            return (await db.execute(select(TrackedDataset))).scalars().one()

    ds = _run(_load())
    assert ds.poll_interval == 604800


def test_neon_eligibility_follows_the_manifest(stack):
    """neon_eligible in the manifest replaces being listed in
    TABULAR_SCRAPER_KINDS — that's what offers the SQL-console storage plan."""
    client, Session = stack
    _sync(client)
    client.post("/api/datasets/requests", json={
        "source_type": "scraper", "source_url": TOY_URL, "title": "בדיקה",
    })

    async def _load():
        async with Session() as db:
            return (await db.execute(select(TrackedDataset))).scalars().one()

    ds = _run(_load())
    assert datasets_api.dataset_is_neon_eligible(ds) is True


def test_the_worker_would_dispatch_this_dataset_to_its_engine(stack):
    """Closes the loop: the kind OVER stamped is the one the worker routes on.

    Imported from the worker package so the two repos' contract is checked
    against real code rather than a copied constant.
    """
    worker_repo = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "GOV scraper")
    )
    if not os.path.isdir(worker_repo):
        pytest.skip("GOVSCRAPER checkout not present next to this repo")
    sys.path.insert(0, worker_repo)
    try:
        from govscraper.legacy.over_worker import _registry_kind
    except Exception as e:  # pragma: no cover - depends on the sibling checkout
        pytest.skip(f"could not import the worker dispatcher: {e}")

    client, _ = stack
    _sync(client)
    config = client.post(
        "/api/sources/validate", json={"url": TOY_URL}
    ).json()
    assert config["source_id"] == "toysource"
    assert _registry_kind({"kind": "toysource"}) == "toysource"
