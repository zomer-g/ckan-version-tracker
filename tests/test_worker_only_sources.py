"""A worker may claim only the sources it actually has an engine for.

The queue was blind to engines. `next_pending_task_q` handed out the
highest-priority pending task to whoever polled first, so a worker missing that
source's engine received work it could not run — and there is no handing a task
back: `report_failure` sets status='failed' terminally. The only outcomes were a
bogus failure on a dataset that was never broken, or a wrong scrape.

`?only_sources=` on /api/worker/poll is the missing declaration: the worker names
the source keys it can process, in the same vocabulary the per-source caps
already use (the manifest id, which is also `scraper_config["kind"]`).

What's pinned here:

  1. the positive filter restricts the claim, even past a higher-priority task
     from a source the worker didn't declare;
  2. it composes with the negative cap filter, and the cap wins;
  3. omitting the parameter builds EXACTLY the old query — there are live
     workers, and an opt-in that changes their behaviour is not opt-in;
  4. an unknown key is refused loudly, because silently matching nothing would
     make a typo look like an empty queue forever;
  5. the key vocabulary is derived from the catalog and the registry, not from a
     second hardcoded list of source names.

In-memory SQLite, asyncio.run, in the style of tests/test_source_load.py.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api import worker as worker_api  # noqa: E402
from app.api.worker import MAX_ONLY_SOURCES, _requested_sources, next_pending_task_q  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.scrape_task import PRIORITY_ROUTINE, ScrapeTask  # noqa: E402
from app.models.source_limit import SourceLimit  # noqa: E402
from app.models.source_registry import SourceRegistry  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import source_registry as sr  # noqa: E402
from app.services.source_load import known_source_keys, saturated_sources  # noqa: E402

NOW = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)

# Bands, so "the task a worker would otherwise be handed" is unambiguous.
PRIORITY_URGENT = PRIORITY_ROUTINE + 10


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def clean_registry_cache():
    """The manifest cache is process-local; a leftover source would make one
    test's known-key set depend on which test ran first."""
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      ScrapeTask.__table__, SourceLimit.__table__,
                      SourceRegistry.__table__):
            await conn.run_sync(lambda c, t=table: t.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _ds(ckan_id: str, source_type: str) -> TrackedDataset:
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=ckan_id, ckan_name=ckan_id, title=ckan_id,
        source_type=source_type, is_active=True,
    )


async def _queue(db, spec):
    """spec: [(ckan_id, source_type, status, minutes_old, priority)]."""
    for ckan_id, source_type, status, age, priority in spec:
        ds = _ds(ckan_id, source_type)
        db.add(ds)
        db.add(ScrapeTask(
            id=uuid.uuid4(), tracked_dataset_id=ds.id, status=status,
            priority=priority, created_at=NOW - timedelta(minutes=age),
        ))
    await db.commit()


async def _claimed(db, *args, **kwargs):
    """The ckan_id the next claim would hand out, or None."""
    row = (await db.execute(next_pending_task_q(*args, **kwargs))).first()
    return None if row is None else row[1].ckan_id


# ── 1. the positive filter restricts the claim ────────────────────────────

def test_a_worker_gets_only_the_sources_it_declared():
    """The govmap task outranks everything and is what an undeclaring worker
    would receive. A worker that only runs munidata gets the munidata task."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "pending", 60, PRIORITY_URGENT),
                ("munidata-scraper-a", "scraper", "pending", 5, PRIORITY_ROUTINE),
            ])
            assert await _claimed(db) == "layer-1", "the unfiltered claim"
            assert await _claimed(db, (), ["munidata"]) == "munidata-scraper-a"
    _run(go())


def test_declaring_several_sources_keeps_the_priority_order_among_them():
    """The filter narrows the candidates; it must not reorder them."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "pending", 60, PRIORITY_URGENT),
                ("jda-scraper-tenders", "scraper", "pending", 90, PRIORITY_ROUTINE),
                ("munidata-scraper-a", "scraper", "pending", 5, PRIORITY_URGENT),
            ])
            # Both declared: the urgent one wins, not the oldest.
            assert await _claimed(db, (), ["jda", "munidata"]) == "munidata-scraper-a"
            # Only the routine one declared: age decides within the band.
            assert await _claimed(db, (), ["jda"]) == "jda-scraper-tenders"
    _run(go())


def test_declaring_a_source_with_nothing_pending_yields_no_task():
    """204, not someone else's task. This is the whole point: a worker that
    cannot run what's queued must go home empty rather than fail it."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [("layer-1", "govmap", "pending", 60, PRIORITY_URGENT)])
            assert await _claimed(db, (), ["munidata"]) is None
    _run(go())


def test_declaring_nothing_claims_nothing_rather_than_everything():
    """An empty-but-present list is a worker saying it can run no source. The
    dangerous reading of it is "no restriction" — that hands it every task."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [("layer-1", "govmap", "pending", 60, PRIORITY_URGENT)])
            assert await _claimed(db, (), []) is None
    _run(go())


def test_the_positive_filter_selects_the_same_rows_the_negative_one_drops():
    """Both directions are built from source_filter, so declaring exactly the
    sources a cap would exclude must be the complement of that exclusion."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "pending", 60, PRIORITY_ROUTINE),
                ("munidata-scraper-a", "scraper", "pending", 30, PRIORITY_ROUTINE),
                ("jda-scraper-tenders", "scraper", "pending", 5, PRIORITY_ROUTINE),
            ])
            assert await _claimed(db, ["govmap"]) == "munidata-scraper-a"
            assert await _claimed(db, (), ["munidata", "jda"]) == "munidata-scraper-a"
    _run(go())


# ── 2. the two filters compose, and the cap wins ──────────────────────────

def test_a_capped_source_stays_excluded_even_when_the_worker_asked_for_it():
    """A cap is the server's call about an upstream. A worker declaring it can
    run govmap must not be able to vote itself past govmap's limit."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30, PRIORITY_ROUTINE),
                ("layer-2", "govmap", "running", 25, PRIORITY_ROUTINE),
                ("layer-3", "govmap", "pending", 20, PRIORITY_URGENT),
                ("munidata-scraper-a", "scraper", "pending", 5, PRIORITY_ROUTINE),
            ])
            db.add(SourceLimit(source_key="govmap", max_workers=2))
            await db.commit()

            blocked = await saturated_sources(db)
            assert blocked == {"govmap": (2, 2)}

            # Declares both; govmap is full, so munidata is what's left.
            assert await _claimed(
                db, blocked.keys(), ["govmap", "munidata"]
            ) == "munidata-scraper-a"

            # Declares only the capped source: nothing, not the munidata task.
            assert await _claimed(db, blocked.keys(), ["govmap"]) is None
    _run(go())


# ── 3. the parameter is opt-in, and absence changes nothing ───────────────

def test_omitting_the_parameter_builds_exactly_the_old_query():
    """There are live workers that will never send this. The claim they get must
    be byte-for-byte the query that shipped before the parameter existed."""
    before = str(next_pending_task_q(["govmap"]).compile(
        compile_kwargs={"literal_binds": True}))
    after = str(next_pending_task_q(["govmap"], None).compile(
        compile_kwargs={"literal_binds": True}))
    assert before == after, "an absent only_sources must add no SQL at all"


def test_an_undeclaring_worker_still_gets_any_source():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "pending", 60, PRIORITY_URGENT),
                ("munidata-scraper-a", "scraper", "pending", 5, PRIORITY_ROUTINE),
            ])
            assert await _claimed(db, (), None) == "layer-1"
    _run(go())


# ── 4. parsing, and what happens to a typo ────────────────────────────────

def test_absent_parameter_parses_to_no_restriction_without_touching_the_db():
    """None short-circuits: an undeclaring worker must not pay for a lookup it
    doesn't use, on a path that runs ~720 times an hour per worker."""
    class _Exploding:
        async def execute(self, stmt):
            raise AssertionError("known_source_keys must not run for an absent param")

    assert _run(_requested_sources(_Exploding(), None)) is None


def test_keys_are_split_stripped_and_deduplicated():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add_all([_ds("layer-1", "govmap"), _ds("munidata-scraper-a", "scraper")])
            await db.commit()
            got = await _requested_sources(db, " govmap , munidata ,govmap, ")
            assert got == ["govmap", "munidata"]
    _run(go())


def test_an_unknown_key_is_refused_instead_of_matching_nothing():
    """The nasty failure mode this avoids: 'govmpa' matches no dataset, so the
    worker would poll a queue that looks permanently empty, forever, silently."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add(_ds("layer-1", "govmap"))
            await db.commit()
            with pytest.raises(HTTPException) as exc:
                await _requested_sources(db, "govmap,govmpa")
            assert exc.value.status_code == 400
            assert "govmpa" in exc.value.detail
            assert "govmap" in exc.value.detail, "must say what IS known"
    _run(go())


def test_a_key_is_not_case_folded():
    """A source key is the manifest id, which is also the scraper_config['kind']
    the worker looks its engine up by — normalising here would invent a second
    spelling of a name that has exactly one (see source_registry._check_id)."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add(_ds("layer-1", "govmap"))
            await db.commit()
            with pytest.raises(HTTPException) as exc:
                await _requested_sources(db, "GovMap")
            assert exc.value.status_code == 400
    _run(go())


def test_an_empty_value_is_an_error_not_a_default():
    """It can only be a worker that meant to name its engines and produced
    nothing. Both readings of it — everything, nothing — are wrong quietly."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add(_ds("layer-1", "govmap"))
            await db.commit()
            for raw in ("", "   ", ",", " , "):
                with pytest.raises(HTTPException) as exc:
                    await _requested_sources(db, raw)
                assert exc.value.status_code == 400, raw
    _run(go())


def test_an_absurd_number_of_keys_is_refused():
    """The list travels in a URL, and each key adds an OR branch to the claim."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add(_ds("layer-1", "govmap"))
            await db.commit()
            raw = ",".join(f"s{i}" for i in range(MAX_ONLY_SOURCES + 1))
            with pytest.raises(HTTPException) as exc:
                await _requested_sources(db, raw)
            assert exc.value.status_code == 400
            assert str(MAX_ONLY_SOURCES) in exc.value.detail
    _run(go())


# ── 5. where the vocabulary comes from ────────────────────────────────────

def test_known_keys_are_derived_from_the_catalog():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            db.add_all([
                _ds("munidata-scraper-a", "scraper"),
                _ds("jda-scraper-tenders", "scraper"),
                _ds("layer-1", "govmap"),
                _ds("bus-lines", "ckan"),
            ])
            await db.commit()
            assert await known_source_keys(db) == {"munidata", "jda", "govmap", "ckan"}
    _run(go())


def test_a_registered_source_is_known_before_its_first_dataset_exists():
    """A source onboarded via POST /api/worker/sources/sync has no datasets for
    a while. A worker shipping its engine must be able to declare it in that
    window, or onboarding would deadlock on the queue being empty."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            manifest = {
                "manifest_version": 1,
                "id": "toysource",
                "label_he": "מקור צעצוע",
                "label_en": "Toy Source",
                "site_url": "https://toy.example.org/",
                "badge": {"bg": "#fae8ff", "fg": "#86198f", "accent": "#c026d3"},
                "url_patterns": [{"regex": r"^https://toy\.example\.org/x/?$"}],
            }
            db.add(SourceRegistry(
                id="toysource", manifest=manifest,
                manifest_hash=sr.manifest_hash(manifest),
            ))
            db.add(_ds("layer-1", "govmap"))
            await db.commit()
            await sr.load_enabled(db, force=True)

            assert await known_source_keys(db) == {"govmap", "toysource"}
            assert await _requested_sources(db, "toysource") == ["toysource"]
    _run(go())


# ── 6. the HTTP surface ───────────────────────────────────────────────────
#
# The query parameter and its 400 are the contract a deployed worker sees, so
# they are checked through the endpoint and not only through the helper.


class _CatalogDB:
    """Enough AsyncSession for the poll's pre-dispatch work.

    `known_source_keys` reads (ckan_id, source_type) pairs; the fleet
    bookkeeping (worker_fleet.touch_worker) looks a machine up and adds one; the
    stuck-task sweep and the claim find nothing.
    """

    def __init__(self, rows):
        self.rows = rows
        # _acquire_claim_lock asks the dialect before reaching for an advisory
        # lock; off Postgres it is a no-op (see app/api/worker.py).
        self.bind = type("_Bind", (), {"dialect": type("_D", (), {"name": "sqlite"})()})()

    async def get(self, model, key):
        return None

    def add(self, obj):
        pass

    async def rollback(self):
        pass

    async def commit(self):
        pass

    async def execute(self, stmt):
        # Only the catalog read gets rows. Answering every statement with them
        # would feed (ckan_id, source_type) pairs to the per-source cap lookup,
        # which reads them as (key, max_workers).
        compiled = str(stmt)
        rows = (self.rows
                if "tracked_datasets.ckan_id" in compiled and "scrape_tasks" not in compiled
                else [])

        class _Result:
            def all(self_inner):
                return list(rows)

            def first(self_inner):
                return None

            def scalars(self_inner):
                class _S:
                    def all(self_deep):
                        return []
                return _S()

            def scalar_one_or_none(self_inner):
                return None
        return _Result()


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")
    monkeypatch.setattr(settings, "worker_version_check_enabled", False)

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)

    async def _db():
        yield _CatalogDB([("layer-1", "govmap"), ("munidata-scraper-a", "scraper")])

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _poll(client, query=""):
    return client.get(
        f"/api/worker/poll{query}",
        headers={"Authorization": "Bearer workerkey", "X-Worker-Id": "tp14"},
    )


def test_poll_accepts_a_declared_source(client):
    assert _poll(client, "?only_sources=govmap,munidata").status_code == 204


def test_poll_without_the_parameter_is_unchanged(client):
    assert _poll(client).status_code == 204


def test_poll_rejects_an_unknown_source_key(client):
    r = _poll(client, "?only_sources=govmap,nosuchsource")
    assert r.status_code == 400
    assert "nosuchsource" in r.json()["detail"]


def test_poll_rejects_an_unknown_key_before_it_is_an_empty_queue(client):
    """Ordering matters: the refusal must come from the key, not from the claim
    finding nothing — otherwise a typo answers 204 and looks like idle."""
    r = _poll(client, "?only_sources=typo")
    assert r.status_code == 400


def test_poll_requires_the_worker_key_regardless_of_the_parameter(client):
    r = client.get("/api/worker/poll?only_sources=govmap")
    assert r.status_code == 401
