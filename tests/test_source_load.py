"""Per-source worker caps: one upstream must not be able to eat the fleet.

The queue's priority bands answer "what next"; they never answered "how many at
once, to the same site". With GovMap at 866 of ~1,100 tracked datasets, a sweep
can put every worker on one server — rude, and a good way to get blocked while
every other source waits behind work aimed at one place.

Four things are pinned here:

  1. source_key / source_filter — the Python and SQL directions of "which
     upstream is this dataset from" must select the same rows, or the cap would
     count one set and block another;
  2. the cap excludes a saturated source from the CLAIM, letting the next-best
     task from another source through rather than stalling the fleet;
  3. an uncapped system builds exactly the query it built before;
  4. a lowered cap drains — it never aborts running work.

In-memory SQLite, asyncio.run, in the repo's dependency-light style.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api.worker import next_pending_task_q  # noqa: E402
from app.models.scrape_task import PRIORITY_ROUTINE, ScrapeTask  # noqa: E402
from app.models.source_limit import SourceLimit  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.services.source_load import (  # noqa: E402
    running_by_source,
    saturated_sources,
    source_filter,
    source_key,
    source_load,
)

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for table in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      ScrapeTask.__table__, SourceLimit.__table__):
            await conn.run_sync(lambda c, t=table: t.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _ds(ckan_id: str, source_type: str, title: str | None = None) -> TrackedDataset:
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=ckan_id, ckan_name=ckan_id, title=title or ckan_id,
        source_type=source_type, is_active=True,
    )


# ── 1. the key and its SQL twin ───────────────────────────────────────────

def test_source_key_reads_the_scraper_prefix_then_the_type():
    """Every shape present in the live catalog."""
    assert source_key("munidata-scraper-tel-aviv", "scraper") == "munidata"
    assert source_key("jda-scraper-tenders", "scraper") == "jda"
    assert source_key("ykpubdata-scraper-licences", "scraper") == "ykpubdata"
    # Non-scraper rows have no prefix — the type IS the upstream.
    assert source_key("6e1b1b0a-1234", "govmap") == "govmap"
    assert source_key("bus-lines-abc", "ckan") == "ckan"
    assert source_key("cbs-index", "cbs") == "cbs"


def test_unclassifiable_dataset_is_not_invisible():
    """A row that matches nothing must land somewhere a human can see and cap,
    rather than quietly escaping every limit."""
    assert source_key(None, None) == "unknown"
    assert source_key("", "  ") == "unknown"


def test_source_filter_selects_exactly_what_source_key_counts():
    """The whole cap rests on these two agreeing: counting one set of rows and
    excluding a different set would enforce a number nobody configured."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            rows = [
                _ds("munidata-scraper-a", "scraper"),
                _ds("munidata-scraper-b", "scraper"),
                _ds("jda-scraper-tenders", "scraper"),
                _ds("layer-52", "govmap"),
                _ds("bus-lines", "ckan"),
                _ds("cbs-index", "cbs"),
            ]
            db.add_all(rows)
            await db.commit()

            expected: dict[str, set] = {}
            for r in rows:
                expected.setdefault(source_key(r.ckan_id, r.source_type), set()).add(r.id)

            for key, ids in expected.items():
                got = set((await db.scalars(
                    select(TrackedDataset.id).where(source_filter(key))
                )).all())
                assert got == ids, f"source_filter({key!r}) disagrees with source_key"
    _run(go())


# ── 2. the cap on the claim path ──────────────────────────────────────────

async def _queue(db, spec):
    """spec: [(ckan_id, source_type, status, minutes_old)] → {ckan_id: dataset}."""
    out = {}
    for ckan_id, source_type, status, age in spec:
        ds = _ds(ckan_id, source_type)
        db.add(ds)
        db.add(ScrapeTask(
            id=uuid.uuid4(), tracked_dataset_id=ds.id, status=status,
            priority=PRIORITY_ROUTINE, created_at=NOW - timedelta(minutes=age),
        ))
        out[ckan_id] = ds
    await db.commit()
    return out


def test_saturated_source_is_skipped_and_another_source_goes_out():
    """The point of excluding rather than stalling: govmap is full, so the
    munidata task behind it is handed out instead of the fleet idling."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-2", "govmap", "running", 25),
                ("layer-3", "govmap", "pending", 20),   # oldest pending — would win
                ("munidata-scraper-a", "scraper", "pending", 5),
            ])
            db.add(SourceLimit(source_key="govmap", max_workers=2))
            await db.commit()

            blocked = await saturated_sources(db)
            assert blocked == {"govmap": (2, 2)}

            _, ds = (await db.execute(next_pending_task_q(blocked.keys()))).first()
            assert ds.ckan_id == "munidata-scraper-a"
    _run(go())


def test_source_under_its_cap_still_gets_work():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-3", "govmap", "pending", 20),
                ("munidata-scraper-a", "scraper", "pending", 5),
            ])
            db.add(SourceLimit(source_key="govmap", max_workers=2))
            await db.commit()

            blocked = await saturated_sources(db)
            assert blocked == {}
            _, ds = (await db.execute(next_pending_task_q(blocked.keys()))).first()
            assert ds.ckan_id == "layer-3"
    _run(go())


def test_zero_stops_a_source_without_touching_the_rest():
    """0 is the "this upstream is erroring, back off" setting."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("jda-scraper-tenders", "scraper", "pending", 60),
                ("munidata-scraper-a", "scraper", "pending", 5),
            ])
            db.add(SourceLimit(source_key="jda", max_workers=0))
            await db.commit()

            blocked = await saturated_sources(db)
            assert blocked == {"jda": (0, 0)}
            _, ds = (await db.execute(next_pending_task_q(blocked.keys()))).first()
            assert ds.ckan_id == "munidata-scraper-a"
    _run(go())


def test_every_source_capped_out_means_no_task_not_a_wrong_task():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [("jda-scraper-tenders", "scraper", "pending", 60)])
            db.add(SourceLimit(source_key="jda", max_workers=0))
            await db.commit()

            blocked = await saturated_sources(db)
            assert (await db.execute(next_pending_task_q(blocked.keys()))).first() is None
    _run(go())


def test_no_limits_configured_changes_nothing():
    """The default state of the table is empty; the fleet must behave exactly
    as it did before this feature existed."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-2", "govmap", "running", 25),
                ("layer-3", "govmap", "pending", 20),
                ("munidata-scraper-a", "scraper", "pending", 5),
            ])
            assert await saturated_sources(db) == {}
            _, ds = (await db.execute(next_pending_task_q())).first()
            assert ds.ckan_id == "layer-3", "oldest pending, exactly as before"
    _run(go())


def test_lowered_cap_drains_rather_than_aborting_running_work():
    """Three workers are on govmap and the cap is cut to 1. The running tasks
    keep running — the cap takes effect by handing govmap nothing new."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-2", "govmap", "running", 25),
                ("layer-3", "govmap", "running", 20),
                ("layer-4", "govmap", "pending", 10),
            ])
            db.add(SourceLimit(source_key="govmap", max_workers=1))
            await db.commit()

            blocked = await saturated_sources(db)
            assert blocked == {"govmap": (3, 1)}, "over the cap, and honest about it"
            assert (await db.execute(next_pending_task_q(blocked.keys()))).first() is None

            still_running = (await db.scalars(
                select(ScrapeTask).where(ScrapeTask.status == "running")
            )).all()
            assert len(still_running) == 3, "a cap must never kill work in flight"
    _run(go())


# ── 3. what the admin panel reads ─────────────────────────────────────────

def test_source_load_reports_counts_per_source():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-2", "govmap", "pending", 25),
                ("munidata-scraper-a", "scraper", "running", 5),
            ])
            # A dataset with no task at all still belongs to a source.
            db.add(_ds("munidata-scraper-b", "scraper"))
            db.add(SourceLimit(source_key="govmap", max_workers=4))
            await db.commit()

            rows = {r["source_key"]: r for r in await source_load(db)}
            assert rows["govmap"] == {
                "source_key": "govmap", "datasets": 2, "active_datasets": 2,
                "running": 1, "pending": 1, "max_workers": 4,
            }
            assert rows["munidata"]["datasets"] == 2
            assert rows["munidata"]["running"] == 1
            assert rows["munidata"]["max_workers"] is None, "uncapped by default"
    _run(go())


def test_missing_table_degrades_to_uncapped_instead_of_stopping_dispatch():
    """saturated_sources runs on EVERY worker poll. If the app deploys ahead of
    its migration, an uncaught error here would 500 the claim path and halt the
    whole fleet — so the caps are what gets lost, never dispatch.

    The transaction must also stay usable afterwards: a poisoned session would
    fail the claim query right after, which is the outage this prevents."""
    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            # Every table EXCEPT source_limits — prod, mid-deploy.
            for table in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                          ScrapeTask.__table__):
                await conn.run_sync(lambda c, t=table: t.create(c))
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-3", "govmap", "pending", 20),
            ])

            assert await saturated_sources(db) == {}, "no caps, but no crash"

            # The claim still works — this is the assertion that matters.
            _, ds = (await db.execute(next_pending_task_q())).first()
            assert ds.ckan_id == "layer-3"
    _run(go())


# ── 4. the claim lock ─────────────────────────────────────────────────────
#
# The cap is only exact because claims serialize on a Postgres advisory lock.
# Every test above runs on SQLite, where that branch is skipped — so the branch
# that actually runs in prod would otherwise be the one nothing covers, and a
# mistake in it (an AttributeError on `db.bind`, say) would stop task dispatch
# fleet-wide rather than fail a test.

class _FakeBind:
    def __init__(self, name):
        self.dialect = type("D", (), {"name": name})()


class _FakeSession:
    def __init__(self, dialect, lock_result=True):
        self.bind = _FakeBind(dialect)
        self._lock_result = lock_result
        self.calls = []

    async def scalar(self, stmt, params=None):
        self.calls.append((str(stmt), params))
        return self._lock_result


def test_claim_lock_is_a_noop_off_postgres():
    """SQLite has no advisory locks — and no concurrency to protect against."""
    from app.api.worker import _acquire_claim_lock

    db = _FakeSession("sqlite")
    assert _run(_acquire_claim_lock(db)) is True
    assert db.calls == [], "must not send Postgres-only SQL to SQLite"


def test_claim_lock_asks_postgres_and_reports_the_answer():
    from app.api.worker import _CLAIM_LOCK_KEY, _acquire_claim_lock

    got = _FakeSession("postgresql", lock_result=True)
    assert _run(_acquire_claim_lock(got)) is True
    sql, params = got.calls[0]
    assert "pg_try_advisory_xact_lock" in sql, "must not use the BLOCKING variant"
    assert params == {"k": _CLAIM_LOCK_KEY}

    # Lock held by another poll → this one claims nothing and answers 204.
    lost = _FakeSession("postgresql", lock_result=False)
    assert _run(_acquire_claim_lock(lost)) is False


def test_running_by_source_counts_workers_not_datasets():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            await _queue(db, [
                ("layer-1", "govmap", "running", 30),
                ("layer-2", "govmap", "running", 25),
                ("layer-3", "govmap", "pending", 20),
                ("jda-scraper-x", "scraper", "failed", 5),
            ])
            assert await running_by_source(db) == {"govmap": 2}
    _run(go())
