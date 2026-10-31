"""Tests for the version read inside _compute_dataset_sizes (app/api/admin.py).

That function feeds the admin dataset-sizes cache from a scheduled job every
20 minutes. It used to start with `select(VersionIndex)` over the entire
table — every row hydrated into a mapped instance and pinned in the session
identity map, including the ~2,900 committee singles and the inactive
datasets the aggregation loop never looks up. Measured against the
production database that read cost +91MB RSS per tick on a 512MB dyno, and
the process never handed it all back; the cadence ratcheted the floor up
until an ordinary NEON push tipped it into OOM.

It now selects columns for the in-scope datasets only, and reduces
change_summary to the single "type" key it actually reads. These tests pin
the behaviour that refactor had to preserve — the per-version type still
surfaces (it drives suggest_delta_archive), sizes still dedupe across
mappings, and out-of-scope datasets stay out.

Runs on in-memory SQLite, driven by asyncio.run, matching the repo's
dependency-light style (see tests/test_auth_codes.py).
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import tests.conftest  # noqa: F401,E402  — installs the JSONB→JSON sqlite shim

from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api import admin as admin_api  # noqa: F401,E402 — registers every model
from app.database import Base  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


async def _session_factory():
    """Whole schema, not just the two tables in play: TrackedDataset eagerly
    loads its tags relationship, so selecting one needs dataset_tags and tags
    to exist too."""
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _ds(db, *, name, active=True, status="active", storage_mode="snapshot"):
    d = TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=str(uuid.uuid4()),
        ckan_name=name,
        title=name,
        is_active=active,
        status=status,
        storage_mode=storage_mode,
    )
    db.add(d)
    return d


def _ver(db, ds, *, n, mappings=None, vtype=None):
    v = VersionIndex(
        id=uuid.uuid4(),
        tracked_dataset_id=ds.id,
        version_number=n,
        metadata_modified=f"2026-01-{n:02d}",
        resource_mappings=mappings or {},
        change_summary=({"type": vtype} if vtype else {}),
    )
    db.add(v)
    return v


async def _compute(db):
    """Call the real thing with the odata/R2 fan-out neutralised.

    Both size sources are external I/O; these tests are about which rows the
    aggregation reads and what it derives from them, so the dataset carries no
    odata id and storage is treated as unconfigured. Every size resolves to 0
    and the assertions below are about structure, not bytes.
    """
    from app.api import admin

    return await admin._compute_dataset_sizes(db)


def test_out_of_scope_datasets_are_not_aggregated():
    """Inactive, non-active-status and committee-single datasets stay out.

    This is the predicate the version read now joins on. Before, their rows
    were loaded anyway and discarded — the exclusion held for the output but
    not for the memory.
    """
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            keep = _ds(db, name="keep-me")
            gone = _ds(db, name="inactive-one", active=False)
            paused = _ds(db, name="status-paused", status="paused")
            single = _ds(db, name="knesset-committee-single-0042")
            await db.flush()
            for d in (keep, gone, paused, single):
                _ver(db, d, n=1, mappings={"r": "r2:a"})
            await db.commit()

            out = await _compute(db)

        names = {d["title"] for d in out["datasets"]}
        assert names == {"keep-me"}

    _run(go())


def test_version_type_still_comes_through():
    """change_summary->>'type' is projected in SQL now instead of being read
    off a hydrated dict — the latest version's type must still surface."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            d = _ds(db, name="typed")
            await db.flush()
            _ver(db, d, n=1, vtype="full_snapshot")
            _ver(db, d, n=2, vtype="large_dataset")
            await db.commit()

            out = await _compute(db)

        ds = out["datasets"][0]
        assert ds["latest_version_type"] == "large_dataset"
        assert ds["version_count"] == 2
        by_n = {v["version_number"]: v["type"] for v in ds["versions"]}
        assert by_n == {1: "full_snapshot", 2: "large_dataset"}

    _run(go())


def test_missing_or_empty_change_summary_yields_no_type():
    """A version with no change_summary must give type None, not blow up —
    the old code guarded with isinstance(dict); the SQL projection has to be
    just as tolerant."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            d = _ds(db, name="untyped")
            await db.flush()
            v = _ver(db, d, n=1)
            v.change_summary = None
            _ver(db, d, n=2, vtype=None)
            await db.commit()

            out = await _compute(db)

        types = [v["type"] for v in out["datasets"][0]["versions"]]
        assert types == [None, None]
        assert out["datasets"][0]["latest_version_type"] is None

    _run(go())


def test_suggest_delta_archive_still_keys_off_the_latest_type():
    """The one consumer of the projected type: a stub-only dataset that has
    not opted into append_only should be flagged."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            flagged = _ds(db, name="stub-only")
            opted = _ds(db, name="already-append", storage_mode="append_only")
            opted.scraper_config = {"append_key": "id"}
            await db.flush()
            _ver(db, flagged, n=1, vtype="large_dataset")
            _ver(db, opted, n=1, vtype="large_dataset")
            await db.commit()

            out = await _compute(db)

        flags = {d["title"]: d["suggest_delta_archive"] for d in out["datasets"]}
        assert flags == {"stub-only": True, "already-append": False}

    _run(go())


def test_resource_mappings_are_still_deduped_per_version():
    """Per-version totals sum over the mapping VALUES, counting each distinct
    resource once even when several keys point at it, and flattening lists.

    Sizes are all 0 here (no odata id, storage unconfigured), so this asserts
    the traversal survives the switch from ORM attribute to Row column rather
    than the arithmetic.
    """
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            d = _ds(db, name="mapped")
            await db.flush()
            _ver(db, d, n=1, mappings={"a": "r2:x", "b": "r2:x", "c": ["r2:y", "r2:z"]})
            await db.commit()

            out = await _compute(db)

        assert out["datasets"][0]["versions"][0]["total_bytes"] == 0
        assert out["datasets"][0]["version_count"] == 1

    _run(go())


def test_the_version_read_is_scoped_and_does_not_select_the_whole_table():
    """The actual regression guard.

    Every other test here passes on the old code too — the old read produced
    the same aggregate, it just paid ~91MB to do it. What was wrong was the
    statement itself: `select(VersionIndex)` with no join and no predicate,
    pulling whole entities including change_summary. So assert on the SQL.

    Fails on the old implementation, which emitted an unqualified
    SELECT ... FROM version_index with no JOIN.
    """
    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        seen: list[str] = []
        from sqlalchemy import event

        @event.listens_for(engine.sync_engine, "before_cursor_execute")
        def _capture(conn, cursor, statement, params, context, executemany):
            if "version_index" in statement and statement.lstrip().upper().startswith("SELECT"):
                seen.append(" ".join(statement.split()))

        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as db:
            d = _ds(db, name="scoped")
            await db.flush()
            _ver(db, d, n=1, vtype="large_dataset")
            await db.commit()
            seen.clear()
            await _compute(db)
        return seen

    stmts = _run(go())
    assert stmts, "no SELECT against version_index was captured"
    read = stmts[0]

    # Scoped to the datasets being aggregated, not the whole table.
    # (The committee-single pattern travels as a bound parameter, so the
    # predicate is what's visible in the statement, not the literal.)
    assert "JOIN tracked_datasets" in read, read
    assert "tracked_datasets.is_active" in read, read
    assert "ckan_name NOT LIKE" in read, read

    # Columns, not the whole entity: change_summary is reduced to its one
    # read key rather than hauled back as JSON and parsed per row.
    assert "->>" in read, read
    assert "version_index.change_summary AS" not in read, read


def test_a_dataset_with_no_versions_still_reports():
    """The grouping dict is built from the filtered read; a dataset with no
    rows in it must still appear with an empty version list."""
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            _ds(db, name="brand-new")
            await db.commit()
            out = await _compute(db)

        assert out["datasets"][0]["versions"] == []
        assert out["datasets"][0]["version_count"] == 0
        assert out["datasets"][0]["total_bytes"] == 0

    _run(go())


def test_r2_sizes_are_remembered_between_runs(monkeypatch):
    """An R2 key is minted fresh per upload (storage_client.build_key), so its
    size cannot change; re-HEADing every object of every version on every run
    is what pushed the job past its interval. A second run must HEAD only what
    it has not sized yet, a failed HEAD must be retried rather than cached as
    0, and keys no longer referenced must fall out of the cache."""
    from app.api import admin
    from app.services import storage_client as sc

    heads: list[str] = []
    missing = {"r2:flaky"}

    async def fake_size(val):
        heads.append(val)
        return None if val in missing else 100

    monkeypatch.setattr(sc.storage_client, "is_configured", lambda: True)
    monkeypatch.setattr(sc.storage_client, "object_size", fake_size)
    monkeypatch.setattr(admin, "_r2_object_size_cache", {})

    async def go():
        Session = await _session_factory()
        async with Session() as db:
            d = _ds(db, name="cached")
            await db.flush()
            v1 = _ver(db, d, n=1, mappings={"a": "r2:one", "b": "r2:flaky"})
            await db.commit()
            first = await _compute(db)

            _ver(db, d, n=2, mappings={"a": "r2:two"})
            await db.commit()
            second = await _compute(db)

            await db.delete(v1)
            await db.commit()
            third = await _compute(db)
        return first, second, third

    first, second, third = _run(go())
    # Run 1 sized both; run 2 sized only the new key and retried the failure.
    assert sorted(heads[:2]) == ["r2:flaky", "r2:one"]
    assert sorted(heads[2:4]) == ["r2:flaky", "r2:two"]
    # Run 3 needed no HEAD for the cached key.
    assert heads[4:] == []
    # Totals unchanged by the cache: a cached size counts like a fresh one.
    assert first["datasets"][0]["total_bytes"] == 100
    assert second["datasets"][0]["total_bytes"] == 200
    assert third["datasets"][0]["total_bytes"] == 100
    # v1 gone: its keys are pruned, the surviving one is kept.
    assert set(admin._r2_object_size_cache) == {"r2:two"}
