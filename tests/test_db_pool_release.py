"""A request must not hold a pooled connection through work that isn't SQLAlchemy's.

2026-09-27, production: ~12,000 ``QueuePool limit of size 5 overflow 10 reached``
across three containers, and during each burst most of the API answered 500.
The pool was not too small. /api/append/<id>/rows read the dataset row through
the request's session, then waited 45-70s for a free-text ILIKE over the 3.8M-row
deals table on the archive's own asyncpg pool — and the session sat idle in
transaction, holding its connection, the whole time. The archive pool has five
slots, so queued searches waited there too, each still holding one of ours.
Fifteen of them and nothing else in the app could get a connection.

These tests pin the mechanism with a one-connection pool: without a release the
second session times out, exactly as production did; with it, it doesn't.
"""
import asyncio
import contextlib
import os
import uuid

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.exc import TimeoutError as PoolTimeout  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.pool import AsyncAdaptedQueuePool  # noqa: E402

from app.database import Base, release_connection  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402

_TABLES = [Organization.__table__, TrackedDataset.__table__, Tag.__table__,
           dataset_tags, VersionIndex.__table__]
_DS_ID = uuid.UUID("fd06f5ae-8a4f-4120-b275-8a514ad23499")


@contextlib.asynccontextmanager
async def _one_conn(path):
    """An engine whose pool holds ONE connection and gives up after 1s — the
    production pool in miniature."""
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{path / 'pool.db'}",
        poolclass=AsyncAdaptedQueuePool, pool_size=1, max_overflow=0, pool_timeout=1,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        db.add(TrackedDataset(
            id=_DS_ID, ckan_id="nadlan", ckan_name="taxes_nadlan_full",
            title="עסקאות נדל\"ן", poll_interval=3600, is_active=True,
            status="active", storage_mode="append_only",
        ))
        await db.commit()
    try:
        yield engine, Session
    finally:
        await engine.dispose()


async def _read_ds(db):
    return (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == _DS_ID)
    )).scalar_one()


def test_a_session_left_open_starves_the_next_request(tmp_path):
    """The failure, reproduced: a read, then a wait, and nobody else gets in."""
    async def run():
        async with _one_conn(tmp_path) as (engine, Session):
            async with Session() as slow:
                await _read_ds(slow)          # ... and now the 45-second search runs
                assert engine.pool.checkedout() == 1
                async with Session() as other:
                    with pytest.raises(PoolTimeout):
                        await _read_ds(other)
    asyncio.run(run())


def test_release_hands_the_connection_back(tmp_path):
    async def run():
        async with _one_conn(tmp_path) as (engine, Session):
            async with Session() as slow:
                ds = await _read_ds(slow)
                await release_connection(slow)
                assert engine.pool.checkedout() == 0
                async with Session() as other:
                    assert (await _read_ds(other)).id == _DS_ID
                # The row read before the release is still usable without the DB —
                # routes go on to read ds.scraper_config etc. after releasing.
                assert ds.ckan_name == "taxes_nadlan_full"
                # And the session itself still works if the route needs it again.
                assert (await _read_ds(slow)).id == _DS_ID
    asyncio.run(run())


def test_release_refuses_to_commit_pending_writes(tmp_path):
    """A read path helper must never publish a half-made change by accident."""
    async def run():
        async with _one_conn(tmp_path) as (engine, Session):
            async with Session() as db:
                ds = await _read_ds(db)
                ds.title = "changed"
                with pytest.raises(RuntimeError):
                    await release_connection(db)
    asyncio.run(run())


def test_release_on_an_unused_session_is_a_no_op(tmp_path):
    async def run():
        async with _one_conn(tmp_path) as (engine, Session):
            async with Session() as db:
                await release_connection(db)
                assert engine.pool.checkedout() == 0
    asyncio.run(run())


def test_append_resolve_releases_before_the_archive_query(tmp_path, monkeypatch):
    """The route that did it. Every /api/append endpoint goes through _resolve
    and then talks only to the archive pool, so the release lives there."""
    async def run():
        async with _one_conn(tmp_path) as (engine, Session):
            from app.api import append as api
            from app.services import append_store

            monkeypatch.setattr(append_store, "is_configured", lambda: True)
            async with Session() as db:
                ds, table, tables = await api._resolve(str(_DS_ID), db)
                assert engine.pool.checkedout() == 0, "_resolve kept its connection"
                assert table == append_store.table_name(ds)
                # A concurrent request can get in while this one waits on the archive.
                async with Session() as other:
                    assert (await _read_ds(other)).id == _DS_ID
    asyncio.run(run())


def test_pending_count_is_served_from_memory_between_reads(monkeypatch):
    """The busiest route on the site: one DB read per TTL, not per open tab."""
    from app.api import datasets as api

    monkeypatch.setattr(api, "_pending_count_cache", [])
    calls = []

    class _Result:
        def scalar(self):
            return 3

    class _Db:
        async def execute(self, _stmt):
            calls.append(1)
            return _Result()

    route = api.pending_count.__wrapped__   # past slowapi's decorator

    def ask():
        return asyncio.run(route(request=None, db=_Db()))

    assert ask() == {"count": 3}
    assert ask() == {"count": 3}
    assert len(calls) == 1

    # Once the TTL has passed, the next request reads again.
    api._pending_count_cache[0] -= api._PENDING_COUNT_TTL + 1
    ask()
    assert len(calls) == 2
