"""Unit tests for the /data console's short-link store (app/services/sql_shares.py).

Runs against an in-memory SQLite DB, driven by asyncio.run so no pytest-asyncio
is required — matching tests/test_auth_codes.py and the repo's dependency-light
test style.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.sql_share import SqlShare  # noqa: E402
from app.services import sql_shares  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


async def _session_factory():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: SqlShare.__table__.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


# ── params filtering ─────────────────────────────────────────────────────

def test_filter_params_keeps_only_console_keys():
    got = sql_shares.filter_params("chart=scatter&evil=http%3A%2F%2Fx&cx=a&table=t")
    assert "chart=scatter" in got
    assert "cx=a" in got
    assert "table=t" in got
    assert "evil" not in got


def test_filter_params_is_order_stable_so_dedup_works():
    """The same view reached by two click paths must hash the same."""
    a = sql_shares.filter_params("cx=a&chart=scatter&ctop=20")
    b = sql_shares.filter_params("ctop=20&cx=a&chart=scatter")
    assert a == b


def test_filter_params_tolerates_empty_and_leading_question_mark():
    assert sql_shares.filter_params(None) == ""
    assert sql_shares.filter_params("") == ""
    assert sql_shares.filter_params("?chart=bar") == "chart=bar"


# ── slugs ────────────────────────────────────────────────────────────────

def test_slug_avoids_lookalike_characters():
    """Slugs get retyped off slides; 0/O and 1/l/I must not appear."""
    slugs = "".join(sql_shares._new_slug() for _ in range(200))
    assert not (set(slugs) & set("0O1lI"))
    assert len(sql_shares._new_slug()) == 8


# ── create / resolve round trip ──────────────────────────────────────────

def test_create_then_resolve_returns_the_query():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            slug = await sql_shares.create(db, "SELECT 1", "chart=bar")
            got = await sql_shares.resolve(db, slug)
            assert got == {"sql": "SELECT 1", "params": "chart=bar"}
    _run(go())


def test_same_view_shared_twice_reuses_one_slug():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            a = await sql_shares.create(db, "SELECT 1", "chart=bar")
            b = await sql_shares.create(db, "SELECT 1", "chart=bar")
            assert a == b
            rows = (await db.execute(SqlShare.__table__.select())).all()
            assert len(rows) == 1
    _run(go())


def test_different_view_of_same_query_gets_its_own_slug():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            a = await sql_shares.create(db, "SELECT 1", "chart=bar")
            b = await sql_shares.create(db, "SELECT 1", "chart=scatter")
            assert a != b
    _run(go())


def test_resolve_unknown_slug_is_none_not_an_error():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            assert await sql_shares.resolve(db, "nosuchid") is None
    _run(go())


def test_resolve_counts_views():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            slug = await sql_shares.create(db, "SELECT 1", None)
            await sql_shares.resolve(db, slug)
            await sql_shares.resolve(db, slug)
            row = await db.get(SqlShare, slug)
            assert row.view_count == 2
            assert row.last_viewed_at is not None
    _run(go())


# ── the case the URL form could never handle ─────────────────────────────

def test_a_query_far_too_long_for_a_url_still_shares():
    """The point of the feature: past ~4,000 encoded characters the `?q=` form
    produced no link at all. Length must not decide shareability any more."""
    async def go():
        Session = await _session_factory()
        big = "SELECT " + ", ".join(f"'{i}' AS c{i}" for i in range(4000))
        assert len(big) > 40_000
        async with Session() as db:
            slug = await sql_shares.create(db, big, None)
            assert len(slug) == 8
            got = await sql_shares.resolve(db, slug)
            assert got["sql"] == big
    _run(go())


def test_absurdly_large_payload_is_refused():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            try:
                await sql_shares.create(db, "x" * (sql_shares.MAX_SQL_CHARS + 1), None)
            except ValueError as e:
                assert "too long" in str(e)
            else:
                raise AssertionError("expected ValueError")
    _run(go())


def test_empty_query_is_refused():
    async def go():
        Session = await _session_factory()
        async with Session() as db:
            try:
                await sql_shares.create(db, "   ", None)
            except ValueError:
                pass
            else:
                raise AssertionError("expected ValueError")
    _run(go())


def test_query_is_stored_verbatim_including_hebrew_identifiers():
    """Hebrew column names with gershayim are the norm in this corpus."""
    async def go():
        Session = await _session_factory()
        sql = 'SELECT "סה״כ הכנסות" FROM t WHERE "שם הרשות" = \'תל אביב -יפו\''
        async with Session() as db:
            slug = await sql_shares.create(db, sql, None)
            got = await sql_shares.resolve(db, slug)
            assert got["sql"] == sql
    _run(go())
