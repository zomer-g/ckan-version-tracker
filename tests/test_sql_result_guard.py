"""A console query can no longer buffer an unbounded result.

Rows and time were capped; bytes were not, so
``SELECT repeat('x', 400000000) FROM generate_series(1, 20)`` ran inside the
timeout and the process was killed holding its result. The row guard runs in
Postgres; these tests need one, named by OVER_TEST_PG_DSN, and skip without it.
"""
import os

import asyncpg
import pytest

from app.services import sql_result_guard as g

DSN = os.environ.get("OVER_TEST_PG_DSN")
pg = pytest.mark.skipif(not DSN, reason="OVER_TEST_PG_DSN not set")


async def _run(sql: str, limit: int = 1000, **kw):
    conn = await asyncpg.connect(DSN)
    try:
        async with conn.transaction(readonly=True):
            stmt = await conn.prepare(g.guard_rows(sql, kw.pop("row_max", g.ROW_MAX_BYTES)))
            return await g.fetch_capped(stmt, limit, **kw)
    finally:
        await conn.close()


@pg
@pytest.mark.asyncio
async def test_ordinary_query_unchanged():
    rows, cut = await _run(
        "SELECT * FROM (SELECT i, i::text AS t FROM generate_series(1, 50) i ORDER BY i DESC) _q LIMIT 11")
    assert not cut
    assert [r["i"] for r in rows] == list(range(50, 39, -1))


@pg
@pytest.mark.asyncio
async def test_duplicate_column_names_still_work():
    rows, _ = await _run("SELECT 1 AS a, 2 AS a")
    assert list(rows[0].values()) == [1, 2]


@pg
@pytest.mark.asyncio
async def test_oversized_row_refused_in_postgres():
    with pytest.raises(asyncpg.PostgresError) as ei:
        await _run("SELECT repeat('x', 3000000) AS big", row_max=1_000_000)
    assert g.row_too_large(ei.value, 1_000_000)


@pg
@pytest.mark.asyncio
async def test_small_rows_after_constant_fold_are_fine():
    # The guard must not fail at plan time on a query whose rows are all small.
    rows, cut = await _run("SELECT 'a' AS x", row_max=1_000_000)
    assert [r["x"] for r in rows] == ["a"] and not cut


@pg
@pytest.mark.asyncio
async def test_result_cut_short_by_total_size():
    rows, cut = await _run(
        "SELECT repeat('x', 100000) AS v FROM generate_series(1, 100)",
        max_bytes=1_000_000)
    assert cut
    assert 0 < len(rows) < 100


def test_row_too_large_ignores_other_errors():
    assert g.row_too_large(ValueError("OVER_ROW_TOO_LARGE")) is None
