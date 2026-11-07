"""Byte limits for the result of a user-written SQL query.

The public consoles cap a query's rows and its time, which left its SIZE
unbounded: ``SELECT repeat('x', 400000000) FROM generate_series(1, 20)``
finishes well inside the timeout, and the result was buffered whole — by
asyncpg, then as Python dicts, then as JSON — until the shared app process was
killed for memory. One request took the site down for everyone.

Two limits, one on each side of the wire:

* **Per row, in Postgres.** :func:`guard_rows` wraps the statement so a row
  whose datum is larger than :data:`ROW_MAX_BYTES` raises inside the database,
  before a single byte of it is sent. That is the only place a single huge
  value can be refused: once a row is on the wire the driver decodes all of it.
* **Per result, in Python.** :func:`fetch_capped` reads through a cursor in
  small batches and stops once the rows read add up to
  :data:`RESULT_MAX_BYTES`, returning what it has as a truncated result.

Together they bound what one query can hold in this process to roughly
``FETCH_BATCH * ROW_MAX_BYTES + RESULT_MAX_BYTES``.
"""
from __future__ import annotations

import asyncpg

# A row larger than this is refused by the database. Generous on purpose: a
# municipal boundary rendered as WKT runs to a few MB, and that is a real
# query people run on /data.
ROW_MAX_BYTES = 8 * 1024 * 1024

# What an interactive result may add up to before it is cut short. The
# consoles show at most a few thousand rows; this is far above any honest one.
RESULT_MAX_BYTES = 64 * 1024 * 1024

# Rows per cursor round trip for interactive results.
FETCH_BATCH = 16

# Rows per round trip for streamed exports, which never hold more than one
# batch at a time.
EXPORT_FETCH_BATCH = 32

_MARKER = "OVER_ROW_TOO_LARGE"


def guard_rows(sql: str, row_max_bytes: int = ROW_MAX_BYTES) -> str:
    """``sql`` wrapped so that any row larger than ``row_max_bytes`` fails the
    query in Postgres.

    The failure is a cast of a non-constant string to int. It has to be
    non-constant: the planner folds a constant ``'x'::int`` inside a CASE at
    plan time and would fail every query, not just the oversized ones.
    The filter does not reorder rows, so the inner ORDER BY and LIMIT hold.
    """
    n = int(row_max_bytes)
    return (
        f"SELECT * FROM (\n{sql}\n) _g WHERE CASE WHEN pg_column_size(_g.*) > {n} "
        f"THEN ('{_MARKER}:' || pg_column_size(_g.*)::text)::int ELSE 1 END = 1"
    )


def row_too_large(exc: BaseException, row_max_bytes: int = ROW_MAX_BYTES) -> str | None:
    """The message to show when ``exc`` is the row guard firing, else None."""
    if isinstance(exc, asyncpg.InvalidTextRepresentationError) and _MARKER in str(exc):
        mb = row_max_bytes // (1024 * 1024)
        return (
            f"שורה בתוצאה גדולה מ-{mb} MB, ולכן השאילתה נעצרה. "
            "בחרו פחות עמודות או קצרו ערכים ארוכים (left(col, 1000)). "
            f"A result row is larger than {mb} MB; select fewer columns or "
            "shorten long values."
        )
    return None


def approx_size(rec) -> int:
    """A cheap estimate of what a fetched row holds in memory, in bytes."""
    total = 0
    for v in rec.values():
        if v is None or isinstance(v, (bool, int, float)):
            total += 8
        elif isinstance(v, (str, bytes, bytearray, memoryview)):
            total += len(v)
        else:
            total += len(str(v))
    return total


async def fetch_capped(stmt, limit: int, max_bytes: int = RESULT_MAX_BYTES,
                       batch: int = FETCH_BATCH) -> tuple[list, bool]:
    """Up to ``limit`` rows of a prepared statement, stopping early once they
    add up to ``max_bytes``. Returns ``(rows, cut_by_size)``.

    Must be called inside a transaction (asyncpg cursors require one), which
    every console query already runs in.
    """
    rows: list = []
    used = 0
    async for rec in stmt.cursor(prefetch=batch):
        used += approx_size(rec)
        if used > max_bytes:
            return rows, True
        rows.append(rec)
        if len(rows) >= limit:
            break
    return rows, False
