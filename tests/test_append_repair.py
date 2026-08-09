"""append_repair: purging mojibake rows and indexing join keys."""
import asyncio
import contextlib

import pytest

from app.services import append_repair as AR
from app.services import append_store


class FakeConn:
    """Enough asyncpg surface for scan/purge/ensure_index."""

    def __init__(self, *, columns, total, corrupt, indexes=()):
        self.columns = columns
        self.total = total
        self.corrupt = corrupt
        self.indexes = set(indexes)
        self.executed: list[tuple[str, tuple]] = []
        self.deleted = False

    async def fetch(self, sql, *args):
        return [{"column_name": c} for c in self.columns]

    async def fetchval(self, sql, *args):
        if "pg_indexes" in sql:
            return 1 if args[1] in self.indexes else None
        if "count(*)" in sql and "WHERE" in sql:
            return self.corrupt
        if "count(*)" in sql:
            return self.total - (self.corrupt if self.deleted else 0)
        return None

    async def execute(self, sql, *args):
        self.executed.append((sql, args))
        if sql.startswith("DELETE"):
            self.deleted = True
            return f"DELETE {self.corrupt}"
        return "CREATE INDEX"


def _install(monkeypatch, conn):
    monkeypatch.setattr(append_store, "is_configured", lambda: True)

    class FakePool:
        @contextlib.asynccontextmanager
        async def acquire(self):
            yield conn

    async def fake_pool():
        return FakePool()

    monkeypatch.setattr(append_store, "get_pool", fake_pool)


# ── the scan ─────────────────────────────────────────────────────────────────

def test_scan_counts_corrupt_rows_and_flags_a_table_that_would_empty(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=18689, corrupt=18689)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.scan("append_blocks"))
    assert (s["total"], s["corrupt"], s["clean"]) == (18689, 18689, 0)
    assert s["would_empty"] is True


def test_scan_does_not_flag_a_partially_corrupt_table(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=1_247_775, corrupt=150_000)
    _install(monkeypatch, conn)
    assert asyncio.run(AR.scan("append_parcels"))["would_empty"] is False


def test_bookkeeping_columns_are_never_scanned(monkeypatch):
    # geometry_wkt is machine-written; a stray glyph there must not condemn a row.
    conn = FakeConn(columns=["LOCALITY_N", "geometry_wkt", "row_hash", "first_seen"],
                    total=10, corrupt=0)
    _install(monkeypatch, conn)
    assert asyncio.run(AR.scan("t"))["columns_scanned"] == 1


# ── the purge, and the ordering it enforces ──────────────────────────────────

def test_purge_refuses_to_empty_a_fully_corrupt_table(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=18689, corrupt=18689)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.purge("append_blocks", apply=True))
    assert "refused" in s and not conn.deleted
    assert "Re-poll" in s["refused"] or "re-poll" in s["refused"]


def test_allow_empty_overrides_the_refusal(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=18689, corrupt=18689)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.purge("append_blocks", apply=True, allow_empty=True))
    assert s["applied"] is True and conn.deleted


def test_dry_run_is_the_default_and_deletes_nothing(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=1000, corrupt=150)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.purge("t"))
    assert s["applied"] is False and not conn.deleted
    assert s["corrupt"] == 150


def test_purge_deletes_and_reports_what_remains(monkeypatch):
    conn = FakeConn(columns=["LOCALITY_N"], total=1_247_775, corrupt=150_000)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.purge("append_parcels", apply=True))
    assert s["deleted"] == 150_000
    assert s["remaining"] == 1_097_775
    sql, args = conn.executed[0]
    assert sql.startswith("DELETE")
    # The replacement character travels as a parameter, never inlined into SQL.
    assert args == (AR.REPLACEMENT_CHAR,)


def test_the_predicate_covers_every_text_column(monkeypatch):
    conn = FakeConn(columns=["A", "B", "C"], total=5, corrupt=1)
    _install(monkeypatch, conn)
    asyncio.run(AR.purge("t", apply=True))
    sql, _ = conn.executed[0]
    assert sql.count("position($1 in") == 3


# ── the index ────────────────────────────────────────────────────────────────

def test_index_is_created_on_the_join_key(monkeypatch):
    conn = FakeConn(columns=["GUSH_NUM", "PARCEL"], total=1, corrupt=0)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.ensure_index("append_parcels", ["GUSH_NUM"], apply=True))
    assert s["created"] is True
    sql, _ = conn.executed[0]
    assert "CREATE INDEX" in sql and '"GUSH_NUM"' in sql


def test_index_dry_run_creates_nothing(monkeypatch):
    conn = FakeConn(columns=["GUSH_NUM"], total=1, corrupt=0)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.ensure_index("t", ["GUSH_NUM"]))
    assert s["would_create"] is True and not conn.executed


def test_an_existing_index_is_left_alone(monkeypatch):
    name = append_store._index_name("t", "gush_num_idx")
    conn = FakeConn(columns=["GUSH_NUM"], total=1, corrupt=0, indexes=[name])
    _install(monkeypatch, conn)
    s = asyncio.run(AR.ensure_index("t", ["GUSH_NUM"], apply=True))
    assert s["created"] is False and not conn.executed


def test_a_missing_column_is_an_error_not_a_broken_index(monkeypatch):
    conn = FakeConn(columns=["GUSH_NUM"], total=1, corrupt=0)
    _install(monkeypatch, conn)
    s = asyncio.run(AR.ensure_index("t", ["NO_SUCH"], apply=True))
    assert "no such column" in s["error"] and not conn.executed


def test_unconfigured_append_db_is_reported_not_raised(monkeypatch):
    monkeypatch.setattr(append_store, "is_configured", lambda: False)
    assert "error" in asyncio.run(AR.scan("t"))
    assert "error" in asyncio.run(AR.ensure_index("t", ["a"]))
