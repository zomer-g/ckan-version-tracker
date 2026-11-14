"""Fill twins: one row stored twice because a later version filled a column in.

The deals register got 626,608 of these on 2026-09-19 (settlement_code empty in
one copy, filled in the other, every other column equal). The SQL runs on a
real DuckDB table here, with ``ctid`` read as DuckDB's ``rowid``.
"""
import asyncio
import types
from datetime import datetime, timezone

import pytest

duckdb = pytest.importorskip("duckdb")

from app.services import append_store  # noqa: E402
from app.services.nadlan_query import _deal_row  # noqa: E402

UTC = timezone.utc
EARLY = datetime(2026, 9, 19, 9, 50, tzinfo=UTC)
LATE = datetime(2026, 9, 19, 11, 21, tzinfo=UTC)
COLS = ["settlement", "gush", "chelka", "deal_amount", "scraped_at"]


def _duck(sql: str) -> str:
    return sql.replace("ctid", "rowid")


def _table():
    conn = duckdb.connect(":memory:")
    conn.execute('CREATE TABLE "t" ("settlement_code" VARCHAR, "settlement" VARCHAR, '
                 '"gush" VARCHAR, "chelka" VARCHAR, "deal_amount" VARCHAR, '
                 '"scraped_at" VARCHAR, "first_seen" TIMESTAMPTZ, "row_hash" VARCHAR)')
    rows = [
        # the screenshot: two deals, each loaded without and with the code
        ("", "אליקים", "11834", "171", "2225000", "s1", EARLY, "a"),
        ("682", "אליקים", "11834", "171", "2225000", "s1", LATE, "b"),
        ("", "אליקים", "11834", "171", "3050000", "s1", EARLY, "c"),
        ("682", "אליקים", "11834", "171", "3050000", "s1", LATE, "d"),
        # a settlement that never has a code: no twin, kept
        ("", "כפר בלי קוד", "1", "1", "100", "s1", EARLY, "e"),
        # same deal, different scrape stamp: not provably the same row, kept
        ("", "אליקים", "11834", "172", "999", "s1", EARLY, "f"),
        ("682", "אליקים", "11834", "172", "999", "s2", LATE, "g"),
        # a NULL in a compared column never matches: kept
        ("", "אליקים", None, "173", "5", "s1", EARLY, "h"),
        ("682", "אליקים", None, "173", "5", "s1", LATE, "i"),
    ]
    for r in rows:
        conn.execute('INSERT INTO "t" VALUES (?, ?, ?, ?, ?, ?, ?, ?)', list(r))
    return conn


def test_content_columns_skip_the_filled_column_and_the_bookkeeping():
    cols = ["settlement_code", *COLS, "first_seen", "last_seen", "row_hash"]
    assert append_store.fill_twin_content_cols(cols, "settlement_code") == COLS


def test_only_provable_twins_are_counted_and_deleted():
    conn = _table()
    count = conn.execute(_duck(append_store.fill_twin_count_sql(
        "t", "settlement_code", COLS))).fetchone()[0]
    assert count == 2
    conn.execute(_duck(append_store.fill_twin_first_seen_sql("t", "settlement_code", COLS)))
    while True:
        before = conn.execute('SELECT count(*) FROM "t"').fetchone()[0]
        conn.execute(_duck(append_store.fill_twin_delete_sql(
            "t", "settlement_code", COLS, batch=1)))
        if conn.execute('SELECT count(*) FROM "t"').fetchone()[0] == before:
            break
    left = {r[0]: r[1] for r in conn.execute('SELECT row_hash, first_seen FROM "t"').fetchall()}
    assert set(left) == {"b", "d", "e", "f", "g", "h", "i"}
    # the kept copy takes the earlier archive date of the one deleted
    assert left["b"] == EARLY and left["d"] == EARLY
    assert left["g"] == LATE  # not a twin, untouched
    # idempotent
    assert conn.execute(_duck(append_store.fill_twin_count_sql(
        "t", "settlement_code", COLS))).fetchone()[0] == 0


def test_no_columns_to_compare_is_refused():
    with pytest.raises(ValueError):
        append_store.fill_twin_where("t", "c", [])


class _Conn:
    def __init__(self, twins):
        self.twins, self.sql = twins, []

    async def fetchval(self, sql, *a, **k):
        self.sql.append(sql)
        return self.twins

    async def execute(self, sql, *a, **k):
        self.sql.append(sql)
        if sql.startswith("DELETE"):
            n, self.twins = self.twins, 0
            return f"DELETE {n}"
        return "OK"


def _patch(monkeypatch, conn, cols):
    class _Acq:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *a):
            return False

    async def get_pool():
        return types.SimpleNamespace(acquire=lambda: _Acq())

    async def all_columns(table):
        return cols

    monkeypatch.setattr(append_store, "get_pool", get_pool)
    monkeypatch.setattr(append_store, "_all_columns", all_columns)


def test_dry_run_writes_nothing(monkeypatch):
    conn = _Conn(626_608)
    _patch(monkeypatch, conn, ["settlement_code", *COLS, "first_seen"])
    out = asyncio.run(append_store.collapse_fill_twins("t", "settlement_code", dry_run=True))
    assert out["twins"] == 626_608 and out["deleted"] == 0
    assert all(s.startswith("SELECT") for s in conn.sql)


def test_a_real_run_moves_first_seen_deletes_and_vacuums(monkeypatch):
    conn = _Conn(3)
    _patch(monkeypatch, conn, ["settlement_code", *COLS, "first_seen"])
    out = asyncio.run(append_store.collapse_fill_twins("t", "settlement_code", dry_run=False))
    assert out["deleted"] == 3
    kinds = [s.split()[0] for s in conn.sql]
    assert kinds[:3] == ["SELECT", "UPDATE", "DELETE"]
    assert conn.sql[-1].startswith("VACUUM (ANALYZE)")


def test_a_missing_column_is_an_error_not_a_delete(monkeypatch):
    conn = _Conn(5)
    _patch(monkeypatch, conn, COLS)
    with pytest.raises(ValueError):
        asyncio.run(append_store.collapse_fill_twins("t", "settlement_code", dry_run=False))
    assert conn.sql == []


@pytest.mark.parametrize("raw,want", [("4.5", 4.5), ("4", 4), ("4.0", 4), ("", None),
                                      ("nan", None), ("x", None)])
def test_half_rooms_are_kept(raw, want):
    assert _deal_row({"room_num": raw})["rooms"] == want
