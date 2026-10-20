"""Compaction of emptied tables (phase 4 follow-up, 2026-10-20).

A plain VACUUM gives back a heap's trailing pages but never an index page, so
a table whose every row went to R2 kept its indexes (60 rain forecast tables,
1.96 GB). TRUNCATE releases heap, TOAST and indexes; it is safe only for an
empty table, which is the one thing these tests pin down.
"""
import asyncio
import types

import pytest

from app.services import append_store
from app.services import history_export as hx


def _pool_of(conn):
    class _Acq:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *a):
            return False

    async def get_pool():
        return types.SimpleNamespace(acquire=lambda: _Acq())
    return get_pool


class _Conn:
    """Answers count(*) and the size query from a table map and records
    every other statement."""

    def __init__(self, rows: dict, sizes: dict):
        self.rows, self.sizes, self.executed = rows, sizes, []

    async def fetchval(self, sql, *args):
        if "count(*)" in sql:
            table = sql.split('"public".')[1].strip('"')
            return self.rows[table]
        if "pg_total_relation_size" in sql:
            table = args[0].split('"public".')[1].strip('"')
            return self.sizes[table]
        raise AssertionError(sql)

    async def execute(self, sql, *args, **kw):
        self.executed.append(sql)
        if sql.startswith("TRUNCATE"):
            table = sql.split('"public".')[1].strip('"')
            self.sizes[table] = 8192
        return "OK"


def test_compaction_choice():
    assert hx.compaction_for(0) == "truncate"
    assert hx.compaction_for(1) == "vacuum"
    assert hx.compaction_for(10_000) == "vacuum"


def test_truncate_sql_quotes_the_table():
    assert hx.truncate_sql("append_x_1") == 'TRUNCATE TABLE "public"."append_x_1"'
    assert hx.relation_ref("append_x_1") == '"public"."append_x_1"'


def test_an_empty_table_is_truncated_and_the_bytes_reported(monkeypatch):
    conn = _Conn({"append_a": 0}, {"append_a": 50_000_000})
    monkeypatch.setattr(append_store, "get_pool", _pool_of(conn))
    out = asyncio.run(hx.compact_table("append_a"))
    assert out == {"table": "append_a", "rows": 0, "action": "truncate",
                   "before": 50_000_000, "after": 8192}
    assert conn.executed == ['TRUNCATE TABLE "public"."append_a"']


def test_a_table_with_rows_is_vacuumed_never_truncated(monkeypatch):
    conn = _Conn({"append_b": 3}, {"append_b": 1000})
    monkeypatch.setattr(append_store, "get_pool", _pool_of(conn))
    out = asyncio.run(hx.compact_table("append_b"))
    assert out["action"] == "vacuum" and out["rows"] == 3
    assert conn.executed == ['VACUUM (ANALYZE) "public"."append_b"']


def test_compact_cold_tables_skips_rows_left_and_survives_a_failure(monkeypatch):
    conn = _Conn({"append_empty": 0, "append_full": 12, "append_gone": 0},
                 {"append_empty": 3_000_000, "append_full": 500, "append_gone": 1})

    real_fetchval = conn.fetchval

    async def fetchval(sql, *args):
        if "append_gone" in sql or any("append_gone" in str(a) for a in args):
            raise RuntimeError("relation does not exist")
        return await real_fetchval(sql, *args)
    conn.fetchval = fetchval
    monkeypatch.setattr(append_store, "get_pool", _pool_of(conn))
    out = asyncio.run(hx.compact_cold_tables(["append_empty", "append_full", "append_gone"]))
    assert out["tables"] == 3
    assert [d["table"] for d in out["compacted"]] == ["append_empty"]
    assert out["skipped"] == [{"table": "append_full", "rows": 12}]
    assert out["failed"][0]["table"] == "append_gone"
    assert out["freed_bytes"] == 3_000_000 - 8192
    assert conn.executed == ['TRUNCATE TABLE "public"."append_empty"']


def test_the_admin_endpoint_refuses_during_a_tier_move(monkeypatch):
    from fastapi import HTTPException

    from app.api import admin_storage as api
    monkeypatch.setattr(hx, "tier_status", lambda: {"running": True})
    with pytest.raises(HTTPException) as e:
        asyncio.run(api.history_compact(_=None))
    assert e.value.status_code == 409


def test_the_admin_endpoint_returns_the_report(monkeypatch):
    from app.api import admin_storage as api
    monkeypatch.setattr(hx, "tier_status", lambda: {"running": False})

    async def fake():
        return {"tables": 0, "compacted": [], "skipped": [], "failed": [], "freed_bytes": 0}
    monkeypatch.setattr(hx, "compact_cold_tables", fake)
    assert asyncio.run(api.history_compact(_=None))["tables"] == 0


def test_access_log_compact_is_single_flight(monkeypatch):
    from app.api import admin_storage as api
    monkeypatch.setitem(api._access_log_job, "running", True)
    out = asyncio.run(api.access_log_compact(_=None))
    assert out == {"started": False, "reason": "already running"}
    monkeypatch.setitem(api._access_log_job, "running", False)
    status = asyncio.run(api.access_log_compact_status(_=None))
    assert set(status) >= {"running", "purged", "before", "after", "error"}
