"""The worker progress limit is per (IP, task) and the deals indexes self-heal."""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import asyncio
from unittest import mock

from starlette.requests import Request

from app.api.worker import _progress_limit_key
from app.services import nadlan_index


def _req(task_id: str) -> Request:
    return Request({
        "type": "http", "method": "POST", "path": f"/api/worker/progress/{task_id}",
        "headers": [], "client": ("84.95.179.119", 1234),
        "path_params": {"task_id": task_id},
    })


def test_two_tasks_behind_one_ip_get_separate_buckets():
    a, b = _progress_limit_key(_req("t-1")), _progress_limit_key(_req("t-2"))
    assert a != b
    assert a == _progress_limit_key(_req("t-1"))


class _Conn:
    def __init__(self, have):
        self.have = have
        self.executed: list[str] = []

    async def fetch(self, sql, *args):
        return [{"indexname": n} for n in self.have]

    async def execute(self, sql, timeout=None):
        self.executed.append(sql)


def _run(conn):
    with mock.patch.object(nadlan_index, "find_deals_table",
                           mock.AsyncMock(return_value=("public", "append_deals"))):
        return asyncio.run(nadlan_index._ensure_deals_indexes(conn))


def test_only_the_missing_deals_indexes_are_built():
    conn = _Conn({"append_deals_gush_chelka_idx"})
    res = _run(conn)
    assert res["made"] == ["append_deals_settlement_idx", "append_deals_date_idx"]
    assert sum(s.startswith("CREATE INDEX") for s in conn.executed) == 2
    assert any(s.startswith("ANALYZE") for s in conn.executed)


def test_nothing_is_built_or_analyzed_when_all_indexes_exist():
    names = {n for n, _ in nadlan_index._deals_index_specs("append_deals")}
    conn = _Conn(names)
    res = _run(conn)
    assert res == {"made": [], "failed": []}
    assert conn.executed == []


def test_broad_deals_series_is_cached_with_the_aggregate_ceiling():
    from app.services import deals_query as dq
    calls = []

    async def fake_fetch(sql, *args, timeout_ms=dq._TIMEOUT_MS):
        calls.append(timeout_ms)
        return [{"year": "2024", "deals": 1, "median_amount": 1, "median_area": 1,
                 "median_ppsqm_normalized": 1}]

    dq.invalidate_cache()
    with mock.patch.object(dq, "_fetch", fake_fetch), \
         mock.patch.object(dq, "_src", mock.AsyncMock(return_value=("public", "d"))):
        asyncio.run(dq.series({}))
        asyncio.run(dq.series({}))
        asyncio.run(dq.series({"settlement": "חולון"}))
    dq.invalidate_cache()
    assert calls == [dq._AGGREGATE_TIMEOUT_MS, dq._TIMEOUT_MS]
