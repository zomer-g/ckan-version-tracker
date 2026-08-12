"""נדל"ן לעם — the GovMap geocoding queue.

Two rules here can lose work silently, and both are pinned:

  * ``failed`` must NOT be recorded. It means "we could not ask", not "there is
    nothing there". Recording it would make an available address look like a
    settled miss and drop it from `point IS NULL` forever.
  * ``not_found`` MUST be recorded, or the same address is handed to every
    future batch and the queue never drains.

Plus the priority promise the whole design exists for: a GovMap layer entering
the queue is claimed before the next geocoding batch.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import asyncio  # noqa: E402
import pytest  # noqa: E402

from app.models.scrape_task import (  # noqa: E402
    PRIORITY_BACKFILL, PRIORITY_COVERAGE, PRIORITY_GEOCODE, PRIORITY_MANUAL,
    PRIORITY_PROMOTED, PRIORITY_ROUTINE,
)
from app.services import geocode_queue as gq  # noqa: E402


# ── the priority promise ──────────────────────────────────────────────────────
def test_geocoding_yields_to_every_govmap_band():
    """The user's rule: a GovMap layer entering the queue takes precedence over
    the next geocoding batch. Claim order is `priority DESC, created_at ASC`, so
    that is exactly `PRIORITY_GEOCODE` below BOTH GovMap bands."""
    assert PRIORITY_GEOCODE < PRIORITY_COVERAGE   # GovMap quarterly refresh
    assert PRIORITY_GEOCODE < PRIORITY_BACKFILL   # GovMap whole-catalog re-scrape
    assert PRIORITY_GEOCODE < PRIORITY_ROUTINE
    assert PRIORITY_GEOCODE < PRIORITY_MANUAL < PRIORITY_PROMOTED


def test_geocoding_is_the_lowest_band_there_is():
    bands = [PRIORITY_PROMOTED, PRIORITY_MANUAL, PRIORITY_ROUTINE,
             PRIORITY_COVERAGE, PRIORITY_BACKFILL, PRIORITY_GEOCODE]
    assert min(bands) == PRIORITY_GEOCODE
    assert sorted(bands, reverse=True)[-1] == PRIORITY_GEOCODE


# ── the query the worker is given ─────────────────────────────────────────────
def test_query_is_street_number_locality():
    assert gq.build_query("פתח תקווה", "אבימלך", 8, None) == "אבימלך 8 פתח תקווה"
    # A house suffix is part of the doorway and must survive into the query.
    assert gq.build_query("פתח תקווה", "אבימלך", 10, "א") == "אבימלך 10א פתח תקווה"
    # Missing pieces collapse rather than leaving double spaces GovMap would
    # have to cope with.
    assert gq.build_query("אבו גוש", "בית הבד", None, None) == "בית הבד אבו גוש"
    assert gq.build_query(None, None, None, None) == ""


# ── the three outcomes ────────────────────────────────────────────────────────
class _FakeConn:
    def __init__(self, sink): self.sink = sink
    async def execute(self, sql, *a, **k): self.sink.append(("execute", sql)); return "UPDATE 0"
    async def executemany(self, sql, rows): self.sink.append(("many", sql, list(rows)))
    async def fetchval(self, sql, *a, **k): return 0
    async def fetchrow(self, sql, *a, **k): return {}
    def transaction(self):
        class _T:
            async def __aenter__(s): return s
            async def __aexit__(s, *e): return False
        return _T()


class _FakePool:
    def __init__(self, sink): self.sink = sink
    def acquire(self):
        sink = self.sink
        class _A:
            async def __aenter__(s): return _FakeConn(sink)
            async def __aexit__(s, *e): return False
        return _A()


@pytest.fixture
def sink(monkeypatch):
    calls = []
    async def _pool(): return _FakePool(calls)
    monkeypatch.setattr(gq.append_store, "get_pool", _pool)
    monkeypatch.setattr(gq.append_store, "get_readonly_pool", _pool)
    async def _noop(): return None
    monkeypatch.setattr(gq, "ensure_tables", _noop)
    return calls


def _written_keys(sink, marker):
    """Every address_key written by an INSERT mentioning `marker`."""
    keys = set()
    for call in sink:
        if call[0] == "many" and marker in call[1]:
            keys |= {row[0] for row in call[2]}
    return keys


def test_failed_is_never_recorded_so_it_comes_back(sink):
    """The single most dangerous confusion in this pipeline.

    'failed' = we never got an answer. If it were written as a miss, an address
    that was perfectly available would disappear from the work list for good."""
    out = asyncio.run(gq.record_results({
        "task_id": "t1",
        "results": [{"address_key": "a1", "lat": 32.0, "lon": 34.9,
                     "govmap_id": "address|ADDR|1", "matched_text": "x", "score": 4000}],
        "not_found": ["a2"],
        "failed": [{"address_key": "a3", "reason": "HTTP 500"}],
        "attempted": 3, "batch_size": 10000, "aborted": False,
    }))
    written = _written_keys(sink, "over_re_geocode")
    assert "a1" in written and "a2" in written
    assert "a3" not in written, "a failed address must stay in the work list"
    assert out["recorded_hits"] == 1
    assert out["recorded_not_found"] == 1
    assert out["requeued_failed"] == 1


def test_not_found_is_recorded_or_the_queue_never_drains(sink):
    asyncio.run(gq.record_results({"results": [], "not_found": ["x1", "x2"], "failed": []}))
    assert {"x1", "x2"} <= _written_keys(sink, "over_re_geocode")


def test_misses_and_not_found_are_the_same_list(sink):
    """The worker sends both spellings for backwards compatibility; taking the
    union means either one alone still works, and both together do not double."""
    out = asyncio.run(gq.record_results({"results": [], "not_found": ["k"], "misses": ["k"]}))
    assert out["recorded_not_found"] == 1


def test_a_result_without_coordinates_is_not_stored_as_a_hit(sink):
    asyncio.run(gq.record_results({"results": [{"address_key": "bad", "lat": None, "lon": None}]}))
    assert "bad" not in _written_keys(sink, "over_re_geocode")


# ── the selection ─────────────────────────────────────────────────────────────
def test_selection_reoffers_failed_but_not_settled_hits():
    """No checkpoint exists by design, so recovery rests entirely on this SQL
    re-deriving the work list from `point IS NULL` on every call."""
    sql = gq._selection_sql(10)
    assert "a.point IS NULL" in sql
    assert "g.address_key IS NULL" in sql          # never asked → offer
    assert "g.status <> 'hit'" in sql              # a hit is done
    assert f"g.attempts < {gq.MAX_ATTEMPTS}" in sql  # a repeated miss goes terminal
    assert "ORDER BY a.address_key" in sql
    # It must not filter on a reservation column — that would be the second copy
    # of the truth the worker deliberately avoided.
    assert "reserved" not in sql and "claimed" not in sql


def test_a_miss_becomes_terminal_only_after_several_refusals():
    assert gq.MAX_ATTEMPTS >= 2, "one refusal must not permanently retire an address"


def test_batch_size_is_ten_thousand():
    assert gq.BATCH_SIZE == 10_000
