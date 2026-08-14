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
    assert "NOT IN ('hit', 'wrong_locality')" in sql  # both are settled
    assert f"g.attempts < {gq.MAX_ATTEMPTS}" in sql  # a repeated miss goes terminal
    assert "a.address_key" in sql.split("ORDER BY")[1]   # deterministic order
    # It must not filter on a reservation column — that would be the second copy
    # of the truth the worker deliberately avoided.
    assert "reserved" not in sql and "claimed" not in sql


def test_a_miss_becomes_terminal_only_after_several_refusals():
    assert gq.MAX_ATTEMPTS >= 2, "one refusal must not permanently retire an address"


def test_batch_size_is_ten_thousand():
    assert gq.BATCH_SIZE == 10_000


# ── a low hit rate is normal; a refusal is not ────────────────────────────────
@pytest.fixture
def govmap_answering(monkeypatch):
    """GovMap resolves its control addresses — i.e. it is up and talking."""
    async def _yes(): return True
    monkeypatch.setattr(gq, "govmap_is_answering", _yes)


@pytest.fixture
def govmap_silent(monkeypatch):
    async def _no(): return False
    monkeypatch.setattr(gq, "govmap_is_answering", _no)


def test_a_sparse_batch_is_believed_when_govmap_answers(sink, govmap_answering):
    """The correction that matters. GovMap's address index is sparse at the
    house-number level outside the metro core — "חטיבת הנגב 10 שדרות" returns
    nothing while "חטיבת הנגב שדרות" returns 2,184 — so misses are usually
    real. Measured per-locality hit rates run 6.9% to 64.4%. Throwing those
    away would re-ask 170k addresses forever and never drain the queue."""
    out = asyncio.run(gq.record_results({
        "results": [{"address_key": "h1", "lat": 32.0, "lon": 34.9}],
        "not_found": [f"m{i}" for i in range(400)]}))
    assert out["quarantined"] is None, "0.25% is low, but GovMap is answering"
    assert out["recorded_not_found"] == 400


def test_misses_are_requeued_when_govmap_fails_its_own_controls(sink, govmap_silent):
    """The only signature that actually distinguishes a refusal: addresses
    GovMap is known to resolve stop resolving."""
    out = asyncio.run(gq.record_results({
        "results": [], "not_found": [f"k{i}" for i in range(400)],
        "samples": ["נהלל 12 נהלל"]}))
    assert out["quarantined"]
    assert out["recorded_not_found"] == 0
    assert out["requeued_failed"] == 400
    assert _written_keys(sink, "over_re_geocode") == set()


def test_an_ordinary_batch_never_reaches_the_canary(monkeypatch, sink):
    """Three requests to GovMap on every batch would be a rude way to find out
    what a hit rate already tells you."""
    called = []
    async def _spy(): called.append(1); return True
    monkeypatch.setattr(gq, "govmap_is_answering", _spy)
    asyncio.run(gq.record_results({
        "results": [{"address_key": f"h{i}", "lat": 32.0, "lon": 34.9} for i in range(120)],
        "not_found": [f"m{i}" for i in range(180)]}))
    assert not called


def test_a_small_batch_is_never_questioned(sink, govmap_silent):
    """Below the sample floor, zero hits is luck rather than evidence."""
    out = asyncio.run(gq.record_results({"results": [], "not_found": ["a", "b"]}))
    assert out["quarantined"] is None
    assert out["recorded_not_found"] == 2


def test_an_aborted_batch_never_spends_an_attempt(sink):
    """The run that triggered the abort is the least trustworthy one there is."""
    asyncio.run(gq.record_results({
        "results": [{"address_key": f"h{i}", "lat": 32.0, "lon": 34.9} for i in range(60)],
        "not_found": [f"m{i}" for i in range(60)],
        "aborted": True, "abort_reason": "100 consecutive no-response",
    }))
    rows = [r for c in sink if c[0] == "many" and "not_found" in c[1] for r in c[2]]
    assert rows, "the misses should still be recorded"
    assert all(r[1] == 0 for r in rows), "an aborted batch must add 0 to attempts"


def test_the_control_addresses_are_ones_govmap_really_resolves():
    """Verified live 2026-08-13: 6, 10 and 203 results respectively."""
    assert len(gq.CANARIES) >= 3
    assert "אבימלך 8 פתח תקווה" in gq.CANARIES


# ── the merge guard ───────────────────────────────────────────────────────────
def test_a_wrong_locality_answer_is_terminal_not_limbo():
    """Asked for גדרה, GovMap answers חדרה — one letter apart, 69 km away, full
    confidence, high score. 196 of those landed in one locality. They must not
    sit as `hit, merged=false` forever: unusable, unretryable, and still counted
    as hits. `wrong_locality` is terminal, and re-asking is pointless because
    GovMap will answer חדרה again."""
    sql = gq._selection_sql(10)
    assert "'wrong_locality'" in sql
    assert "g.status NOT IN ('hit', 'wrong_locality')" in sql


def test_a_locality_with_no_parcels_does_not_reject_its_points():
    """אחוזת ברק has 0 parcels, so every point in it failed a guard it could
    never satisfy. Absence of evidence is not evidence of a bad point."""
    import inspect
    src = inspect.getsource(gq.merge_into_addresses)
    assert "checkable" in src
    assert "j.near OR NOT j.checkable" in src,         "a point must be accepted when there is nothing to check it against"


def test_a_miss_is_not_re_asked_the_same_day():
    """GovMap's answer is deterministic within a day. Re-asking immediately
    produced 0 hits on 1,800 addresses — every one of them a row GovMap had
    already refused, which a plain key order had parked at the head of the
    queue. Three attempts each meant two thirds of the throughput went on
    re-confirming misses while 165k never-asked addresses waited behind them."""
    sql = gq._selection_sql(10)
    assert f"interval '{gq.RETRY_AFTER_DAYS} days'" in sql
    assert gq.RETRY_AFTER_DAYS >= 7, "a retry must outlive the run that caused it"


def test_never_asked_addresses_go_first():
    sql = gq._selection_sql(10)
    assert "ORDER BY (g.address_key IS NOT NULL), a.address_key" in sql
