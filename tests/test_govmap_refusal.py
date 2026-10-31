"""GovMap refusing us is not GovMap saying "no such address".

Since 2026-09-18 the search service answers the worker fleet with HTTP 403. A
miss in ``over_re_geocode`` counts towards retiring an address for good
(``MAX_ATTEMPTS``), so the one rule pinned here above all: a refusal — 403,
429, 5xx, or an HTML page where JSON belongs — never becomes a miss. And the
reaction to one is to stop asking (early, and for a while), not to go on
collecting the same answer 10,000 times.

No request leaves this file: every client is a fake.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import asyncio  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

import pytest  # noqa: E402

from app.services import geocode_queue as gq  # noqa: E402
from app.services import govmap_refusal as gr  # noqa: E402
from app.services import street_geocode as sg  # noqa: E402

from tests.test_geocode_queue import _FakePool, _written_keys  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_state():
    gr.clear()
    sg._cache.clear()
    yield
    gr.clear()
    sg._cache.clear()


@pytest.fixture
def sink(monkeypatch):
    calls = []
    async def _pool(): return _FakePool(calls)
    monkeypatch.setattr(gq.append_store, "get_pool", _pool)
    async def _noop(): return None
    monkeypatch.setattr(gq, "ensure_tables", _noop)
    return calls


@pytest.fixture
def no_canary(monkeypatch):
    """A refused batch must not spend three more requests confirming it."""
    async def _boom():
        raise AssertionError("the canary must not run on a refused batch")
    monkeypatch.setattr(gq, "govmap_is_answering", _boom)


# ── what a refusal is ─────────────────────────────────────────────────────────
@pytest.mark.parametrize("code", [403, 429, 500, 502, 503])
def test_status_codes_that_are_refusals(code):
    assert gr.classify(code, "application/json", b"{}") == f"HTTP {code}"


def test_an_html_page_with_200_is_a_refusal():
    """What a block or maintenance page looks like."""
    assert gr.classify(200, "text/html; charset=utf-8",
                       b"<!DOCTYPE html><html>Access denied") == "non-JSON answer"


def test_a_json_answer_is_not_a_refusal_even_when_empty():
    """An empty result list IS GovMap's 'no such address' — the real miss."""
    assert gr.classify(200, "application/json", b'{"results": []}') is None
    # A missing content-type with a JSON body is still an answer.
    assert gr.classify(200, None, b'{"results": []}') is None


def test_refusal_text_from_a_worker_reason():
    assert gr.refusal_in_text("HTTP 403 Forbidden") == "HTTP 403"
    assert gr.refusal_in_text("http 429") == "HTTP 429"
    assert gr.refusal_in_text("HTTP 404") is None
    assert gr.refusal_in_text("timeout") is None
    assert gr.refusal_in_text(None) is None


# ── the ingest side: a refused batch records no misses ───────────────────────
def test_a_batch_of_403s_records_no_misses(sink, no_canary):
    out = asyncio.run(gq.record_results({
        "results": [{"address_key": "h1", "lat": 32.0, "lon": 34.9}],
        "not_found": ["m1", "m2"],
        "failed": [{"address_key": f"f{i}", "reason": "HTTP 403"} for i in range(5)],
        "aborted": True, "abort_reason": "5 consecutive errors",
    }))
    written = _written_keys(sink, "over_re_geocode")
    assert "h1" in written, "a hit is evidence whatever followed it"
    assert not {"m1", "m2"} & written, "a refused batch's misses must be requeued"
    assert out["refused"] == "HTTP 403"
    assert out["recorded_not_found"] == 0
    assert out["requeued_failed"] == 7
    assert gr.last_refusal()["status"] == "HTTP 403"


def test_an_explicit_refusal_from_the_worker(sink, no_canary):
    out = asyncio.run(gq.record_results({
        "results": [], "not_found": ["m1"], "refusal": {"status": 403}}))
    assert out["refused"] == "HTTP 403"
    assert _written_keys(sink, "over_re_geocode") == set()


def test_an_abort_for_a_refusal(sink, no_canary):
    out = asyncio.run(gq.record_results({
        "results": [], "not_found": ["m1"], "aborted": True,
        "abort_reason": "GovMap answered HTTP 429 Too Many Requests"}))
    assert out["refused"] == "HTTP 429"
    assert out["recorded_not_found"] == 0


def test_html_failures_are_refusals_too(sink, no_canary):
    out = asyncio.run(gq.record_results({
        "results": [], "not_found": ["m1"],
        "failed": [{"address_key": f"f{i}", "reason": "non-JSON answer"} for i in range(3)]}))
    assert out["refused"]
    assert out["recorded_not_found"] == 0


def test_one_server_error_in_a_batch_is_a_hiccup_not_a_refusal(sink):
    """Pinned by the existing three-outcomes test too: a lone 500 must not
    throw away a batch's real misses."""
    out = asyncio.run(gq.record_results({
        "results": [], "not_found": ["m1"],
        "failed": [{"address_key": "f1", "reason": "HTTP 500"}]}))
    assert out["refused"] is None
    assert out["recorded_not_found"] == 1
    assert gr.last_refusal() is None


# ── stop early: no new batch, no batch handed out, canary stops at one ───────
def test_no_batch_is_queued_while_govmap_refuses(monkeypatch):
    gr.note("HTTP 403", "worker")
    async def _boom():
        raise AssertionError("must not even count the work list")
    monkeypatch.setattr(gq, "remaining_count", _boom)
    out = asyncio.run(gq.enqueue_next_batch(db=None))
    assert out["queued"] is False
    assert out["last_refusal"]["status"] == "HTTP 403"


def test_the_refusal_wears_off_after_the_backoff():
    gr.note("HTTP 403", "worker", now=datetime.now(timezone.utc) - gr.BACKOFF
            - timedelta(minutes=1))
    assert not gr.active()
    assert gr.last_refusal() is not None, "still shown to the admin"


class _Resp:
    def __init__(self, code, ctype="application/json", body=b'{"results": []}'):
        self.status_code = code
        self.headers = {"content-type": ctype}
        self.content = body
    def json(self):
        import json
        return json.loads(self.content)


def _fake_httpx(monkeypatch, responses, posts):
    import httpx

    class _Client:
        def __init__(self, *a, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *e): return False
        async def post(self, url, **k):
            posts.append(k.get("json", {}).get("searchText"))
            return responses.pop(0) if len(responses) > 1 else responses[0]

    monkeypatch.setattr(httpx, "AsyncClient", _Client)


def test_the_canary_stops_at_the_first_refusal(monkeypatch):
    posts = []
    _fake_httpx(monkeypatch, [_Resp(403)], posts)
    assert asyncio.run(gq.govmap_is_answering()) is False
    assert len(posts) == 1, "one 403 answers the question; do not ask twice more"
    assert gr.active()


def test_one_log_line_per_episode_not_per_address(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger=gr.__name__):
        for _ in range(500):
            gr.note("HTTP 403", "worker")
    assert len([r for r in caplog.records if "refused" in r.getMessage()]) == 1


# ── the live street lookup ────────────────────────────────────────────────────
def test_street_lookup_raises_a_distinct_refusal(monkeypatch):
    posts = []
    _fake_httpx(monkeypatch, [_Resp(403, "text/html", b"<html>")], posts)
    with pytest.raises(gr.GovMapRefused):
        asyncio.run(sg._ask("הר הצופים דימונה"))


def test_street_refusal_is_not_memoised_and_pauses_asking(monkeypatch):
    posts = []
    _fake_httpx(monkeypatch, [_Resp(403)], posts)
    assert asyncio.run(sg.locate_street("דימונה", "הר הצופים")) is None
    assert "הר הצופים דימונה" not in sg._cache, "a refusal is not 'no such street'"
    # While the refusal holds, no further request is sent at all.
    assert asyncio.run(sg.locate_street("דימונה", "הרצל")) is None
    assert len(posts) == 1


def test_a_genuine_empty_answer_is_still_memoised(monkeypatch):
    posts = []
    _fake_httpx(monkeypatch, [_Resp(200)], posts)
    assert asyncio.run(sg.locate_street("דימונה", "הר הצופים")) is None
    assert "הר הצופים דימונה" in sg._cache
    assert gr.last_refusal() is None
