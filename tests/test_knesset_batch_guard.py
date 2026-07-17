"""Guards on the public /api/knesset-db/protocols/batch.zip path.

The endpoint fetches protocol files live from fs.knesset.gov.il, holds each
whole in memory and repacks a ZIP — so an anonymous request is bounded three
ways: a process-wide concurrency slot (immediate 429 when full), a low
file-count cap, and a cumulative-byte ceiling enforced mid-stream. These cover
the two pure guards (no DB / network needed for the slot; a fake httpx client
for the stream)."""
import io
import os
import sys
import zipfile

os.environ.setdefault("JWT_SECRET_KEY", "test")

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import pytest  # noqa: E402

from app.api import knesset_db as A  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_slots():
    """Every test starts with the global build counter at zero."""
    A._active_zip_builds = 0
    yield
    A._active_zip_builds = 0


# ── Concurrency slot ─────────────────────────────────────────────────────────

def test_slots_reserve_up_to_max_then_reject(monkeypatch):
    monkeypatch.setattr(A, "_MAX_CONCURRENT_ZIP_BUILDS", 2)
    assert A._reserve_zip_slot() is True   # 1
    assert A._reserve_zip_slot() is True   # 2
    assert A._reserve_zip_slot() is False  # full → caller returns 429
    A._release_zip_slot()
    assert A._reserve_zip_slot() is True   # a freed slot is reusable


def test_release_never_goes_negative():
    A._release_zip_slot()
    A._release_zip_slot()
    assert A._active_zip_builds == 0


# ── Streaming byte cap + slot release ────────────────────────────────────────

class _FakeResp:
    def __init__(self, n: int):
        self.content = b"x" * n

    def raise_for_status(self):
        pass


class _FakeClient:
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def get(self, url):
        # size encoded in the url as ...#<bytes>
        return _FakeResp(int(url.rsplit("#", 1)[-1]))


def _rows(sizes):
    return [
        {"filepath": f"https://fs.knesset.gov.il/f{i}.pdf#{n}", "document_id": i,
         "session_id": i, "startdate": "2026-01-01", "knessetnum": 25,
         "committee_name": "ועדת הכספים"}
        for i, n in enumerate(sizes)
    ]


async def _drain(gen):
    out = b""
    async for chunk in gen:
        out += chunk
    return out


def _read_zip(raw: bytes):
    zf = zipfile.ZipFile(io.BytesIO(raw))
    return {name: zf.read(name) for name in zf.namelist()}


def test_stream_stops_at_total_byte_cap(monkeypatch):
    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(A, "BATCH_MAX_TOTAL_BYTES", 250)
    A._reserve_zip_slot()  # the endpoint would have reserved before streaming

    # Three 100-byte files: after the second (200) we're still under; the third
    # tips to 300 ≥ 250, gets included, then the loop breaks — file #4 is never
    # fetched.
    import asyncio
    raw = asyncio.run(_drain(A._zip_stream(_rows([100, 100, 100, 100]))))

    members = _read_zip(raw)
    packed = [n for n in members if not n.startswith("_")]
    assert len(packed) == 3, packed          # 4th never fetched
    assert "_errors.txt" in members
    assert "TRUNCATED" in members["_errors.txt"].decode("utf-8")
    # slot released by the generator's finally
    assert A._active_zip_builds == 0


def test_stream_releases_slot_on_clean_run(monkeypatch):
    import asyncio

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(A, "BATCH_MAX_TOTAL_BYTES", 10_000)
    A._reserve_zip_slot()

    raw = asyncio.run(_drain(A._zip_stream(_rows([50, 50]))))
    members = _read_zip(raw)
    assert len([n for n in members if not n.startswith("_")]) == 2
    assert "_errors.txt" not in members       # nothing failed, no truncation
    assert A._active_zip_builds == 0


def test_stream_releases_slot_even_if_consumer_aborts(monkeypatch):
    """Client disconnect → the generator is closed mid-stream; the finally must
    still free the slot so the endpoint doesn't leak capacity."""
    import asyncio

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(A, "BATCH_MAX_TOTAL_BYTES", 10_000)
    A._reserve_zip_slot()

    async def _partial():
        gen = A._zip_stream(_rows([50, 50, 50]))
        await gen.__anext__()       # pull one chunk
        await gen.aclose()          # simulate client going away

    asyncio.run(_partial())
    assert A._active_zip_builds == 0
