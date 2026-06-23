"""odata resource_create must retry transient 5xx instead of failing the
whole version push.

Regression: odata.org.il runs xloader on every uploaded resource and gets
hammered by concurrent version pushes, so resource_create intermittently
returns 5xx (seen consistently on the mevaker datasets while avodata
pushed fine seconds apart — same endpoint, so it's load, not payload). A
single POST then failed the whole version with "all_pushes_failed". The
upload now retries 5xx / network errors with backoff.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.services.odata_client as oc  # noqa: E402


class _Resp:
    def __init__(self, status):
        self.status_code = status
        self.text = "boom"

    def raise_for_status(self):
        if self.status_code >= 400:
            raise oc.httpx.HTTPStatusError("e", request=None, response=self)

    def json(self):
        return {"success": True, "result": {"id": "res-1"}}


class _Client:
    seq: list = []

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, *a, **k):
        return _Resp(_Client.seq.pop(0))


def _client():
    c = oc.ODataClient.__new__(oc.ODataClient)
    c.api_url = "http://x/api/3/action"
    c._headers = lambda: {}
    return c


def _patch(monkeypatch):
    monkeypatch.setattr(oc.httpx, "AsyncClient", _Client)

    async def _nosleep(_):
        return None

    monkeypatch.setattr(oc.asyncio, "sleep", _nosleep) if hasattr(oc, "asyncio") else None
    monkeypatch.setattr(asyncio, "sleep", _nosleep)


def test_retries_transient_5xx_then_succeeds(monkeypatch):
    _patch(monkeypatch)
    _Client.seq = [500, 503, 200]
    r = asyncio.run(_client().upload_resource(dataset_id="d", file_content=b"x", filename="f.csv"))
    assert r == {"id": "res-1"}
    assert _Client.seq == []  # all three attempts consumed


def test_exhausts_retries_then_raises(monkeypatch):
    _patch(monkeypatch)
    _Client.seq = [500, 500, 500, 500]
    try:
        asyncio.run(_client().upload_resource(dataset_id="d", file_content=b"x", filename="f.csv"))
        assert False, "should have raised after exhausting retries"
    except Exception as e:
        assert "500" in str(e) or "resource_create" in str(e)


def test_4xx_does_not_retry(monkeypatch):
    _patch(monkeypatch)
    _Client.seq = [400, 200]  # a 4xx must raise immediately, not retry into the 200
    try:
        asyncio.run(_client().upload_resource(dataset_id="d", file_content=b"x", filename="f.csv"))
        assert False, "4xx should raise"
    except oc.httpx.HTTPStatusError:
        pass
    assert _Client.seq == [200]  # the second (200) was NOT consumed


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
