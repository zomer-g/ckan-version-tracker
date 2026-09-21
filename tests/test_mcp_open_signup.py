"""MCP access is open: a first Google login self-registers at tier beta, a
disabled user stays out, and every usage row names the MCP server it hit."""
import asyncio
import time

import jwt
import pytest

from app.mcp import oauth
from app.mcp.usage import current_server, server_from_path
from app.models.mcp import ApiUser, McpOauthCode


class _Result:
    def __init__(self, row):
        self._row = row

    def scalar_one_or_none(self):
        return self._row


class _FakeDb:
    def __init__(self, existing=None):
        self.existing = existing
        self.added = []

    async def execute(self, _stmt):
        return _Result(self.existing)

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        pass

    async def commit(self):
        pass


class _Resp:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


def _google(monkeypatch, info):
    class _Client:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            return _Resp({"access_token": "t"})

        async def get(self, *a, **k):
            return _Resp(info)

    monkeypatch.setattr(oauth.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(oauth, "mcp_jwt_secret", lambda: "test-secret-" + "x" * 32)


def _request():
    state = jwt.encode({"client_id": "00000000-0000-4000-8000-0000000000aa",
                        "redirect_uri": "http://localhost:9/cb", "code_challenge": "x" * 43,
                        "exp": int(time.time()) + 60}, oauth.mcp_jwt_secret(), algorithm="HS256")

    class _Req:
        query_params = {"code": "c", "state": state}
        headers = {"host": "over.org.il", "x-forwarded-proto": "https"}

        class url:
            scheme = "https"
            netloc = "over.org.il"

    return _Req()


def test_a_first_login_self_registers_at_beta(monkeypatch):
    _google(monkeypatch, {"email": "New@Example.com", "id": "g1", "name": "N",
                          "verified_email": True})
    db = _FakeDb(existing=None)
    resp = asyncio.run(oauth.google_callback(_request(), db))
    assert resp.status_code in (302, 307)
    users = [o for o in db.added if isinstance(o, ApiUser)]
    assert len(users) == 1
    assert users[0].email == "new@example.com"
    assert users[0].tier == "beta"
    assert users[0].invited_by is None
    assert any(isinstance(o, McpOauthCode) for o in db.added)


def test_a_disabled_user_is_still_refused(monkeypatch):
    _google(monkeypatch, {"email": "off@example.com", "id": "g2", "verified_email": True})
    db = _FakeDb(existing=ApiUser(email="off@example.com", tier="beta", is_active=False))
    resp = asyncio.run(oauth.google_callback(_request(), db))
    assert resp.status_code == 403
    assert not any(isinstance(o, McpOauthCode) for o in db.added)


def test_an_unverified_google_email_is_refused(monkeypatch):
    _google(monkeypatch, {"email": "x@example.com", "id": "g3", "verified_email": False})
    db = _FakeDb(existing=None)
    resp = asyncio.run(oauth.google_callback(_request(), db))
    assert resp.status_code == 403
    assert db.added == []


@pytest.mark.parametrize("path,server", [
    ("/mcp", "over"), ("/mcp/", "over"), ("/deals/mcp", "deals"),
    ("/knesset/mcp", "knesset"), ("/data/mcp", "data"), ("/nadlan/mcp", "nadlan"),
])
def test_the_server_is_read_off_the_mount_path(path, server):
    assert server_from_path(path) == server


def test_log_usage_stamps_the_current_server(monkeypatch):
    from app.mcp import usage
    written = []

    class _S:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def add(self, obj):
            written.append(obj)

        async def commit(self):
            pass

    monkeypatch.setattr(usage, "async_session", lambda: _S())

    async def go():
        current_server.set("deals")
        await usage.log_usage(api_user_id=None, client_id=None, session_id=None,
                              tool_name="search_deals", request_params={}, result_count=1,
                              result_bytes=1, latency_ms=1, status="ok", error_message=None)

    asyncio.run(go())
    # The in-process deep search has no MCP request, so it is named by its session.
    asyncio.run(usage.log_usage(api_user_id=None, client_id=None, session_id="deep-search",
                                tool_name="search", request_params={}, result_count=1,
                                result_bytes=1, latency_ms=1, status="ok", error_message=None))
    assert [w.mcp_server for w in written] == ["deals", "deep_search"]
