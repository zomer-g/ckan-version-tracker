"""MCP OAuth: redirect URIs are limited to safe kinds, and /authorize shows a
consent page naming the client and where access goes before any Google sign-in."""
import asyncio
import time
import uuid

import jwt
import pytest

from app.config import settings
from app.mcp import oauth

SECRET = "s" * 40


@pytest.fixture(autouse=True)
def _cfg(monkeypatch):
    monkeypatch.setattr(oauth, "mcp_jwt_secret", lambda: SECRET)
    monkeypatch.setattr(settings, "google_client_id", "gid")
    monkeypatch.setattr(settings, "google_client_secret", "gsecret")


@pytest.mark.parametrize("uri,ok", [
    ("https://claude.ai/api/mcp/auth_callback", True),
    ("https://chatgpt.com/connector/oauth/x", True),
    ("http://127.0.0.1:33418/callback", True),
    ("http://localhost:6274/oauth/callback", True),
    ("http://[::1]:5000/cb", True),
    ("com.example.app:/oauth2redirect", True),
    ("cursor://anysphere.cursor-retrieval/oauth/callback", True),
    ("vscode://vscode.mcp/callback", True),
    ("http://evil.example/cb", False),
    ("javascript:alert(1)", False),
    ("data:text/html,x", False),
    ("file:///etc/passwd", False),
    ("https:///nohost", False),
    ("", False),
])
def test_redirect_uri_rules(uri, ok):
    assert oauth.redirect_uri_allowed(uri) is ok


class _Req:
    def __init__(self, body=None, query=None, form=None):
        self._body, self.query_params, self._form = body, query or {}, form or {}
        self.headers = {"host": "over.org.il", "x-forwarded-proto": "https"}
        self.url = type("U", (), {"scheme": "https", "netloc": "over.org.il"})()

    async def json(self):
        return self._body

    async def form(self):
        return self._form


class _Db:
    def __init__(self, client=None):
        self.client, self.added = client, []

    def add(self, o):
        self.added.append(o)

    async def commit(self):
        pass

    async def refresh(self, o):
        o.client_id = o.client_id or uuid.uuid4()

    async def execute(self, _stmt):
        c = self.client
        return type("R", (), {"scalar_one_or_none": lambda self: c})()


def test_register_refuses_a_remote_http_redirect():
    r = asyncio.run(oauth.register_client(
        _Req({"client_name": "x", "redirect_uris": ["http://evil.example/cb"]}), _Db()))
    assert r.status_code == 400 and b"invalid_redirect_uri" in r.body


def test_authorize_shows_consent_with_escaped_name_and_destination():
    cid = uuid.uuid4()
    client = type("C", (), {"client_id": cid, "client_name": "<b>Evil</b>",
                            "redirect_uris": ["https://evil.example/cb"]})()
    q = {"response_type": "code", "client_id": str(cid),
         "redirect_uri": "https://evil.example/cb", "code_challenge": "x" * 43, "state": "s1"}
    r = asyncio.run(oauth.authorize(_Req(query=q), _Db(client)))
    html = r.body.decode()
    assert r.status_code == 200
    assert "&lt;b&gt;Evil&lt;/b&gt;" in html and "<b>Evil</b>" not in html
    assert "evil.example" in html
    assert "accounts.google.com" not in html            # no straight redirect
    assert "error=access_denied" in html and "state=s1" in html
    assert r.headers["X-Frame-Options"] == "DENY"


def test_continue_needs_a_valid_state_and_then_goes_to_google():
    bad = asyncio.run(oauth.authorize_continue(_Req(form={"state": "nope"})))
    assert bad.status_code == 400
    st = jwt.encode({"client_id": "c", "aud": oauth._STATE_AUDIENCE,
                     "exp": int(time.time()) + 60}, SECRET, algorithm="HS256")
    ok = asyncio.run(oauth.authorize_continue(_Req(form={"state": st})))
    assert ok.status_code == 303
    assert ok.headers["location"].startswith(oauth.GOOGLE_AUTH_URL)
