"""The hardening from the 2026-10 security review, pinned so it is not undone
by accident: response headers, the registration rate limit, and verified TLS
for the app engine on a public database host.
"""
import os
import ssl

os.environ.setdefault("JWT_SECRET_KEY", "test")

from fastapi.testclient import TestClient  # noqa: E402

import app.database as D  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)


def test_every_response_says_nosniff_and_refuses_foreign_frames():
    for path in ("/api/tables", "/api/definitely-not-a-route"):
        r = client.get(path)
        assert r.headers.get("x-content-type-options") == "nosniff", path
        assert r.headers.get("x-frame-options") == "SAMEORIGIN", path


def test_oauth_client_registration_is_rate_limited():
    # RFC 7591 registration is the one MCP endpoint an anonymous caller can make
    # write a row. slowapi wraps the endpoint, so the wrapped function is the
    # evidence the limit is there.
    route = next(r for r in app.routes
                 if getattr(r, "path", None) == "/mcp/oauth/register")
    assert getattr(route.endpoint, "__wrapped__", None) is not None


def test_app_engine_verifies_the_certificate_of_a_public_database_host():
    args = D.app_db_connect_args("postgresql+asyncpg://u:p@db.example.com:5432/over")
    ctx = args["ssl"]
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True


def test_app_engine_speaks_plain_tcp_on_the_platform_network():
    assert D.app_db_connect_args("postgresql+asyncpg://u:p@10.200.2.2:5432/over") == {}
    assert D.app_db_connect_args("sqlite+aiosqlite:///:memory:") == {}
