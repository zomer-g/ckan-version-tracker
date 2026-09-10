"""Who may run a query over the data, pinned at the HTTP layer.

The SQL consoles need a signed-in Google account (attribution and a per-account
budget). The first version of that gate shipped with no test that an anonymous
request is actually refused, which meant the only proof was curl against
production. These tests are that proof, and they also pin the one endpoint that
must stay open: TAG-IT imports government decisions anonymously through
POST /api/append/{id}/sql, so gating it would silently break that import.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi.testclient import TestClient

import app.main as M

REFUSED = {401, 403}


@pytest.fixture(scope="module")
def client():
    # No context manager: the lifespan (scheduler, boot proofs) must not run.
    return TestClient(M.app, raise_server_exceptions=False)


@pytest.mark.parametrize("method,path,kwargs", [
    ("post", "/api/tables/sql", {"json": {"sql": "SELECT 1"}}),
    ("post", "/api/knesset-db/sql", {"json": {"sql": "SELECT 1"}}),
    ("get", "/api/tables/export.csv", {"params": {"sql": "SELECT 1"}}),
    ("get", "/api/knesset-db/export.csv", {"params": {"sql": "SELECT 1"}}),
])
def test_anonymous_console_query_is_refused(client, method, path, kwargs):
    r = getattr(client, method)(path, **kwargs)
    assert r.status_code in REFUSED, (path, r.status_code, r.text[:200])


def test_anonymous_free_text_query_cannot_run(client):
    """run=true (the default) executes SQL. Anonymous, that was a way around the
    console gate. Refused before any language-model call is made."""
    r = client.post("/api/nl/query", json={"q": "כמה רכבים יש", "run": True})
    assert r.status_code == 401, r.text[:200]
    r = client.post("/api/nl/query", json={"q": "כמה רכבים יש"})
    assert r.status_code == 401, r.text[:200]


def test_anonymous_free_text_compile_only_is_not_auth_gated(client):
    """The /data page compiles only (run=false) and then runs the SQL through the
    gated console, so compiling stays public."""
    r = client.post("/api/nl/query", json={"q": "כמה רכבים יש", "run": False})
    assert r.status_code not in REFUSED, r.text[:200]


def test_append_sql_stays_anonymous_for_tagit(client):
    r = client.post("/api/append/00000000-0000-0000-0000-000000000000/sql",
                    json={"sql": "SELECT 1"})
    assert r.status_code not in REFUSED, r.text[:200]
