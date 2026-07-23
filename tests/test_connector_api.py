"""Unit tests for the key-gated Looker Studio connector API (/api/connector).

No Postgres needed: run_readonly_sql / build_catalog are monkeypatched, which
is enough to exercise the key gate, the max_rows clamp and the trimmed catalog
projection — the actual SQL guards are covered by test_append_store.py.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import connector
from app.config import settings
from app.database import get_db
from app.rate_limit import limiter
from app.services import append_store, data_catalog


async def _fake_db():
    yield None


def _client() -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(connector.router)
    app.dependency_overrides[get_db] = _fake_db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def configured(monkeypatch):
    monkeypatch.setattr(settings, "connector_api_key", "testkey")
    monkeypatch.setattr(append_store, "is_configured", lambda: True)


def test_feature_off_when_key_unset(monkeypatch):
    monkeypatch.setattr(settings, "connector_api_key", "")
    c = _client()
    assert c.post("/api/connector/sql", json={"sql": "SELECT 1"}).status_code == 503
    assert c.get("/api/connector/tables").status_code == 503


def test_missing_or_wrong_key_is_401(configured):
    c = _client()
    assert c.post("/api/connector/sql", json={"sql": "SELECT 1"}).status_code == 401
    r = c.post("/api/connector/sql", json={"sql": "SELECT 1"},
               headers={"X-Connector-Key": "wrong"})
    assert r.status_code == 401
    assert c.get("/api/connector/tables",
                 headers={"X-Connector-Key": "wrong"}).status_code == 401


def test_sql_passthrough_and_defaults(configured, monkeypatch):
    seen = {}

    async def fake_sql(sql, **kw):
        seen["sql"] = sql
        seen.update(kw)
        return {"columns": ["x"], "fields": [{"id": "x", "type": "int"}],
                "rows": [{"x": 1}], "truncated": False, "row_count": 1}

    monkeypatch.setattr(append_store, "run_readonly_sql", fake_sql)
    c = _client()
    r = c.post("/api/connector/sql", json={"sql": "SELECT 1 AS x"},
               headers={"X-Connector-Key": "testkey"})
    assert r.status_code == 200
    assert r.json()["fields"] == [{"id": "x", "type": "int"}]
    assert seen["sql"] == "SELECT 1 AS x"
    assert seen["max_rows"] == connector.DEFAULT_MAX_ROWS
    assert seen["timeout_ms"] == connector.TIMEOUT_MS
    assert seen["search_path"] == data_catalog.CONSOLE_SEARCH_PATH


@pytest.mark.parametrize("requested,effective", [
    (999_999, connector.HARD_MAX_ROWS),
    (0, connector.DEFAULT_MAX_ROWS),   # falsy → default
    (-5, 1),
    (250, 250),
])
def test_max_rows_clamp(configured, monkeypatch, requested, effective):
    seen = {}

    async def fake_sql(sql, **kw):
        seen.update(kw)
        return {"columns": [], "fields": [], "rows": [],
                "truncated": False, "row_count": 0}

    monkeypatch.setattr(append_store, "run_readonly_sql", fake_sql)
    c = _client()
    r = c.post("/api/connector/sql",
               json={"sql": "SELECT 1", "max_rows": requested},
               headers={"X-Connector-Key": "testkey"})
    assert r.status_code == 200
    assert seen["max_rows"] == effective


def test_sql_validation_error_becomes_400(configured, monkeypatch):
    async def fake_sql(sql, **kw):
        raise ValueError("only a single SELECT is allowed")

    monkeypatch.setattr(append_store, "run_readonly_sql", fake_sql)
    c = _client()
    r = c.post("/api/connector/sql", json={"sql": "DROP TABLE x"},
               headers={"X-Connector-Key": "testkey"})
    assert r.status_code == 400
    assert "single SELECT" in r.json()["detail"]


def test_tables_returns_trimmed_projection(configured, monkeypatch):
    async def fake_catalog(db):
        return [{"table": "t1", "schema": "public", "title": "מאגר", "kind": "dataset",
                 "est_rows": 42, "columns": [{"name": "secret"}], "extra": "x"}]

    monkeypatch.setattr(data_catalog, "build_catalog", fake_catalog)
    c = _client()
    r = c.get("/api/connector/tables", headers={"X-Connector-Key": "testkey"})
    assert r.status_code == 200
    assert r.json() == {"tables": [
        {"table": "t1", "schema": "public", "title": "מאגר",
         "kind": "dataset", "est_rows": 42}
    ]}
