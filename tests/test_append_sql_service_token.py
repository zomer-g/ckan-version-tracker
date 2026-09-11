"""The per-dataset SQL endpoints and SQL_SERVICE_TOKEN, at the HTTP layer.

Pins the rollout order: with APPEND_SQL_REQUIRE_AUTH off nothing changes for
anonymous callers (TAG-IT keeps importing); with it on, anonymous callers and
wrong tokens get 401 while the service token and signed-in users get through;
and the token never reaches the log.
"""
import logging
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi.testclient import TestClient

import app.api.append as append_api
import app.main as M
from app.auth import sql_access
from app.services import append_store

TOKEN = "svc-token-for-tests-" + "x" * 40
DS = "00000000-0000-0000-0000-000000000000"


@pytest.fixture()
def client(monkeypatch):
    async def fake_resolve(dataset_id, db, selector):
        return None, "append_test", []

    async def fake_sql(sql, table=None, **kwargs):
        return {"columns": ["n"], "fields": [{"id": "n", "type": "int"}], "rows": [{"n": 1}],
                "truncated": False, "row_count": 1}

    monkeypatch.setattr(append_api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "run_readonly_sql", fake_sql)
    monkeypatch.setattr(M.settings, "sql_service_token", TOKEN)
    return TestClient(M.app, raise_server_exceptions=False)


def _post(c, headers=None):
    return c.post(f"/api/append/{DS}/sql", json={"sql": "SELECT 1 AS n"}, headers=headers or {})


def _get(c, headers=None):
    return c.get(f"/api/append/{DS}/datastore_search_sql", params={"sql": "SELECT 1 AS n"}, headers=headers or {})


def test_step_one_nothing_closes_for_anonymous_callers(client, monkeypatch):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", False)
    assert _post(client).status_code == 200
    assert _get(client).status_code == 200


@pytest.mark.parametrize("call", [_post, _get])
def test_step_three_anonymous_and_wrong_tokens_are_refused(client, monkeypatch, call):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", True)
    assert call(client).status_code == 401
    assert call(client, {"Authorization": "Bearer not-the-token"}).status_code == 401
    assert call(client, {"Authorization": f"Bearer {TOKEN}x"}).status_code == 401


@pytest.mark.parametrize("call", [_post, _get])
def test_step_three_the_service_token_gets_through(client, monkeypatch, call):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", True)
    r = call(client, {"Authorization": f"Bearer {TOKEN}"})
    assert r.status_code == 200, r.text


def test_an_unconfigured_token_matches_nothing(client, monkeypatch):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", True)
    monkeypatch.setattr(M.settings, "sql_service_token", "")
    assert _post(client, {"Authorization": "Bearer "}).status_code in (401, 403)
    assert not sql_access.service_token_matches("")
    assert not sql_access.service_token_matches(None)


def test_the_token_never_reaches_the_log(client, monkeypatch, caplog):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", True)
    with caplog.at_level(logging.INFO):
        assert _post(client, {"Authorization": f"Bearer {TOKEN}"}).status_code == 200
    assert "caller=service:sql-service" in caplog.text
    assert TOKEN not in caplog.text


def test_a_signed_in_user_is_admitted_and_logged_by_id(monkeypatch):
    monkeypatch.setattr(M.settings, "append_sql_require_auth", True)
    sql_access.admit(sql_access.SqlCaller("user", "8b0c6d0e"), dataset_id=DS, sql="SELECT 1")
    with pytest.raises(Exception):
        sql_access.admit(sql_access.ANONYMOUS, dataset_id=DS, sql="SELECT 1")
