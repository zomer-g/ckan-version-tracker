"""Endpoint tests for the guided explorer (/api/nl/suggest, /api/nl/joinable).

These two endpoints replaced the free-text ANSWER box, and the properties that
made the replacement worth doing are properties of the HTTP surface, not just of
the scorer: no model is reachable from here, no budget is consumed, and every
suggestion carries the reason it was offered so a person can reject it.

The scorer itself is covered in test_semantic_model.py. What is pinned here is
that the endpoints expose it faithfully and cannot quietly acquire a cost.
"""
import os

import pytest
from fastapi.testclient import TestClient

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.main import app
from app.services import semantic_model as sm
from tests.test_semantic_model import ENTITY

OTHER = {**ENTITY, "key": "append_springs", "title": "מעיינות - ספיקה מדודה",
         "summary": "", "synonyms": [], "rows": 900}
NO_GEO = {**ENTITY, "key": "append_nogeo", "title": "בלי יישוב", "geo_dims": []}
MODEL = [ENTITY, OTHER, NO_GEO]


@pytest.fixture(autouse=True)
def _stub(monkeypatch):
    async def _model(db, use_cache=True):
        return MODEL
    monkeypatch.setattr(sm, "build_model", _model)
    monkeypatch.setattr("app.api.nl_query.semantic_model.build_model", _model)
    monkeypatch.setattr("app.api.nl_query.append_store.is_configured", lambda: True)


@pytest.fixture
def client():
    return TestClient(app)


def test_suggest_returns_candidates_with_a_reason(client):
    r = client.post("/api/nl/suggest", json={"q": "רישיונות עסק"})
    assert r.status_code == 200
    body = r.json()
    assert body["suggestions"]
    top = body["suggestions"][0]
    assert top["table"] == "append_business"
    # The reason is what turns a ranked list into a decision someone can make.
    assert top["why"] and top["why"] != "התאמה חלשה"
    assert "title" in top["matched"]


def test_suggest_reaches_no_model_and_spends_no_budget(client, monkeypatch):
    """The whole point of the redesign. If either of these is ever wired in,
    the endpoint has quietly reacquired the cost and the failure mode the
    answer box was retired for."""
    called = []
    monkeypatch.setattr("app.services.nl_query._ask_deepseek",
                        lambda *a, **k: called.append("deepseek"))
    monkeypatch.setattr("app.services.nl_query._ask_anthropic",
                        lambda *a, **k: called.append("anthropic"))
    monkeypatch.setattr("app.services.llm_budget.reserve_llm_call",
                        lambda *a, **k: called.append("budget"))
    client.post("/api/nl/suggest", json={"q": "רישיונות עסק"})
    assert called == []


def test_suggest_marks_a_prefix_guess_as_approximate(client):
    """Hebrew morphology gets a fallback so the flow is never a dead end — but
    a guess shown as a match is how the previous version went wrong."""
    r = client.post("/api/nl/suggest", json={"q": "כמה מעיינותיים"})
    for s in r.json()["suggestions"]:
        if s.get("approximate"):
            assert s["score"] < 1.0
            break


def test_suggest_rejects_empty_and_overlong_input(client):
    assert client.post("/api/nl/suggest", json={"q": "  "}).status_code == 400
    assert client.post("/api/nl/suggest", json={"q": "א" * 500}).status_code == 400


def test_suggest_flags_which_candidates_can_be_crossed(client):
    """Surfaced on the shortlist, before the user has invested in choosing —
    otherwise step 4 is only discoverable by accident."""
    body = client.post("/api/nl/suggest", json={"q": "רישיונות עסק"}).json()
    flags = {s["table"]: s["can_join"] for s in body["suggestions"]}
    assert flags.get("append_business") is True


def test_joinable_lists_only_locality_bearing_datasets(client):
    body = client.get("/api/nl/joinable/append_business").json()
    keys = {j["table"] for j in body["joinable"]}
    assert "append_springs" in keys
    assert "append_nogeo" not in keys, "a dataset with no locality has no join key"
    assert "append_business" not in keys, "never offer to join a dataset with itself"


def test_joinable_explains_itself_when_nothing_can_be_crossed(client):
    """A bare empty list reads as a bug. The reason names the missing column."""
    body = client.get("/api/nl/joinable/append_nogeo").json()
    assert body["joinable"] == []
    assert "יישוב" in body["reason"]


def test_joinable_can_be_narrowed_by_text(client):
    all_rows = client.get("/api/nl/joinable/append_business").json()["joinable"]
    narrowed = client.get("/api/nl/joinable/append_business?q=מעיינות").json()["joinable"]
    assert narrowed and len(narrowed) <= len(all_rows)
    assert narrowed[0]["table"] == "append_springs"


def test_every_joinable_row_names_the_column_it_joins_on(client):
    """The user is being asked to trust a cross-dataset join; the least it can
    do is say which field on the other side it is matching."""
    for j in client.get("/api/nl/joinable/append_business").json()["joinable"]:
        assert j["via"]
