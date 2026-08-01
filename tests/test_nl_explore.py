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
SHAMAUT = {**ENTITY, "key": "append_shamaut", "title": "מאגר נתוני שמאות מכריעה",
           "summary": "", "synonyms": [], "geo_dims": []}
MODEL = [ENTITY, OTHER, NO_GEO, SHAMAUT]


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
    a guess shown as a match is how the previous version went wrong.

    The first version of this test looped over suggestions and asserted inside
    an `if s.get("approximate")` — so when the endpoint DROPPED the flag (the
    actual launch bug, caught in live verification), the if never ran and the
    test passed vacuously. Now it asserts the flagged entry exists."""
    r = client.post("/api/nl/suggest", json={"q": "כמה שמאויות מכריעות היו"})
    hits = r.json()["suggestions"]
    approx = [s for s in hits if s["table"] == "append_shamaut"]
    assert approx, "the prefix fallback should have surfaced the inflected match"
    assert approx[0]["approximate"] is True
    assert "דמיון בכתיב" in approx[0]["why"], "a guess must not read like an exact match"


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


# ── /api/nl/cross: the button step 4 exists for ──────────────────────────────

def test_cross_compiles_the_fan_trap_safe_join(client):
    r = client.post("/api/nl/cross",
                    json={"left": "append_business", "right": "append_springs"})
    body = r.json()
    assert body["ok"] is True
    sql = body["sql"]
    # The two invariants that make a locality cross honest: each side is
    # aggregated to the canonical code BEFORE the join, and the join is FULL
    # OUTER so a settlement present on one side only survives.
    assert sql.count("GROUP BY 1") == 2
    assert "FULL JOIN" in sql
    assert sql.count("over_settlement_code") == 2
    from app.services.append_store import validate_readonly_sql
    validate_readonly_sql(sql)
    assert body["explanation"]


def test_cross_refuses_a_pair_without_a_join_key_with_a_reason(client):
    body = client.post("/api/nl/cross",
                       json={"left": "append_business", "right": "append_nogeo"}).json()
    assert body["ok"] is False
    assert body["reason"]


def test_cross_reaches_no_model_and_spends_no_budget(client, monkeypatch):
    called = []
    monkeypatch.setattr("app.services.nl_query._ask_deepseek",
                        lambda *a, **k: called.append("deepseek"))
    monkeypatch.setattr("app.services.llm_budget.reserve_llm_call",
                        lambda *a, **k: called.append("budget"))
    client.post("/api/nl/cross", json={"left": "append_business", "right": "append_springs"})
    assert called == []


# ── click-through logging + learned synonyms ─────────────────────────────────
# Every explorer use is a labelled example: the person who typed the words says
# which dataset they meant. These tests pin the loop's safety properties — a
# broken log must never break a search, and a pick cannot inject text.

def test_a_logging_failure_never_fails_the_search(client, monkeypatch):
    """The stub DB has no nl_suggest_log table, so the INSERT raises — and the
    search must still return its suggestions, with suggest_id null."""
    r = client.post("/api/nl/suggest", json={"q": "רישיונות עסק"})
    assert r.status_code == 200
    body = r.json()
    assert body["suggestions"]
    assert body["suggest_id"] is None


def test_picked_is_update_only_and_never_errors(client):
    """UPDATE against a row /suggest created: a bogus id writes nothing, and
    the endpoint stays 200 — pick reporting is fire-and-forget by contract, so
    the UI never has to handle its failure."""
    r = client.post("/api/nl/picked",
                    json={"suggest_id": 999999, "table": "append_x", "rank": 1})
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_learned_synonyms_become_exact_subject_matches():
    """The loop's payoff, at the scorer level: an adopted word ranks the dataset
    as an EXACT match (score >= floor, not approximate) — where before adoption
    the same word only surfaced it as a labelled skeleton guess."""
    base = {**ENTITY, "key": "append_divorce", "geo_dims": [], "synonyms": [],
            "title": "מספר זוגות שהתגרשו לפי מקום מגורים", "summary": ""}
    before = sm.suggest([base], "גירושין")
    assert before and before[0]["approximate"] is True

    adopted = {**base, "synonyms": ["גירושין"]}
    after = sm.suggest([adopted], "גירושין")
    assert after and not after[0].get("approximate")
    assert after[0]["score"] > before[0]["score"]
