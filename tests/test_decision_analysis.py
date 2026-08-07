"""The decision-analysis page must stay invisible until it is published.

The page at /rationale/1933 is written before it is meant to be read: the whole
document is drafted and edited in the admin panel while the public must see
nothing at all. That gate is the point of these tests —

  * a fresh install (no row) 404s the public endpoint and lists nothing, so a
    deploy can never leak a draft;
  * saving content does NOT publish it — the two are separate calls, and a save
    that quietly flipped visibility would be the exact failure this design
    exists to prevent;
  * only once `published` is true does the document appear on both endpoints.

The rest pins the validation that stands between a hand-edited payload and the
public page: the bundled default must satisfy it, and a bad status, a duplicate
id or a missing id must 422 rather than render as a broken page.

Runs on in-memory SQLite (conftest maps JSONB → JSON) through a bare FastAPI app
carrying only the two routers, in the repo's dependency-light style.
"""
import asyncio
import copy
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.decision_analysis import (  # noqa: E402
    admin_router, router, DEFAULT_DOCS, MAX_DOC_CHARS,
)
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.data.decision_1933 import DECISION_KEY, DEFAULT_DOC, TASK_STATUSES  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.decision_analysis import DecisionAnalysis  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402

KEY = DECISION_KEY


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(
                Base.metadata.create_all, tables=[DecisionAnalysis.__table__]
            )

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(router)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


# ── the publish gate ─────────────────────────────────────────────────────

def test_unpublished_is_invisible_to_the_public(client):
    """No row at all — the state a fresh deploy is in."""
    assert client.get(f"/api/decision-analysis/{KEY}").status_code == 404
    assert client.get("/api/decision-analysis").json() == []


def test_admin_sees_the_bundled_default_before_any_edit(client):
    body = client.get(f"/api/admin/decision-analysis/{KEY}").json()
    assert body["published"] is False
    assert body["is_customized"] is False
    assert body["doc"]["decision_number"] == "1933"
    assert len(body["doc"]["sections"]) == len(DEFAULT_DOC["sections"])


def test_saving_content_does_not_publish_it(client):
    """The two are separate calls on purpose — drafting must stay private."""
    doc = copy.deepcopy(DEFAULT_DOC)
    doc["title"] = "טיוטה בעבודה"
    assert client.put(
        f"/api/admin/decision-analysis/{KEY}", json={"doc": doc}
    ).status_code == 200

    assert client.get(f"/api/decision-analysis/{KEY}").status_code == 404
    assert client.get("/api/decision-analysis").json() == []

    draft = client.get(f"/api/admin/decision-analysis/{KEY}").json()
    assert draft["published"] is False
    assert draft["is_customized"] is True
    assert draft["doc"]["title"] == "טיוטה בעבודה"
    assert draft["updated_by"] == "admin@test"


def test_publishing_then_unpublishing_flips_public_visibility(client):
    doc = copy.deepcopy(DEFAULT_DOC)
    doc["title"] = "הכותרת שפורסמה"
    client.put(f"/api/admin/decision-analysis/{KEY}", json={"doc": doc})

    client.put(f"/api/admin/decision-analysis/{KEY}", json={"published": True})
    public = client.get(f"/api/decision-analysis/{KEY}")
    assert public.status_code == 200
    assert public.json()["doc"]["title"] == "הכותרת שפורסמה"
    assert client.get("/api/decision-analysis").json() == [
        {"key": KEY, "title": "הכותרת שפורסמה", "subtitle": doc["subtitle"]}
    ]

    client.put(f"/api/admin/decision-analysis/{KEY}", json={"published": False})
    assert client.get(f"/api/decision-analysis/{KEY}").status_code == 404
    assert client.get("/api/decision-analysis").json() == []


def test_publishing_without_edits_serves_the_bundled_default(client):
    """Publishing first and editing later must not serve an empty page."""
    client.put(f"/api/admin/decision-analysis/{KEY}", json={"published": True})
    body = client.get(f"/api/decision-analysis/{KEY}").json()
    assert body["doc"]["title"] == DEFAULT_DOC["title"]
    assert len(body["doc"]["sections"]) == len(DEFAULT_DOC["sections"])


def test_revert_drops_the_edits_but_keeps_the_page_published(client):
    doc = copy.deepcopy(DEFAULT_DOC)
    doc["title"] = "נוסח שנערך"
    client.put(f"/api/admin/decision-analysis/{KEY}", json={"doc": doc, "published": True})
    assert client.get(f"/api/decision-analysis/{KEY}").json()["doc"]["title"] == "נוסח שנערך"

    assert client.delete(f"/api/admin/decision-analysis/{KEY}").status_code == 200
    public = client.get(f"/api/decision-analysis/{KEY}").json()
    assert public["doc"]["title"] == DEFAULT_DOC["title"]
    assert client.get(f"/api/admin/decision-analysis/{KEY}").json()["is_customized"] is False


def test_unknown_decision_is_404_everywhere(client):
    assert client.get("/api/decision-analysis/9999").status_code == 404
    assert client.get("/api/admin/decision-analysis/9999").status_code == 404
    assert client.put(
        "/api/admin/decision-analysis/9999", json={"published": True}
    ).status_code == 404


# ── the bundled default is itself valid content ──────────────────────────

def test_default_doc_round_trips_through_validation(client):
    """The text shipped in the repo must satisfy the same rules a hand edit
    does — otherwise the first save of an untouched document would 422."""
    resp = client.put(
        f"/api/admin/decision-analysis/{KEY}", json={"doc": copy.deepcopy(DEFAULT_DOC)}
    )
    assert resp.status_code == 200, resp.text
    stored = client.get(f"/api/admin/decision-analysis/{KEY}").json()["doc"]
    assert [s["id"] for s in stored["sections"]] == [
        s["id"] for s in DEFAULT_DOC["sections"]
    ]


def test_default_doc_has_operative_tasks_with_the_three_analysis_texts():
    """The page's whole argument is the three-part analysis per task; a task
    shipped with an empty column would render as 'טרם נכתב' in production."""
    tasks = [t for s in DEFAULT_DOC["sections"] for t in s["tasks"]]
    assert len(tasks) >= 20, "the analysis is supposed to cover the decision"
    for task in tasks:
        where = task["id"]
        assert task["title"].strip(), where
        assert task["obligation"].strip(), where
        assert task["status"] in TASK_STATUSES, where
        for column in ("potential", "actual", "damage"):
            assert task[column].strip(), f"{where}.{column} is empty"


def test_default_doc_sections_are_complete_and_uniquely_identified():
    ids = [s["id"] for s in DEFAULT_DOC["sections"]]
    assert len(ids) == len(set(ids))
    task_ids = [t["id"] for s in DEFAULT_DOC["sections"] for t in s["tasks"]]
    assert len(task_ids) == len(set(task_ids))
    for section in DEFAULT_DOC["sections"]:
        assert section["part"].strip(), section["id"]
        assert section["heading"].strip(), section["id"]
        assert section["text"].strip(), section["id"]


def test_default_doc_is_registered_and_within_the_size_cap():
    assert DEFAULT_DOCS[KEY] is DEFAULT_DOC
    import json

    assert len(json.dumps(DEFAULT_DOC, ensure_ascii=False)) < MAX_DOC_CHARS


# ── validation of hand-edited payloads ───────────────────────────────────

def _minimal_doc(**over) -> dict:
    doc = {
        "key": KEY,
        "title": "כותרת",
        "subtitle": "",
        "intro": "",
        "decision_number": "1933",
        "decision_date": "",
        "decision_url": "",
        "labels": {},
        "sections": [
            {
                "id": "s1",
                "part": "חלק",
                "label": "1",
                "heading": "כותרת",
                "text": "נוסח",
                "tasks": [],
            }
        ],
    }
    doc.update(over)
    return doc


def _put(client, doc):
    return client.put(f"/api/admin/decision-analysis/{KEY}", json={"doc": doc})


def test_minimal_document_is_accepted(client):
    assert _put(client, _minimal_doc()).status_code == 200


def test_unknown_task_status_is_rejected(client):
    doc = _minimal_doc()
    doc["sections"][0]["tasks"] = [{"id": "t1", "title": "x", "status": "מצוין"}]
    resp = _put(client, doc)
    assert resp.status_code == 422
    assert "status" in resp.json()["detail"]


def test_missing_status_defaults_to_unknown(client):
    doc = _minimal_doc()
    doc["sections"][0]["tasks"] = [{"id": "t1", "title": "x"}]
    assert _put(client, doc).status_code == 200
    stored = client.get(f"/api/admin/decision-analysis/{KEY}").json()["doc"]
    assert stored["sections"][0]["tasks"][0]["status"] == "unknown"


def test_duplicate_section_ids_are_rejected(client):
    """Ids are React keys and in-page anchors; a duplicate breaks both."""
    doc = _minimal_doc()
    doc["sections"].append(dict(doc["sections"][0]))
    resp = _put(client, doc)
    assert resp.status_code == 422
    assert "duplicated" in resp.json()["detail"]


def test_duplicate_task_ids_are_rejected(client):
    doc = _minimal_doc()
    doc["sections"][0]["tasks"] = [
        {"id": "t1", "title": "a", "status": "done"},
        {"id": "t1", "title": "b", "status": "done"},
    ]
    resp = _put(client, doc)
    assert resp.status_code == 422
    assert "duplicated" in resp.json()["detail"]


def test_blank_section_id_is_rejected(client):
    doc = _minimal_doc()
    doc["sections"][0]["id"] = "   "
    assert _put(client, doc).status_code == 422


def test_non_string_field_is_rejected(client):
    doc = _minimal_doc()
    doc["sections"][0]["text"] = {"oops": True}
    assert _put(client, doc).status_code == 422


def test_sections_must_be_a_list(client):
    assert _put(client, _minimal_doc(sections={"a": 1})).status_code == 422


def test_unknown_fields_are_dropped_rather_than_stored(client):
    doc = _minimal_doc()
    doc["injected"] = "<script>"
    doc["sections"][0]["injected"] = "x"
    assert _put(client, doc).status_code == 200
    stored = client.get(f"/api/admin/decision-analysis/{KEY}").json()["doc"]
    assert "injected" not in stored
    assert "injected" not in stored["sections"][0]


def test_an_empty_put_is_rejected(client):
    assert client.put(f"/api/admin/decision-analysis/{KEY}", json={}).status_code == 422


def test_an_oversized_document_is_rejected(client):
    doc = _minimal_doc()
    doc["sections"][0]["text"] = "א" * (MAX_DOC_CHARS + 1)
    resp = _put(client, doc)
    assert resp.status_code == 422
    assert "too large" in resp.json()["detail"]


# ── rate limits ──────────────────────────────────────────────────────────

def test_public_routes_are_rate_limited():
    """Both public endpoints run a query per request; neither may be unlimited."""
    from fastapi.routing import APIRoute

    for route in router.routes:
        if isinstance(route, APIRoute):
            key = f"{route.endpoint.__module__}.{route.endpoint.__name__}"
            assert limiter._route_limits.get(key), f"{route.path} has no rate limit"
