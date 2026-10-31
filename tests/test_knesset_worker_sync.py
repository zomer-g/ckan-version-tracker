"""The Knesset mirror's sync on the worker fleet (knesset_db plan/ingest,
knesset_sync_queue, /api/worker/knesset-db/*).

The cadence is the owner's: daily for what is NEW, weekly for what was updated
AFTER THE FACT. These pin what each mode asks the worker to fetch.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.scrape_task import ScrapeTask  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.services import knesset_db as kd  # noqa: E402
from app.services import knesset_sync_queue as q  # noqa: E402

NOW = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)
WITH_LUD = kd.EntitySet("KNS_Bill", [("Id", "Edm.Int32"), ("Name", "Edm.String"),
                                     ("LastUpdatedDate", "Edm.DateTimeOffset")])
NO_LUD = kd.EntitySet("V_Lobbyists", [("Id", "Edm.Int32"), ("Name", "Edm.String")])
LOADED = {"full_loaded": True, "last_id": 0,
          "updated_watermark": NOW - timedelta(days=1)}

METADATA = """<?xml version="1.0"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
 <edmx:DataServices><Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="P">
  <EntityType Name="KNS_Bill"><Property Name="Id" Type="Edm.Int32"/>
   <Property Name="Name" Type="Edm.String"/>
   <Property Name="LastUpdatedDate" Type="Edm.DateTimeOffset"/></EntityType>
  <EntityContainer Name="C"><EntitySet Name="KNS_Bill" EntityType="P.KNS_Bill"/></EntityContainer>
 </Schema></edmx:DataServices></edmx:Edmx>"""


# ── what each mode fetches ───────────────────────────────────────────────────

def test_daily_asks_only_for_rows_updated_since_the_watermark():
    j = kd.plan_job(WITH_LUD, LOADED, kd.MODE_DAILY, NOW)
    assert j["kind"] == "updated"
    # watermark (1 day ago) minus the 48 h overlap = 3 days back
    assert j["base_filter"] == "LastUpdatedDate gt 2026-10-28T12:00:00Z"
    assert j["url"].endswith("/ParliamentInfo/KNS_Bill") and not j["want_count"]


def test_weekly_looks_back_thirty_days_whatever_the_watermark():
    j = kd.plan_job(WITH_LUD, LOADED, kd.MODE_WEEKLY, NOW)
    assert j["base_filter"] == "LastUpdatedDate gt 2026-10-01T12:00:00Z"


def test_a_set_without_last_updated_gets_new_ids_daily_and_a_rewalk_weekly():
    daily = kd.plan_job(NO_LUD, LOADED, kd.MODE_DAILY, NOW, max_id=812)
    assert (daily["kind"], daily["after_id"], daily["base_filter"]) == ("new_ids", 812, "")
    assert daily["url"].endswith("/OdataV4/Lobbyist/V_Lobbyists")
    weekly = kd.plan_job(NO_LUD, LOADED, kd.MODE_WEEKLY, NOW, max_id=812)
    assert (weekly["kind"], weekly["after_id"], weekly["want_count"]) == ("rewalk", 0, True)


def test_an_unfinished_table_resumes_its_full_walk_in_either_mode():
    st = {"full_loaded": False, "last_id": 4100, "updated_watermark": None}
    for mode in kd.MODES:
        j = kd.plan_job(WITH_LUD, st, mode, NOW)
        assert (j["kind"], j["after_id"], j["base_filter"], j["want_count"]) == ("full", 4100, "", False)
    fresh = kd.plan_job(WITH_LUD, {"full_loaded": False, "last_id": 0}, kd.MODE_DAILY, NOW)
    assert fresh["want_count"] is True


def test_metadata_from_the_worker_is_parsed_the_same_way():
    (es,) = kd.parse_metadata(METADATA)
    assert es.table == "kns_bill" and es.has_last_updated


def test_plan_asks_for_metadata_first(monkeypatch):
    monkeypatch.setattr(kd, "_metadata_cache", None)
    assert asyncio.run(kd.build_plan(kd.MODE_DAILY)) == {"metadata_needed": True}


def test_ingest_refuses_before_metadata(monkeypatch):
    monkeypatch.setattr(kd, "_metadata_cache", None)
    try:
        asyncio.run(kd.ingest("kns_bill", [], last_id=0))
    except LookupError as e:
        assert "metadata first" in str(e)
    else:
        raise AssertionError("expected LookupError")


# ── the queue ────────────────────────────────────────────────────────────────

def test_saturday_is_the_weekly_look_back():
    sat = datetime(2026, 10, 31, 4, 0, tzinfo=timezone.utc)   # Saturday in Israel
    assert q.mode_for(sat) == kd.MODE_WEEKLY
    assert q.mode_for(sat + timedelta(days=1)) == kd.MODE_DAILY


async def _db():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__, ScrapeTask.__table__):
            await conn.run_sync(lambda c, t=t: t.create(c))
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def test_one_run_at_a_time_on_a_hidden_unpolled_dataset():
    async def go():
        S = await _db()
        async with S() as db:
            first = await q.enqueue(db, kd.MODE_DAILY)
            again = await q.enqueue(db, kd.MODE_WEEKLY)
            ds = await q.ensure_dataset(db)
            task = await db.get(ScrapeTask, uuid.UUID(first["task_id"]))
        assert first["queued"] and not again["queued"]
        assert ds.status == "hidden" and ds.poll_interval == 0
        assert ds.scraper_config == {"kind": "knesset_db_sync"}
        assert task.params["mode"] == "daily"
    asyncio.run(go())


def test_catch_up_queues_only_when_the_mirror_is_a_day_behind(monkeypatch):
    calls = []

    async def fake_enqueue(db, mode, **kw):
        calls.append(mode)
        return {"queued": True}

    class _S:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(q, "enqueue", fake_enqueue)
    monkeypatch.setattr(kd, "is_configured", lambda: True)
    import app.database as D
    monkeypatch.setattr(D, "async_session", lambda: _S())

    async def fresh():
        return datetime.now(timezone.utc) - timedelta(hours=3)

    async def stale():
        return datetime.now(timezone.utc) - timedelta(days=3)

    monkeypatch.setattr(q, "last_synced_at", fresh)
    asyncio.run(q.catch_up())
    assert calls == []
    monkeypatch.setattr(q, "last_synced_at", stale)
    asyncio.run(q.catch_up())
    assert calls == ["daily"]


# ── the endpoints ────────────────────────────────────────────────────────────

def test_worker_endpoints(monkeypatch):
    from fastapi.testclient import TestClient

    from app.api import worker as W
    from app.database import get_db
    from app.main import app

    S = asyncio.run(_db())

    async def _get_db():
        async with S() as db:
            yield db

    async def setup():
        async with S() as db:
            return (await q.enqueue(db, kd.MODE_WEEKLY))["task_id"]
    tid = asyncio.run(setup())

    seen = {}

    async def fake_accept(xml):
        seen["xml"] = xml
        return 1

    async def fake_plan(mode, tables):
        return {"mode": mode, "jobs": [], "pass_started": NOW.isoformat()}

    async def fake_ingest(table, rows, **kw):
        seen["ingest"] = (table, len(rows), kw["done"], kw["last_id"])
        return {"table": table, "rows": len(rows)}

    monkeypatch.setattr(kd, "accept_metadata", fake_accept)
    monkeypatch.setattr(kd, "build_plan", fake_plan)
    monkeypatch.setattr(kd, "ingest", fake_ingest)
    monkeypatch.setattr(W, "_verify_worker_key", lambda request: None)
    app.dependency_overrides[get_db] = _get_db
    try:
        c = TestClient(app)
        assert c.post("/api/worker/knesset-db/metadata", json={"xml": ""}).status_code == 400
        assert c.post("/api/worker/knesset-db/metadata", json={"xml": METADATA}).json() == {"entity_sets": 1}
        plan = c.get(f"/api/worker/knesset-db/plan/{tid}").json()
        assert plan["mode"] == "weekly" and plan["task_id"] == tid
        r = c.post("/api/worker/knesset-db/ingest",
                   json={"table": "kns_bill", "rows": [{"Id": 1}], "last_id": 1, "done": True})
        assert r.status_code == 200 and seen["ingest"] == ("kns_bill", 1, True, 1)
        fin = c.post("/api/worker/knesset-db/finish",
                     json={"task_id": tid, "message": "49 טבלאות", "failed_tables": ["kns_vote"]})
        assert fin.json() == {"status": "completed"}

        async def check():
            async with S() as db:
                t = await db.get(ScrapeTask, uuid.UUID(tid))
                ds = await db.get(TrackedDataset, t.tracked_dataset_id)
                return t.status, t.message, ds.last_error
        st, msg, err = asyncio.run(check())
        assert st == "completed" and msg == "49 טבלאות" and "kns_vote" in err
    finally:
        app.dependency_overrides.clear()
