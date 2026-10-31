"""System health checks: the registry, the per-source scraper verdict, the run
log and the diff between runs (app/services/health_checks.py).

The point of the log is to say WHICH scraper broke and SINCE WHEN, and to alert
once — not every morning. These pin that on a real (SQLite) database.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.models.datastore_push_job import DatastorePushJob  # noqa: E402
from app.models.drive_export_job import DriveExportJob  # noqa: E402
from app.models.govmap_coverage import GovmapCoverage  # noqa: E402
from app.models.health_run import HealthRun  # noqa: E402
from app.models.scrape_task import INTERRUPTED_MESSAGE, PHASE_INTERRUPTED, ScrapeTask  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services import health_checks as hc  # noqa: E402

NOW = datetime.now(timezone.utc)
DB_CHECKS = ["db.app", "queue.scrape", "queue.background", "scrapers.runs",
             "scrapers.overdue", "scrapers.silent", "datasets.quality"]


def _ds(ckan_id, source_type="scraper", interval=86400, polled_hours_ago=1, **kw):
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=ckan_id, ckan_name=ckan_id, title=ckan_id,
        source_type=source_type, poll_interval=interval, is_active=True,
        status="active", storage_mode="full_snapshot", source_url="u",
        last_polled_at=NOW - timedelta(hours=polled_hours_ago),
        created_at=NOW - timedelta(days=60), **kw)


def _task(ds, status, hours_ago, phase="scraping", error=None):
    at = NOW - timedelta(hours=hours_ago)
    return ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds.id, status=status,
                      phase=phase, params={}, created_at=at, updated_at=at, completed_at=at,
                      error=error if error else ("boom" if status == "failed" else None))


async def _db():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__, ScrapeTask.__table__,
                  VersionIndex.__table__, HealthRun.__table__, GovmapCoverage.__table__,
                  DatastorePushJob.__table__, DriveExportJob.__table__):
            await conn.run_sync(lambda c, t=t: t.create(c))
        await conn.exec_driver_sql("DROP INDEX IF EXISTS uq_scrape_tasks_active_per_dataset")
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _ctx(S, deep=False):
    return hc.Ctx(deep=deep, session_factory=S)


# ── the registry ─────────────────────────────────────────────────────────

def test_weekly_runs_everything_daily_runs_only_daily():
    daily = {c.key for c in hc.checks_for("daily")}
    weekly = {c.key for c in hc.checks_for("weekly")}
    assert weekly == set(hc.REGISTRY)
    assert daily < weekly
    # the expensive ones are weekly-only
    assert {"scrapers.silent", "site.api_contract", "site.source_types"} <= weekly - daily


def test_manual_selection_validates_keys():
    assert [c.key for c in hc.checks_for("manual", ["db.app"])] == ["db.app"]
    with pytest.raises(ValueError):
        hc.checks_for("manual", ["no.such.check"])


def test_overall_status_counts_a_crashed_check_as_failure():
    assert hc.overall(["ok", "skip"]) == "ok"
    assert hc.overall(["ok", "warn"]) == "warn"
    assert hc.overall(["warn", "error"]) == "fail"


# ── per-source verdict ───────────────────────────────────────────────────

def test_source_verdict_half_failing_is_broken():
    assert hc._source_verdict(10, 0) == "ok"
    assert hc._source_verdict(10, 2) == "warn"
    assert hc._source_verdict(10, 5) == "fail"
    assert hc._source_verdict(1, 1) == "fail"   # a one-dataset source, failing


def test_scraper_runs_names_the_broken_source():
    async def go():
        S = await _db()
        good = [_ds(f"govmap-scraper-{i}", "govmap") for i in range(4)]
        bad = [_ds(f"mevaker-scraper-{i}") for i in range(2)]
        fixed = _ds("munidata-scraper-a")
        cut = _ds("jda-scraper-a")
        async with S() as db:
            db.add_all(good + bad + [fixed, cut])
            await db.flush()
            db.add_all([_task(d, "completed", 3) for d in good])
            db.add(_task(good[0], "failed", 5))                 # resolved by the 3h run
            db.add_all([_task(d, "failed", 2, error="selector not found") for d in bad])
            db.add_all([_task(fixed, "failed", 6), _task(fixed, "completed", 1)])
            # an interrupted run is not a failed scrape
            db.add(_task(cut, "failed", 1, phase=PHASE_INTERRUPTED,
                         error=INTERRUPTED_MESSAGE.format(hb=12, age=40)))
            db.add(_task(cut, "completed", 8))
            await db.commit()
        r = await hc.REGISTRY["scrapers.runs"].fn(_ctx(S))
        by = {i["key"]: i for i in r.items}
        assert r.status == "fail"
        assert "mevaker" in r.message
        assert by["mevaker"]["status"] == "fail" and by["mevaker"]["failing"] == 2
        assert by["mevaker"]["samples"][0]["error"] == "selector not found"
        assert by["govmap"]["status"] == "ok"
        assert by["munidata"]["status"] == "ok"
        assert by["jda"]["status"] == "ok"
    asyncio.run(go())


def test_ckan_sources_are_judged_by_last_error():
    async def go():
        S = await _db()
        async with S() as db:
            db.add_all([_ds("a", "ckan", last_error="403 WAF"), _ds("b", "ckan"), _ds("c", "ckan")])
            await db.commit()
        r = await hc.REGISTRY["scrapers.runs"].fn(_ctx(S))
        (ckan,) = r.items
        assert ckan["key"] == "ckan" and ckan["failing"] == 1 and ckan["status"] == "warn"
    asyncio.run(go())


def test_overdue_and_silent_breakage():
    async def go():
        S = await _db()
        late = _ds("eden-scraper-a", polled_hours_ago=24 * 5)          # daily, not polled in 5 days
        silent = _ds("idf-scraper-a", polled_hours_ago=1)              # polled, never succeeded
        healthy = _ds("idf-scraper-b", polled_hours_ago=1)
        async with S() as db:
            db.add_all([late, silent, healthy])
            await db.flush()
            db.add(_task(silent, "completed", 24 * 10))
            db.add(_task(healthy, "completed", 2))
            await db.commit()
        over = await hc.REGISTRY["scrapers.overdue"].fn(_ctx(S))
        by = {i["key"]: i for i in over.items}
        assert by["eden"]["status"] == "warn" and by["eden"]["late_count"] == 1
        assert by["idf"]["status"] == "ok"
        sil = await hc.REGISTRY["scrapers.silent"].fn(_ctx(S, deep=True))
        by = {i["key"]: i for i in sil.items}
        assert by["idf"]["stale_count"] == 1 and by["idf"]["status"] == "fail"
    asyncio.run(go())


# ── the diff between runs ────────────────────────────────────────────────

def test_compare_new_persisting_worsened_resolved():
    prev = {
        "a": {"status": "warn", "since": "2026-10-01"},
        "b": {"status": "fail", "since": "2026-09-01"},
        "c": {"status": "fail", "since": "2026-10-20"},
        "d": {"status": "warn", "since": "2026-10-25"},
    }
    results = [
        {"key": "a", "status": "fail"},            # worsened
        {"key": "b", "status": "fail"},            # persisting
        {"key": "c", "status": "ok"},              # resolved
        {"key": "d", "status": "warn",
         "items": [{"key": "x", "status": "fail"}]},  # new item, check persisting
    ]
    new, resolved = hc.compare(results, prev, "NOW")
    assert set(new) == {"a", "d/x"}
    assert resolved == ["c"]
    flat = hc._flat(results)
    assert flat["a"]["since"] == "2026-10-01"
    assert flat["b"]["since"] == "2026-09-01"
    assert flat["d/x"]["since"] == "NOW"


def test_run_is_logged_and_the_second_run_only_reports_what_changed(monkeypatch):
    async def go():
        S = await _db()
        bad = _ds("mevaker-scraper-a")
        async with S() as db:
            db.add(bad)
            await db.flush()
            db.add(_task(bad, "failed", 1))
            await db.commit()
        sent = []

        async def fake_alert(run):
            sent.append(run["summary"]["new_problems"])
        monkeypatch.setattr(hc, "_alert", fake_alert)

        first = await hc.run_checks("manual", DB_CHECKS, trigger="t@x", session_factory=S)
        assert first["status"] == "fail"
        assert "scrapers.runs/mevaker" in first["summary"]["new_problems"]
        second = await hc.run_checks("manual", DB_CHECKS, trigger="t@x", session_factory=S)
        assert second["summary"]["new_problems"] == []
        assert len(sent) == 1                       # alerted once, not twice

        async with S() as db:
            bad_now = await db.get(TrackedDataset, bad.id)
            db.add(_task(bad_now, "completed", 0))
            await db.commit()
        third = await hc.run_checks("manual", DB_CHECKS, trigger="t@x", session_factory=S)
        assert "scrapers.runs/mevaker" in third["summary"]["resolved"]

        async with S() as db:
            runs = (await db.execute(select(HealthRun).order_by(HealthRun.started_at))).scalars().all()
        assert [r.trigger for r in runs] == ["t@x"] * 3
        assert all(r.finished_at and r.duration_ms is not None for r in runs)
        mev = hc._flat(runs[1].results)["scrapers.runs/mevaker"]
        assert mev["since"] == hc._flat(runs[0].results)["scrapers.runs/mevaker"]["since"]
    asyncio.run(go())


def test_a_crashing_check_is_recorded_not_raised(monkeypatch):
    async def go():
        S = await _db()

        async def boom(ctx):
            raise RuntimeError("kaput")
        monkeypatch.setitem(hc.REGISTRY, "test.boom", hc.CheckDef(
            "test.boom", "x", "x", "x", frozenset({"daily"}), 5, boom))
        out = await hc.run_checks("manual", ["db.app", "test.boom"], session_factory=S)
        by = {r["key"]: r for r in out["results"]}
        assert by["test.boom"]["status"] == "error" and "kaput" in by["test.boom"]["message"]
        assert by["db.app"]["status"] == "ok"
        assert out["status"] == "fail"
    asyncio.run(go())


def test_one_run_at_a_time(monkeypatch):
    async def go():
        S = await _db()
        gate = asyncio.Event()

        async def slow(ctx):
            await gate.wait()
            return hc.CheckResult("ok", "done")
        monkeypatch.setitem(hc.REGISTRY, "test.slow", hc.CheckDef(
            "test.slow", "x", "x", "x", frozenset({"daily"}), 5, slow))
        first = asyncio.create_task(hc.run_checks("manual", ["test.slow"], session_factory=S))
        await asyncio.sleep(0.05)
        with pytest.raises(hc.HealthBusy):
            await hc.run_checks("manual", ["db.app"], session_factory=S)
        gate.set()
        assert (await first)["status"] == "ok"
    asyncio.run(go())


def test_a_run_cut_off_by_a_restart_is_closed_as_error():
    async def go():
        S = await _db()
        async with S() as db:
            db.add(HealthRun(tier="daily", trigger="scheduler", status="running",
                             started_at=NOW - timedelta(hours=5)))
            await db.commit()
        await hc.run_checks("manual", ["db.app"], session_factory=S)
        async with S() as db:
            st = sorted(r.status for r in (await db.execute(select(HealthRun))).scalars())
        assert st == ["error", "ok"]
    asyncio.run(go())


def test_alert_text_lists_only_new_problems():
    run = {"tier": "daily", "status": "fail",
           "summary": {"new_problems": ["scrapers.runs/mevaker"]},
           "results": [{"key": "scrapers.runs", "status": "fail", "message": "m",
                        "items": [{"key": "mevaker", "status": "fail", "message": "0/2 תקינים"}]}]}
    txt = hc.alert_text(run)
    assert "scrapers.runs/mevaker" in txt and "0/2" in txt


# ── the admin API ────────────────────────────────────────────────────────

def test_admin_api_overview_log_and_run_detail():
    from fastapi.testclient import TestClient

    from app.api import admin_health
    from app.auth.dependencies import get_admin_user
    from app.database import get_db
    from app.main import app

    async def setup():
        S = await _db()
        await hc.run_checks("manual", ["db.app", "queue.scrape"], trigger="a@b", session_factory=S)
        return S

    S = asyncio.run(setup())

    async def _get_db():
        async with S() as db:
            yield db

    app.dependency_overrides[get_db] = _get_db
    app.dependency_overrides[get_admin_user] = lambda: type("U", (), {"email": "a@b"})()
    try:
        c = TestClient(app)
        checks = c.get("/api/admin/health/checks").json()
        assert {x["key"] for x in checks["checks"]} == set(hc.REGISTRY)
        ov = c.get("/api/admin/health/overview").json()
        assert [x["key"] for x in ov["checks"]] == ["db.app", "queue.scrape"]
        assert ov["latest"]["manual"]["status"] == "ok"
        runs = c.get("/api/admin/health/runs?tier=manual").json()
        assert runs["total"] == 1 and "results" not in runs["runs"][0]
        one = c.get(f"/api/admin/health/runs/{runs['runs'][0]['id']}").json()
        assert len(one["results"]) == 2
        assert c.get(f"/api/admin/health/runs/{uuid.uuid4()}").status_code == 404
        assert c.post("/api/admin/health/run", json={"checks": ["nope"]}).status_code == 400
        assert c.post("/api/admin/health/run", json={"tier": "hourly"}).status_code == 400
    finally:
        app.dependency_overrides.clear()
    del admin_health


def test_public_summary_and_catch_up(monkeypatch):
    async def go():
        S = await _db()
        hc._summary, hc._summary_at = None, None
        assert await hc.public_summary(S) == {"daily": None, "weekly": None}

        ran = []

        async def fake_scheduled(tier):
            ran.append(tier)
            await hc.run_checks(tier if tier == "daily" else "manual", ["db.app"], session_factory=S)
        monkeypatch.setattr(hc, "scheduled", fake_scheduled)
        await hc.catch_up(S)
        assert ran == ["daily", "weekly"]           # both never ran

        ran.clear()
        await hc.catch_up(S)
        assert ran == ["weekly"]                    # daily is fresh now
        s = await hc.public_summary(S)
        assert s["daily"]["status"] == "ok" and s["weekly"] is None
        hc._summary, hc._summary_at = None, None
    asyncio.run(go())
