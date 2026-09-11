"""The worker contract for results that outlive a pause (GOVSCRAPER's outbox).

A worker on the `worker-503-resilience` branch holds push-version and /fail
through a 503 and sends them again later, naming the task. These pin what OVER
does with such a late result:

- push-version with task_id checks THAT task: running goes ahead; interrupted
  by the sweep and last held by the same worker is reclaimed; completed answers
  already_committed; another active task on the dataset means superseded and is
  left untouched. Without task_id nothing changes.
- a neon-csv reference whose staged file is gone is refused before any write.
- /fail never turns a completed task into a failed one.
- the 503 says which pause it is, in X-Over-Mode.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

import app.main as M
from app.api import worker as worker_api
from app.config import settings
from app.database import get_db
from app.models.scrape_task import PHASE_INTERRUPTED, ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.rate_limit import limiter

DS_ID = uuid.uuid4()
WORKER = "GZ-14#1"


class _DB:
    """Serves one dataset, the named task, an optional other active task and
    an optional latest version, by the shape of each query."""

    def __init__(self, task, *, other_active=None, latest_version=None):
        self.task = task
        self.other_active = other_active
        self.latest_version = latest_version
        self.commits = 0
        self.added = []
        self.ds = TrackedDataset(
            id=DS_ID, ckan_id="x", ckan_name="x", title="t", source_type="scraper",
            scraper_config={"kind": "workagreements", "archive": True},
        )

    async def execute(self, stmt):
        text = str(stmt)
        db = self

        class _Result:
            def scalar_one_or_none(self):
                if "version_index" in text:
                    return db.latest_version
                if "scrape_tasks" not in text:
                    return db.ds
                if "scrape_tasks.id !=" in text:
                    return db.other_active
                if "scrape_tasks.id =" in text:
                    return db.task
                # the "a task is running for this dataset" lookups
                if db.task is not None and db.task.status == "running":
                    return db.task
                return db.other_active if (db.other_active and db.other_active.status == "running") else None

            def scalars(self):
                class _S:
                    def all(self_inner):
                        return []
                return _S()
        return _Result()

    async def commit(self):
        self.commits += 1

    def add(self, obj):
        self.added.append(obj)


def _task(status, *, phase=None, worker_id=WORKER, ds_id=DS_ID):
    return ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds_id, status=status,
                      phase=phase, worker_id=worker_id,
                      error="no heartbeat" if phase == PHASE_INTERRUPTED else None)


def _client(db):
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)

    async def _db():
        yield db

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def worker_key(monkeypatch):
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")


def _push(db, task_id=None, *, worker=WORKER, header=False, **extra):
    body = {"tracked_dataset_id": str(DS_ID), "metadata_modified": "2026-09-11T00:00:00",
            "skip_version": True, **extra}
    headers = {"Authorization": "Bearer workerkey", "X-Worker-Id": worker}
    if task_id and header:
        headers["X-Worker-Task-Id"] = str(task_id)
    elif task_id:
        body["task_id"] = str(task_id)
    return _client(db).post("/api/worker/push-version", json=body, headers=headers)


# ── push-version with task_id ─────────────────────────────────────────────

def test_a_running_task_goes_ahead_as_before():
    task = _task("running")
    r = _push(_DB(task), task.id)
    assert r.status_code == 200, r.text
    assert task.status == "completed"


def test_the_task_id_header_works_like_the_body_field():
    task = _task("running")
    r = _push(_DB(task), task.id, header=True)
    assert r.status_code == 200, r.text
    assert task.status == "completed"


def test_a_task_interrupted_by_the_sweep_is_reclaimed_by_the_same_worker():
    task = _task("failed", phase=PHASE_INTERRUPTED)
    r = _push(_DB(task), task.id)
    assert r.status_code == 200, r.text
    assert task.status == "completed"
    assert task.error is None


def test_another_worker_cannot_reclaim_it():
    task = _task("failed", phase=PHASE_INTERRUPTED)
    r = _push(_DB(task), task.id, worker="NucBox_G10#1")
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "not_reclaimable"
    assert task.status == "failed"


def test_a_task_the_worker_itself_failed_is_not_reclaimed():
    task = _task("failed", phase="download")
    r = _push(_DB(task), task.id)
    assert r.status_code == 409
    assert task.status == "failed"


@pytest.mark.parametrize("other_status", ["running", "pending"])
def test_a_newer_active_task_supersedes_and_is_left_alone(other_status):
    task = _task("failed", phase=PHASE_INTERRUPTED)
    other = _task(other_status, worker_id="NucBox_G10#1")
    db = _DB(task, other_active=other)
    r = _push(db, task.id)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "superseded"
    assert other.status == other_status
    assert task.status == "failed"
    assert db.commits == 0


def test_a_completed_task_answers_already_committed():
    task = _task("completed")
    latest = VersionIndex(tracked_dataset_id=DS_ID, version_number=7, metadata_modified="m")
    db = _DB(task, latest_version=latest)
    r = _push(db, task.id)
    assert r.status_code == 200
    assert r.json() == {"already_committed": True, "version_number": 7}
    assert db.commits == 0


def test_a_task_of_another_dataset_is_refused():
    task = _task("running", ds_id=uuid.uuid4())
    r = _push(_DB(task), task.id)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "unknown_task"


def test_without_task_id_the_guard_is_unchanged():
    assert _push(_DB(_task("running"))).status_code == 200
    r = _push(_DB(_task("failed", phase=PHASE_INTERRUPTED)))
    assert r.status_code == 409
    assert "stale" in r.json()["detail"]


# ── neon-csv staging that is gone ─────────────────────────────────────────

def test_a_neon_csv_reference_to_a_missing_file_is_refused_before_any_write(tmp_path):
    task = _task("running")
    db = _DB(task)
    gone = worker_api._neon_csv_ref(str(tmp_path / "gone.csv"))
    r = _push(db, task.id, skip_version=False, csv_resource_ids={"נתוני הסורק": gone})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "staged_csv_missing"
    assert r.json()["detail"]["resources"] == ["נתוני הסורק"]
    assert db.added == [] and db.commits == 0
    assert task.status == "running"


def test_a_neon_csv_reference_to_a_present_file_passes_the_check(tmp_path):
    staged = tmp_path / "here.csv"
    staged.write_text("a\n1\n", encoding="utf-8")
    body = worker_api.PushVersionRequest(
        tracked_dataset_id=str(DS_ID), metadata_modified="m",
        csv_resource_ids={"r": worker_api._neon_csv_ref(str(staged)), "other": "r2-resource-id"})
    assert worker_api._missing_neon_csv(body) == []


# ── /fail ─────────────────────────────────────────────────────────────────

def test_fail_does_not_overwrite_a_completed_task():
    task = _task("completed")
    task.phase = "complete"
    db = _DB(task)
    r = _client(db).post(f"/api/worker/fail/{task.id}", json={"error": "late", "phase": "push"},
                         headers={"Authorization": "Bearer workerkey"})
    assert r.status_code == 200
    assert r.json() == {"status": "completed", "ignored": True}
    assert task.status == "completed" and task.phase == "complete"
    assert db.commits == 0


def test_fail_still_fails_a_running_task():
    task = _task("running")
    r = _client(_DB(task)).post(f"/api/worker/fail/{task.id}", json={"error": "boom", "phase": "download"},
                                headers={"Authorization": "Bearer workerkey"})
    assert r.status_code == 200
    assert task.status == "failed"


# ── which pause is this ───────────────────────────────────────────────────

def test_the_freeze_503_says_freeze(monkeypatch):
    monkeypatch.setattr(M.settings, "version_freeze", True)
    monkeypatch.setattr(M.settings, "maintenance_mode", False)
    r = TestClient(M.app, raise_server_exceptions=False).get("/api/worker/poll")
    assert r.status_code == 503
    assert r.headers["X-Over-Mode"] == "freeze"
    assert r.headers["Retry-After"] == "3600"


def test_the_maintenance_503_says_maintenance(monkeypatch):
    monkeypatch.setattr(M.settings, "version_freeze", False)
    monkeypatch.setattr(M.settings, "maintenance_mode", True)
    r = TestClient(M.app, raise_server_exceptions=False).get("/api/worker/poll")
    assert r.status_code == 503
    assert r.headers["X-Over-Mode"] == "maintenance"
    assert r.headers["Retry-After"] == "300"


def test_both_on_reads_as_the_longer_pause(monkeypatch):
    monkeypatch.setattr(M.settings, "version_freeze", True)
    monkeypatch.setattr(M.settings, "maintenance_mode", True)
    r = TestClient(M.app, raise_server_exceptions=False).get("/api/worker/poll")
    assert r.headers["X-Over-Mode"] == "freeze"
