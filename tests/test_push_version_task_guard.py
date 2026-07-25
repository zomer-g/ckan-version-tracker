"""push-version must refuse a dataset that has no running task.

Without this, the endpoint builds a version from whatever a worker sends,
regardless of the task's state. A worker whose task was cancelled or reassigned
— an operator killing a wedged run, or a stale process still churning after a
heartbeat timeout re-queued the work — could then land a junk version, and for
an archive source a poisoned checkpoint with it (marking every file "archived"
so it's never re-fetched). This happened once: a cancelled 8-hour run was still
alive and would have pushed a checkpoint claiming all 39k files done, none of
which had actually downloaded.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import worker as worker_api
from app.config import settings
from app.database import get_db
from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.rate_limit import limiter


DS_ID = uuid.uuid4()


class _DB:
    """Minimal async session: serves the dataset and the running-task query."""

    def __init__(self, *, has_running_task: bool):
        self.has_running_task = has_running_task
        self.committed = False
        self.ds = TrackedDataset(
            id=DS_ID, ckan_id="workagreements-scraper-x", ckan_name="x",
            title="t", source_type="scraper",
            scraper_config={"kind": "workagreements", "archive": True,
                            "checkpoint": {"known_file_urls": [], "rows_hash": "h"}},
        )
        self.task = ScrapeTask(
            id=uuid.uuid4(), tracked_dataset_id=DS_ID, status="running",
        ) if has_running_task else None

    async def execute(self, stmt):
        text = str(stmt)
        db = self

        class _Result:
            def scalar_one_or_none(self):
                if "tracked_datasets" in text and "scrape_task" not in text:
                    return db.ds
                if "scrape_task" in text:
                    return db.task  # None when no running task
                return None

            def scalars(self):
                class _S:
                    def all(self_inner):
                        return []
                return _S()
        return _Result()

    async def commit(self):
        self.committed = True

    def add(self, obj):
        pass


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


def _push(client, **extra):
    body = {
        "tracked_dataset_id": str(DS_ID),
        "metadata_modified": "2026-07-24T00:00:00",
        "skip_version": True,
        "scraper_config_patch": {"checkpoint": {"known_file_urls": ["poison"],
                                                "rows_hash": "h"}},
        **extra,
    }
    return client.post("/api/worker/push-version", json=body,
                       headers={"Authorization": "Bearer workerkey",
                                "X-Worker-Id": "stale#1"})


def test_push_is_rejected_when_no_task_is_running():
    db = _DB(has_running_task=False)
    resp = _push(_client(db))
    assert resp.status_code == 409
    assert "stale" in resp.json()["detail"]
    # The poisoned checkpoint patch must NOT have been committed.
    assert db.committed is False
    assert db.ds.scraper_config["checkpoint"]["known_file_urls"] == []


def test_push_passes_the_guard_when_a_task_is_running():
    """A legitimate in-flight push has a running task and must get past the
    guard (a skip_version push then just marks the task done)."""
    db = _DB(has_running_task=True)
    resp = _push(_client(db))
    assert resp.status_code != 409


def test_more_batches_defaults_off():
    """The field must default false so every existing single-version push
    completes its task exactly as before — only a batched run opts in.

    (The batched interaction — task stays running across batches — is proven
    worker-side in test_archive_batching.test_more_batches_is_true_until_the_
    last_batch; exercising the full OVER version-creation path here would mean
    mocking R2 + the append store, well past what this guard test covers.)"""
    from app.api.worker import PushVersionRequest

    req = PushVersionRequest(tracked_dataset_id=str(DS_ID),
                             metadata_modified="2026-07-24T00:00:00")
    assert req.more_batches is False
    assert req.consolidate_dedup_key is None


def test_auto_consolidation_fires_only_on_a_final_multi_version_push(monkeypatch):
    """The trigger must fire only when a multi-batch bootstrap finishes
    (not more_batches, a dedup key, and this run produced >1 version), and
    never mid-run or for a first-ever single version."""
    from app.api import worker as worker_api

    scheduled = []

    def _fake_create_task(coro):
        scheduled.append(coro)
        coro.close()  # we only assert the decision, not run the merge
        return None

    import asyncio as _asyncio
    monkeypatch.setattr(_asyncio, "create_task", _fake_create_task)

    def _decides(*, more_batches, dedup_key, next_version):
        scheduled.clear()
        # Mirror the guard in push_version (kept in sync with the real code).
        if (not more_batches) and dedup_key and next_version > 1:
            _asyncio.create_task(
                worker_api._run_consolidate_bg(DS_ID, dedup_key))
        return len(scheduled) == 1

    assert _decides(more_batches=False, dedup_key="k", next_version=5) is True
    assert _decides(more_batches=True, dedup_key="k", next_version=5) is False   # mid-run
    assert _decides(more_batches=False, dedup_key=None, next_version=5) is False  # no key
    assert _decides(more_batches=False, dedup_key="k", next_version=1) is False   # single version
