"""A progress report on a task that is no longer running tells the worker to stop."""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from types import SimpleNamespace

from app.api.worker import progress_verdict
from app.models.scrape_task import PHASE_INTERRUPTED


def _task(status, phase="scraping", worker_id="NucBox#1"):
    return SimpleNamespace(status=status, phase=phase, worker_id=worker_id)


def test_a_running_task_carries_on():
    assert progress_verdict(_task("running"), "NucBox#1", superseded=False) == "ok"


def test_a_task_cancelled_by_an_admin_is_stopped():
    task = _task("failed", phase="cancelled")
    assert progress_verdict(task, "NucBox#1", superseded=False) == "stop"


def test_completed_and_ordinary_failures_are_stopped():
    assert progress_verdict(_task("completed"), "NucBox#1", superseded=False) == "stop"
    assert progress_verdict(_task("failed"), "NucBox#1", superseded=False) == "stop"


def test_the_worker_that_held_an_interrupted_task_takes_it_back():
    task = _task("failed", phase=PHASE_INTERRUPTED)
    assert progress_verdict(task, "NucBox#1", superseded=False) == "reclaim"


def test_an_interrupted_task_is_not_reclaimed_by_another_worker_or_over_a_newer_one():
    task = _task("failed", phase=PHASE_INTERRUPTED)
    assert progress_verdict(task, "GZ-14#2", superseded=False) == "stop"
    assert progress_verdict(task, "", superseded=False) == "stop"
    assert progress_verdict(task, "NucBox#1", superseded=True) == "stop"


def test_a_task_that_no_longer_exists_is_stopped(monkeypatch):
    import asyncio
    import uuid
    from unittest import mock

    from app.api import worker

    monkeypatch.setattr(worker, "_verify_worker_key", lambda request: None)
    db = mock.AsyncMock()
    db.execute.return_value = mock.Mock(scalar_one_or_none=mock.Mock(return_value=None))
    # __wrapped__: past the rate limiter, which wants a real Request.
    answer = asyncio.run(worker.update_progress.__wrapped__(
        mock.Mock(), str(uuid.uuid4()), worker.ProgressUpdate(phase="scraping"), db,
    ))
    assert answer == {"status": "stop", "task_status": "deleted"}
