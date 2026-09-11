"""VERSION_FREEZE: while the archive is copied to xhostd, nothing creates or changes a version.

Pins what is refused (the worker fleet, dataset writes, every admin write,
including ones added later) and what must keep working for days (reading,
the consoles, sign-in).
"""
import os
import re

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi.testclient import TestClient

import app.main as M


@pytest.fixture()
def frozen(monkeypatch):
    monkeypatch.setattr(M.settings, "version_freeze", True)
    return TestClient(M.app, raise_server_exceptions=False)


def _concrete(path_regex) -> str:
    """A request path that matches a route's compiled pattern."""
    pattern = path_regex.pattern.lstrip("^").rstrip("$")
    return re.sub(r"\(\?P<[^>]+>[^)]*\)", "00000000-0000-0000-0000-000000000000", pattern).replace("\\", "")


@pytest.mark.parametrize("method,path", [
    ("get", "/api/worker/tasks/next"),
    ("post", "/api/worker/push-version"),
    ("post", "/api/worker/upload-csv/00000000-0000-0000-0000-000000000000"),
    ("post", "/api/datasets"),
    ("patch", "/api/datasets/00000000-0000-0000-0000-000000000000"),
    ("delete", "/api/datasets/00000000-0000-0000-0000-000000000000"),
    ("post", "/api/datasets/00000000-0000-0000-0000-000000000000/poll"),
])
def test_version_writes_are_refused(frozen, method, path):
    r = getattr(frozen, method)(path)
    assert r.status_code == 503, (path, r.status_code)
    assert r.headers.get("retry-after") == "3600"
    assert "xhostd" in r.json()["detail"]


def test_every_admin_write_route_is_refused(frozen):
    routes = M._admin_write_patterns()
    assert len(routes) > 50, "the admin write set is read from the routes; it cannot be this small"
    for rx, methods in routes:
        path = _concrete(rx)
        method = sorted(methods)[0].lower()
        r = frozen.request(method.upper(), path)
        assert r.status_code == 503, (method, path, r.status_code)


@pytest.mark.parametrize("method,path,kwargs", [
    ("get", "/api/datasets?limit=1", {}),
    ("get", "/healthz", {}),
    ("post", "/api/tables/sql", {"json": {"sql": "SELECT 1"}}),
    ("post", "/api/append/00000000-0000-0000-0000-000000000000/sql", {"json": {"sql": "SELECT 1"}}),
    ("post", "/api/auth/sso/exchange", {"json": {"code": "x"}}),
    ("get", "/api/auth/sso/google", {}),
    ("post", "/api/nl/query", {"json": {"q": "x", "run": False}}),
])
def test_reading_the_consoles_and_sign_in_keep_working(frozen, method, path, kwargs):
    r = getattr(frozen, method)(path, **kwargs)
    assert r.status_code != 503, (path, r.status_code, r.text[:200])


def test_nothing_is_refused_when_not_frozen(monkeypatch):
    monkeypatch.setattr(M.settings, "version_freeze", False)
    monkeypatch.setattr(M.settings, "maintenance_mode", False)
    c = TestClient(M.app, raise_server_exceptions=False)
    assert c.post("/api/worker/push-version").status_code != 503
    assert c.post("/api/datasets/00000000-0000-0000-0000-000000000000/poll").status_code != 503


def test_the_freeze_stops_the_scheduler_and_boot_writers(monkeypatch):
    monkeypatch.setattr(M.settings, "scheduler_enabled", True)
    monkeypatch.setattr(M.settings, "maintenance_mode", False)
    monkeypatch.setattr(M.settings, "version_freeze", True)
    assert M.settings.writers_enabled is False


def test_healthz_reports_the_freeze(frozen):
    assert frozen.get("/healthz").json()["version_freeze"] is True
