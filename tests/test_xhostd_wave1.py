"""What OVER needs to run on xhostd without changing how it runs on Render.

TLS chosen by host, R2 names that win over the platform's reserved S3_* names,
the bucket guard, the two switches, maintenance mode and /healthz, and the env
mapping launch.sh sources.
"""
import os
import pathlib
import shutil
import subprocess

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi.testclient import TestClient

import app.main as M
from app.config import Settings
from app.pg_ssl import asyncpg_ssl_for

ROOT = pathlib.Path(__file__).resolve().parents[1]


# ── TLS by host ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("dsn,tls", [
    ("postgresql://u:p@ep-x-123.us-east-1.aws.neon.tech/neondb?sslmode=require", True),
    ("postgresql://u:p@ep-x-123-pooler.us-east-1.aws.neon.tech/neondb", True),
    ("postgresql://u:p@db.xhostd.com:5432/over", True),
    ("postgresql://u:p@some-new-provider.example.com/db", True),
    ("postgresql://u:p@10.200.2.2:5432/over", False),
    ("postgresql://u:p@172.18.0.5/over", False),
    ("postgresql://u:p@127.0.0.1/over", False),
    ("postgresql://u:p@localhost/over", False),
    ("postgresql://u:p@postgres:5432/over", False),
    ("postgresql://u:p@pg.channel.internal/over", False),
])
def test_tls_follows_the_host(monkeypatch, dsn, tls):
    monkeypatch.delenv("DATABASE_HOST", raising=False)
    assert (asyncpg_ssl_for(dsn) is not False) is tls


def test_the_database_the_platform_injected_never_gets_tls(monkeypatch):
    monkeypatch.setenv("DATABASE_HOST", "ch-8f2.db.platform.example.net")
    assert asyncpg_ssl_for("postgresql://u:p@ch-8f2.db.platform.example.net:5432/over") is False
    assert asyncpg_ssl_for("postgresql://u:p@ep-x.aws.neon.tech/neondb") is not False


# ── R2 names win over the platform's S3_* ────────────────────────────────────

def test_r2_names_override_the_injected_platform_bucket():
    s = Settings(_env_file=None,
                 s3_endpoint="https://blob.platform.example", s3_bucket="platform-bucket",
                 s3_region="platform-1",
                 r2_endpoint="https://acct.r2.cloudflarestorage.com", r2_bucket="over-archive",
                 r2_region="auto", r2_access_key="ak", r2_secret_key="sk",
                 r2_public_base_url="https://files.over.org.il")
    assert (s.s3_endpoint, s.s3_bucket, s.s3_region) == (
        "https://acct.r2.cloudflarestorage.com", "over-archive", "auto")
    assert (s.s3_access_key, s.s3_secret_key, s.s3_public_base_url) == (
        "ak", "sk", "https://files.over.org.il")


def test_without_r2_names_s3_is_left_alone():
    s = Settings(_env_file=None, s3_endpoint="https://acct.r2.cloudflarestorage.com", s3_bucket="b")
    assert (s.s3_endpoint, s.s3_bucket) == ("https://acct.r2.cloudflarestorage.com", "b")


def test_boot_refuses_the_platform_bucket_on_xhostd(monkeypatch):
    monkeypatch.setenv("XHOST_SHA", "abc123")
    monkeypatch.setattr(M.settings, "r2_endpoint", "")
    with pytest.raises(RuntimeError, match="R2_ENDPOINT"):
        M._refuse_platform_bucket_as_archive()
    monkeypatch.setattr(M.settings, "r2_endpoint", "https://acct.r2.cloudflarestorage.com")
    M._refuse_platform_bucket_as_archive()


def test_boot_does_not_ask_for_r2_names_elsewhere(monkeypatch):
    monkeypatch.delenv("XHOST_SHA", raising=False)
    monkeypatch.delenv("XHOST_HTTP_PORT", raising=False)
    monkeypatch.setattr(M.settings, "r2_endpoint", "")
    M._refuse_platform_bucket_as_archive()


# ── the switches ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("scheduler,maintenance,writers", [
    (True, False, True), (False, False, False), (True, True, False), (False, True, False),
])
def test_writers_run_only_when_allowed(monkeypatch, scheduler, maintenance, writers):
    monkeypatch.setattr(M.settings, "scheduler_enabled", scheduler)
    monkeypatch.setattr(M.settings, "maintenance_mode", maintenance)
    assert M.settings.writers_enabled is writers


@pytest.fixture()
def client():
    return TestClient(M.app, raise_server_exceptions=False)


@pytest.mark.parametrize("method,path", [
    ("post", "/api/worker/progress/abc"),
    ("get", "/api/worker/tasks/next"),
    ("post", "/api/datasets/00000000-0000-0000-0000-000000000000/poll"),
    ("post", "/api/admin/datasets"),
    ("post", "/api/datasets"),
    ("delete", "/api/datasets/00000000-0000-0000-0000-000000000000"),
])
def test_maintenance_refuses_writes_and_the_fleet(client, monkeypatch, method, path):
    monkeypatch.setattr(M.settings, "maintenance_mode", True)
    r = getattr(client, method)(path)
    assert r.status_code == 503, (path, r.status_code)
    assert r.headers.get("retry-after") == "300"


@pytest.mark.parametrize("method,path,kwargs", [
    ("get", "/api/datasets?limit=1", {}),
    ("post", "/api/tables/sql", {"json": {"sql": "SELECT 1"}}),
    ("post", "/api/append/00000000-0000-0000-0000-000000000000/sql", {"json": {"sql": "SELECT 1"}}),
    ("get", "/healthz", {}),
])
def test_maintenance_keeps_reads_working(client, monkeypatch, method, path, kwargs):
    monkeypatch.setattr(M.settings, "maintenance_mode", True)
    r = getattr(client, method)(path, **kwargs)
    assert r.status_code != 503, (path, r.text[:200])


def test_writes_pass_when_not_in_maintenance(client, monkeypatch):
    monkeypatch.setattr(M.settings, "maintenance_mode", False)
    assert client.post("/api/worker/progress/abc").status_code != 503


def test_healthz_reports_the_switches(client, monkeypatch):
    monkeypatch.setattr(M.settings, "maintenance_mode", True)
    monkeypatch.setattr(M.settings, "scheduler_enabled", False)
    body = client.get("/healthz").json()
    assert body["ok"] is True
    assert body["maintenance_mode"] is True
    assert body["scheduler_enabled"] is False
    assert body["scheduler_running"] is False


# ── the env mapping launch.sh sources ────────────────────────────────────────

SH = shutil.which("sh")


def _source_env(env: dict) -> tuple[int, dict, str]:
    base = {k: v for k, v in os.environ.items() if k in ("PATH", "SYSTEMROOT", "TEMP", "TMP")}
    proc = subprocess.run(
        [SH, "-c", ". ./xhostd-env.sh && env"],
        cwd=ROOT, env={**base, **env}, capture_output=True, text=True, timeout=30)
    out = dict(line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line)
    return proc.returncode, out, proc.stderr


@pytest.mark.skipif(SH is None, reason="no POSIX sh")
def test_one_database_is_the_default_on_xhostd():
    rc, env, err = _source_env({
        "DATABASE_URL": "postgres://app:pw@10.200.2.2:5432/over",
        "DATABASE_URL_READONLY": "postgres://r_ro:pw@10.200.2.2:5432/over",
    })
    assert rc == 0, err
    assert env["DATABASE_URL"] == "postgresql+asyncpg://app:pw@10.200.2.2:5432/over"
    assert env["APPEND_DATABASE_URL"] == env["DATABASE_URL"]
    assert env["APPEND_READONLY_DATABASE_URL"] == "postgres://r_ro:pw@10.200.2.2:5432/over"
    assert env["OCAL_DATABASE_URL"] == env["OCOI_DATABASE_URL"] == env["DATABASE_URL"]
    assert env["MALLOC_ARENA_MAX"] == "2"


@pytest.mark.skipif(SH is None, reason="no POSIX sh")
def test_pointing_the_app_elsewhere_requires_the_archive_to_be_named_too():
    rc, _env, err = _source_env({
        "DATABASE_URL": "postgres://app:pw@10.200.2.2:5432/over",
        "DATABASE_URL_READONLY": "postgres://r_ro:pw@10.200.2.2:5432/over",
        "OVER_DATABASE_URL": "postgresql://u:p@ep-x.aws.neon.tech/neondb?sslmode=require",
    })
    assert rc != 0
    assert "APPEND_DATABASE_URL" in err


@pytest.mark.skipif(SH is None, reason="no POSIX sh")
def test_wave_two_points_everything_at_neon_explicitly():
    rc, env, err = _source_env({
        "DATABASE_URL": "postgres://app:pw@10.200.2.2:5432/over",
        "DATABASE_URL_READONLY": "postgres://r_ro:pw@10.200.2.2:5432/over",
        "OVER_DATABASE_URL": "postgresql://u:p@ep-app.aws.neon.tech/neondb?sslmode=require",
        "APPEND_DATABASE_URL": "postgresql://u:p@ep-archive.aws.neon.tech/neondb?sslmode=require",
        "APPEND_READONLY_DATABASE_URL": "postgresql://over_readonly:p@ep-archive.aws.neon.tech/neondb",
        "OCAL_DATABASE_URL": "postgresql://ocal_app:p@ep-archive-pooler.aws.neon.tech/neondb",
    })
    assert rc == 0, err
    assert env["DATABASE_URL"] == "postgresql+asyncpg://u:p@ep-app.aws.neon.tech/neondb?sslmode=require"
    assert env["APPEND_DATABASE_URL"].startswith("postgresql://u:p@ep-archive")
    assert env["APPEND_READONLY_DATABASE_URL"].startswith("postgresql://over_readonly:")
    assert env["OCAL_DATABASE_URL"].startswith("postgresql://ocal_app:")
    assert env["OCOI_DATABASE_URL"] == env["APPEND_DATABASE_URL"]
