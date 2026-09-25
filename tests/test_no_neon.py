"""Neon is gone: no DB setting may address it, and boot refuses if one does."""
import pytest

from app.config import Settings


_DB_ENV = ("DATABASE_URL", "APPEND_DATABASE_URL", "APPEND_READONLY_DATABASE_URL",
           "OCAL_DATABASE_URL", "OCOI_DATABASE_URL")


@pytest.fixture(autouse=True)
def _no_env(monkeypatch):
    # Only what each test passes — never the developer's .env or shell.
    for k in _DB_ENV:
        monkeypatch.delenv(k, raising=False)


def _settings(**kw):
    return Settings(_env_file=None, **kw)


def test_local_urls_pass():
    s = _settings(
        database_url="postgresql+asyncpg://u:p@db.xhostd.internal:5432/over",
        append_database_url="postgresql://u:p@db.xhostd.internal/over",
    )
    assert s.neon_database_urls() == []


@pytest.mark.parametrize("field", [
    "database_url", "append_database_url", "append_readonly_database_url",
    "ocal_database_url", "ocoi_database_url",
])
def test_every_db_setting_is_checked(field):
    url = "postgresql://u:p@ep-cool-name-123456-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
    s = _settings(**{field: url})
    assert s.neon_database_urls() == [field]


def test_lookalike_host_is_not_flagged():
    s = _settings(append_database_url="postgresql://u:p@notneon.tech.example.com/db")
    assert s.neon_database_urls() == []


def test_boot_refuses_neon(monkeypatch):
    import app.main as m
    monkeypatch.setattr(m.settings, "ocal_database_url",
                        "postgresql://u:p@ep-x.eu-central-1.aws.neon.tech/neondb")
    with pytest.raises(RuntimeError, match="OCAL_DATABASE_URL"):
        m._refuse_neon()
