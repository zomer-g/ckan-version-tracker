"""The application's own tables stay out of `public`.

On xhostd the public SQL console's role reads everything in `public`, including
tables created later. Alembic 066 moved every table the application owns into
`app`, and the app engine resolves names there through search_path. These tests
pin the three places that can drift apart (the migration's list, the runtime
list, the models) and the two boot checks that stop a shared database from
starting when they have.
"""
import asyncio
import importlib.util
import os
import pathlib

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

import app.database as D
import app.main as M
from app.services import append_store

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _migration_066():
    spec = importlib.util.spec_from_file_location(
        "migration_066", ROOT / "alembic" / "versions" / "066_app_schema.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── the lists agree ──────────────────────────────────────────────────────────

def test_migration_moves_exactly_the_runtime_app_tables():
    assert set(_migration_066().TABLES) == set(D.APP_TABLES)


def test_every_model_without_a_schema_is_a_known_app_table():
    """A new model is invisible to the boot check until it is in APP_TABLES."""
    missing = sorted(t.name for t in D.Base.metadata.tables.values()
                     if t.schema is None and t.name not in D.APP_TABLES)
    assert not missing, f"add to app/database.py:APP_TABLES: {missing}"


def test_no_model_names_a_schema_other_than_auth():
    odd = sorted(f"{t.schema}.{t.name}" for t in D.Base.metadata.tables.values()
                 if t.schema not in (None, "auth"))
    assert not odd


# ── only the app engine gets the search_path ────────────────────────────────

@pytest.mark.parametrize("url,pinned,ssl", [
    ("postgresql+asyncpg://u:p@ep-x-123.eu-central-1.aws.neon.tech/neondb", True, True),
    # PgBouncer transaction mode would hand a session SET to other clients.
    ("postgresql+asyncpg://u:p@ep-x-123-pooler.eu-central-1.aws.neon.tech/neondb", False, True),
    ("postgresql+asyncpg://u:p@10.200.2.2:5432/over", True, False),
    ("sqlite+aiosqlite:///:memory:", False, False),
])
def test_app_search_path_is_pinned_only_on_direct_postgres(url, pinned, ssl):
    assert D.app_search_path_applies(url) is pinned
    args = D.app_db_connect_args(url)
    assert "server_settings" not in args  # a startup parameter did not survive Neon's endpoint
    assert ("ssl" in args) is ssl


# ── the console role cannot read an app table ───────────────────────────────

class _FakePool:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, _sql):
        return self._rows


def _console_can_read(monkeypatch, rows):
    async def fake_pool():
        return _FakePool(rows)
    monkeypatch.setattr(append_store, "get_readonly_pool", fake_pool)


def test_console_proof_refuses_anything_in_app(monkeypatch):
    _console_can_read(monkeypatch, [{"schema": "app", "name": "nl_query_log"}])
    with pytest.raises(RuntimeError, match="app.nl_query_log"):
        asyncio.run(M._prove_console_role_cannot_read_secrets(shared_db=True))


def test_console_proof_refuses_an_app_table_left_in_public(monkeypatch):
    _console_can_read(monkeypatch, [{"schema": "public", "name": "decision_analysis"}])
    with pytest.raises(RuntimeError, match="public.decision_analysis"):
        asyncio.run(M._prove_console_role_cannot_read_secrets(shared_db=True))


def test_console_proof_allows_a_data_schema_that_reuses_a_name(monkeypatch):
    """ocal.organizations is published data; it only shares a name."""
    _console_can_read(monkeypatch, [
        {"schema": "ocal", "name": "organizations"},
        {"schema": "public", "name": "append_shape_ff3176b1"},
    ])
    asyncio.run(M._prove_console_role_cannot_read_secrets(shared_db=True))


# ── the app engine really resolves to `app` ─────────────────────────────────

class _Result:
    def __init__(self, value):
        self._value = value

    def scalar(self):
        return self._value


class _Conn:
    def __init__(self, values):
        self._values = list(values)

    async def execute(self, *_args, **_kwargs):
        return _Result(self._values.pop(0))


class _Connect:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_exc):
        return False


class _Engine:
    def __init__(self, dialect, values):
        self.dialect = type("Dialect", (), {"name": dialect})()
        self._values = values
        self.connected = False

    def connect(self):
        self.connected = True
        return _Connect(_Conn(self._values))


def _engine(monkeypatch, dialect, values):
    fake = _Engine(dialect, values)
    monkeypatch.setattr(D, "engine", fake)
    return fake


def test_private_check_passes_when_names_resolve_to_app(monkeypatch):
    _engine(monkeypatch, "postgresql", ["app", []])
    asyncio.run(M._prove_app_tables_are_private(shared_db=True))


def test_private_check_stops_a_shared_db_whose_search_path_was_not_applied(monkeypatch):
    _engine(monkeypatch, "postgresql", ["public", []])
    with pytest.raises(RuntimeError, match="not 'app'"):
        asyncio.run(M._prove_app_tables_are_private(shared_db=True))


def test_private_check_stops_a_shared_db_with_app_tables_in_public(monkeypatch):
    _engine(monkeypatch, "postgresql", ["app", ["nl_query_log", "workers"]])
    with pytest.raises(RuntimeError, match="nl_query_log, workers"):
        asyncio.run(M._prove_app_tables_are_private(shared_db=True))


def test_private_check_only_logs_when_databases_are_separate(monkeypatch):
    _engine(monkeypatch, "postgresql", ["public", ["workers"]])
    asyncio.run(M._prove_app_tables_are_private(shared_db=False))


def test_private_check_skips_sqlite(monkeypatch):
    fake = _engine(monkeypatch, "sqlite", [])
    asyncio.run(M._prove_app_tables_are_private(shared_db=True))
    assert fake.connected is False
