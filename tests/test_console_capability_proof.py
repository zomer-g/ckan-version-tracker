"""The boot-time proof that the public SQL console cannot read a credential table.

On xhostd the platform injects a read-only role that holds SELECT on everything in
`public`, including tables created after the grant. So the boundary protecting
credentials is which schema a table lives in, and the way it gets lost is a future
migration that creates a table without naming a schema. These tests pin the
tripwire: startup must refuse whenever the console role can read one.
"""
import asyncio

import pytest

import app.main as M
from app.services import append_store


class _FakePool:
    def __init__(self, rows):
        self._rows = rows

    async def fetch(self, _sql):
        return self._rows


def _with_rows(monkeypatch, rows):
    async def fake_pool():
        return _FakePool(rows)

    monkeypatch.setattr(append_store, "get_readonly_pool", fake_pool)


def test_passes_when_the_role_reads_only_public_data(monkeypatch):
    _with_rows(monkeypatch, [
        {"schema": "public", "name": "append_shape_ff3176b1"},
        {"schema": "knesset", "name": "kns_bill"},
        {"schema": "idx", "name": "govmap_286_0f8ac82b_796b6664"},
    ])
    asyncio.run(M._prove_console_role_cannot_read_secrets())


def test_refuses_when_anything_in_auth_is_readable(monkeypatch):
    _with_rows(monkeypatch, [
        {"schema": "public", "name": "append_shape_ff3176b1"},
        {"schema": "auth", "name": "something_new"},
    ])
    with pytest.raises(RuntimeError, match="auth.something_new"):
        asyncio.run(M._prove_console_role_cannot_read_secrets())


def test_refuses_a_credential_table_that_landed_in_public(monkeypatch):
    """The realistic failure: a migration that forgot the schema."""
    _with_rows(monkeypatch, [
        {"schema": "public", "name": "users"},
    ])
    with pytest.raises(RuntimeError, match="public.users"):
        asyncio.run(M._prove_console_role_cannot_read_secrets())


@pytest.mark.parametrize("table", sorted(M.SENSITIVE_TABLES))
def test_every_sensitive_name_trips_it_in_any_schema(monkeypatch, table):
    _with_rows(monkeypatch, [{"schema": "odata", "name": table}])
    with pytest.raises(RuntimeError):
        asyncio.run(M._prove_console_role_cannot_read_secrets())


def test_no_console_role_means_no_console_so_nothing_to_prove(monkeypatch):
    """get_readonly_pool now fails closed, so a missing role disables the
    consoles outright. The proof must not turn that into a boot failure."""
    async def no_role():
        raise RuntimeError("APPEND_READONLY_DATABASE_URL is not set")

    monkeypatch.setattr(append_store, "get_readonly_pool", no_role)
    asyncio.run(M._prove_console_role_cannot_read_secrets())


def test_one_database_no_longer_aborts_by_address(monkeypatch):
    """xhostd gives an app one database, so the addresses match by design. The
    match must be reported, not fatal — the capability proof is the guard."""
    one = "postgresql://over:pw@postgres.internal:5432/over"
    monkeypatch.setattr(M.settings, "database_url", one)
    monkeypatch.setattr(M.settings, "append_database_url", one)
    assert M._guard_db_separation() is True


def test_one_database_still_refuses_a_readable_credential_table(monkeypatch):
    """The realistic one-database mistake: APPEND_READONLY_DATABASE_URL set to the
    read/write URL, whose owner role can read everything."""
    _with_rows(monkeypatch, [
        {"schema": "public", "name": "append_shape_ff3176b1"},
        {"schema": "auth", "name": "users"},
        {"schema": "auth", "name": "api_users"},
    ])
    with pytest.raises(RuntimeError, match="auth.api_users"):
        asyncio.run(M._prove_console_role_cannot_read_secrets(shared_db=True))


def test_separate_databases_report_no_collision(monkeypatch):
    monkeypatch.setattr(M.settings, "database_url", "postgresql://a:pw@main-host:5432/app")
    monkeypatch.setattr(M.settings, "append_database_url", "postgresql://a:pw@append-host:5432/archive")
    assert M._guard_db_separation() is False
