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
