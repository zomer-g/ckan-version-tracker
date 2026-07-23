"""Tests for the least-privilege read-only SQL-console role.

Two layers:

  * Pure unit tests (always run) — the DSN normalization for the read-only URL
    and the fallback-to-read/write-pool behavior with its one-time warning.

  * Integration tests (run ONLY when APPEND_READONLY_DATABASE_URL is set) — these
    connect as the read-only role and PROVE the DB itself refuses writes and
    refuses reads outside the granted schemas. This is the "בוצע כאשר" check:
    an INSERT / CREATE through the role fails at the Postgres permission layer,
    not merely at the application layer. Provision the role first with
    scripts/create_append_readonly_role.sql. When the containment check also has
    the owner URL (APPEND_DATABASE_URL) available it creates a throwaway secret
    schema to prove the role cannot read outside its grants.
"""
import asyncio
import os
import ssl
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import asyncpg  # noqa: E402
import pytest  # noqa: E402

from app.config import settings  # noqa: E402
from app.services import append_store as A  # noqa: E402


# ── Pure unit tests (no DB) ──────────────────────────────────────────────────

def test_readonly_dsn_strips_libpq_params_and_dialect_suffix():
    dsn = A._dsn_from(
        "postgresql+asyncpg://ro:pw@ep-x.us-east-1.aws.neon.tech/neondb"
        "?sslmode=require&channel_binding=require"
    )
    assert dsn.startswith("postgresql://")       # dialect suffix dropped
    assert "sslmode" not in dsn                    # libpq-only param dropped
    assert "channel_binding" not in dsn
    assert "ep-x.us-east-1.aws.neon.tech/neondb" in dsn
    assert "ro:pw@" in dsn


def test_get_readonly_pool_falls_back_to_rw_pool_with_warning(monkeypatch, caplog):
    """When APPEND_READONLY_DATABASE_URL is unset, the consoles fall back to the
    read/write pool (so dev/prod keep working) and warn exactly once."""
    sentinel = object()

    async def fake_get_pool():
        return sentinel

    monkeypatch.setattr(A, "get_pool", fake_get_pool)
    monkeypatch.setattr(settings, "append_readonly_database_url", "")
    monkeypatch.setattr(A, "_ro_fallback_warned", False)

    with caplog.at_level("WARNING"):
        got = asyncio.run(A.get_readonly_pool())
        assert got is sentinel
        # Second call still falls back but does NOT warn again (one-time).
        got2 = asyncio.run(A.get_readonly_pool())
        assert got2 is sentinel

    warnings = [r for r in caplog.records if "APPEND_READONLY_DATABASE_URL not set" in r.message]
    assert len(warnings) == 1


# ── Integration tests (need the provisioned read-only role) ──────────────────

_RO_URL = os.environ.get("APPEND_READONLY_DATABASE_URL", "").strip()
_OWNER_URL = os.environ.get("APPEND_DATABASE_URL", "").strip()
_needs_ro = pytest.mark.skipif(
    not _RO_URL, reason="APPEND_READONLY_DATABASE_URL not set — role not provisioned"
)


def _ssl_ctx():
    return ssl.create_default_context()


async def _connect(url):
    return await asyncpg.connect(dsn=A._dsn_from(url), ssl=_ssl_ctx())


@_needs_ro
def test_readonly_role_is_least_privilege():
    async def go():
        conn = await _connect(_RO_URL)
        try:
            assert (await conn.fetchval("SELECT current_setting('is_superuser')")) == "off"
            assert (await conn.fetchval(
                "SELECT pg_has_role(current_user, 'pg_read_server_files', 'MEMBER')")) is False
            # pg_read_all_data would let it read every table regardless of GRANTs.
            assert (await conn.fetchval(
                "SELECT rolbypassrls FROM pg_roles WHERE rolname = current_user")) is False
        finally:
            await conn.close()

    asyncio.run(go())


@_needs_ro
def test_readonly_role_cannot_create_table():
    """DDL is refused by Postgres (no CREATE on schema public), not just by the
    app denylist."""
    async def go():
        conn = await _connect(_RO_URL)
        try:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await conn.execute("CREATE TABLE _ro_write_probe (x int)")
        finally:
            await conn.close()

    asyncio.run(go())


@_needs_ro
def test_readonly_role_cannot_insert_into_granted_table():
    """Even a table the role CAN read, it cannot write — INSERT is denied at the
    permission layer."""
    async def go():
        conn = await _connect(_RO_URL)
        try:
            target = await conn.fetchval(
                "SELECT quote_ident(table_schema) || '.' || quote_ident(table_name) "
                "FROM information_schema.tables "
                "WHERE table_schema IN ('public', 'knesset', 'idx') AND table_type = 'BASE TABLE' "
                "  AND has_table_privilege(current_user, "
                "        quote_ident(table_schema) || '.' || quote_ident(table_name), 'SELECT') "
                "ORDER BY table_schema, table_name LIMIT 1"
            )
            if target is None:
                pytest.skip("no readable base table in the append DB yet")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                # No VALUES needed: permission is checked before row construction.
                await conn.execute(f"INSERT INTO {target} DEFAULT VALUES")
        finally:
            await conn.close()

    asyncio.run(go())


@_needs_ro
def test_readonly_role_can_read_every_console_schema():
    """The role must be able to USE all three schemas the central /data console
    puts on its search_path (data_catalog.CONSOLE_SEARCH_PATH). ``idx`` is the
    one that post-dates the provisioning script's first version: it is created
    at RUNTIME by the mirror, so a role provisioned before that commit — or a DB
    where INDEX_MIRROR_ENABLED is false, so ensure_schema() never runs — would
    fail every /data query against a mirrored index table with
    "permission denied for schema idx"."""
    async def go():
        conn = await _connect(_RO_URL)
        try:
            for schema in ("public", "knesset", "idx"):
                assert await conn.fetchval(
                    "SELECT has_schema_privilege(current_user, $1, 'USAGE')",
                    schema), f"role lacks USAGE on schema {schema}"

            # USAGE alone is not enough — prove an actual mirrored table reads.
            target = await conn.fetchval(
                "SELECT quote_ident(table_name) FROM information_schema.tables "
                "WHERE table_schema = 'idx' AND table_type = 'BASE TABLE' "
                "ORDER BY table_name LIMIT 1")
            if target is None:
                pytest.skip("no mirrored idx table in this DB yet")
            await conn.fetchval(f"SELECT count(*) FROM idx.{target} LIMIT 1")
        finally:
            await conn.close()

    asyncio.run(go())


@_needs_ro
@pytest.mark.skipif(not _OWNER_URL, reason="APPEND_DATABASE_URL (owner) not set")
def test_readonly_role_cannot_read_outside_granted_schemas():
    """Containment: a table in a schema the role was never granted USAGE on is
    unreadable. Uses the owner connection to stage a throwaway secret, then
    proves the read-only role is denied, then cleans up."""
    async def go():
        owner = await _connect(_OWNER_URL)
        try:
            await owner.execute("DROP SCHEMA IF EXISTS _ro_probe_secret CASCADE")
            await owner.execute("CREATE SCHEMA _ro_probe_secret")
            await owner.execute(
                "CREATE TABLE _ro_probe_secret.tokens (secret text)")
            await owner.execute(
                "INSERT INTO _ro_probe_secret.tokens VALUES ('top-secret')")

            ro = await _connect(_RO_URL)
            try:
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await ro.fetchval("SELECT secret FROM _ro_probe_secret.tokens")
            finally:
                await ro.close()
        finally:
            await owner.execute("DROP SCHEMA IF EXISTS _ro_probe_secret CASCADE")
            await owner.close()

    asyncio.run(go())
