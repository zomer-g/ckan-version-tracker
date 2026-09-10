"""Shared test setup.

Several tests build the real ORM tables on an in-memory SQLite DB (see
tests/test_auth_codes.py for the style). Postgres-specific column types don't
compile there, so the mapping lives here — once, for the whole suite, instead of
in each test module. It used to be copy-pasted into three of them, which made
whether a test passed depend on which other module pytest had imported first.

The same applies to the `auth` schema: the credential-bearing tables moved out
of `public` (alembic 065) so the public SQL console's read-only role cannot be
granted on them. SQLite has no schemas, but it does have ATTACH, and an attached
database is addressed with exactly the same `schema.table` syntax — so attaching
an in-memory database under the name `auth` makes the real models build and
query unchanged. The listener is on Engine rather than on one fixture because
tests create their own engines all over the suite.
"""

from sqlalchemy import event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.engine import Engine
from sqlalchemy.ext.compiler import compiles


@event.listens_for(Engine, "connect")
def _attach_auth_schema_on_sqlite(dbapi_connection, connection_record):
    """Give SQLite an `auth` schema, so models declaring one still build.

    The object handed here is SQLAlchemy's DBAPI adapter, not the driver's own
    connection — for the async driver it is
    ``sqlalchemy.dialects.sqlite.aiosqlite.AsyncAdapt_aiosqlite_connection``.
    So identify it by name rather than by module, and go through a cursor, which
    both the sync and the async adapters present.
    """
    name = f"{type(dbapi_connection).__module__}.{type(dbapi_connection).__name__}"
    if "sqlite" not in name.lower():
        return
    cur = dbapi_connection.cursor()
    try:
        cur.execute("ATTACH DATABASE ':memory:' AS auth")
    except Exception:
        pass  # already attached on this connection
    finally:
        cur.close()


@compiles(JSONB, "sqlite")
def _jsonb_as_json_on_sqlite(type_, compiler, **kw):
    """SQLite has no JSONB; it stores JSON as text and has a JSON type name."""
    return "JSON"
