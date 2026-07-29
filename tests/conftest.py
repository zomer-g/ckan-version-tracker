"""Shared test setup.

Several tests build the real ORM tables on an in-memory SQLite DB (see
tests/test_auth_codes.py for the style). Postgres-specific column types don't
compile there, so the mapping lives here — once, for the whole suite, instead of
in each test module. It used to be copy-pasted into three of them, which made
whether a test passed depend on which other module pytest had imported first.
"""

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles


@compiles(JSONB, "sqlite")
def _jsonb_as_json_on_sqlite(type_, compiler, **kw):
    """SQLite has no JSONB; it stores JSON as text and has a JSON type name."""
    return "JSON"
