"""Where PostGIS lives on the append DB, read from the database itself.

PostGIS is installed in a schema of its own so its ~1,000 functions stay out of
the console's autocomplete, and every PostGIS name in the code is therefore
schema-qualified. That schema used to be ``extensions``; xhostd is moving it to
``postgis``. Rather than hard-code either, each connection pool asks the
database when it starts (``DETECT_SQL``) and the code qualifies every PostGIS
name with :func:`schema` / :func:`q`.

``extensions`` stays the default until a pool has answered, so a database that
has not been moved (or has no PostGIS at all) behaves exactly as before.

This module only reads. It never creates, drops or moves the extension.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA = "extensions"

DETECT_SQL = ("SELECT extnamespace::regnamespace::text "
              "FROM pg_extension WHERE extname = 'postgis'")

# The search_path tail for paths that name the PostGIS schema. Both are listed so
# a path works before and after the move: Postgres skips a schema that does not
# exist.
SEARCH_PATH_SCHEMAS = ("extensions", "postgis")

# Stands for the PostGIS schema in SQL built at import time (module-level DDL),
# before any pool has answered. fill() swaps in the real name when it runs.
TOKEN = "__OVER_POSTGIS_SCHEMA__"

_schema = DEFAULT_SCHEMA
_detected = False


def schema() -> str:
    """The schema PostGIS is installed in (unquoted)."""
    return _schema


def q() -> str:
    """The PostGIS schema as a quoted identifier, ready for ``{q()}.ST_X(...)``."""
    return '"' + _schema.replace('"', '""') + '"'


def fill(sql: str) -> str:
    """``sql`` with every :data:`TOKEN` replaced by the quoted PostGIS schema."""
    return sql.replace(TOKEN, q())


def detected() -> bool:
    return _detected


def forget() -> None:
    """Mark the schema as unknown, so the next connection asks again.

    Called when a pool starts. The last known value is kept until then."""
    global _detected
    _detected = False


def _unquote(name: str) -> str:
    # regnamespace::text quotes a name that needs it ("My Schema").
    if len(name) >= 2 and name.startswith('"') and name.endswith('"'):
        return name[1:-1].replace('""', '"')
    return name


async def detect(conn) -> str:
    """Ask the database where PostGIS is and remember it. Returns the schema.

    Without PostGIS the default stays. A failed probe is logged and leaves the
    current value; it is never a reason a connection fails."""
    global _schema, _detected
    try:
        found = await conn.fetchval(DETECT_SQL)
    except Exception:  # noqa: BLE001
        logger.warning("pg_ext: could not read where PostGIS lives; keeping %r",
                       _schema, exc_info=True)
        return _schema
    if found:
        name = _unquote(str(found))
        if name != _schema:
            logger.info("pg_ext: PostGIS is in schema %r", name)
        _schema = name
    _detected = True
    return _schema


async def ensure(conn) -> str:
    """:func:`detect` once per pool start; a no-op once it has answered."""
    if not _detected:
        return await detect(conn)
    return _schema
