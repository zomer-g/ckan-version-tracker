"""The schema PostGIS is installed in, read from the database, not assumed.

PostGIS lives in its own schema so its ~1,000 functions and spatial_ref_sys stay
out of the console's autocomplete. That schema was ``extensions``; xhostd is
moving it to ``postgis``. Every PostGIS name the code writes is schema-qualified
(the worker's connections carry no search_path), so a hard-coded
``extensions.ST_MakePoint`` would stop resolving the moment the extension moves.

So the name is read when a connection opens (``resolve``, wired into the
append pools' ``init``) and at boot (``ensure_resolved``), and every SQL
builder asks ``schema()`` / ``qschema()`` at the time it builds a statement.
Until something has been read — or when PostGIS is absent — the old name is
used, which is what the database had before the move.

This module only reads ``pg_extension``. It never creates, drops or moves the
extension.
"""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

DEFAULT_SCHEMA = "extensions"
RESOLVE_SQL = ("SELECT extnamespace::regnamespace::text FROM pg_extension "
               "WHERE extname = 'postgis'")

_schema = DEFAULT_SCHEMA
_resolved = False


def schema() -> str:
    """The PostGIS schema name, unquoted."""
    return _schema


def qschema() -> str:
    """The PostGIS schema name as a quoted SQL identifier, ready to prefix."""
    return '"' + _schema.replace('"', '""') + '"'


def is_resolved() -> bool:
    return _resolved


def _unquote(name: str) -> str:
    # regnamespace::text quotes a name only when it needs quoting.
    if len(name) >= 2 and name[0] == name[-1] == '"':
        return name[1:-1].replace('""', '"')
    return name


def set_schema(name: str | None) -> str:
    """Record where PostGIS lives. ``None`` (no PostGIS) keeps the current name."""
    global _schema, _resolved
    if name:
        name = _unquote(str(name).strip())
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", name):
            logger.warning("postgis_schema: ignoring unexpected schema name %r", name)
            return _schema
        if name != _schema:
            logger.info("postgis_schema: PostGIS is in schema %r (was %r)", name, _schema)
        _schema = name
        _resolved = True
    return _schema


async def resolve(conn) -> str:
    """Read the PostGIS schema on an asyncpg connection and remember it.

    Never raises: a failed lookup must not stop a connection from opening."""
    try:
        return set_schema(await conn.fetchval(RESOLVE_SQL))
    except Exception:  # noqa: BLE001
        logger.debug("postgis_schema: lookup failed", exc_info=True)
        return _schema


async def ensure_resolved() -> str:
    """Resolve once at boot through the append pool, so statements built before
    the first pooled connection opens already carry the right name."""
    if _resolved:
        return _schema
    try:
        from app.config import settings
        if not (settings.append_database_url or "").strip():
            return _schema
        from app.services.append_store import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await resolve(conn)
    except Exception:  # noqa: BLE001
        logger.warning("postgis_schema: boot lookup failed; using %r", _schema,
                       exc_info=True)
    return _schema
