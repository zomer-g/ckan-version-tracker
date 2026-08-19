"""Access layer for the migrated "ניגוד עניינים לעם" (OCOI) database.

OCOI was a standalone Python/FastAPI/SQLAlchemy app on its own Render Postgres
(github.com/zomer-g/ocoi). Its data, admin and MCP server are being folded into
OVER (over.org.il/projects/ocoi). Unlike the ocal port — which rewrote a Node
service — ocoi already runs the same stack as OVER, so the port is mostly a
re-homing of queries rather than a translation.

The DATA tables live in schema ``ocoi`` of the APPEND DB, so the public /data SQL
console can live-JOIN them (same arrangement as schema ``ocal``). ocoi's SIX
auth/billing tables — users, oauth_clients, oauth_authorization_codes,
oauth_refresh_tokens, billing_accounts, usage_events — are deliberately NOT
migrated there: they carry hashed OAuth codes, refresh tokens and Stripe customer
ids. Isolation is by EXCLUSION, exactly as for ocal, and the console's read-only
role is granted SELECT on schema ``ocoi`` only.

Two things about this schema that differ from every other DB in OVER, both
load-bearing:

1. **UUIDs are CHAR(36) strings, not native ``uuid``.** ocoi's ``DBUUID`` is a
   TypeDecorator over CHAR(36) and generates ids in Python (a portability
   concession to its SQLite dev mode). So bind ids as ``str``; do NOT cast to
   ``uuid.UUID`` and do not expect asyncpg's UUID codec to apply. Joins are
   string comparisons.

2. **Naive timestamps hold Asia/Jerusalem wall clock, NOT UTC.** ocoi's engine
   set ``timezone: Asia/Jerusalem`` as a connect-time server setting, so every
   ``now()`` default was written in Israel local time into a
   ``TIMESTAMP WITHOUT TIME ZONE`` column. Only three columns were ever converted
   to ``timestamptz`` (documents.converted_at, documents.extracted_at,
   registry_sync_status.last_synced_at). We replicate the session timezone here
   ON PURPOSE: writing UTC into those naive columns would interleave rows that
   are silently 2-3 hours apart from the ones already there, and no later reader
   could tell which convention a given row follows. Normalising the corpus to UTC
   is a data migration to make deliberately, not a side effect of a new pool.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def is_configured() -> bool:
    return bool(settings.ocoi_database_url)


def _dsn_from(raw: str) -> str:
    """Normalize a Postgres URL into a DSN asyncpg accepts.

    Neon hands out ``postgresql://…?sslmode=require&channel_binding=require`` and
    a SQLAlchemy ``+asyncpg`` suffix may be present. asyncpg takes the plain
    ``postgresql://`` scheme and gets SSL via the ``ssl`` kwarg, not query params
    — so strip the libpq-only params and the dialect suffix.
    """
    u = urlsplit((raw or "").strip())
    scheme = u.scheme.split("+", 1)[0] or "postgresql"
    q = [
        (k, v) for k, v in parse_qsl(u.query)
        if k.lower() not in ("sslmode", "channel_binding", "options")
    ]
    return urlunsplit((scheme, u.netloc, u.path, urlencode(q), ""))


async def _init_conn(conn: asyncpg.Connection) -> None:
    """Decode json/jsonb to Python objects instead of raw strings.

    ocoi declares its JSON columns as SQLAlchemy ``JSON`` (which renders as
    ``json``, not ``jsonb``, on Postgres) — sources.metadata_json,
    registry_records.raw_data, extraction_runs.raw_output_json. Both codecs are
    registered so the distinction never matters at a call site.

    Note that several conceptually-JSON fields are plain TEXT holding a JSON
    string (aliases, permissions, reasons, redirect_uris) — another SQLite
    concession. Those decode in Python at the call site, not here.
    """
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )


# ``ocoi`` first ⇒ reads AND writes land on the co-located tables in the append
# DB; ``extensions`` keeps any extension-provided operator resolvable there;
# ``public`` is the fallback for reading a standalone ocoi DB (e.g. during the
# migration, when pointing this pool at the original Render Postgres to compare).
# Passed as a startup option so it survives the pool's RESET ALL between
# acquisitions — a plain per-connection ``SET`` does not (the lesson from ocal).
_SEARCH_PATH = "ocoi, extensions, public"

# See the module docstring: matches what ocoi's own engine did, so timestamps
# written by OVER stay on the same clock as the rows already in the table.
_TIMEZONE = "Asia/Jerusalem"

# The pooler honours ``search_path`` but NOT ``timezone`` — measured against
# ep-restless-tree: SHOW search_path returns ours, SHOW TimeZone returns GMT.
# So SQL ``now()`` cast to a naive ``timestamp`` yields UTC, three hours off the
# corpus. Never rely on the session clock: use these two helpers, or write
# ``now() AT TIME ZONE 'Asia/Jerusalem'`` explicitly in SQL.
_JERUSALEM = ZoneInfo(_TIMEZONE)

# Naive Jerusalem wall clock — for the schema's ``timestamp`` columns, which is
# almost all of them. Note this must NOT be `datetime.now().astimezone()`: that
# reads the container clock, which is UTC on Render and Jerusalem on a dev box,
# so the same code wrote two different times depending on where it ran.
def now_local() -> datetime:
    return datetime.now(timezone.utc).astimezone(_JERUSALEM).replace(tzinfo=None)


# Aware UTC — for the five ``timestamptz`` columns (registry_sync_status.
# last_synced_at, documents.converted_at/extracted_at, ocoi_jobs.started_at/
# finished_at). Binding a naive value there would be read as UTC by asyncpg.
def now_utc() -> datetime:
    return datetime.now(timezone.utc)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                if not settings.ocoi_database_url:
                    raise RuntimeError(
                        "OCOI_DATABASE_URL is not configured — the /projects/ocoi "
                        "feature is off. Set it in the Render dashboard."
                    )
                ctx = ssl.create_default_context()
                _pool = await asyncpg.create_pool(
                    dsn=_dsn_from(settings.ocoi_database_url),
                    ssl=ctx,
                    server_settings={
                        "search_path": _SEARCH_PATH,
                        "timezone": _TIMEZONE,
                    },
                    min_size=0,            # let Neon scale to zero between requests
                    max_size=5,
                    command_timeout=60,
                    statement_cache_size=0,  # Neon pooler-safe
                    init=_init_conn,
                )
                logger.info("ocoi_db: connection pool created")
    return _pool


async def fetch(sql: str, *args) -> list[asyncpg.Record]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetchrow(sql: str, *args) -> asyncpg.Record | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def fetchval(sql: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


async def execute(sql: str, *args) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


def rows_to_dicts(rows: list[asyncpg.Record]) -> list[dict]:
    return [dict(r) for r in rows]


def decode_aliases(raw) -> list[str]:
    """``aliases`` is TEXT holding a JSON array (SQLite concession), not JSON.

    Shared here rather than repeated per call site because every entity read
    (persons/companies/associations/domains) needs it, and the column is
    inconsistent in practice: NULL, empty string, and a JSON array all occur.
    Anything unparseable degrades to an empty list — an alias list is decoration,
    never a reason to fail an entity read.
    """
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x]
    try:
        val = json.loads(raw)
    except (TypeError, ValueError):
        return []
    if isinstance(val, list):
        return [str(x) for x in val if x]
    return []


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
