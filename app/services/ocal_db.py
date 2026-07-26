"""Access layer for the migrated "יומן לעם" (Ocal) database.

Ocal was a standalone Node/Express/Knex app on its own Postgres. Its data (and
its automations + admin) are being folded into OVER (over.org.il/projects/ocal).
Rather than port 34 Knex migrations to Alembic, the whole database is migrated
Render-Postgres → Neon with pg_dump/restore, and OVER connects to it directly
through this module — a THIRD physical database alongside the operational DB
(app/database.py) and the append archive DB (app/services/append_store.py).

Why a dedicated DB and not the append DB: the ocal DB carries sensitive auth
tables (api_users bearer tokens, admin_users). The append DB backs the PUBLIC
SQL consoles. Keeping ocal separate — and asserting it at startup
(_guard_ocal_db_separation in app/main.py) — is what keeps those tokens off a
public console. NEVER point a public SQL console at this pool.

Connection: a lazily-created asyncpg pool with ``min_size=0`` so the Neon
compute can scale to zero between requests. ``statement_cache_size=0`` keeps it
safe behind a Neon pooler (pgbouncer transaction mode breaks prepared
statements). A jsonb/json codec is registered per connection so columns like
``other_fields``/``ckan_metadata`` and the computed ``json_agg`` aggregates come
back as Python objects (matching what the Node ``pg`` driver did), not raw JSON
strings.
"""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()


def is_configured() -> bool:
    return bool(settings.ocal_database_url)


def _dsn_from(raw: str) -> str:
    """Normalize a Postgres URL into a DSN asyncpg accepts.

    Neon hands out ``postgresql://…?sslmode=require&channel_binding=require`` and
    the SQLAlchemy ``+asyncpg`` suffix may be present. asyncpg takes the plain
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
    """Decode jsonb/json to Python objects instead of raw strings."""
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog",
    )


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                if not settings.ocal_database_url:
                    raise RuntimeError(
                        "OCAL_DATABASE_URL is not configured — the /projects/ocal "
                        "feature is off. Set it in the Render dashboard."
                    )
                ctx = ssl.create_default_context()
                _pool = await asyncpg.create_pool(
                    dsn=_dsn_from(settings.ocal_database_url),
                    ssl=ctx,
                    min_size=0,            # let Neon scale to zero between requests
                    max_size=5,
                    command_timeout=60,
                    statement_cache_size=0,  # Neon pooler-safe
                    init=_init_conn,
                )
                logger.info("ocal_db: connection pool created")
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


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
