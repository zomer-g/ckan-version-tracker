"""Expose the ocal (יומן לעם) Neon DB in the /data SQL console via postgres_fdw.

ocal lives in its OWN Neon database (``OCAL_DATABASE_URL``), separate from the
append DB the console runs on — and Postgres cannot query across databases. So
instead of copying, this sets up **foreign tables** in an ``ocal`` schema of the
append DB that point LIVE at the ocal database: always fresh, no snapshot.

The data is PROCESSED (badged "יומן לעם" in /data), read-only. Setup is
idempotent and best-effort — if postgres_fdw is unavailable or the connection
fails, ``list_tables`` simply returns nothing and the console is unaffected.
"""
from __future__ import annotations

import asyncio
import logging
from urllib.parse import unquote, urlsplit

from app.config import settings
from app.services import append_store

logger = logging.getLogger(__name__)

SCHEMA = "ocal"
SERVER = "ocal_srv"

# The ocal tables worth exposing (its real data — skip the site_content admin
# copy and the mv_entity_counts materialised view).
TABLES = [
    "diary_events", "diary_sources", "organizations", "people",
    "event_entities", "entity_cross_refs", "similar_events",
]
TITLES = {
    "diary_events": "יומן לעם — אירועי יומן",
    "diary_sources": "יומן לעם — מקורות יומן",
    "organizations": "יומן לעם — ארגונים",
    "people": "יומן לעם — אנשים",
    "event_entities": "יומן לעם — ישויות באירועים",
    "entity_cross_refs": "יומן לעם — הצלבות בין ישויות",
    "similar_events": "יומן לעם — אירועים דומים",
}

# One-shot setup guard: the first catalog build after boot runs ensure_fdw();
# later builds skip it. The admin endpoint can force a fresh run.
_setup_attempted = False
_setup_lock = asyncio.Lock()


def _qi(name: str) -> str:
    return append_store._qi(name)


def _lit(value: str) -> str:
    """A single-quoted SQL string literal (for FDW OPTIONS, which can't be
    parameterised)."""
    return "'" + str(value or "").replace("'", "''") + "'"


def _target() -> dict | None:
    """Parse OCAL_DATABASE_URL into FDW connection parts."""
    raw = (settings.ocal_database_url or "").strip()
    if not raw:
        return None
    for pref in ("postgresql+asyncpg://", "postgres+asyncpg://"):
        if raw.startswith(pref):
            raw = "postgresql://" + raw[len(pref):]
    u = urlsplit(raw)
    return {
        "host": u.hostname or "",
        "port": str(u.port or 5432),
        "dbname": (u.path or "/").lstrip("/") or "ocal",
        "user": unquote(u.username or ""),
        "password": unquote(u.password or ""),
    }


def _readonly_role() -> str | None:
    raw = (settings.append_readonly_database_url or "").strip()
    if not raw:
        return None
    return urlsplit(raw).username or None


def is_configured() -> bool:
    return bool(settings.ocal_database_url) and append_store.is_configured()


async def ensure_fdw() -> dict:
    """Create/refresh the postgres_fdw server + foreign tables. Idempotent.

    Returns a status dict (never raises) so an admin trigger can show what
    happened on Neon, where fdw support has to be confirmed empirically."""
    if not is_configured():
        return {"ok": False, "reason": "OCAL_DATABASE_URL or append DB not configured"}
    tgt = _target()
    if not tgt or not tgt["host"]:
        return {"ok": False, "reason": "could not parse OCAL_DATABASE_URL"}
    role = _readonly_role()
    imported: list[str] = []
    try:
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")

            has_server = await conn.fetchval(
                "SELECT 1 FROM pg_foreign_server WHERE srvname = $1", SERVER)
            if not has_server:
                await conn.execute(
                    f"CREATE SERVER {_qi(SERVER)} FOREIGN DATA WRAPPER postgres_fdw "
                    f"OPTIONS (host {_lit(tgt['host'])}, port {_lit(tgt['port'])}, "
                    f"dbname {_lit(tgt['dbname'])}, sslmode 'require')")
                # One mapping for everyone (the console read-only role included);
                # only roles GRANTed below can actually reach the foreign tables.
                await conn.execute(
                    f"CREATE USER MAPPING FOR PUBLIC SERVER {_qi(SERVER)} "
                    f"OPTIONS (user {_lit(tgt['user'])}, password {_lit(tgt['password'])})")

            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(SCHEMA)}")

            have = {r["table_name"] for r in await conn.fetch(
                "SELECT table_name FROM information_schema.foreign_tables "
                "WHERE foreign_table_schema = $1", SCHEMA)}
            missing = [t for t in TABLES if t not in have]
            if missing:
                tbls = ", ".join(_qi(t) for t in missing)
                await conn.execute(
                    f"IMPORT FOREIGN SCHEMA public LIMIT TO ({tbls}) "
                    f"FROM SERVER {_qi(SERVER)} INTO {_qi(SCHEMA)}")
                imported = missing

            if role:
                r = _qi(role)
                for stmt in (
                    f"GRANT USAGE ON FOREIGN SERVER {_qi(SERVER)} TO {r}",
                    f"GRANT USAGE ON SCHEMA {_qi(SCHEMA)} TO {r}",
                    f"GRANT SELECT ON ALL TABLES IN SCHEMA {_qi(SCHEMA)} TO {r}",
                    f"ALTER DEFAULT PRIVILEGES IN SCHEMA {_qi(SCHEMA)} "
                    f"GRANT SELECT ON TABLES TO {r}",
                ):
                    try:
                        await conn.execute(stmt)
                    except Exception:  # noqa: BLE001
                        logger.warning("ocal fdw: grant failed: %s", stmt, exc_info=True)
    except Exception as e:  # noqa: BLE001 — surface, never crash the caller
        logger.warning("ocal fdw: setup failed", exc_info=True)
        return {"ok": False, "reason": f"{type(e).__name__}: {e}"[:400]}

    from app.services.data_catalog import invalidate_catalog_cache
    invalidate_catalog_cache()
    logger.info("ocal fdw: ready (imported %d new foreign tables)", len(imported))
    return {"ok": True, "tables": TABLES, "imported": imported}


async def ensure_setup_once() -> None:
    """Attempt the fdw setup a single time per process (first catalog build)."""
    global _setup_attempted
    if _setup_attempted or not is_configured():
        return
    async with _setup_lock:
        if _setup_attempted:
            return
        _setup_attempted = True          # attempt only once; admin can force a retry
        await ensure_fdw()


async def list_tables() -> list[dict]:
    """Foreign tables currently exposed in the ``ocal`` schema, with titles."""
    if not is_configured():
        return []
    await ensure_setup_once()
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT table_name FROM information_schema.foreign_tables "
                "WHERE foreign_table_schema = $1", SCHEMA)
    except Exception:  # noqa: BLE001
        logger.debug("ocal fdw: list_tables failed", exc_info=True)
        return []
    return [{"table": r["table_name"],
             "title": TITLES.get(r["table_name"], r["table_name"])} for r in rows]
