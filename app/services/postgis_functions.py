"""Recreate the stored SQL functions whose bodies name the PostGIS schema.

A LANGUAGE sql / plpgsql function body is stored as text and parsed when it
runs, so ``public.over_parcel_at`` written with the schema-qualified ST_Contains(...)
keeps naming ``extensions`` after xhostd moves PostGIS to ``postgis`` — and
fails on every call until it is created again. Tables, views and generated
columns are not affected: Postgres stores those by object id.

These functions are otherwise recreated only by a nadlan build
(``nadlan_index.ensure_functions``) and once per process by the index mirror
(``index_mirror.ensure_schema``). This module recreates them as soon as the
schema they name differs from the one ``postgis_schema`` read from the
database: at boot, and when a new connection sees the schema change.

It only runs CREATE OR REPLACE FUNCTION. It never creates, drops or moves the
extension, and touches no data.
"""
from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)

#: (schema, function) whose body names the PostGIS schema, and who recreates it.
NADLAN_FUNCTIONS = (("public", "over_parcel_at"), ("public", "over_parcels_near"))
MIRROR_FUNCTIONS = (("idx", "try_geom"),)

_STALE_SQL = """
    SELECT n.nspname, p.proname
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE (n.nspname, p.proname) IN (SELECT * FROM unnest($1::text[], $2::text[]))
      AND strpos(pg_get_functiondef(p.oid), $3 || '.') = 0
"""

_lock = asyncio.Lock()


async def stale_functions(conn, qschema: str) -> set[tuple[str, str]]:
    """The tracked functions that exist but do not name ``qschema``."""
    funcs = NADLAN_FUNCTIONS + MIRROR_FUNCTIONS
    rows = await conn.fetch(_STALE_SQL, [s for s, _ in funcs], [f for _, f in funcs],
                            qschema)
    return {(r["nspname"], r["proname"]) for r in rows}


async def refresh_if_stale() -> dict:
    """Recreate any tracked function that names a schema PostGIS is not in.

    Only on an instance allowed to write. Never raises: a failure is logged and
    the next boot or schema change tries again."""
    from app.config import settings
    from app.services import postgis_schema
    if not settings.writers_enabled or not (settings.append_database_url or "").strip():
        return {"skipped": "not a writer"}
    if _lock.locked():
        return {"skipped": "already running"}
    async with _lock:
        try:
            from app.services import append_store, index_mirror, nadlan_index
            q = postgis_schema.qschema()
            pool = await append_store.get_pool()
            async with pool.acquire() as conn:
                stale = await stale_functions(conn, q)
                if stale & set(MIRROR_FUNCTIONS):
                    await index_mirror.ensure_schema(conn)
            if stale & set(NADLAN_FUNCTIONS):
                await nadlan_index.ensure_functions()
            if stale:
                logger.info("postgis_functions: recreated %s for schema %s",
                            sorted(".".join(f) for f in stale), q)
            return {"schema": q, "recreated": sorted(".".join(f) for f in stale)}
        except Exception:  # noqa: BLE001
            logger.warning("postgis_functions: refresh failed", exc_info=True)
            return {"error": "refresh failed"}


def schedule_refresh() -> None:
    """Run :func:`refresh_if_stale` in the background, if a loop is running."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(refresh_if_stale())
