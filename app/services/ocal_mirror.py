"""Co-locate יומן לעם (ocal) in the /data console DB by materialising it.

ocal lives in its OWN Neon database (``OCAL_DATABASE_URL``); the console runs on
the append DB, and Postgres can't query across databases. postgres_fdw proved
unreliable on Neon, so instead we COPY ocal's tables into a local ``ocal`` schema
of the append DB — real local tables, so the console can JOIN them freely with
public / knesset / idx / odata, fast and with no cross-DB dependency.

The copy streams (bounded memory), swaps atomically, and is all-``text`` like the
other processed sources (cast in SQL as needed). A scheduled job + an admin
trigger keep the snapshot fresh.
"""
from __future__ import annotations

import json
import logging
from urllib.parse import urlsplit

from app.config import settings
from app.services import append_store, ocal_db

logger = logging.getLogger(__name__)

SCHEMA = "ocal"
STATE_TABLE = "_sync"

# ocal's real data tables (skip site_content admin copy + mv_entity_counts).
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

_COPY_BATCH = 5000


def _qi(name: str) -> str:
    return append_store._qi(name)


def _qt(table: str, schema: str = SCHEMA) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def _readonly_role() -> str | None:
    raw = (settings.append_readonly_database_url or "").strip()
    if not raw:
        return None
    return urlsplit(raw).username or None


def is_configured() -> bool:
    return bool(settings.ocal_database_url) and append_store.is_configured()


def _to_text(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, default=str)
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b.hex()
    if hasattr(v, "isoformat"):        # datetime / date / time
        return v.isoformat()
    return str(v)


async def ensure_schema(conn) -> None:
    """Create the ``ocal`` schema + state table, readable by the console role."""
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(SCHEMA)}")
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_qt(STATE_TABLE)} (
            table_name text PRIMARY KEY,
            rows       bigint,
            columns    integer,
            synced_at  timestamptz NOT NULL DEFAULT now()
        )
    """)
    role = _readonly_role()
    if not role:
        return
    r = _qi(role)
    try:
        await conn.execute(f"GRANT USAGE ON SCHEMA {_qi(SCHEMA)} TO {r}")
        await conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {_qi(SCHEMA)} TO {r}")
        await conn.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {_qi(SCHEMA)} "
            f"GRANT SELECT ON TABLES TO {r}")
    except Exception:  # noqa: BLE001 — a missing role must not break the sync
        logger.warning("ocal mirror: grant to %r failed", role, exc_info=True)


async def _ocal_columns(oconn, table: str) -> list[str]:
    rows = await oconn.fetch(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = $1 "
        "ORDER BY ordinal_position", table)
    return [r["column_name"] for r in rows]


async def sync_table(table: str) -> dict:
    """Stream one ocal table into ``ocal.<table>`` (all text), swap atomically."""
    ocal_pool = await ocal_db.get_pool()
    append_pool = await append_store.get_pool()
    async with ocal_pool.acquire() as oconn:
        cols = await _ocal_columns(oconn, table)
        if not cols:
            raise ValueError(f"ocal table {table!r} not found / has no columns")
        safe = append_store.safe_column_names(cols)
        defs = ", ".join(f"{_qi(c)} text" for c in safe)
        col_sql = ", ".join(f'"{c}"' for c in cols)
        staging = append_store.clip_ident_bytes(table, 63 - len("__stg")) + "__stg"

        async with append_pool.acquire() as aconn:
            await ensure_schema(aconn)
            await aconn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
            await aconn.execute(f"CREATE TABLE {_qt(staging)} ({defs})")
            rows = 0
            try:
                async with oconn.transaction():
                    batch: list[tuple] = []
                    async for rec in oconn.cursor(
                            f'SELECT {col_sql} FROM public."{table}"'):
                        batch.append(tuple(_to_text(rec[c]) for c in cols))
                        if len(batch) >= _COPY_BATCH:
                            await aconn.copy_records_to_table(
                                staging, schema_name=SCHEMA, columns=safe,
                                records=batch)
                            rows += len(batch)
                            batch = []
                    if batch:
                        await aconn.copy_records_to_table(
                            staging, schema_name=SCHEMA, columns=safe, records=batch)
                        rows += len(batch)
                async with aconn.transaction():
                    await aconn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                    await aconn.execute(
                        f"ALTER TABLE {_qt(staging)} RENAME TO {_qi(table)}")
            except BaseException:
                try:
                    await aconn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
                except Exception:  # noqa: BLE001 — cleanup is best-effort
                    logger.debug("ocal mirror: staging cleanup failed", exc_info=True)
                raise
            await aconn.execute(f"ANALYZE {_qt(table)}")
            role = _readonly_role()
            if role:
                try:
                    await aconn.execute(
                        f"GRANT SELECT ON {_qt(table)} TO {_qi(role)}")
                except Exception:  # noqa: BLE001
                    logger.warning("ocal mirror: grant on %s failed", table,
                                   exc_info=True)
            await aconn.execute(f"""
                INSERT INTO {_qt(STATE_TABLE)} (table_name, rows, columns, synced_at)
                VALUES ($1, $2, $3, now())
                ON CONFLICT (table_name) DO UPDATE SET
                    rows = EXCLUDED.rows, columns = EXCLUDED.columns,
                    synced_at = now()
            """, table, rows, len(safe))
    return {"table": table, "rows": rows, "columns": len(safe)}


async def sync_all() -> dict:
    """Refresh every ocal table. Never raises — reports per-table outcome."""
    if not is_configured():
        return {"ok": False, "reason": "OCAL_DATABASE_URL or append DB not configured"}
    results: list[dict] = []
    for t in TABLES:
        try:
            r = await sync_table(t)
            results.append({**r, "ok": True})
        except Exception as e:  # noqa: BLE001 — one bad table must not stop the rest
            logger.warning("ocal mirror: sync failed for %s", t, exc_info=True)
            results.append({"table": t, "ok": False, "error": str(e)[:300]})
    from app.services.data_catalog import invalidate_catalog_cache
    invalidate_catalog_cache()
    ok = [r for r in results if r.get("ok")]
    logger.info("ocal mirror: synced %d/%d tables, %d rows",
                len(ok), len(results), sum(r.get("rows") or 0 for r in ok))
    return {"ok": True, "synced": len(ok), "failed": len(results) - len(ok),
            "rows": sum(r.get("rows") or 0 for r in ok), "results": results}


async def list_tables() -> list[dict]:
    """Local ocal tables currently materialised, with titles + row counts."""
    if not is_configured():
        return []
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            names = await conn.fetch(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = $1 AND c.relkind = 'r' "
                "  AND c.relname <> $2", SCHEMA, STATE_TABLE)
            sync = {}
            try:
                for r in await conn.fetch(
                        f"SELECT table_name, rows FROM {_qt(STATE_TABLE)}"):
                    sync[r["table_name"]] = r["rows"]
            except Exception:  # noqa: BLE001 — state table not created yet
                pass
    except Exception:  # noqa: BLE001 — schema not created yet
        logger.debug("ocal mirror: list_tables before first sync", exc_info=True)
        return []
    return [{"table": r["relname"],
             "title": TITLES.get(r["relname"], r["relname"]),
             "rows": sync.get(r["relname"])} for r in names]
