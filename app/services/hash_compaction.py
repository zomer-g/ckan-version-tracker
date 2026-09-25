"""Convert the dedup hash columns from 64-char hex text to a 16-byte uuid.

Every keyless append table (``public.append_*``) carries ``row_hash`` and every
index-mirror table (``idx.*``) carries ``_row_hash``, each with an index on it.
As text they cost ~23 GB of a ~60 GB database (measured 2026-09-25): wider than
the data in narrow tables. As uuid (the digest's first 128 bits) the column
shrinks by ~75% and its index by about half.

    ALTER TABLE t ALTER COLUMN row_hash TYPE uuid USING left(row_hash, 32)::uuid

``left(…, 32)`` keeps the first 32 hex characters, which is exactly what
append_store.compact_hash takes from a new SHA-256, and the whole of an md5
(content-diff and idx tables hash in SQL with md5, which is already 32). So a
converted table and the code writing to it agree on every existing row's
identity, and nothing is re-inserted. The writers ask each table for its type
(append_store.hash_column_is_uuid), so tables in either state keep working
while this runs.

How it runs:
  * one table per transaction, smallest first, so the space freed early pays
    for the temporary copy of the bigger ones (ALTER TYPE rewrites the table
    and its indexes, and needs room for one extra copy of the table at a time);
  * ``lock_timeout`` 5s: a table busy with a load is skipped and retried at the
    end instead of queueing every reader behind the ALTER;
  * the rewrite itself holds an exclusive lock for its duration (seconds for
    most tables, a minute or two for the largest), during which reads of that
    table wait.

In-process state, one instance (single uvicorn worker): started from the admin
storage tab, progress readable there.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)

HASH_COLUMNS = (("public", "row_hash"), ("idx", "_row_hash"))
LOCK_TIMEOUT = "5s"
# The largest table (~3 GB) rewrites in a couple of minutes; this is a ceiling
# for a bad day, not an expectation.
STATEMENT_TIMEOUT_S = 3600
PAUSE_S = 0.5
MAX_ROUNDS = 3

_state: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "current": None,
    "done": 0,
    "done_bytes_before": 0,
    "done_bytes_after": 0,
    "failed": [],
    "skipped_busy": [],
    "stop": False,
}


def status() -> dict:
    return {k: v for k, v in _state.items() if k != "stop"}


def request_stop() -> None:
    _state["stop"] = True


def alter_sql(schema: str, table: str, column: str) -> str:
    """The one statement that converts a column (pure, for tests)."""
    t = f'"{schema}"."{table.replace(chr(34), chr(34) * 2)}"'
    c = f'"{column}"'
    return f"ALTER TABLE {t} ALTER COLUMN {c} TYPE uuid USING left({c}, 32)::uuid"


async def pending(conn) -> list[dict]:
    """Every hash column still stored as text, smallest table first."""
    rows = await conn.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS table, a.attname AS column,
               pg_total_relation_size(c.oid) AS bytes
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND NOT a.attisdropped
          AND a.atttypid = 'text'::regtype
          AND (n.nspname, a.attname) IN (('public', 'row_hash'), ('idx', '_row_hash'))
          AND c.relname NOT LIKE '%\\_\\_stg'
        ORDER BY pg_total_relation_size(c.oid)
        """
    )
    return [dict(r) for r in rows]


async def summary() -> dict:
    """What is left to convert, for the admin tab."""
    from app.services import append_store

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        left = await pending(conn)
    return {
        "pending_tables": len(left),
        "pending_bytes": sum(int(r["bytes"] or 0) for r in left),
        **status(),
    }


async def _convert_one(pool, t: dict) -> str:
    """'done' | 'busy' | 'failed:<reason>'."""
    from app.services import append_store

    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                # The column may have been converted (or the table dropped or
                # rebuilt by the mirror) since the list was read.
                typ = await conn.fetchval(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema=$1 AND table_name=$2 AND column_name=$3",
                    t["schema"], t["table"], t["column"])
                if typ != "text":
                    return "done"
                await conn.execute(alter_sql(t["schema"], t["table"], t["column"]),
                                   timeout=STATEMENT_TIMEOUT_S)
        except asyncpg.LockNotAvailableError:
            return "busy"
        except Exception as e:  # noqa: BLE001 — one bad table must not stop the rest
            return f"failed:{type(e).__name__}: {e}"[:400]
        append_store.mark_hash_uuid(t["table"], t["column"], t["schema"])
        try:
            await conn.execute(f'ANALYZE "{t["schema"]}"."{t["table"]}"',
                               timeout=STATEMENT_TIMEOUT_S)
        except Exception:  # noqa: BLE001 — stats are a nicety
            pass
        return "done"


async def run() -> None:
    """Convert every pending column. Safe to re-run; converted ones are skipped."""
    from app.services import append_store

    if _state["running"]:
        return
    _state.update(running=True, stop=False, started_at=datetime.now(timezone.utc).isoformat(),
                  finished_at=None, current=None, done=0, done_bytes_before=0,
                  done_bytes_after=0, failed=[], skipped_busy=[])
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            todo = await pending(conn)
        for _round in range(MAX_ROUNDS):
            busy: list[dict] = []
            for t in todo:
                if _state["stop"]:
                    return
                _state["current"] = f'{t["schema"]}.{t["table"]}'
                t0 = time.monotonic()
                outcome = await _convert_one(pool, t)
                if outcome == "done":
                    async with pool.acquire() as conn:
                        after = await conn.fetchval(
                            "SELECT pg_total_relation_size(to_regclass($1))",
                            f'"{t["schema"]}"."{t["table"]}"')
                    _state["done"] += 1
                    _state["done_bytes_before"] += int(t["bytes"] or 0)
                    _state["done_bytes_after"] += int(after or 0)
                    if time.monotonic() - t0 > 30:
                        logger.info("hash compaction: %s.%s %d → %d bytes in %.0fs",
                                    t["schema"], t["table"], t["bytes"], after or 0,
                                    time.monotonic() - t0)
                elif outcome == "busy":
                    busy.append(t)
                else:
                    _state["failed"].append({"table": _state["current"], "error": outcome[7:]})
                    logger.warning("hash compaction: %s %s", _state["current"], outcome)
                await asyncio.sleep(PAUSE_S)
            todo = busy
            if not todo:
                break
            await asyncio.sleep(30)
        _state["skipped_busy"] = [f'{t["schema"]}.{t["table"]}' for t in todo]
    except Exception:  # noqa: BLE001 — surface in status, never crash the app
        logger.exception("hash compaction aborted")
        _state["failed"].append({"table": _state["current"], "error": "aborted, see log"})
    finally:
        _state.update(running=False, current=None,
                      finished_at=datetime.now(timezone.utc).isoformat())
        logger.info("hash compaction finished: %d converted, %d → %d bytes, %d failed, %d busy",
                    _state["done"], _state["done_bytes_before"], _state["done_bytes_after"],
                    len(_state["failed"]), len(_state["skipped_busy"]))
