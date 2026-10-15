"""Make every append table's dedup index PARTIAL: ``WHERE last_seen IS NULL``.

Phase 1 of docs/history-storage-strategy.md gives each archive row a validity
interval: ``last_seen`` is NULL while the row is in force and set when a full
pass of the source no longer returns it. For a closed row whose exact content
comes back to open a NEW interval, the UNIQUE index that dedups the archive may
only cover the open rows:

    UNIQUE (row_hash)                          -- before: one row per content, ever
    UNIQUE (row_hash) WHERE last_seen IS NULL  -- after:  one OPEN row per content

New tables are created that way (append_store.ensure_table). This job converts
the existing ones, table by table, smallest first:

  1. ``ADD COLUMN IF NOT EXISTS last_seen timestamptz`` (catalog only, no
     rewrite; ``lock_timeout`` so a table busy with a load is retried later);
  2. ``CREATE UNIQUE INDEX CONCURRENTLY … WHERE last_seen IS NULL`` beside the
     old index (no write lock; a leftover INVALID index from an interrupted run
     is dropped and rebuilt);
  3. ``DROP INDEX CONCURRENTLY`` of the old full index.

Between 2 and 3 both indexes exist and both clauses work: the old
``ON CONFLICT (row_hash)`` infers the full index, the new one infers both. The
writers ask each table which index it has (append_store.open_index_ready), so a
table in any state keeps loading while this runs. Nothing closes rows on a
table until its partial index exists (append_store.begin_pass).

Only the indexes append_store itself created are candidates (``<t>_uq`` and
``<t>_hash_uq``). Other unique indexes on ``append_*`` tables, e.g. the CBS
content index's ``url``, back their own ``ON CONFLICT`` clauses and must stay
as they are.

In-process state, one instance, like hash_compaction: started from the admin
storage tab, progress readable there.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import asyncpg

logger = logging.getLogger(__name__)

LOCK_TIMEOUT = "5s"
# Building an index on the largest table (~1.1M parcel rows) is minutes; this
# is a ceiling for a bad day.
STATEMENT_TIMEOUT_S = 3600
PAUSE_S = 0.5
MAX_ROUNDS = 3
OPEN_SUFFIX = "open_uq"

_state: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "current": None,
    "done": 0,
    "failed": [],
    "skipped_busy": [],
    "stop": False,
}


def status() -> dict:
    return {k: v for k, v in _state.items() if k != "stop"}


def request_stop() -> None:
    _state["stop"] = True


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_sql(table: str, index_name: str, column: str) -> str:
    """The partial index, built without blocking writes (pure, for tests)."""
    return (f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {_qi(index_name)} "
            f'ON "public".{_qi(table)} ({_qi(column)}) WHERE "last_seen" IS NULL')


def drop_sql(index_name: str) -> str:
    return f'DROP INDEX CONCURRENTLY IF EXISTS "public".{_qi(index_name)}'


def plan(existing_indexes: list[dict]) -> list[dict]:
    """The tables still carrying a FULL dedup index, in the order given.

    ``existing_indexes`` rows are ``{table, indexname, indexdef, bytes?}`` (as
    pg_indexes reports them). A table whose partial index already exists but
    whose old index is still there (a stop between the two steps) is listed
    with ``partial_exists=True`` so the next run only has to drop; a table with
    nothing but its partial index is done and does not appear. Pure."""
    from app.services import append_store

    by_table: dict[str, list[dict]] = {}
    order: list[str] = []
    for r in existing_indexes:
        t = r["table"]
        if t not in by_table:
            by_table[t] = []
            order.append(t)
        by_table[t].append(r)
    out: list[dict] = []
    for t in order:
        rows = by_table[t]
        ours = {append_store._index_name(t, "uq"), append_store._index_name(t, "hash_uq")}
        uniq = append_store.classify_unique_indexes(rows)
        partial_cols = {u["column"] for u in uniq if u["partial"] and u["column"]}
        size = max((int(r.get("bytes") or 0) for r in rows), default=0)
        for u in uniq:
            if u["partial"] or not u["column"] or u["name"] not in ours:
                continue
            out.append({
                "table": t,
                "column": u["column"],
                "old_index": u["name"],
                "new_index": append_store._index_name(t, OPEN_SUFFIX),
                "partial_exists": u["column"] in partial_cols,
                "bytes": size,
            })
    return out


async def existing_indexes(conn) -> list[dict]:
    """Every unique index on a ``public.append_*`` table, smallest table first."""
    rows = await conn.fetch(
        r"""
        SELECT i.tablename AS table, i.indexname, i.indexdef,
               pg_total_relation_size(format('%I.%I', i.schemaname, i.tablename)::regclass) AS bytes
        FROM pg_indexes i
        WHERE i.schemaname = 'public'
          AND i.tablename LIKE 'append\_%'
          AND i.indexdef LIKE 'CREATE UNIQUE INDEX%'
        ORDER BY 4, 1, 2
        """
    )
    return [dict(r) for r in rows]


async def pending(conn) -> list[dict]:
    return plan(await existing_indexes(conn))


async def summary() -> dict:
    """What is left to convert, for the admin tab."""
    from app.services import append_store

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        left = await pending(conn)
    return {
        "pending_tables": len({p["table"] for p in left}),
        "pending_bytes": sum(int(p["bytes"] or 0) for p in left),
        **status(),
    }


async def _index_valid(conn, name: str) -> bool | None:
    """True/False for an existing index's validity, None when it is absent."""
    return await conn.fetchval(
        "SELECT x.indisvalid FROM pg_index x WHERE x.indexrelid = to_regclass($1)",
        f'"public".{_qi(name)}')


async def _convert_one(pool, p: dict) -> str:
    """'done' | 'busy' | 'failed:<reason>'. Every step is idempotent."""
    from app.services import append_store

    t = p["table"]
    async with pool.acquire() as conn:
        try:
            if not p["partial_exists"]:
                async with conn.transaction():
                    await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
                    await conn.execute(
                        f'ALTER TABLE "public".{_qi(t)} '
                        f'ADD COLUMN IF NOT EXISTS "last_seen" timestamptz')
                if await _index_valid(conn, p["new_index"]) is False:
                    # An interrupted CONCURRENTLY build leaves an INVALID index
                    # that IF NOT EXISTS would happily keep.
                    await conn.execute(drop_sql(p["new_index"]), timeout=STATEMENT_TIMEOUT_S)
                await conn.execute(create_sql(t, p["new_index"], p["column"]),
                                   timeout=STATEMENT_TIMEOUT_S)
                if not await _index_valid(conn, p["new_index"]):
                    return "failed:the partial index was built INVALID (duplicate open rows?)"
            await conn.execute(drop_sql(p["old_index"]), timeout=STATEMENT_TIMEOUT_S)
        except asyncpg.LockNotAvailableError:
            return "busy"
        except Exception as e:  # noqa: BLE001 — one bad table must not stop the rest
            return f"failed:{type(e).__name__}: {e}"[:400]
    append_store.mark_open_index(t, p["column"])
    return "done"


async def run() -> None:
    """Convert every pending table. Safe to re-run; converted ones are skipped."""
    from app.services import append_store

    if _state["running"]:
        return
    _state.update(running=True, stop=False, started_at=datetime.now(timezone.utc).isoformat(),
                  finished_at=None, current=None, done=0, failed=[], skipped_busy=[])
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            todo = await pending(conn)
        for _round in range(MAX_ROUNDS):
            busy: list[dict] = []
            for p in todo:
                if _state["stop"]:
                    return
                _state["current"] = p["table"]
                t0 = time.monotonic()
                outcome = await _convert_one(pool, p)
                if outcome == "done":
                    _state["done"] += 1
                    if time.monotonic() - t0 > 30:
                        logger.info("index migration: %s in %.0fs", p["table"],
                                    time.monotonic() - t0)
                elif outcome == "busy":
                    busy.append(p)
                else:
                    _state["failed"].append({"table": p["table"], "error": outcome[7:]})
                    logger.warning("index migration: %s %s", p["table"], outcome)
                await asyncio.sleep(PAUSE_S)
            todo = busy
            if not todo:
                break
            await asyncio.sleep(30)
        _state["skipped_busy"] = [p["table"] for p in todo]
    except Exception:  # noqa: BLE001 — surface in status, never crash the app
        logger.exception("index migration aborted")
        _state["failed"].append({"table": _state["current"], "error": "aborted, see log"})
    finally:
        _state.update(running=False, current=None,
                      finished_at=datetime.now(timezone.utc).isoformat())
        logger.info("index migration finished: %d converted, %d failed, %d busy",
                    _state["done"], len(_state["failed"]), len(_state["skipped_busy"]))
