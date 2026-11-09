"""``current.<table>``: a dataset's rows as they stand now, without history.

Every append table keeps history in place. A row is written once, stamped
``first_seen``, and stamped ``last_seen`` when a full pass of the source no
longer returns it (it was removed, or one of its values changed and the new
version is a new row). So the table holds every state the source has ever
had, and "what does the source say today" is ``WHERE last_seen IS NULL``.

That predicate was the only way to ask it in SQL, and nothing in the console
said so: ``SELECT * FROM append_…`` returns current and superseded rows mixed
together, with nothing to tell them apart unless the reader already knows
the convention. In the construction-sites register, for example, 2,068 of
13,029 rows are superseded states of sites that are still listed.

This module puts a VIEW for every such table in a schema of its own, under
the SAME name:

    SELECT * FROM current.append_b072e36c_…   -- the source as it is now
    SELECT * FROM append_b072e36c_…           -- every state ever seen

A view costs no storage and is never stale: it reads the live table. A
separate schema rather than a suffix because these table names are already at
Postgres' 63-byte limit, and because ``current.`` reads as what it is.

The view carries the table's own columns plus ``first_seen``. It leaves out
``last_seen`` (always NULL here) and the dedup hash. Leaving the hash out
matters: hash_compaction changes that column's TYPE, and Postgres refuses to
change the type of a column a view depends on.

Tables are dropped with CASCADE (append_store.drop_table), which takes the
view with them. A table that gains a column gets its view rebuilt, because the
view's comment holds a signature of the column list. Both are checked at boot
and when the /data catalog is read, at most every ``_CHECK_TTL`` seconds.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time

from app.services import append_store

logger = logging.getLogger(__name__)

SCHEMA = "current"
VALIDITY_COL = "last_seen"
# Columns a "current" view does not carry: the validity stamp is always NULL
# in it, and the dedup hash is bookkeeping whose type hash_compaction alters.
_OMIT = {VALIDITY_COL, "row_hash", "_row_hash"}
_CHECK_TTL = 600
_state: dict = {"checked_at": 0.0, "views": frozenset()}


def _qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def view_columns(columns: list[str]) -> list[str]:
    """The base table's columns a view exposes, in table order (pure)."""
    return [c for c in columns if c not in _OMIT]


def signature(columns: list[str]) -> str:
    """Changes whenever the base table's column list does (pure)."""
    raw = json.dumps(columns, ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def view_sql(table: str, columns: list[str]) -> str | None:
    """Body of ``current.<table>`` (pure). None when there is nothing to show."""
    cols = view_columns(columns)
    if not cols or VALIDITY_COL not in columns:
        return None
    return (f"SELECT {', '.join(_qi(c) for c in cols)}\n"
            f"FROM public.{_qi(table)}\n"
            f"WHERE {_qi(VALIDITY_COL)} IS NULL")


def view_ref(table: str) -> str:
    """How a query names the view: ``current.<table>``."""
    return f"{SCHEMA}.{append_store._ident_ref(table)}"


def known_views() -> frozenset[str]:
    """Base-table names that have a current view, as of the last check. Empty
    until the first check has run; a reader treats absence as "no view"."""
    return _state["views"]


async def _tables(conn) -> dict[str, list[str]]:
    """{table: [columns in order]} for every public append table whose
    ``last_seen`` is OVER's validity stamp (timestamptz). A source that
    publishes its own ``last_seen`` column gets no validity column
    (append_store.ensure_table), and its text column must not be mistaken
    for one."""
    rows = await conn.fetch(
        """
        SELECT c.table_name, c.column_name
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
          AND c.table_name LIKE 'append\\_%'
          AND EXISTS (
              SELECT 1 FROM information_schema.columns v
              WHERE v.table_schema = 'public' AND v.table_name = c.table_name
                AND v.column_name = $1
                AND v.data_type = 'timestamp with time zone')
          AND EXISTS (
              SELECT 1 FROM information_schema.tables t
              WHERE t.table_schema = 'public' AND t.table_name = c.table_name
                AND t.table_type = 'BASE TABLE')
        ORDER BY c.table_name, c.ordinal_position
        """,
        VALIDITY_COL,
    )
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["table_name"], []).append(r["column_name"])
    return out


async def _existing(conn) -> dict[str, str]:
    """{view name: its comment} for the views already in the schema."""
    rows = await conn.fetch(
        """
        SELECT c.relname, obj_description(c.oid, 'pg_class') AS comment
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind = 'v'
        """,
        SCHEMA,
    )
    return {r["relname"]: r["comment"] or "" for r in rows}


async def _ensure_schema(conn, role: str | None) -> None:
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(SCHEMA)}")
    if role:
        await conn.execute(f"GRANT USAGE ON SCHEMA {_qi(SCHEMA)} TO {_qi(role)}")


async def ensure_views(*, force: bool = False) -> dict:
    """Create, rebuild or drop views until every validity table has exactly one
    up-to-date view. Cheap when nothing changed: two catalog queries."""
    from app.services.index_mirror import _readonly_role

    now = time.monotonic()
    if not force and now - _state["checked_at"] < _CHECK_TTL:
        return {"skipped": "checked recently"}
    _state["checked_at"] = now

    role = _readonly_role()
    pool = await append_store.get_pool()
    built = dropped = failed = 0
    async with pool.acquire() as conn:
        await _ensure_schema(conn, role)
        tables = await _tables(conn)
        existing = await _existing(conn)
        have: set[str] = set()

        for table, columns in tables.items():
            body = view_sql(table, columns)
            if body is None:
                continue
            sig = signature(columns)
            if not force and f"sig={sig}" in existing.get(table, ""):
                have.add(table)
                continue
            ref = f"{_qi(SCHEMA)}.{_qi(table)}"
            try:
                async with conn.transaction():
                    # DROP + CREATE rather than CREATE OR REPLACE: REPLACE
                    # cannot remove or reorder columns.
                    await conn.execute(f"DROP VIEW IF EXISTS {ref}")
                    await conn.execute(f"CREATE VIEW {ref} AS\n{body}")
                    await conn.execute(
                        f"COMMENT ON VIEW {ref} IS "
                        f"{_lit('השורות שבתוקף עכשיו ב-public.' + table + ' · sig=' + sig)}")
                    if role:
                        await conn.execute(f"GRANT SELECT ON {ref} TO {_qi(role)}")
                built += 1
                have.add(table)
            except Exception as e:  # noqa: BLE001 — one bad table must not stop the rest
                failed += 1
                logger.warning("current view for %s not built: %s", table, e)

        # A view whose table is gone, or no longer has a validity column.
        for view in existing:
            if view in tables:
                continue
            try:
                await conn.execute(f"DROP VIEW IF EXISTS {_qi(SCHEMA)}.{_qi(view)}")
                dropped += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("stale current view %s not dropped: %s", view, e)

    _state["views"] = frozenset(have)
    if built or dropped or failed:
        logger.info("current views: %d built, %d dropped, %d failed, %d total",
                    built, dropped, failed, len(have))
    return {"built": built, "dropped": dropped, "failed": failed, "total": len(have)}


async def ensure_views_safely() -> None:
    try:
        await ensure_views()
    except Exception as e:  # noqa: BLE001 — never break a read or boot on this
        logger.warning("current views not checked: %s", e)
