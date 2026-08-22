"""Repair append tables whose rows were ingested with a broken text encoding.

A shapefile carries its attribute text in a DBF whose codepage is declared in a
sidecar `.cpg`. Israeli government shapefiles routinely ship without one, and
until GOVSCRAPER 169d98c the reader assumed UTF-8 and replaced every unmappable
byte with U+FFFD. The Hebrew columns — יישוב, מועצה אזורית, נפה, מחוז, סטטוס —
arrived as `????` and were stored that way.

Fixing the reader fixes FUTURE loads. It cannot fix rows already in the table,
and an append table makes that worse rather than better: the corrupt text is
different bytes from the correct text, so `row_hash` differs, so a re-scrape
adds the good row BESIDE the bad one instead of replacing it. Both then answer
the same query and every count is inflated.

Hence this module. It finds rows carrying U+FFFD in any text column and deletes
them, leaving the correctly-decoded twins behind.

ORDER MATTERS, and the module enforces it. A table loaded ENTIRELY before the
reader fix has no good twins to fall back on — purging first would empty it and
take the dataset out of /data until a re-scrape lands, which can be hours. So a
purge that would remove every row is refused unless the caller passes
``allow_empty``; the correct sequence is re-poll first, purge second, so the
table always holds a complete answer.
"""
from __future__ import annotations

import logging

from app.services import append_store
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

# The character a decoder leaves behind when a byte has no mapping in the
# assumed encoding. Its presence in stored text is not a stylistic problem —
# the original letter is unrecoverable from the row, so the row is waste.
REPLACEMENT_CHAR = "\ufffd"

# Columns the pipeline adds, never the source. `geometry_wkt` is machine-written
# ASCII and `row_hash`/`first_seen` are bookkeeping; scanning them wastes a pass
# and would let a stray glyph in a coordinate string condemn a good row.
_BOOKKEEPING = {"first_seen", "row_hash", "geometry_wkt", "geom"}


async def _text_columns(conn, table: str) -> list[str]:
    rows = await conn.fetch(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema = 'public' AND table_name = $1
             AND data_type IN ('text', 'character varying')
           ORDER BY ordinal_position""",
        table,
    )
    return [r["column_name"] for r in rows if r["column_name"] not in _BOOKKEEPING]


def _predicate(columns: list[str]) -> str:
    """`WHERE` fragment matching a row with U+FFFD in ANY of ``columns``.

    The character itself is passed as `$1`, never interpolated: it is data, and
    a literal in the SQL text would have to survive this file's own encoding to
    stay correct. `position()` rather than LIKE because it searches for a
    literal, not a pattern — no escaping, no concatenated operand per column."""
    return " OR ".join(f"position($1 in {_qi(c)}) > 0" for c in columns)


async def scan(table: str) -> dict:
    """Count rows carrying the replacement character. Read-only."""
    if not append_store.is_configured():
        return {"error": "append DB not configured"}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        cols = await _text_columns(conn, table)
        if not cols:
            return {"table": table, "error": "no text columns"}
        total = await conn.fetchval(f"SELECT count(*) FROM {_qi(table)}")
        corrupt = await conn.fetchval(
            f"SELECT count(*) FROM {_qi(table)} WHERE {_predicate(cols)}",
            REPLACEMENT_CHAR)
    return {
        "table": table,
        "total": total,
        "corrupt": corrupt,
        "clean": total - corrupt,
        "columns_scanned": len(cols),
        # The caller needs this to choose the safe order, not just to report.
        "would_empty": bool(total) and corrupt == total,
    }


async def purge(table: str, *, apply: bool = False,
                allow_empty: bool = False) -> dict:
    """Delete rows carrying U+FFFD. ``apply=False`` reports the plan only.

    Refuses a purge that would empty the table unless ``allow_empty`` — see the
    module docstring on ordering."""
    s = await scan(table)
    if s.get("error"):
        return s
    if s["would_empty"] and not allow_empty:
        return {**s, "refused": (
            "every row is corrupt; purging would empty the table and take the "
            "dataset out of /data. Re-poll the dataset first so correctly "
            "decoded rows land, then purge — or pass allow_empty to override.")}
    if not apply or not s["corrupt"]:
        return {**s, "applied": False}

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        cols = await _text_columns(conn, table)
        deleted = await conn.execute(
            f"DELETE FROM {_qi(table)} WHERE {_predicate(cols)}",
            REPLACEMENT_CHAR)
        remaining = await conn.fetchval(f"SELECT count(*) FROM {_qi(table)}")
    logger.info("append_repair purge: %s deleted=%s remaining=%s",
                table, deleted, remaining)
    return {**s, "applied": True, "deleted": s["corrupt"], "remaining": remaining}


async def ensure_index(table: str, columns: list[str], *,
                       apply: bool = False) -> dict:
    """Create a btree index on ``columns`` if absent.

    An append table is born with two indexes — unique on `row_hash` for the
    dedup, GiST on `geom` for PostGIS — and neither helps a lookup by cadastral
    key. On the 1.1M-row parcels table a single-גוש filter is a sequential scan
    that burns most of the /data console's 10-second statement budget, and any
    join across גוש exceeds it outright. This is what makes the register usable
    as a join target rather than only as a download.

    CONCURRENTLY is deliberately NOT used: it cannot run inside the pool's
    transaction and this runs on a table nothing else writes to between polls.
    """
    if not append_store.is_configured():
        return {"error": "append DB not configured"}
    name = append_store._index_name(table, "_".join(columns).lower()[:20] + "_idx")
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        present = {r["column_name"] for r in await conn.fetch(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='public' AND table_name=$1""", table)}
        missing = [c for c in columns if c not in present]
        if missing:
            return {"table": table, "error": f"no such column(s): {missing}"}
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_indexes WHERE tablename=$1 AND indexname=$2",
            table, name)
        if exists:
            return {"table": table, "index": name, "created": False,
                    "note": "already present"}
        if not apply:
            return {"table": table, "index": name, "created": False,
                    "would_create": True}
        cols_sql = ", ".join(_qi(c) for c in columns)
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS {_qi(name)} ON {_qi(table)} ({cols_sql})")
    logger.info("append_repair index: %s on %s(%s)", name, table, columns)
    return {"table": table, "index": name, "created": True, "columns": columns}


# The columns an append table adds; never part of what makes a row's CONTENT.
# `first_seen` and the source's own sampling stamp are both dates ABOUT the row
# rather than facts IN it, and row_hash is derived.
def _content_columns(all_cols: list[str], stamp_col: str | None) -> list[str]:
    skip = {"first_seen", "row_hash", "geom"} | ({stamp_col} if stamp_col else set())
    return [c for c in all_cols if c not in skip]


def _content_hash_sql(cols: list[str]) -> str:
    """md5 over the content columns, with the two spellings of "absent" and the
    two spellings of a whole number folded together.

    NULL and '' both mean the source published nothing, and a scraper that
    switched between them would otherwise mark every row as changed. Likewise
    "180" and "180.0" are one number written twice — the plan register carries
    39,748 rows reading "0.0" against 183,841 reading "0", alternating by run.
    Neither is a content difference and neither should keep a duplicate alive."""
    parts = []
    for c in cols:
        q = _qi(c)
        parts.append(
            f"coalesce(CASE WHEN {q} LIKE '%.0' THEN left({q}, length({q}) - 2) "
            f"ELSE nullif({q}, '') END, '')")
    return "md5(concat_ws(chr(31)," + ",".join(parts) + "))"


async def collapse_duplicates(table: str, *, stamp_col: str | None = None,
                              apply: bool = False) -> dict:
    """Keep one row per distinct CONTENT, dropping the re-readings of it.

    The repair for tables filled before a sampling stamp was excluded from the
    dedup identity: every pass filed a fresh row for an unchanged item, so the
    archive holds one row per LOOK where it should hold one row per CHANGE. The
    plan register measured 225,277 rows over 88,854 distinct states.

    The survivor is the EARLIEST sighting of each state, and it inherits the
    LATEST stamp seen for it — the same split the fixed insert path maintains
    going forward, so a cleaned table and a table that was never broken end up
    identical: first_seen says when this state first appeared, the stamp says
    when it was last confirmed.

    ``apply=False`` reports what it would remove."""
    if not append_store.is_configured():
        return {"error": "append DB not configured"}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        all_cols = [r["column_name"] for r in await conn.fetch(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='public' AND table_name=$1
               ORDER BY ordinal_position""", table)]
        if not all_cols:
            return {"table": table, "error": "no such table"}
        if stamp_col and stamp_col not in all_cols:
            stamp_col = None
        cols = _content_columns(all_cols, stamp_col)
        h = _content_hash_sql(cols)
        total = await conn.fetchval(f"SELECT count(*) FROM {_qi(table)}")
        states = await conn.fetchval(f"SELECT count(DISTINCT {h}) FROM {_qi(table)}")
        plan = {"table": table, "rows": total, "distinct_states": states,
                "would_delete": total - states, "stamp_col": stamp_col,
                "content_columns": len(cols)}
        if not apply or total == states:
            return {**plan, "applied": False}

        async with conn.transaction():
            # ctid is the physical row address — the only handle a table with no
            # primary key has on ONE specific duplicate.
            await conn.execute(f"""
                CREATE TEMP TABLE _keep ON COMMIT DROP AS
                SELECT DISTINCT ON ({h}) ctid AS keep_ctid, {h} AS h
                FROM {_qi(table)} ORDER BY {h}, first_seen ASC, ctid""")
            if stamp_col:
                # Carry the freshest confirmation onto the row that survives,
                # BEFORE its siblings are deleted and that date is gone with
                # them. Grouped by the same content hash the survivor was chosen
                # by, so the max is taken over exactly the rows about to be
                # collapsed into it.
                await conn.execute(f"""
                    UPDATE {_qi(table)} t
                    SET {_qi(stamp_col)} = m.newest
                    FROM _keep k
                    JOIN (SELECT {h} AS h, max({_qi(stamp_col)}) AS newest
                          FROM {_qi(table)} GROUP BY 1) m ON m.h = k.h
                    WHERE t.ctid = k.keep_ctid
                      AND t.{_qi(stamp_col)} IS DISTINCT FROM m.newest""")
            deleted = await conn.execute(
                f"DELETE FROM {_qi(table)} t WHERE t.ctid NOT IN "
                f"(SELECT keep_ctid FROM _keep)")
            remaining = await conn.fetchval(f"SELECT count(*) FROM {_qi(table)}")
    logger.info("append_repair collapse: %s %s -> %s", table, total, remaining)
    return {**plan, "applied": True, "deleted": total - remaining,
            "remaining": remaining}
