#!/usr/bin/env python3
"""Classify every table in a Postgres database as cold / hot, for a migration.

WHY TWO SAMPLES
---------------
The obvious source for "did this table change recently" is
``pg_stat_user_tables.n_tup_ins/upd/del``. Those counters are CUMULATIVE since
the statistics were last reset — they are not a 30-day window, and on OVER's
append DB ``pg_stat_database.stats_reset`` is NULL, so there is not even a
reference point to subtract. A table bulk-loaded once in June and untouched
since looks exactly as busy as one written every hour.

So this tool works in two passes:

    # now
    python scripts/db_classify.py sample --dsn "$APPEND_DSN" --out sample1.json
    # >= 24h later
    python scripts/db_classify.py sample --dsn "$APPEND_DSN" --out sample2.json
    python scripts/db_classify.py classify --a sample1.json --b sample2.json \
        --out classify.csv

The DIFFERENCE between two samples is a real observation window. Anything with
zero writes across a window that spans a full cycle of the scheduler's jobs is
cold; everything else is hot, and only the hot tables need a delta resync in the
cutover window (docs/xhostd-db-migration/over-migration-plan-v2.md §8, §13).

The classifier never decides `hot-derived` on its own: whether a changing table
is rebuilt by a sync job (idx, knesset) or is the only copy of its rows
(ocal, odata, ocoi, public.append_*) is a fact about OVER, not about Postgres.
It is applied from DERIVED_SCHEMAS below and can be overridden per table.

Read-only: nothing here writes to the database.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone

import asyncpg

# Schemas whose contents a scheduled job rebuilds from an external source of
# truth. Copy them once plus their checkpoint table; the next tick self-heals.
DERIVED_SCHEMAS = {"idx", "knesset"}

# Schemas that must never be copied at all.
DROP_SCHEMAS = {"pilot_idx", "idx_backup"}

SAMPLE_SQL = """
SELECT
    n.nspname                                   AS schema,
    c.relname                                   AS "table",
    s.n_tup_ins                                 AS ins,
    s.n_tup_upd                                 AS upd,
    s.n_tup_del                                 AS del,
    s.n_live_tup                                AS live,
    s.n_dead_tup                                AS dead,
    s.last_autoanalyze                          AS last_autoanalyze,
    s.last_autovacuum                           AS last_autovacuum,
    s.last_analyze                              AS last_analyze
FROM pg_stat_user_tables s
JOIN pg_class c     ON c.oid = s.relid
JOIN pg_namespace n ON n.oid = c.relnamespace
"""

SIZE_SQL = """
SELECT n.nspname AS schema,
       c.relname AS "table",
       pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
"""


async def take_sample(dsn: str, with_sizes: bool, timeout_s: float) -> dict:
    conn = await asyncpg.connect(dsn, timeout=30)
    try:
        # The console pools carry a 10s statement_timeout; a direct connection
        # may inherit a role/database default. pg_total_relation_size over
        # thousands of tables stats every fork on disk and is genuinely slow,
        # so give this session room. Read-only, so a long statement is safe.
        await conn.execute(f"SET statement_timeout = {int(timeout_s * 1000)}")
        rows = [dict(r) for r in await conn.fetch(SAMPLE_SQL)]
        sizes: dict[tuple[str, str], int] = {}
        if with_sizes:
            for r in await conn.fetch(SIZE_SQL):
                sizes[(r["schema"], r["table"])] = r["total_bytes"]
        db = await conn.fetchval("SELECT current_database()")
        dbsize = await conn.fetchval("SELECT pg_database_size(current_database())")
        reset = await conn.fetchval(
            "SELECT stats_reset FROM pg_stat_database WHERE datname = current_database()"
        )
    finally:
        await conn.close()

    for r in rows:
        r["total_bytes"] = sizes.get((r["schema"], r["table"]))
        for k in ("last_autoanalyze", "last_autovacuum", "last_analyze"):
            if r[k] is not None:
                r[k] = r[k].isoformat()

    return {
        "sampled_at": datetime.now(timezone.utc).isoformat(),
        "database": db,
        "database_bytes": dbsize,
        "stats_reset": reset.isoformat() if reset else None,
        "tables": rows,
    }


def classify(a: dict, b: dict) -> tuple[list[dict], dict]:
    t0 = datetime.fromisoformat(a["sampled_at"])
    t1 = datetime.fromisoformat(b["sampled_at"])
    window_h = (t1 - t0).total_seconds() / 3600.0
    if window_h < 20:
        print(
            f"WARNING: the two samples are only {window_h:.1f}h apart. "
            "A table written daily can look cold in a window shorter than its "
            "own cycle. Take a later second sample before trusting 'cold'.",
            file=sys.stderr,
        )

    prev = {(r["schema"], r["table"]): r for r in a["tables"]}
    out: list[dict] = []
    for r in b["tables"]:
        key = (r["schema"], r["table"])
        p = prev.get(key)
        writes = None
        if p is not None:
            writes = (
                (r["ins"] - p["ins"]) + (r["upd"] - p["upd"]) + (r["del"] - p["del"])
            )
            # A counter that went backwards means the stats were reset between
            # samples; the window is meaningless for this table.
            if writes < 0:
                writes = None

        if r["schema"] in DROP_SCHEMAS:
            cls = "drop"
        elif p is None:
            cls = "new"  # table did not exist at sample 1 — by definition hot
        elif writes is None:
            cls = "unknown"
        elif writes == 0:
            cls = "cold"
        elif r["schema"] in DERIVED_SCHEMAS:
            cls = "hot-derived"
        else:
            cls = "hot-source"

        out.append(
            {
                "schema": r["schema"],
                "table": r["table"],
                "class": cls,
                "writes_in_window": writes,
                "live_rows": r["live"],
                "total_bytes": r["total_bytes"],
                "last_autoanalyze": r["last_autoanalyze"],
            }
        )

    out.sort(key=lambda r: (r["class"], -(r["total_bytes"] or 0)))

    summary: dict[str, dict[str, int]] = {}
    for r in out:
        s = summary.setdefault(r["class"], {"tables": 0, "bytes": 0})
        s["tables"] += 1
        s["bytes"] += r["total_bytes"] or 0
    summary["_window_hours"] = round(window_h, 1)  # type: ignore[assignment]
    return out, summary


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sample", help="write a counter snapshot")
    s.add_argument("--dsn", default=os.environ.get("CLASSIFY_DSN", ""))
    s.add_argument("--out", required=True)
    s.add_argument("--no-sizes", action="store_true")
    s.add_argument("--timeout", type=float, default=600.0)

    c = sub.add_parser("classify", help="diff two snapshots into a CSV")
    c.add_argument("--a", required=True, help="earlier sample")
    c.add_argument("--b", required=True, help="later sample")
    c.add_argument("--out", required=True)

    args = ap.parse_args()

    if args.cmd == "sample":
        if not args.dsn:
            print("no DSN: pass --dsn or set CLASSIFY_DSN", file=sys.stderr)
            return 2
        data = asyncio.run(take_sample(args.dsn, not args.no_sizes, args.timeout))
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False)
        total = sum(r["total_bytes"] or 0 for r in data["tables"])
        print(
            f"{args.out}: {len(data['tables'])} tables, "
            f"{total / 1024**3:.1f} GiB in user tables, "
            f"db {data['database_bytes'] / 1024**3:.1f} GiB, "
            f"at {data['sampled_at']}"
        )
        return 0

    with open(args.a, encoding="utf-8") as fh:
        a = json.load(fh)
    with open(args.b, encoding="utf-8") as fh:
        b = json.load(fh)
    rows, summary = classify(a, b)
    with open(args.out, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"{args.out}: {len(rows)} tables")
    for k, v in summary.items():
        if k.startswith("_"):
            print(f"  {k}: {v}")
        else:
            print(f"  {k:12s} {v['tables']:5d} tables  {v['bytes'] / 1024**3:8.2f} GiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
