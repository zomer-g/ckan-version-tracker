#!/usr/bin/env python3
"""Classify the append DB's tables as hot / cold from OVER's OWN version log.

WHY NOT pg_stat_user_tables
---------------------------
``scripts/db_classify.py`` diffs two snapshots of ``pg_stat_user_tables``. That
works on a server that stays up. It does not work here: those counters live in
memory and are wiped when Postgres restarts, and Neon suspends and resumes the
compute constantly. Measured 2026-09-09 — the two samples were 158 hours apart,
yet Postgres had started 34 minutes before the second one, so 2,168 of 2,556
tables came back with counters LOWER than the first sample and classified as
"unknown". Only 47 tables had any writes recorded at all. The method cannot be
rescued by waiting longer; a longer window only makes a restart more likely.

So this tool reads the signal OVER already keeps on durable disk: the app DB's
``version_index``. A row there means "a new version of this dataset was
detected at this time" — which is exactly, and by definition, when the dataset's
append table last received rows. It survives restarts because it is data, not
statistics.

MAPPING
-------
``append_store.table_name()`` builds ``append_<base>_<dsid8>`` and
``table_name_for_resource()`` builds ``append_<base>_<dsid8>_<rid8>``, where
``dsid8`` is the first 8 hex characters of the dataset UUID with dashes removed.
So the dataset is recoverable from the table name: scan the underscore-separated
segments from the right and take the first one that is a known ``dsid8``.

Read-only: nothing here writes to either database.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

import asyncpg

# Schemas whose contents are rebuilt wholesale by a sync job. They never need a
# delta resync in the cutover window: re-run the job on the far side instead.
DERIVED_SCHEMAS = {"idx", "knesset"}

# Schemas that are leftovers and should not be copied at all.
DROP_SCHEMAS = {"pilot_idx", "idx_backup"}

# Schemas that hold the only copy of their rows and are written by their own
# importer rather than by the dataset poller, so version_index says nothing
# about them. Treated as hot unless proven otherwise.
SOURCE_SCHEMAS = {"ocal", "odata", "ocoi"}


async def dataset_activity(app_dsn: str) -> tuple[dict[str, datetime | None], list[str]]:
    """dsid8 -> most recent version detection time, plus any ambiguous prefixes."""
    conn = await asyncpg.connect(app_dsn, timeout=60)
    try:
        rows = await conn.fetch(
            """
            SELECT replace(d.id::text, '-', '') AS full_id,
                   left(replace(d.id::text, '-', ''), 8) AS dsid8,
                   max(v.detected_at) AS last_version
            FROM tracked_datasets d
            LEFT JOIN version_index v ON v.tracked_dataset_id = d.id
            GROUP BY d.id
            """
        )
    finally:
        await conn.close()

    by_prefix: dict[str, datetime | None] = {}
    seen: Counter[str] = Counter()
    for r in rows:
        seen[r["dsid8"]] += 1
        prev = by_prefix.get(r["dsid8"], "missing")
        # On a prefix collision keep the MOST RECENT activity: treating a table
        # as hotter than it is costs a delta resync; the other way loses rows.
        if prev == "missing":
            by_prefix[r["dsid8"]] = r["last_version"]
        elif r["last_version"] and (prev is None or r["last_version"] > prev):
            by_prefix[r["dsid8"]] = r["last_version"]
    return by_prefix, [p for p, n in seen.items() if n > 1]


async def append_tables(append_dsn: str) -> list[dict]:
    conn = await asyncpg.connect(append_dsn, timeout=60)
    try:
        rows = await conn.fetch(
            """
            SELECT n.nspname AS schema, c.relname AS table,
                   pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r','m','p')
              AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
            """
        )
    finally:
        await conn.close()
    return [dict(r) for r in rows]


def dsid_of(table: str, known: set[str]) -> str | None:
    """Recover the dataset id prefix from an append table name."""
    for seg in reversed(table.split("_")):
        if len(seg) == 8 and seg in known:
            return seg
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app-dsn", default=os.environ.get("APP_DSN", ""))
    ap.add_argument("--append-dsn", default=os.environ.get("APPEND_DSN", ""))
    ap.add_argument("--hot-days", type=int, default=30,
                    help="a dataset with a version newer than this is hot (default 30)")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    if not a.app_dsn or not a.append_dsn:
        print("need --app-dsn and --append-dsn", file=sys.stderr)
        return 2

    activity, collisions = asyncio.run(dataset_activity(a.app_dsn))
    tables = asyncio.run(append_tables(a.append_dsn))
    known = set(activity)
    cutoff = datetime.now(timezone.utc) - timedelta(days=a.hot_days)

    out = []
    for t in tables:
        schema, name = t["schema"], t["table"]
        dsid = last = None
        if schema in DROP_SCHEMAS:
            cls = "drop"
        elif schema in DERIVED_SCHEMAS:
            cls = "hot-derived"          # rebuilt by a sync job on the far side
        elif schema in SOURCE_SCHEMAS:
            cls = "hot-source"           # own importer; version_index is silent
        elif schema != "public":
            cls = "hot-source"
        elif not name.startswith("append_"):
            cls = "hot-source"           # over_re_*, site index, functions' tables
        else:
            dsid = dsid_of(name, known)
            if dsid is None:
                cls = "orphan"           # no tracked dataset owns this table
            else:
                last = activity.get(dsid)
                cls = "cold" if (last is None or last < cutoff) else "hot-source"
        out.append({
            "schema": schema, "table": name, "class": cls,
            "dsid8": dsid or "", "last_version": last.isoformat() if last else "",
            "total_bytes": t["total_bytes"],
        })

    out.sort(key=lambda r: (r["class"], -(r["total_bytes"] or 0)))
    with open(a.out, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0].keys()))
        w.writeheader()
        w.writerows(out)

    summary: dict[str, list[int]] = {}
    for r in out:
        s = summary.setdefault(r["class"], [0, 0])
        s[0] += 1
        s[1] += r["total_bytes"] or 0
    print("%s: %d tables, hot window = %d days" % (a.out, len(out), a.hot_days))
    for cls in sorted(summary):
        n, b = summary[cls]
        print("  %-14s %6d tables  %8.2f GiB" % (cls, n, b / 1024**3))
    if collisions:
        print("  NOTE: %d dsid8 prefixes are shared by more than one dataset; "
              "those tables took the most recent of them." % len(collisions))
    return 0


if __name__ == "__main__":
    sys.exit(main())
