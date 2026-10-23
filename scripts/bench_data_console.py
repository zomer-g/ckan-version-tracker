#!/usr/bin/env python3
"""Time the real /data console queries on a database, server-side.

WHY EXPLAIN (ANALYZE, BUFFERS) AND NOT A WALL CLOCK
---------------------------------------------------
The question this answers is whether OVER's public SQL console still works when
its 59 GiB lives on xhostd, whose Postgres runs a fixed, shared 1 GiB
``shared_buffers`` (measured 2026-09-03). A wall clock from a laptop measures
the home connection, not the server. ``EXPLAIN (ANALYZE, BUFFERS)`` reports the
server's own execution time AND, in the same output, how many 8 KB blocks came
from cache versus disk — which is the cache question stated directly rather than
inferred.

Run the same file against Neon and against the xhostd copy and compare the two
numbers per query. The absolute time matters against the console's 10 s budget;
the hit/read ratio explains it.

Read-only: every query runs inside a READ ONLY transaction that is rolled back.
"""
from __future__ import annotations

import argparse
import os
import re
import sys

import psycopg2

# The console pins search_path so that PostGIS, which lives in its own schema,
# resolves without every query qualifying it. Same setting here.
SEARCH_PATH = "public, knesset, idx, odata, ocal, extensions"

QUERIES: dict[str, str] = {
    # Index-driven join across the property crosswalk. Small result, four tables.
    "nadlan_address_to_parcel": """
        SELECT a.settlement_name, a.street_name, a.house_num, a.zip7, a.zip_level,
               p.gush, p.parcel, p.legal_area, g.n_dwellings, a.parcel_match
        FROM over_re_addresses a
        JOIN over_re_parcels p               ON p.parcel_key = a.parcel_key
        LEFT JOIN over_re_parcel_gazetteer g ON g.parcel_key = a.parcel_key
        WHERE a.settlement_name = 'פתח תקווה' AND a.street_name = 'אבימלך'
        ORDER BY a.house_num
    """,
    # The heavy one: a PostGIS radius function joined to the 4.5 GiB shape table,
    # with geometry simplified for the map. This is the query that decides it.
    "nadlan_parcels_near_300m": """
        SELECT n.gush, n.parcel, n.locality_name,
               round(ST_Distance(n.centroid::geography,
                     ST_SetSRID(ST_MakePoint(34.9171, 32.0789),4326)::geography)) AS d,
               ST_AsText(ST_SimplifyPreserveTopology(s.geom, 0.00002)) AS geometry_wkt
        FROM over_parcels_near(32.0789, 34.9171, 300) n
        JOIN public.append_shape_ff3176b1 s
          ON s."GUSH_NUM" = n.gush::text
         AND split_part(s."PARCEL",'.',1)::int = n.parcel
         AND coalesce(nullif(split_part(coalesce(s."GUSH_SUFFI",'0'),'.',1),'')::int,0) = n.gush_suffix
        ORDER BY d
    """,
    # A plain wide scan over the hottest big append table — no PostGIS, so it
    # isolates raw sequential-read throughput from geometry work.
    "vehicles_group_scan": """
        SELECT count(*) AS rows
        FROM public.append_private_and_commercial_vehicles_e437ab0b
    """,
}

_TIME = re.compile(r"Execution Time: ([\d.]+) ms")
_PLAN = re.compile(r"Planning Time: ([\d.]+) ms")
_BUF = re.compile(r"shared hit=(\d+)(?: read=(\d+))?")


def bench(dsn: str, label: str, only: str | None, timeout_ms: int) -> int:
    conn = psycopg2.connect(dsn, connect_timeout=30)
    conn.set_session(readonly=True, autocommit=False)
    cur = conn.cursor()

    print("=== %s ===" % label)
    print("%-30s %10s %10s %12s %12s %7s" %
          ("query", "exec_ms", "plan_ms", "buf_hit", "buf_read", "hit%"))
    bad = 0
    for name, sql in QUERIES.items():
        if only and only != name:
            continue
        # Re-applied every iteration on purpose. SET is transactional, and the
        # rollback below reverts it; a connection-level option is not portable
        # either, because Neon's pooler rejects search_path at startup.
        cur.execute("SET search_path = " + SEARCH_PATH)
        cur.execute("SET statement_timeout = %d" % timeout_ms)
        try:
            cur.execute("EXPLAIN (ANALYZE, BUFFERS) " + sql)
            plan = "\n".join(r[0] for r in cur.fetchall())
        except Exception as exc:
            conn.rollback()
            print("%-30s  FAILED: %s" % (name, str(exc).strip().splitlines()[0][:70]))
            bad += 1
            continue
        conn.rollback()
        et = _TIME.search(plan)
        pt = _PLAN.search(plan)
        hit = sum(int(m.group(1)) for m in _BUF.finditer(plan))
        read = sum(int(m.group(2) or 0) for m in _BUF.finditer(plan))
        pct = (100.0 * hit / (hit + read)) if (hit + read) else float("nan")
        print("%-30s %10s %10s %12d %12d %6.1f%%" % (
            name,
            et.group(1) if et else "?",
            pt.group(1) if pt else "?",
            hit, read, pct))
    conn.close()
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dsn", default=os.environ.get("BENCH_DSN", ""))
    ap.add_argument("--label", default="db")
    ap.add_argument("--only", default=None, help="run a single named query")
    ap.add_argument("--timeout-ms", type=int, default=120000)
    a = ap.parse_args()
    if not a.dsn:
        print("need --dsn or BENCH_DSN", file=sys.stderr)
        return 2
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    return 0 if bench(a.dsn, a.label, a.only, a.timeout_ms) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
