"""Catalog-scale load test: the /data console's own metadata queries.

build_catalog() runs list_public_tables() (planner estimates for every table) and
public_table_columns() (information_schema.columns for every table) on EVERY page
load. Today that covers ~114 tables; the rollout would take it to ~3,000. This
measures both against the pilot schema and against public, so we can see how the
cost scales with table count.
"""
import asyncio, json, os, re, sys, time

import asyncpg

sys.stdout.reconfigure(encoding="utf-8")
ENV = dict(l.strip().split("=", 1) for l in open("pilot.env", encoding="utf-8") if "=" in l)


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


# Verbatim shapes from app/services/append_store.py
EST_SQL = """
    SELECT c.relname AS table, c.reltuples::bigint AS est
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = $1 AND c.relkind = 'r'
"""
COLS_SQL = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = $1
    ORDER BY table_name, ordinal_position
"""


async def timed(conn, sql, *a, reps=3):
    best, rows = None, None
    for _ in range(reps):
        t = time.perf_counter()
        rows = await conn.fetch(sql, *a)
        d = time.perf_counter() - t
        best = d if best is None else min(best, d)
    return round(best * 1000, 1), len(rows)


async def main():
    c = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require", command_timeout=600)
    out = {}
    for schema in ("public", "pilot_idx", "knesset"):
        n = await c.fetchval(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace "
            "WHERE ns.nspname=$1 AND c.relkind='r'", schema)
        est_ms, est_n = await timed(c, EST_SQL, schema)
        col_ms, col_n = await timed(c, COLS_SQL, schema)
        out[schema] = {"tables": n, "est_ms": est_ms, "cols_ms": col_ms, "col_rows": col_n}
        print(f"{schema:<11} {n:>5} tables | list_public_tables {est_ms:>7.1f} ms | "
              f"public_table_columns {col_ms:>7.1f} ms ({col_n:,} column rows)")

    # Whole-database sweep = what the console would face post-rollout
    tot = await c.fetchval("SELECT count(*) FROM pg_class WHERE relkind='r'")
    t = time.perf_counter()
    rows = await c.fetch("SELECT table_schema, table_name, column_name FROM information_schema.columns")
    allms = round((time.perf_counter() - t) * 1000, 1)
    print(f"\nwhole-DB: {tot} tables, information_schema.columns → {len(rows):,} rows in {allms} ms")
    out["whole_db"] = {"tables": tot, "col_rows": len(rows), "ms": allms}
    await c.close()
    json.dump(out, open("bench_catalog.json", "w"), indent=1)

asyncio.run(main())
