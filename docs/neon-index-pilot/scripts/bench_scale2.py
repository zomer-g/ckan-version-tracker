"""The catalog cost scales with the number of column-rows the query RETURNS, not
with how many tables exist elsewhere. Trace that curve safely by building the
synthetic tables in their own schema and querying THAT schema — the same shape
public_table_columns() would face once ~2,900 index tables live in public.
"""
import asyncio, json, re, sys, time

import asyncpg

sys.stdout.reconfigure(encoding="utf-8")
ENV = dict(l.strip().split("=", 1) for l in open("pilot.env", encoding="utf-8") if "=" in l)
SCHEMA = "scale_test"
COLS_PER = 13


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


async def measure(c, schema, reps=3):
    best, n = None, 0
    for _ in range(reps):
        t = time.perf_counter()
        rows = await c.fetch("SELECT table_name, column_name, data_type FROM "
                             "information_schema.columns WHERE table_schema = $1 "
                             "ORDER BY table_name, ordinal_position", schema)
        d = time.perf_counter() - t
        n, best = len(rows), (d if best is None else min(best, d))
    return round(best * 1000, 1), n


async def main():
    c = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require", command_timeout=900)
    rtt = None
    t = time.perf_counter()
    for _ in range(5):
        await c.fetchval("SELECT 1")
    rtt = (time.perf_counter() - t) / 5 * 1000
    print(f"network RTT baseline: {rtt:.1f} ms\n")

    await c.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    await c.execute(f"CREATE SCHEMA {SCHEMA}")
    defs = ", ".join(f"c{i} text" for i in range(COLS_PER))
    curve, made = [], 0
    print(f"{'tables':>8}{'col rows':>10}{'query ms':>10}{'net of RTT':>12}{'µs/row':>9}")
    for target in (100, 500, 1000, 2000, 2910):
        while made < target:
            step = min(100, target - made)
            await c.execute("; ".join(
                f"CREATE TABLE {SCHEMA}.t{made+i} ({defs})" for i in range(step)))
            made += step
        ms, n = await measure(c, SCHEMA)
        curve.append({"tables": made, "col_rows": n, "ms": ms})
        print(f"{made:>8}{n:>10}{ms:>10.1f}{ms-rtt:>12.1f}{(ms-rtt)/max(n,1)*1000:>9.1f}")

    print("\ndropping synthetic schema…")
    await c.execute(f"DROP SCHEMA {SCHEMA} CASCADE")
    left = await c.fetchval("SELECT count(*) FROM pg_class WHERE relkind='r'")
    print(f"tables remaining in DB: {left}")
    await c.close()
    json.dump({"rtt_ms": rtt, "curve": curve}, open("bench_scale2.json", "w"), indent=1)

asyncio.run(main())
