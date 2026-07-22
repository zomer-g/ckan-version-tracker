"""Query benchmark on the pilot tables — the actual payoff of the mirror.
Measures cold-ish latency for the query shapes the /data console will run."""
import asyncio, json, os, re, sys, time

import asyncpg

sys.stdout.reconfigure(encoding="utf-8")
SCRATCH = os.path.dirname(os.path.abspath(__file__))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(SCRATCH, "pilot.env"), encoding="utf-8") if "=" in l)
SCHEMA = "pilot_idx"


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


def qi(n): return '"' + n.replace('"', '""') + '"'


async def timed(conn, sql, *a, reps=3):
    best = None
    rows = None
    for _ in range(reps):
        t0 = time.perf_counter()
        rows = await conn.fetch(sql, *a)
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return round(best * 1000, 1), rows


async def main():
    res = json.load(open(os.path.join(SCRATCH, "results.json"), encoding="utf-8"))
    res = [r for r in res if not r.get("error")]
    conn = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require", command_timeout=600)
    out = []
    for r in sorted(res, key=lambda x: x["csv_bytes"]):
        tbl, cols = r["table"], r["col_names"]
        # a text column that isn't the geometry
        txt = next((c for c in cols if c.lower() not in ("geometry_wkt", "objectid", "_id")), cols[0])
        ent = {"table": tbl, "title": r["title"], "rows": r["rows"],
               "mb": round(r["total"] / 2**20, 2)}
        ent["count_ms"], _ = await timed(conn, f"SELECT count(*) FROM {qi(SCHEMA)}.{qi(tbl)}")
        ent["limit20_ms"], _ = await timed(conn, f"SELECT * FROM {qi(SCHEMA)}.{qi(tbl)} LIMIT 20")
        ent["ilike_ms"], rws = await timed(
            conn, f"SELECT count(*) FROM {qi(SCHEMA)}.{qi(tbl)} WHERE {qi(txt)} ILIKE $1", "%א%")
        ent["ilike_hits"] = rws[0][0] if rws else None
        ent["ilike_col"] = txt
        ent["groupby_ms"], _ = await timed(
            conn, f"SELECT {qi(txt)}, count(*) FROM {qi(SCHEMA)}.{qi(tbl)} "
                  f"GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
        out.append(ent)
        print(f"{ent['mb']:9.2f} MB {ent['rows']:>9,} rows | count {ent['count_ms']:>8.1f} ms | "
              f"limit20 {ent['limit20_ms']:>7.1f} | ILIKE {ent['ilike_ms']:>9.1f} | "
              f"groupby {ent['groupby_ms']:>9.1f} | {ent['title'][:28]}")

    # Cross-table search: the "search across all index tables" use case
    tables = [r["table"] for r in res]
    print(f"\nCross-table UNION scan over {len(tables)} tables (count only):")
    union = " UNION ALL ".join(f"SELECT count(*) AS n FROM {qi(SCHEMA)}.{qi(t)}" for t in tables)
    ms, rws = await timed(conn, f"SELECT sum(n) FROM ({union}) x")
    print(f"  total rows {rws[0][0]:,} in {ms} ms")

    await conn.close()
    json.dump(out, open(os.path.join(SCRATCH, "bench.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)


asyncio.run(main())
