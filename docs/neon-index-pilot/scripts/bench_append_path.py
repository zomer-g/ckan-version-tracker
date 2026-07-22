"""Cost comparison: the EXISTING append_store path (multi-row INSERT + _row_hash
unique index + ON CONFLICT DO NOTHING) vs. the pilot's COPY path, for the same
CSV. Faithfully replicates app/services/append_store.py's shape but writes into
the pilot schema so production is untouched.
"""
import asyncio, csv, hashlib, json, os, re, sys, time

import asyncpg, boto3
from botocore.config import Config

sys.stdout.reconfigure(encoding="utf-8")
csv.field_size_limit(10 * 1024 * 1024)
SCRATCH = os.path.dirname(os.path.abspath(__file__))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(SCRATCH, "pilot.env"), encoding="utf-8") if "=" in l)
SCHEMA = "pilot_idx"
MAX_PARAMS = 30000


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]
def qi(n): return '"' + n.replace('"', '""') + '"'


def row_hash(row, cols):
    """Mirrors append_store.row_hash."""
    h = hashlib.sha256()
    for c in cols:
        h.update((row.get(c) or "").encode("utf-8", "replace"))
        h.update(b"\x1f")
    return h.hexdigest()


async def run(target_id):
    picks = json.load(open(os.path.join(SCRATCH, "pilot_set_full.json"), encoding="utf-8"))
    c = next(p for p in picks if p["dataset_id"].startswith(target_id))
    cl = boto3.client("s3", endpoint_url=ENV["S3_ENDPOINT"],
                      aws_access_key_id=ENV["S3_ACCESS_KEY"],
                      aws_secret_access_key=ENV["S3_SECRET_KEY"],
                      region_name="auto", config=Config(retries={"max_attempts": 5}))
    tmp = os.path.join(SCRATCH, "tmp_append.csv")
    cl.download_file(ENV["S3_BUCKET"], c["key"], tmp)

    with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.DictReader(fh)
        cols = [x for x in (rdr.fieldnames or []) if x and x != "_id"]
        rows = [{k: (r.get(k) or "") for k in cols} for r in rdr]
    print(f"{c['title'][:40]}: {len(rows):,} rows × {len(cols)} cols  "
          f"({c['size']/2**20:.1f} MB CSV)")

    conn = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require", command_timeout=3600)
    tbl = f"bench_append_{target_id[:8]}"
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(SCHEMA)}")
    await conn.execute(f"DROP TABLE IF EXISTS {qi(SCHEMA)}.{qi(tbl)}")
    defs = ", ".join(f"{qi(x)} text" for x in cols)
    await conn.execute(f"""CREATE TABLE {qi(SCHEMA)}.{qi(tbl)} (
        {defs}, "_row_hash" text, "_first_seen" timestamptz NOT NULL DEFAULT now())""")
    await conn.execute(
        f'CREATE UNIQUE INDEX {qi(tbl + "_rh")} ON {qi(SCHEMA)}.{qi(tbl)} ("_row_hash")')

    t_hash = time.perf_counter()
    hashes = [row_hash(r, cols) for r in rows]
    hash_secs = time.perf_counter() - t_hash

    ins_cols = cols + ["_row_hash"]
    n = len(ins_cols)
    chunk = max(1, MAX_PARAMS // n)
    cols_sql = ", ".join(qi(x) for x in ins_cols)
    t0 = time.perf_counter()
    for i in range(0, len(rows), chunk):
        part, ph, params = rows[i:i + chunk], [], []
        for j, r in enumerate(part):
            vals = [r.get(x) for x in cols] + [hashes[i + j]]
            ph.append("(" + ", ".join(f"${len(params) + k + 1}" for k in range(n)) + ")")
            params.extend(vals)
        await conn.execute(
            f"INSERT INTO {qi(SCHEMA)}.{qi(tbl)} ({cols_sql}) VALUES {', '.join(ph)} "
            f'ON CONFLICT ("_row_hash") DO NOTHING', *params)
    insert_secs = time.perf_counter() - t0

    await conn.execute(f"ANALYZE {qi(SCHEMA)}.{qi(tbl)}")
    sz = await conn.fetchrow(
        "SELECT pg_total_relation_size($1::regclass) t, pg_relation_size($1::regclass) h, "
        "pg_indexes_size($1::regclass) i", f"{SCHEMA}.{tbl}")

    prev = json.load(open(os.path.join(SCRATCH, "results.json"), encoding="utf-8"))
    p = next((r for r in prev if r["dataset_id"].startswith(target_id)), {})
    out = {
        "dataset": c["title"], "rows": len(rows), "cols": len(cols),
        "csv_mb": round(c["size"] / 2**20, 2),
        "append_path": {"hash_secs": round(hash_secs, 2), "insert_secs": round(insert_secs, 2),
                        "total_secs": round(hash_secs + insert_secs, 2),
                        "table_mb": round(sz["t"] / 2**20, 2),
                        "index_mb": round(sz["i"] / 2**20, 2)},
        "copy_path": {"total_secs": p.get("copy_secs"), "table_mb": round(p.get("total", 0) / 2**20, 2)},
    }
    a, b = out["append_path"], out["copy_path"]
    print(f"  APPEND path: {a['total_secs']}s (hash {a['hash_secs']}s + insert {a['insert_secs']}s), "
          f"{a['table_mb']} MB (of which index {a['index_mb']} MB)")
    print(f"  COPY   path: {b['total_secs']}s, {b['table_mb']} MB")
    if b["total_secs"]:
        print(f"  → append is {a['total_secs']/b['total_secs']:.1f}x slower, "
              f"{a['table_mb']/max(b['table_mb'],.01):.2f}x the storage")
    await conn.execute(f"DROP TABLE {qi(SCHEMA)}.{qi(tbl)}")
    await conn.close()
    os.remove(tmp)
    return out


async def main():
    res = []
    for t in sys.argv[1:] or ["29b97437", "03c59ca7"]:
        res.append(await run(t))
    json.dump(res, open(os.path.join(SCRATCH, "bench_append.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)

asyncio.run(main())
