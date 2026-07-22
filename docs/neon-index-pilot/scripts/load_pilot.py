"""Pilot loader — mirror each dataset's latest-version index CSV into NEON.

Semantics (per the pilot spec): LATEST VERSION ONLY. Each run builds a fresh
table and atomically swaps it in; no per-version history in NEON (history stays
in the R2 snapshots). Loaded with COPY (no row_hash / no dedup) — this is the
cheap path a "latest-only" mirror allows, unlike the append_store INSERT path.

Everything lands in schema `pilot_idx` so production catalog/tables are untouched.

Measures per dataset: download secs+bytes, COPY secs, rows, cols, heap/toast/index
bytes, DB logical-size delta, peak RSS.
"""
import asyncio, csv, io, json, os, re, sys, time

import asyncpg, boto3
from botocore.config import Config

sys.stdout.reconfigure(encoding="utf-8")
csv.field_size_limit(10**8)  # match app/api/worker.py

SCRATCH = os.path.dirname(os.path.abspath(__file__))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(SCRATCH, "pilot.env"), encoding="utf-8") if "=" in l)
SCHEMA = "pilot_idx"
COPY_BATCH = 20_000


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


def s3():
    return boto3.client("s3", endpoint_url=ENV["S3_ENDPOINT"],
                        aws_access_key_id=ENV["S3_ACCESS_KEY"],
                        aws_secret_access_key=ENV["S3_SECRET_KEY"],
                        region_name=ENV.get("S3_REGION", "auto"),
                        config=Config(retries={"max_attempts": 5, "mode": "standard"},
                                      max_pool_connections=8))


def qi(n): return '"' + n.replace('"', '""') + '"'


def table_name(c):
    slug = re.sub(r"[^a-z0-9_]+", "_", (c["ckan_name"] or "").lower()).strip("_")[:40] or "ds"
    return f"idx_{slug}_{c['dataset_id'][:8]}"


def clip_bytes(s: str, limit: int) -> str:
    """Truncate to `limit` UTF-8 BYTES without splitting a character.

    Postgres identifiers are capped at NAMEDATALEN-1 = 63 BYTES, not characters.
    A Hebrew header is 2 bytes/char, so a 55-char name is 94 bytes and the server
    silently truncates it to ~38 chars — which is how two distinct Hebrew columns
    collide into one identifier. Dedup must therefore run on the CLIPPED name.
    """
    b = s.encode("utf-8")
    return s if len(b) <= limit else b[:limit].decode("utf-8", "ignore")


def dedup_cols(names):
    """CSV headers can repeat / be blank / collide after byte-truncation;
    make them unique, non-empty and <=63 bytes."""
    out, seen = [], {}
    for i, n in enumerate(names):
        n = (n or "").strip().replace("\x00", "") or f"col_{i+1}"
        n = clip_bytes(n, 63)
        k = n.lower()
        if k in seen:
            seen[k] += 1
            # leave room for the "_N" suffix inside the 63-byte budget
            suffix = f"_{seen[k]}"
            n = clip_bytes(n, 63 - len(suffix.encode())) + suffix
        else:
            seen[k] = 0
        out.append(n)
    return out


async def db_size(conn):
    return int(await conn.fetchval("SELECT pg_database_size(current_database())"))


async def table_sizes(conn, tbl):
    row = await conn.fetchrow("""
        SELECT pg_total_relation_size($1::regclass) AS total,
               pg_relation_size($1::regclass) AS heap,
               pg_indexes_size($1::regclass) AS idx,
               COALESCE(pg_total_relation_size(reltoastrelid),0) AS toast
        FROM pg_class WHERE oid = $1::regclass""", f"{SCHEMA}.{tbl}")
    return dict(row)


def download(cl, key, dest):
    t0 = time.perf_counter()
    cl.download_file(ENV["S3_BUCKET"], key, dest)
    return time.perf_counter() - t0, os.path.getsize(dest)


async def load_one(pool, cl, c, report):
    tbl = table_name(c)
    tmp = os.path.join(SCRATCH, "tmpdl.csv")
    rec = {"dataset_id": c["dataset_id"], "title": c["title"], "source_type": c["source_type"],
           "note": c.get("note") or c.get("bucket") or "", "csv_bytes": c["size"], "table": tbl,
           "version_number": c["version_number"]}
    print(f"\n=== {c['title'][:45]}  ({c['size']/2**20:.3f} MB)  → {tbl}")

    dl_secs, dl_bytes = await asyncio.to_thread(download, cl, c["key"], tmp)
    rec["download_secs"] = round(dl_secs, 2)
    rec["download_mbps"] = round(dl_bytes / 2**20 / max(dl_secs, .001), 2)
    print(f"    downloaded {dl_bytes/2**20:.2f} MB in {dl_secs:.1f}s ({rec['download_mbps']} MB/s)")

    # --- read header, build the staging table
    with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
        hdr = next(csv.reader(fh), None)
    if not hdr:
        rec["error"] = "empty csv"
        return rec
    cols = dedup_cols(hdr)
    rec["cols"] = len(cols)
    rec["col_names"] = cols[:40]
    stg = f"{tbl}__stg"

    async with pool.acquire() as conn:
        size_before = await db_size(conn)
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qi(SCHEMA)}")
        await conn.execute(f"DROP TABLE IF EXISTS {qi(SCHEMA)}.{qi(stg)}")
        defs = ", ".join(f"{qi(x)} text" for x in cols)
        await conn.execute(f"CREATE TABLE {qi(SCHEMA)}.{qi(stg)} ({defs})")

        t0 = time.perf_counter()
        total = 0
        ncols = len(cols)

        def batches():
            with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
                rdr = csv.reader(fh)
                next(rdr, None)
                buf = []
                for row in rdr:
                    if len(row) < ncols:
                        row = row + [None] * (ncols - len(row))
                    elif len(row) > ncols:
                        row = row[:ncols]
                    buf.append([(v.replace("\x00", "") if isinstance(v, str) else v) for v in row])
                    if len(buf) >= COPY_BATCH:
                        yield buf
                        buf = []
                if buf:
                    yield buf

        for buf in batches():
            await conn.copy_records_to_table(stg, schema_name=SCHEMA, columns=cols, records=buf)
            total += len(buf)
            if total % 200_000 == 0:
                print(f"      {total:,} rows… ({total/(time.perf_counter()-t0):,.0f} rows/s)")
        copy_secs = time.perf_counter() - t0
        rec["rows"] = total
        rec["copy_secs"] = round(copy_secs, 2)
        rec["rows_per_sec"] = round(total / max(copy_secs, .001))

        # --- atomic swap: latest-version-only semantics
        t1 = time.perf_counter()
        async with conn.transaction():
            await conn.execute(f"DROP TABLE IF EXISTS {qi(SCHEMA)}.{qi(tbl)}")
            await conn.execute(f"ALTER TABLE {qi(SCHEMA)}.{qi(stg)} RENAME TO {qi(tbl)}")
        rec["swap_secs"] = round(time.perf_counter() - t1, 3)

        await conn.execute(f"ANALYZE {qi(SCHEMA)}.{qi(tbl)}")
        sz = await table_sizes(conn, tbl)
        rec.update({k: int(v) for k, v in sz.items()})
        rec["db_size_delta"] = await db_size(conn) - size_before

    os.remove(tmp)
    rec["bytes_per_row"] = round(rec["total"] / max(rec["rows"], 1), 1)
    rec["neon_vs_csv"] = round(rec["total"] / max(rec["csv_bytes"], 1), 3)
    print(f"    rows={rec['rows']:,} cols={rec['cols']} copy={rec['copy_secs']}s "
          f"({rec['rows_per_sec']:,}/s) table={rec['total']/2**20:.2f} MB "
          f"(heap {sz['heap']/2**20:.1f} / toast {sz['toast']/2**20:.1f}) "
          f"ratio_vs_csv={rec['neon_vs_csv']}x")
    report.append(rec)
    json.dump(report, open(os.path.join(SCRATCH, "results.json"), "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    return rec


async def main():
    only = sys.argv[1:] or None
    picks = json.load(open(os.path.join(SCRATCH, "pilot_set.json"), encoding="utf-8"))
    if only:
        picks = [p for p in picks if any(p["dataset_id"].startswith(o) for o in only)]
    rp = os.path.join(SCRATCH, "results.json")
    report = json.load(open(rp, encoding="utf-8")) if os.path.exists(rp) else []
    done = {r["dataset_id"] for r in report}
    picks = [p for p in picks if p["dataset_id"] not in done]
    print(f"loading {len(picks)} dataset(s)")

    pool = await asyncpg.create_pool(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require",
                                     min_size=1, max_size=2, command_timeout=3600)
    cl = s3()
    t0 = time.perf_counter()
    async with pool.acquire() as conn:
        print("DB logical size at start:",
              round(await db_size(conn) / 2**30, 3), "GB")
    for c in sorted(picks, key=lambda x: x["size"]):   # small → large
        try:
            await load_one(pool, cl, c, report)
        except Exception as e:
            print(f"    FAILED: {type(e).__name__}: {e}")
            report.append({"dataset_id": c["dataset_id"], "title": c["title"],
                           "csv_bytes": c["size"], "error": f"{type(e).__name__}: {e}"})
            json.dump(report, open(rp, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    async with pool.acquire() as conn:
        print("\nDB logical size at end:", round(await db_size(conn) / 2**30, 3), "GB")
    await pool.close()
    print(f"wall clock: {(time.perf_counter()-t0)/60:.1f} min")


asyncio.run(main())
