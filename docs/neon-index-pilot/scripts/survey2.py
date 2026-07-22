"""Full survey via S3 API (no r2.dev rate limit): exact size of every latest-version
index CSV for govmap+scraper datasets. Writes survey.json."""
import asyncio, json, os, re, sys

import asyncpg, boto3
from botocore.config import Config

sys.stdout.reconfigure(encoding="utf-8")
SCRATCH = os.path.dirname(os.path.abspath(__file__))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(SCRATCH, "pilot.env"), encoding="utf-8") if "=" in l)
CSV_KEY = "נתוני הסורק"


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


def s3():
    return boto3.client("s3", endpoint_url=ENV["S3_ENDPOINT"],
                        aws_access_key_id=ENV["S3_ACCESS_KEY"],
                        aws_secret_access_key=ENV["S3_SECRET_KEY"],
                        region_name=ENV.get("S3_REGION", "auto"),
                        config=Config(retries={"max_attempts": 5, "mode": "standard"}))


async def load_candidates():
    conn = await asyncpg.connect(dsn(ENV["DATABASE_URL"]), ssl="require")
    rows = await conn.fetch("""
        SELECT DISTINCT ON (d.id)
               d.id, d.title, d.ckan_name, d.source_type, d.scraper_config,
               v.id AS version_id, v.version_number, v.resource_mappings
        FROM tracked_datasets d
        JOIN version_index v ON v.tracked_dataset_id = d.id
        WHERE d.status = 'active' AND d.source_type IN ('govmap','scraper')
        ORDER BY d.id, v.version_number DESC""")
    await conn.close()
    out = []
    for r in rows:
        m = r["resource_mappings"] or {}
        if isinstance(m, str):
            m = json.loads(m)
        v = m.get(CSV_KEY)
        if not (isinstance(v, str) and v.startswith("r2:")):
            continue
        sc = r["scraper_config"]
        sc = json.loads(sc) if isinstance(sc, str) else (sc or {})
        out.append({"dataset_id": str(r["id"]), "title": r["title"], "ckan_name": r["ckan_name"],
                    "source_type": r["source_type"], "kind": sc.get("kind"),
                    "layer_id": sc.get("layer_id"),
                    "version_id": str(r["version_id"]), "version_number": r["version_number"],
                    "key": v[3:]})
    return out


def bucket_index(cl, bucket):
    """key -> size for the whole bucket (one paginated LIST)."""
    sizes, total, n = {}, 0, 0
    tok = None
    while True:
        kw = {"Bucket": bucket, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        resp = cl.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            sizes[o["Key"]] = o["Size"]
            total += o["Size"]
            n += 1
        if not resp.get("IsTruncated"):
            break
        tok = resp["NextContinuationToken"]
    print(f"bucket objects: {n}, total {total/2**30:.2f} GB")
    return sizes


def main():
    cands = asyncio.run(load_candidates())
    print("candidates (latest version has an index CSV):", len(cands))
    cl = s3()
    sizes = bucket_index(cl, ENV["S3_BUCKET"])
    miss = 0
    for c in cands:
        c["size"] = sizes.get(c["key"], 0)
        miss += c["size"] == 0
    ok = [c for c in cands if c["size"] > 0]
    ok.sort(key=lambda c: -c["size"])
    print(f"with size: {len(ok)}  missing objects: {miss}")
    tot = sum(c["size"] for c in ok)
    print(f"TOTAL index-CSV bytes: {tot/2**30:.3f} GB   mean {tot/len(ok)/2**20:.2f} MB")
    for st in ("govmap", "scraper"):
        g = [c for c in ok if c["source_type"] == st]
        s = sum(c["size"] for c in g)
        print(f"  {st}: {len(g)} datasets, {s/2**30:.3f} GB, median "
              f"{sorted(x['size'] for x in g)[len(g)//2]/2**10:.1f} KB")
    json.dump(ok, open(os.path.join(SCRATCH, "survey.json"), "w", encoding="utf-8"), ensure_ascii=False)
    print("\nTop 12:")
    for c in ok[:12]:
        print(f"  {c['size']/2**20:9.2f} MB {c['source_type']:<7} {c['title'][:45]}")


main()
