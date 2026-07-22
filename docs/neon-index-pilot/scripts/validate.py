"""Validation: predicted vs actual for the 100-dataset run, per size bucket,
plus a re-extrapolation of the whole corpus using the now much larger sample."""
import asyncio, json, os, re, sys

import asyncpg

sys.stdout.reconfigure(encoding="utf-8")
SCRATCH = os.path.dirname(os.path.abspath(__file__))
ENV = dict(l.strip().split("=", 1) for l in open(os.path.join(SCRATCH, "pilot.env"), encoding="utf-8") if "=" in l)


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


picks = {c["dataset_id"]: c for c in json.load(open("pilot_set_100.json", encoding="utf-8"))}
res = json.load(open("results.json", encoding="utf-8"))
ok = [r for r in res if not r.get("error")]
bad = [r for r in res if r.get("error")]
pred = json.load(open("prediction_100.json"))

BUCKETS = ["<10 KB", "10–100 KB", "100 KB–1 MB", "1–10 MB", "10–100 MB", ">100 MB"]
for r in ok:
    p = picks[r["dataset_id"]]
    r["bucket"], r["pred_ratio"], r["pred_mbps"] = p["bucket"], p["pred_ratio"], p["pred_mbps"]

print("=" * 96)
print("PER-BUCKET: predicted vs actual")
print("=" * 96)
print(f"{'bucket':<14}{'n':>4}{'CSV MB':>10}{'pred MB':>10}{'actual MB':>11}{'err':>8}"
      f"{'pred x':>8}{'act x':>8}{'MB/s pred':>11}{'MB/s act':>10}")
new_ratio, new_thru = {}, {}
for b in BUCKETS:
    g = [r for r in ok if r["bucket"] == b]
    if not g:
        continue
    csv_b = sum(r["csv_bytes"] for r in g)
    pr = sum(r["csv_bytes"] * r["pred_ratio"] for r in g)
    ac = sum(r["total"] for r in g)
    secs = sum(r["copy_secs"] for r in g)
    thru = (csv_b / 2**20) / max(secs, .001)
    new_ratio[b] = ac / csv_b
    new_thru[b] = thru
    print(f"{b:<14}{len(g):>4}{csv_b/2**20:>10.2f}{pr/2**20:>10.2f}{ac/2**20:>11.2f}"
          f"{(ac-pr)/max(pr,1)*100:>7.1f}%{g[0]['pred_ratio']:>8.2f}{ac/csv_b:>8.2f}"
          f"{g[0]['pred_mbps']:>11.2f}{thru:>10.2f}")

csv_t = sum(r["csv_bytes"] for r in ok)
act_t = sum(r["total"] for r in ok)
copy_t = sum(r["copy_secs"] for r in ok)
dl_t = sum(r["download_secs"] for r in ok)
print("-" * 96)
print(f"{'TOTAL':<14}{len(ok):>4}{csv_t/2**20:>10.2f}{pred['pred_neon_bytes']/2**20:>10.2f}"
      f"{act_t/2**20:>11.2f}{(act_t-pred['pred_neon_bytes'])/pred['pred_neon_bytes']*100:>7.1f}%"
      f"{0.720:>8.3f}{act_t/csv_t:>8.3f}"
      f"{pred['pred_copy_secs']/60:>11.1f}m{copy_t/60:>9.1f}m")
if bad:
    print(f"\nFAILURES ({len(bad)}):")
    for r in bad:
        print(f"  {r['title'][:45]:<45} {r['csv_bytes']/2**20:8.3f} MB  {r['error'][:70]}")
else:
    print("\nFAILURES: none")

print(f"\nrows loaded: {sum(r['rows'] for r in ok):,}")
print(f"download {dl_t/60:.1f} min | COPY {copy_t/60:.1f} min | swap "
      f"{sum(r['swap_secs'] for r in ok):.1f} s total")


async def main():
    c = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require")
    db = await c.fetchval("SELECT pg_database_size(current_database())")
    pil = await c.fetchval("""SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0) FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pilot_idx' AND c.relkind='r'""")
    n = await c.fetchval("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='pilot_idx' AND c.relkind='r'""")
    stray = await c.fetch("""SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='r' AND (c.relname LIKE 'idx_%' OR c.relname LIKE 'bench_%')""")
    npub = await c.fetchval("""SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relkind='r'""")
    await c.close()
    print(f"\nappend DB logical: {db/2**30:.3f} GB | pilot_idx: {n} tables, {pil/2**30:.3f} GB")
    print(f"public schema: {npub} tables (baseline 114) | leaked: {[r[0] for r in stray] or 'NONE'}")

    # ---- re-extrapolate the corpus with the enlarged sample (110 datasets)
    surv = json.load(open("survey.json", encoding="utf-8"))
    EDGES = [(0, 10 * 2**10, "<10 KB"), (10 * 2**10, 100 * 2**10, "10–100 KB"),
             (100 * 2**10, 2**20, "100 KB–1 MB"), (2**20, 10 * 2**20, "1–10 MB"),
             (10 * 2**20, 100 * 2**20, "10–100 MB"), (100 * 2**20, 10**13, ">100 MB")]
    ten = [r for r in json.load(open("results_pilot10.json", encoding="utf-8")) if not r.get("error")]
    allr = ok + ten
    print("\n" + "=" * 96)
    print("RE-EXTRAPOLATION of the full corpus, using all 110 measured datasets")
    print("=" * 96)
    print(f"{'bucket':<14}{'corpus n':>10}{'sampled':>9}{'CSV GB':>10}{'ratio':>8}{'NEON GB':>10}{'min':>8}")
    tn = tc = tm = 0
    for lo, hi, name in EDGES:
        g = [c for c in surv if lo <= c["size"] < hi]
        s = [r for r in allr if lo <= r["csv_bytes"] < hi]
        if not s:
            continue
        ratio = sum(r["total"] for r in s) / sum(r["csv_bytes"] for r in s)
        thru = sum(r["csv_bytes"] for r in s) / 2**20 / max(sum(r["copy_secs"] for r in s), .001)
        gb = sum(c["size"] for c in g) / 2**30
        mins = gb * 1024 / thru / 60
        tc += gb; tn += gb * ratio; tm += mins
        print(f"{name:<14}{len(g):>10}{len(s):>9}{gb:>10.3f}{ratio:>8.2f}{gb*ratio:>10.3f}{mins:>8.1f}")
    print("-" * 96)
    print(f"{'TOTAL':<14}{len(surv):>10}{len(allr):>9}{tc:>10.3f}{tn/tc:>8.2f}{tn:>10.3f}{tm:>8.1f}")
    print(f"\nSTORAGE  {tn:.2f} GB × $0.35/GB-mo = ${tn*0.35:.2f}/month (${tn*0.35*12:.2f}/yr)")
    print(f"BACKFILL {tm:.0f} min of COPY  → at 0.25 CU = {tm/60*0.25:.2f} CU-h = ${tm/60*0.25*0.106:.3f}")
    json.dump({"neon_gb": tn, "csv_gb": tc, "backfill_min": tm,
               "bucket_ratio": new_ratio, "bucket_thru": new_thru},
              open("extrapolation_v2.json", "w"), indent=1)

asyncio.run(main())
