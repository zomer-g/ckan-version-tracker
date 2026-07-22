"""Wave-3 validation: predicted vs actual for the 200, then re-extrapolate the
corpus from all 310 measured datasets."""
import asyncio, json, re, sys

import asyncpg

sys.stdout.reconfigure(encoding="utf-8")
ENV = dict(l.strip().split("=", 1) for l in open("pilot.env", encoding="utf-8") if "=" in l)


def dsn(raw): return re.sub(r"^postgresql\+asyncpg://", "postgresql://", raw).split("?")[0]


picks = {c["dataset_id"]: c for c in json.load(open("pilot_set_200.json", encoding="utf-8"))}
res = json.load(open("results.json", encoding="utf-8"))
ok = [r for r in res if not r.get("error")]
bad = [r for r in res if r.get("error")]
pred = json.load(open("prediction_200.json"))
for r in ok:
    p = picks[r["dataset_id"]]
    r["bucket"], r["pred_ratio"], r["pred_mbps"] = p["bucket"], p["pred_ratio"], p["pred_mbps"]

BUCKETS = ["<10 KB", "10–100 KB", "100 KB–1 MB", "1–10 MB", "10–100 MB", ">100 MB"]
print("=" * 100)
print("WAVE 3 (200 datasets): predicted vs actual")
print("=" * 100)
print(f"{'bucket':<14}{'n':>4}{'CSV MB':>11}{'pred MB':>10}{'actual MB':>11}{'err':>8}"
      f"{'pred x':>8}{'act x':>8}{'MB/s pred':>11}{'MB/s act':>10}")
for b in BUCKETS:
    g = [r for r in ok if r["bucket"] == b]
    if not g:
        continue
    csv_b = sum(r["csv_bytes"] for r in g)
    pr = sum(r["csv_bytes"] * r["pred_ratio"] for r in g)
    ac = sum(r["total"] for r in g)
    thru = (csv_b / 2**20) / max(sum(r["copy_secs"] for r in g), .001)
    print(f"{b:<14}{len(g):>4}{csv_b/2**20:>11.2f}{pr/2**20:>10.2f}{ac/2**20:>11.2f}"
          f"{(ac-pr)/max(pr,1)*100:>7.1f}%{g[0]['pred_ratio']:>8.2f}{ac/csv_b:>8.2f}"
          f"{g[0]['pred_mbps']:>11.2f}{thru:>10.2f}")
csv_t = sum(r["csv_bytes"] for r in ok)
act_t = sum(r["total"] for r in ok)
copy_t = sum(r["copy_secs"] for r in ok)
print("-" * 100)
print(f"{'TOTAL':<14}{len(ok):>4}{csv_t/2**20:>11.2f}{pred['pred_neon_bytes']/2**20:>10.2f}"
      f"{act_t/2**20:>11.2f}{(act_t-pred['pred_neon_bytes'])/pred['pred_neon_bytes']*100:>7.1f}%"
      f"{0.737:>8.3f}{act_t/csv_t:>8.3f}{pred['pred_copy_secs']/60:>10.1f}m{copy_t/60:>9.1f}m")
print(f"\nfailures: {len(bad)}" + (":" if bad else " — none"))
for r in bad:
    print(f"  {r['title'][:45]:<45} {r['error'][:70]}")
print(f"rows: {sum(r['rows'] for r in ok):,} | download {sum(r['download_secs'] for r in ok)/60:.1f} min "
      f"| swap {sum(r['swap_secs'] for r in ok):.1f} s")


async def main():
    allr = ok[:]
    for f in ("results_pilot100.json", "results_pilot10.json"):
        allr += [r for r in json.load(open(f, encoding="utf-8")) if not r.get("error")]
    surv = json.load(open("survey.json", encoding="utf-8"))
    EDGES = [(0, 10 * 2**10, "<10 KB"), (10 * 2**10, 100 * 2**10, "10–100 KB"),
             (100 * 2**10, 2**20, "100 KB–1 MB"), (2**20, 10 * 2**20, "1–10 MB"),
             (10 * 2**20, 100 * 2**20, "10–100 MB"), (100 * 2**20, 10**13, ">100 MB")]
    print("\n" + "=" * 100)
    print(f"CORPUS RE-EXTRAPOLATION from all {len(allr)} measured datasets")
    print("=" * 100)
    print(f"{'bucket':<14}{'corpus':>8}{'sampled':>9}{'cover':>8}{'CSV GB':>10}{'ratio':>8}{'NEON GB':>10}{'min':>8}")
    tn = tc = tm = 0
    for lo, hi, name in EDGES:
        g = [c for c in surv if lo <= c["size"] < hi]
        s = [r for r in allr if lo <= r["csv_bytes"] < hi]
        ratio = sum(r["total"] for r in s) / sum(r["csv_bytes"] for r in s)
        thru = sum(r["csv_bytes"] for r in s) / 2**20 / max(sum(r["copy_secs"] for r in s), .001)
        gb = sum(c["size"] for c in g) / 2**30
        mins = gb * 1024 / thru / 60
        tc += gb; tn += gb * ratio; tm += mins
        print(f"{name:<14}{len(g):>8}{len(s):>9}{len(s)/len(g)*100:>7.1f}%{gb:>10.3f}"
              f"{ratio:>8.2f}{gb*ratio:>10.3f}{mins:>8.1f}")
    print("-" * 100)
    print(f"{'TOTAL':<14}{len(surv):>8}{len(allr):>9}{len(allr)/len(surv)*100:>7.1f}%"
          f"{tc:>10.3f}{tn/tc:>8.2f}{tn:>10.3f}{tm:>8.1f}")
    print(f"\nSTORAGE  {tn:.2f} GB × $0.35/GB-mo = ${tn*0.35:.2f}/month (${tn*0.35*12:.2f}/yr)")
    print(f"BACKFILL {tm:.0f} min COPY → at 0.25 CU = ${tm/60*0.25*0.106:.3f}")

    c = await asyncpg.connect(dsn(ENV["APPEND_DATABASE_URL"]), ssl="require", command_timeout=600)
    db = await c.fetchval("SELECT pg_database_size(current_database())")
    pil = await c.fetchval("""SELECT COALESCE(sum(pg_total_relation_size(x.oid)),0) FROM pg_class x
        JOIN pg_namespace n ON n.oid=x.relnamespace WHERE n.nspname='pilot_idx' AND x.relkind='r'""")
    npil = await c.fetchval("""SELECT count(*) FROM pg_class x JOIN pg_namespace n ON n.oid=x.relnamespace
        WHERE n.nspname='pilot_idx' AND x.relkind='r'""")
    npub = await c.fetchval("""SELECT count(*) FROM pg_class x JOIN pg_namespace n ON n.oid=x.relnamespace
        WHERE n.nspname='public' AND x.relkind='r'""")
    stray = await c.fetch("""SELECT x.relname FROM pg_class x JOIN pg_namespace n ON n.oid=x.relnamespace
        WHERE n.nspname='public' AND x.relkind='r' AND (x.relname LIKE 'idx_%' OR x.relname LIKE 't%')
        AND x.relname ~ '^(idx_|t[0-9]+$)'""")
    print(f"\nappend DB {db/2**30:.3f} GB | pilot_idx {npil} tables / {pil/2**30:.3f} GB "
          f"| public {npub} tables (baseline 114) | leaked: {[r[0] for r in stray] or 'NONE'}")
    print(f"total rows across pilot: {sum(r['rows'] for r in allr):,} | "
          f"CSV ingested {sum(r['csv_bytes'] for r in allr)/2**30:.3f} GB")
    await c.close()

asyncio.run(main())
