"""Third wave: 200 more datasets, disjoint from the 110 already loaded.
Predictions come from the ratios/throughput MEASURED on those 110, recorded up
front so this stays a forward test."""
import json, random, sys

sys.stdout.reconfigure(encoding="utf-8")
random.seed(20260722_200)

surv = json.load(open("survey.json", encoding="utf-8"))
done = {c["dataset_id"] for c in json.load(open("pilot_set_full.json", encoding="utf-8"))}
done |= {c["dataset_id"] for c in json.load(open("pilot_set_100.json", encoding="utf-8"))}
pool = [c for c in surv if c["dataset_id"] not in done]
print(f"already loaded: {len(done)} | pool: {len(pool)}")

EDGES = [(0, 10 * 2**10, "<10 KB", 50), (10 * 2**10, 100 * 2**10, "10–100 KB", 40),
         (100 * 2**10, 2**20, "100 KB–1 MB", 40), (2**20, 10 * 2**20, "1–10 MB", 40),
         (10 * 2**20, 100 * 2**20, "10–100 MB", 24), (100 * 2**20, 10**13, ">100 MB", 6)]

# ratios + throughput measured across all 110 loaded so far
prev = [r for r in json.load(open("results.json", encoding="utf-8")) if not r.get("error")]
prev += [r for r in json.load(open("results_pilot10.json", encoding="utf-8")) if not r.get("error")]
meas = {}
for lo, hi, label, _ in EDGES:
    g = [r for r in prev if lo <= r["csv_bytes"] < hi]
    if g:
        meas[label] = (sum(r["total"] for r in g) / sum(r["csv_bytes"] for r in g),
                       sum(r["csv_bytes"] for r in g) / 2**20 / max(sum(r["copy_secs"] for r in g), .001))

picked = []
print(f"\n{'bucket':<14}{'avail':>7}{'take':>6}{'CSV MB':>11}{'ratio':>8}{'pred MB':>10}{'pred s':>9}")
for lo, hi, label, n in EDGES:
    g = [c for c in pool if lo <= c["size"] < hi]
    take = random.sample(g, min(n, len(g)))
    ratio, thru = meas[label]
    for c in take:
        c = dict(c); c["bucket"] = label; c["pred_ratio"] = ratio; c["pred_mbps"] = thru
        picked.append(c)
    mb = sum(c["size"] for c in take) / 2**20
    print(f"{label:<14}{len(g):>7}{len(take):>6}{mb:>11.2f}{ratio:>8.2f}{mb*ratio:>10.2f}{mb/thru:>9.1f}")

tot = sum(c["size"] for c in picked)
pred = sum(c["size"] * c["pred_ratio"] for c in picked)
secs = sum((c["size"] / 2**20) / c["pred_mbps"] for c in picked)
print(f"\n{len(picked)} datasets | CSV {tot/2**20:.2f} MB ({tot/2**30:.3f} GB)")
print(f"PREDICTED: NEON {pred/2**20:.2f} MB ({pred/tot:.3f}x) | COPY {secs/60:.1f} min")
json.dump(picked, open("pilot_set_200.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
json.dump({"n": len(picked), "csv_bytes": tot, "pred_neon_bytes": pred, "pred_copy_secs": secs},
          open("prediction_200.json", "w"), indent=1)
from collections import Counter
print("by source:", dict(Counter(c["source_type"] for c in picked)))
