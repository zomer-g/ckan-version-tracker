"""Stratified random sample of 100 datasets (disjoint from the first 10) to
validate the per-bucket NEON/CSV ratio + throughput predictions."""
import json, random, sys

sys.stdout.reconfigure(encoding="utf-8")
random.seed(20260722)

surv = json.load(open("survey.json", encoding="utf-8"))
done = {c["dataset_id"] for c in json.load(open("pilot_set_full.json", encoding="utf-8"))}
pool = [c for c in surv if c["dataset_id"] not in done]

# (lo, hi, n_to_sample, predicted_ratio, predicted_MBps, label)
PLAN = [
    (0, 10 * 2**10, 25, 3.21, 0.02, "<10 KB"),
    (10 * 2**10, 100 * 2**10, 20, 2.80, 0.07, "10–100 KB"),
    (100 * 2**10, 2**20, 20, 1.75, 0.37, "100 KB–1 MB"),
    (2**20, 10 * 2**20, 20, 0.62, 2.70, "1–10 MB"),
    (10 * 2**20, 100 * 2**20, 12, 0.80, 8.00, "10–100 MB"),
    (100 * 2**20, 10**13, 3, 0.68, 7.20, ">100 MB"),
]

picked = []
print(f"{'bucket':<14}{'avail':>7}{'take':>6}{'CSV MB':>10}{'pred NEON MB':>14}")
for lo, hi, n, ratio, mbps, label in PLAN:
    g = [c for c in pool if lo <= c["size"] < hi]
    take = random.sample(g, min(n, len(g)))
    for c in take:
        c = dict(c)
        c["bucket"] = label
        c["pred_ratio"] = ratio
        c["pred_mbps"] = mbps
        picked.append(c)
    mb = sum(c["size"] for c in take) / 2**20
    print(f"{label:<14}{len(g):>7}{len(take):>6}{mb:>10.2f}{mb*ratio:>14.2f}")

tot = sum(c["size"] for c in picked)
pred = sum(c["size"] * c["pred_ratio"] for c in picked)
pred_secs = sum((c["size"] / 2**20) / c["pred_mbps"] for c in picked)
print(f"\n{len(picked)} datasets | CSV {tot/2**20:.2f} MB")
print(f"PREDICTED: NEON {pred/2**20:.2f} MB ({pred/tot:.3f}x) | COPY time {pred_secs/60:.1f} min")
json.dump(picked, open("pilot_set_100.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
json.dump({"n": len(picked), "csv_bytes": tot, "pred_neon_bytes": pred,
           "pred_copy_secs": pred_secs}, open("prediction_100.json", "w"), indent=1)

from collections import Counter
print("\nby source type:", dict(Counter(c["source_type"] for c in picked)))
