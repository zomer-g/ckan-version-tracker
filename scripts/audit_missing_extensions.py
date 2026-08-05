"""How many layers still hold files whose stored key has no extension.

The key is permanent, so only a NEW version fixes the download name. Counts the
latest version of every active GovMap dataset, per file type, and records which
datasets would need a re-scrape.
"""
import json
import re
import time
import urllib.request

UA = {"User-Agent": "over-audit/1.0"}
KNOWN = (".geojson.gz", ".csv.gz", ".gpkg", ".parquet", ".geojson",
         ".json", ".csv", ".zip", ".gz", ".pdf", ".xlsx")
PREFIX = re.compile(r"^[0-9a-f]{8}_(.+)$")


def get(p, tries=3):
    for n in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://www.over.org.il" + p, headers=UA), timeout=90) as r:
                return json.load(r)
        except Exception as e:
            if n == tries - 1:
                return {"__error__": str(e)}
            time.sleep(2 * (n + 1))


items, off = [], 0
while True:
    d = get(f"/api/v1/datasets?limit=500&offset={off}&status=all")
    items += d["items"]
    off += 500
    if off >= d["total"]:
        break
gm = [i for i in items if i["source_type"] == "govmap" and i["status"] in ("active", "pending")]
print(f"active govmap datasets: {len(gm)}", flush=True)

bad, good, rows = [], 0, []
for n, i in enumerate(gm, 1):
    vs = get(f"/api/datasets/{i['id']}/versions")
    if isinstance(vs, dict) or not vs:
        continue
    m = vs[0].get("resource_mappings") or {}
    files = []
    for k, val in m.items():
        for x in (val if isinstance(val, list) else [val]):
            if isinstance(x, str) and x.startswith("r2:"):
                tail = x.rsplit("/", 1)[-1]
                mm = PREFIX.match(tail)
                files.append(mm.group(1) if mm else tail)
    missing = [f for f in files if not any(f.lower().endswith(s) for s in KNOWN)]
    if missing:
        bad.append({"id": i["id"], "title": i["title"], "missing": missing,
                    "detected_at": vs[0]["detected_at"]})
    else:
        good += 1
    if n % 150 == 0:
        print(f"  {n}/{len(gm)} — {len(bad)} affected", flush=True)
    time.sleep(1.05)

json.dump(bad, open("extension_audit.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
from collections import Counter
c = Counter(f for b in bad for f in b["missing"])
print(f"\nDONE  clean: {good}  |  need a re-scrape: {len(bad)}")
print("extensionless file kinds:", c.most_common(12))
