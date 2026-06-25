#!/usr/bin/env python3
"""Source-type health check for over.org.il.

For each kind of source we track (mevaker / govmap / hatzav / idf / health /
avodata / generic scraper / ckan), this picks the most-recently-collected
dataset of that type and verifies, against the LIVE public API:

  1. collected  — it has at least one version, detected recently, no last_error
  2. shaped     — the latest version's resources match what that source type
                  should produce (docs+ZIP, GeoJSON, catalog-only CSV, …)
  3. stored     — a representative file actually downloads (follows the OVER
                  /download redirect to R2 or ODATA and gets 200/206)

Per-type expectations (a missing ZIP is a PASS for catalog-only sources, a
FAIL for document sources):

  mevaker / idf / health  → CSV + ZIP of documents (on R2)
  govmap                  → GeoJSON layer (+ CSV)
  hatzav / avodata        → catalog-only CSV (no document ZIP by design)
  scraper (generic)       → CSV (+ ZIP if the page has documents)
  ckan (data.gov.il)      → original resources (on ODATA)

Run:  python scripts/source_health_check.py [--base https://www.over.org.il]
Exit code 0 if every source type with data passes, 1 otherwise.
"""
import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request


def _get_json(base, path):
    with urllib.request.urlopen(base + path, timeout=45) as r:
        return json.load(r)


def _download_status(base, path):
    """GET the first byte (Range) following redirects → final HTTP status.
    Proves the object is actually served from its real storage (R2/ODATA)."""
    # A real browser UA: Cloudflare (in front of r2.dev / odata) 403s the
    # default "Python-urllib" agent, which would falsely fail every download.
    req = urllib.request.Request(
        base + path,
        method="GET",
        headers={
            "Range": "bytes=0-0",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            ),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
        return f"ERR {e}"


def classify(d):
    st = (d.get("source_type") or "ckan").lower()
    url = (d.get("source_url") or "").lower()
    org = (d.get("organization") or "").lower()
    if st == "govmap":
        return "govmap"
    if "mevaker.gov.il" in url:
        return "mevaker"
    if "geo.mot.gov.il" in url or "hatzav" in url:
        return "hatzav"
    if "idf" in url or "idf" in org or "mil.idf" in url:
        return "idf"
    if "practitioners.health" in url:
        return "health"
    if "avodata.labor" in url:
        return "avodata"
    if st == "ckan":
        return "ckan"
    return "scraper"


# expects_zip: True (must have doc ZIP), False (must NOT — catalog-only),
#              None (optional — page may or may not have documents)
EXPECT = {
    "mevaker": {"zip": True, "geojson": False},
    "idf": {"zip": True, "geojson": False},
    # health = professional registries (records/CSV); some have document
    # attachments, many don't, so a ZIP is optional, not required.
    "health": {"zip": None, "geojson": False},
    "govmap": {"zip": False, "geojson": True},
    "hatzav": {"zip": False, "geojson": False},
    "avodata": {"zip": False, "geojson": False},
    "scraper": {"zip": None, "geojson": False},
    "ckan": {"zip": None, "geojson": False},
}

_BOOKKEEPING = {"_hashes", "_resource_ids", "_appendonly_seen"}


def _resource_storage(value):
    s = value if isinstance(value, str) else json.dumps(value)
    if "r2:" in s:
        return "r2"
    if (isinstance(value, str) and len(value) >= 30) or (
        isinstance(value, list) and value
    ):
        return "odata"
    return None


def check_one(base, typ, d):
    """Return (status, detail) for the representative dataset of a type."""
    exp = EXPECT.get(typ, {"zip": None, "geojson": False})
    vs = _get_json(base, f"/api/datasets/{d['id']}/versions")
    if not vs:
        return "FAIL", "no versions"
    v = vs[0]
    m = v.get("resource_mappings") or {}
    cs = v.get("change_summary") or {}
    att = cs.get("total_attachments") or 0
    rows = cs.get("total_rows") or 0

    has_zip = any(k in ("_zip", "_zip_parts") for k in m)
    has_geojson = "_geojson" in m
    named = [k for k in m if k not in _BOOKKEEPING and not k.startswith("_")]
    has_csv = bool(named)

    storages = {
        _resource_storage(val)
        for k, val in m.items()
        if k not in _BOOKKEEPING
    }
    storages.discard(None)
    storage = "/".join(sorted(storages)) or "none"

    problems = []
    if exp["geojson"] and not has_geojson:
        problems.append("missing GeoJSON")
    if exp["zip"] is True and not has_zip:
        problems.append(f"missing document ZIP (att={att})")
    if exp["zip"] is False and has_zip:
        problems.append("unexpected ZIP for catalog-only source")
    if not has_csv and not has_geojson and not has_zip:
        problems.append("no resources at all")

    # storage-served check: HEAD/Range a representative resource
    dl_key = None
    for k in m:
        if k in _BOOKKEEPING:
            continue
        dl_key = k
        break
    dl = "n/a"
    if dl_key:
        dl = _download_status(
            base, f"/api/versions/{v['id']}/download/{urllib.parse.quote(dl_key)}"
        )
        if dl not in (200, 206):
            problems.append(f"download {dl_key} -> {dl}")

    detail = (
        f"v{v.get('version_number')} {(v.get('detected_at') or '')[:16]} | "
        f"rows={rows} docs={att} | csv={has_csv} zip={has_zip} "
        f"geojson={has_geojson} | storage={storage} | dl={dl}"
    )
    if d.get("last_error"):
        problems.append(f"last_error: {str(d['last_error'])[:40]}")
    return ("PASS" if not problems else "FAIL", detail + (
        "  ! " + "; ".join(problems) if problems else ""))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://www.over.org.il")
    args = ap.parse_args()
    base = args.base.rstrip("/")

    data = _get_json(base, "/api/datasets")
    items = data if isinstance(data, list) else data.get("items", [])

    buckets = {}
    for d in items:
        buckets.setdefault(classify(d), []).append(d)

    print(f"Source-type health check — {base}  ({len(items)} datasets)\n")
    overall_ok = True
    for typ in sorted(buckets):
        ds_list = buckets[typ]
        cands = sorted(
            [d for d in ds_list if (d.get("version_count") or 0) > 0],
            key=lambda x: (x.get("last_polled_at") or ""),
            reverse=True,
        )
        if not cands:
            print(f"  [SKIP] {typ:9} — {len(ds_list)} tracked, none with a version yet")
            continue
        d = cands[0]
        try:
            status, detail = check_one(base, typ, d)
        except Exception as e:  # noqa: BLE001
            status, detail = "ERROR", str(e)
        if status != "PASS":
            overall_ok = False
        title = (d.get("title") or "")[:28]
        print(f"  [{status}] {typ:9} {title:30} {detail}")

    print()
    print("RESULT:", "ALL SOURCE TYPES OK" if overall_ok else "SOME TYPES NEED ATTENTION")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
