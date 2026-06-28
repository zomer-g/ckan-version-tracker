#!/usr/bin/env python3
"""Contract test for the PUBLIC API documented at https://www.over.org.il/api.

Every endpoint advertised on that page is GET, public, and key-less ("אין צורך
באימות, אין מפתח API"). This test calls each one WITHOUT any token and verifies
the documented contract actually holds — not just a 200, but the promised
shape: paging params on /v1/datasets, a `download_url` + `storage` field on
every version resource, the 404 behavior of …/versions/latest and /{number},
and the CKAN-style wrapper ({success, result:{fields, records, total, _links}})
on the append datastore endpoints. It also runs the EXACT example URLs printed
on the page (incl. the Hebrew `filters=` and the `datastore_search_sql` query).

Run: python scripts/api_docs_test.py [--base URL]
Exit code is non-zero if any documented promise is not met.
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# The append dataset the docs use in every append example (vehicle registry).
DOC_APPEND_ID = "e437ab0b-c247-4d35-b2c4-79c2d19dbabd"

results: list[tuple[bool, str, str]] = []  # (ok, label, detail)


def _get(url: str, timeout=60):
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, raw[:300].decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def ok(label, cond, detail=""):
    results.append((bool(cond), label, detail if not cond else (detail or "ok")))
    return bool(cond)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://www.over.org.il")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    print(f"== Public API docs contract test ==\nbase: {base}  (no auth — as documented)\n")

    # Resolve real ids for the generic {id} routes (docs use 0000… placeholders).
    st, lst = _get(base + "/api/v1/datasets?status=active&limit=50")
    items = lst.get("items", []) if isinstance(lst, dict) else []
    ds_id = items[0]["id"] if items else None
    ds_with_versions = next((x for x in items if (x.get("version_count") or 0) > 0), None)
    vds = (ds_with_versions or {}).get("id")
    org_id = (items[0].get("organization") or {}).get("id") if items else None
    st, tags = _get(base + "/api/v1/tags")
    tag_id = (tags[0]["id"] if isinstance(tags, list) and tags
              else (tags.get("items", [{}])[0].get("id") if isinstance(tags, dict) else None))

    # ── 1. /api/v1/datasets (+ documented params) ──
    st, d = _get(base + "/api/v1/datasets?status=active&limit=10")
    ok("/v1/datasets → 200 + {total,limit,offset,items}",
       st == 200 and isinstance(d, dict) and {"total", "limit", "offset", "items"} <= set(d.keys()),
       f"status={st}")
    ok("/v1/datasets ?limit honored (≤10, limit field=10)",
       isinstance(d, dict) and d.get("limit") == 10 and len(d.get("items", [])) <= 10,
       f"limit={d.get('limit') if isinstance(d, dict) else '?'} n={len(d.get('items', [])) if isinstance(d, dict) else '?'}")
    st_all, d_all = _get(base + "/api/v1/datasets?status=all&limit=1")
    st_pend, d_pend = _get(base + "/api/v1/datasets?status=pending&limit=1")
    ok("/v1/datasets ?status=all / =pending work",
       st_all == 200 and st_pend == 200 and isinstance(d_all, dict) and isinstance(d_pend, dict),
       f"all={st_all} pending={st_pend}")
    if org_id:
        st_o, d_o = _get(base + f"/api/v1/datasets?organization_id={org_id}&limit=5")
        good = st_o == 200 and all((it.get("organization") or {}).get("id") == org_id for it in d_o.get("items", []))
        ok("/v1/datasets ?organization_id filters correctly", good, f"status={st_o}")

    # ── 2. /api/v1/datasets/{id} ──
    if ds_id:
        st, one = _get(base + f"/api/v1/datasets/{ds_id}")
        ok("/v1/datasets/{id} → 200 + source/tags/odata fields",
           st == 200 and isinstance(one, dict) and "source_type" in one and "tags" in one
           and ("odata_url" in one or "odata_dataset_id" in one),
           f"status={st}")
    st, _ = _get(base + "/api/v1/datasets/00000000-0000-0000-0000-000000000000")
    ok("/v1/datasets/{bad id} → 404", st == 404, f"status={st}")

    # ── 3. /api/v1/datasets/{id}/versions  (download_url + storage contract) ──
    if vds:
        st, vers = _get(base + f"/api/v1/datasets/{vds}/versions")
        vlist = vers if isinstance(vers, list) else vers.get("versions", []) if isinstance(vers, dict) else []
        ok("/v1/datasets/{id}/versions → 200 + list", st == 200 and isinstance(vlist, list) and vlist, f"status={st}")
        # every resource of every version must expose download_url + a known storage
        bad = []
        for v in vlist:
            for r in (v.get("resources") or []):
                if "download_url" not in r or not r.get("download_url"):
                    bad.append(f"v{v.get('version_number')}:{r.get('name')} no download_url")
                if r.get("storage") not in ("odata", "r2"):
                    bad.append(f"v{v.get('version_number')}:{r.get('name')} storage={r.get('storage')}")
        ok("every version resource has download_url + storage∈{odata,r2}", not bad,
           "; ".join(bad[:3]) if bad else "")

        # ── 4. …/versions/latest (highest number) ──
        st, latest = _get(base + f"/api/v1/datasets/{vds}/versions/latest")
        maxn = max((v.get("version_number") or 0) for v in vlist) if vlist else None
        ok("…/versions/latest → 200 with the highest version",
           st == 200 and isinstance(latest, dict) and latest.get("version_number") == maxn,
           f"status={st} latest={latest.get('version_number') if isinstance(latest, dict) else '?'} max={maxn}")

        # ── 5. …/versions/{number} (200 valid, 404 invalid) ──
        st1, _ = _get(base + f"/api/v1/datasets/{vds}/versions/1")
        st404, _ = _get(base + f"/api/v1/datasets/{vds}/versions/99999")
        ok("…/versions/{n} → 200 for valid, 404 for missing", st1 == 200 and st404 == 404,
           f"v1={st1} v99999={st404}")

    # ── 6-7. tags ──
    st, t = _get(base + "/api/v1/tags")
    ok("/v1/tags → 200 (list with dataset counts)",
       st == 200 and (isinstance(t, list) or isinstance(t, dict)), f"status={st}")
    if tag_id:
        st, td = _get(base + f"/api/v1/tags/{tag_id}")
        ok("/v1/tags/{id} → 200 + datasets list",
           st == 200 and isinstance(td, dict) and ("datasets" in td or "items" in td), f"status={st}")

    # ── 8-9. organizations ──
    st, o = _get(base + "/api/v1/organizations")
    ok("/v1/organizations → 200", st == 200 and (isinstance(o, list) or isinstance(o, dict)), f"status={st}")
    if org_id:
        st, od = _get(base + f"/api/v1/organizations/{org_id}")
        ok("/v1/organizations/{id} → 200", st == 200 and isinstance(od, dict), f"status={st}")

    # ── 10. /api/append/{id}/datastore_search  (CKAN wrapper + documented example) ──
    st, sr = _get(base + f"/api/append/{DOC_APPEND_ID}/datastore_search?limit=2")
    wrapper_ok = (st == 200 and isinstance(sr, dict) and sr.get("success") is True
                  and isinstance(sr.get("result"), dict)
                  and {"fields", "records", "total", "_links"} <= set(sr["result"].keys()))
    ok("/append/{id}/datastore_search → CKAN wrapper {success,result:{fields,records,total,_links}}",
       wrapper_ok, f"status={st}")
    ok("datastore_search ?limit honored", wrapper_ok and len(sr["result"]["records"]) <= 2,
       f"n={len(sr['result']['records']) if wrapper_ok else '?'}")
    # the EXACT documented filters example (Hebrew JSON value)
    filt = urllib.parse.quote('{"tozeret_nm":"קיה קוריאה"}')
    st, fr = _get(base + f"/api/append/{DOC_APPEND_ID}/datastore_search?limit=5&filters={filt}")
    ok("datastore_search documented filters example runs",
       st == 200 and isinstance(fr, dict) and fr.get("success") is True, f"status={st}")
    # fields projection + sort
    st, pr = _get(base + f"/api/append/{DOC_APPEND_ID}/datastore_search?limit=1&fields=tozeret_nm&sort=tozeret_nm")
    proj_ok = (st == 200 and isinstance(pr, dict) and pr.get("success") is True
               and (not pr["result"]["records"] or set(pr["result"]["records"][0].keys()) <= {"tozeret_nm"}))
    ok("datastore_search fields= projection + sort= work", proj_ok, f"status={st}")

    # ── 11. /api/append/{id}/datastore_search_sql  (documented SQL example) ──
    sql = ("SELECT tozeret_nm, count(*) FROM append_private_and_commercial_vehicles_"
           "e437ab0b GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
    st, sqlr = _get(base + f"/api/append/{DOC_APPEND_ID}/datastore_search_sql?sql={urllib.parse.quote(sql)}")
    ok("/append/{id}/datastore_search_sql documented SQL example runs",
       st == 200 and isinstance(sqlr, dict) and sqlr.get("success") is True
       and isinstance(sqlr.get("result", {}).get("records"), list),
       f"status={st} {str(sqlr)[:80] if st != 200 else ''}")
    # read-only guard: a write must be rejected (not executed)
    st_w, _ = _get(base + f"/api/append/{DOC_APPEND_ID}/datastore_search_sql?sql={urllib.parse.quote('DELETE FROM x')}")
    ok("datastore_search_sql rejects non-SELECT (read-only)", st_w in (400, 403, 422), f"status={st_w}")

    # ── 12. /api/append/{id}/schema ──
    st, sc = _get(base + f"/api/append/{DOC_APPEND_ID}/schema")
    ok("/append/{id}/schema → 200 + table/total/columns/first_seen_column",
       st == 200 and isinstance(sc, dict) and {"table", "total", "columns", "first_seen_column"} <= set(sc.keys()),
       f"status={st}")

    # ── docs links promised on the page ──
    for path in ("/docs", "/redoc", "/openapi.json"):
        st, _ = _get(base + path, timeout=30)
        ok(f"{path} reachable", st == 200, f"status={st}")

    # ---- report ----
    print(f"{'RESULT':7} CHECK")
    print("-" * 90)
    n_ok = 0
    for good, label, detail in results:
        n_ok += good
        mark = "✅ PASS " if good else "❌ FAIL "
        line = f"{mark}{label}"
        if not good:
            line += f"   [{detail}]"
        print(line)
    print("-" * 90)
    n_fail = len(results) - n_ok
    print(f"TOTAL: {len(results)}   ✅ {n_ok} pass   ❌ {n_fail} fail")
    print("\nEvery documented endpoint was called WITHOUT a token (public, as the page states)."
          if not n_fail else "")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
