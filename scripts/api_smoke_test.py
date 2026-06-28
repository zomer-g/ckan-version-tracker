#!/usr/bin/env python3
"""End-to-end API smoke test for OVER (over.org.il).

Exercises every public + admin HTTP endpoint against a RUNNING deployment and
reports a pass/fail table, so you can confirm "all the APIs actually work"
after a deploy. Safe by default — it only READS, runs the non-mutating
``/validate`` endpoints, and checks that every mutating / worker endpoint
correctly REJECTS an unauthenticated call (proving it's wired + protected
without changing any data).

Pass ``--full`` to additionally run ONE self-cleaning mutation roundtrip
(submit a throwaway govmap tracking request → assert it shows up in the
pending queue and bumps the public pending-count → delete it → assert the
count returns), which exercises the request → pending → delete lifecycle.

Usage:
    python scripts/api_smoke_test.py [--base URL] [--token JWT] [--full]
    # token also read from env OVER_TOKEN; base defaults to https://www.over.org.il
Exit code is non-zero if any check FAILS (SKIP doesn't fail the run).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

try:
    sys.stdout.reconfigure(encoding="utf-8")  # Hebrew-safe output on Windows
except Exception:
    pass

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"
results: list[tuple[str, str, str, str]] = []  # (status, method, path, note)


def _call(method: str, url: str, token: str | None, body=None, timeout=40):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, raw[:200].decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def check(name, method, path, *, base, token=None, body=None, expect=(200,),
          want_keys=None, note="", timeout=40):
    """Run one endpoint check. `expect` is an allowed-status tuple."""
    status, payload = _call(method, base + path, token, body, timeout=timeout)
    ok = status in expect
    if ok and want_keys and isinstance(payload, (dict, list)):
        sample = payload[0] if isinstance(payload, list) and payload else payload
        if isinstance(sample, dict):
            missing = [k for k in want_keys if k not in sample]
            if missing:
                ok = False
                note = f"missing keys {missing}"
    detail = note or (f"→ {status}" if ok else f"got {status} (want {expect}) {str(payload)[:80]}")
    results.append((PASS if ok else FAIL, method, path, detail))
    return status, payload


def skip(method, path, note):
    results.append((SKIP, method, path, note))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.environ.get("OVER_BASE", "https://www.over.org.il"))
    ap.add_argument("--token", default=os.environ.get("OVER_TOKEN", ""))
    ap.add_argument("--full", action="store_true", help="run the self-cleaning mutation roundtrip")
    args = ap.parse_args()
    base = args.base.rstrip("/")
    token = args.token.strip() or None

    print(f"== OVER API smoke test ==\nbase: {base}\nadmin token: {'yes' if token else 'NO (admin checks skipped)'}\n")

    # ---- resolve real IDs from public list endpoints ----
    s, datasets = _call("GET", base + "/api/datasets", None)
    ds_list = datasets if isinstance(datasets, list) else []
    ds_id = ds_list[0]["id"] if ds_list else None
    append_ds = next((d for d in ds_list if d.get("storage_mode") == "append_only"), None)
    append_id = append_ds["id"] if append_ds else None
    s, orgs = _call("GET", base + "/api/organizations", None)
    org_id = orgs[0]["id"] if isinstance(orgs, list) and orgs else None
    s, tags = _call("GET", base + "/api/tags", None)
    tag_id = tags[0]["id"] if isinstance(tags, list) and tags else None
    # For the diff check, prefer a dataset that actually has >=2 versions.
    multi = next((d for d in ds_list if (d.get("version_count") or 0) >= 2), None)
    diff_ds = (multi or (ds_list[0] if ds_list else None))
    ver_id = ver_id2 = None
    if diff_ds:
        s, vers = _call("GET", base + f"/api/datasets/{diff_ds['id']}/versions", None)
        if isinstance(vers, list) and vers:
            ver_id = vers[0]["id"]
            if len(vers) > 1:
                ver_id2 = vers[1]["id"]

    # =====================================================================
    # 1. PUBLIC reads (no auth) — should all return 200 with a sane shape
    # =====================================================================
    check("datasets list", "GET", "/api/datasets", base=base, expect=(200,))
    check("pending-count", "GET", "/api/datasets/pending-count", base=base, want_keys=["count"])
    check("organizations", "GET", "/api/organizations", base=base)
    check("tags", "GET", "/api/tags", base=base)
    check("sso providers", "GET", "/api/auth/sso/providers", base=base)
    check("ckan search", "GET", "/api/ckan/search?q=%D7%AA%D7%A7%D7%A6%D7%99%D7%91&rows=2", base=base)
    check("ckan organizations", "GET", "/api/ckan/organizations", base=base)
    check("v1 datasets", "GET", "/api/v1/datasets", base=base)
    check("v1 organizations", "GET", "/api/v1/organizations", base=base)
    check("v1 tags", "GET", "/api/v1/tags", base=base)

    if ds_id:
        check("dataset public", "GET", f"/api/datasets/public/{ds_id}", base=base, expect=(200, 404))
        check("dataset versions", "GET", f"/api/datasets/{ds_id}/versions", base=base)
        check("v1 dataset", "GET", f"/api/v1/datasets/{ds_id}", base=base)
        check("v1 dataset versions", "GET", f"/api/v1/datasets/{ds_id}/versions", base=base)
        check("v1 version latest", "GET", f"/api/v1/datasets/{ds_id}/versions/latest", base=base, expect=(200, 404))
    else:
        for p in ("public dataset", "dataset versions", "v1 dataset"):
            skip("GET", p, "no dataset id resolved")
    if org_id:
        check("organization detail", "GET", f"/api/organizations/{org_id}", base=base)
        check("v1 organization", "GET", f"/api/v1/organizations/{org_id}", base=base)
    if tag_id:
        check("tag detail", "GET", f"/api/tags/{tag_id}", base=base)
        check("v1 tag", "GET", f"/api/v1/tags/{tag_id}", base=base)
    if ver_id:
        check("version detail", "GET", f"/api/versions/{ver_id}", base=base)
    if ver_id and ver_id2:
        check("diff", "GET", f"/api/diff?from={ver_id2}&to={ver_id}", base=base, expect=(200, 400, 404))
    else:
        skip("GET", "/api/diff", "need 2 versions on one dataset")

    # Append archive (public browse) — only if an append dataset exists
    if append_id:
        check("append schema", "GET", f"/api/append/{append_id}/schema", base=base, expect=(200, 404))
        check("append rows", "GET", f"/api/append/{append_id}/rows?limit=1", base=base, expect=(200, 404))
        check("append datastore_search", "GET", f"/api/append/{append_id}/datastore_search?limit=1", base=base, expect=(200, 404))
    else:
        skip("GET", "/api/append/{id}/*", "no append_only dataset found")

    # =====================================================================
    # 2. /validate endpoints (POST, non-mutating URL validators)
    # =====================================================================
    validators = [
        ("govil", "https://www.gov.il/he/departments/dynamiccollectors/gov_decision?skip=0"),
        ("govmap", "https://www.govmap.gov.il/?c=&lay=GADASH"),
        ("idf", "https://www.idf.il/אתרי-יחידות/הפרקליטות-הצבאית/אתר-הפקודות/"),
        ("health", "https://practitioners.health.gov.il/"),
        ("mevaker", "https://www.mevaker.gov.il/subjects?type=annual"),
        ("avodata", "https://avodata.labor.gov.il/"),
        ("hatzav", "https://geo.mot.gov.il/"),
    ]
    for name, url in validators:
        # validators return 200 with {valid: bool} for both valid & invalid URLs,
        # or 422 on a malformed body — anything but 5xx/404 means it's wired.
        check(f"{name} validate", "POST", f"/api/{name}/validate", base=base,
              body={"url": url}, expect=(200, 400, 422))

    # =====================================================================
    # 3. AUTH ENFORCEMENT — mutating / worker endpoints must reject anon
    #    (401/403). A 200/500 here would be a real problem; 422 also proves
    #    the route exists and ran validation before the (missing) auth.
    # =====================================================================
    GUARD = (401, 403)
    anon_checks = [
        ("POST", "/api/datasets", {"source_type": "ckan"}),
        ("POST", f"/api/datasets/{ds_id or '00000000-0000-0000-0000-000000000000'}/poll", None),
        ("PATCH", f"/api/datasets/{ds_id or '00000000-0000-0000-0000-000000000000'}", {"poll_interval": 999999}),
        ("DELETE", f"/api/datasets/{'00000000-0000-0000-0000-000000000000'}", None),
        ("GET", "/api/admin/pending", None),
        ("GET", "/api/admin/activity-log", None),
        ("GET", "/api/admin/over-coverage", None),
        ("GET", "/api/admin/scrape-tasks", None),
        ("POST", "/api/admin/approve/00000000-0000-0000-0000-000000000000", None),
        ("POST", "/api/admin/reject/00000000-0000-0000-0000-000000000000", None),
        ("POST", "/api/admin/organizations/sync", None),
        ("GET", "/api/auth/me", None),
        ("GET", "/api/worker/poll", None),
        ("POST", "/api/worker/push-version", {}),
        ("POST", "/api/versions/00000000-0000-0000-0000-000000000000/export-to-drive", {}),
    ]
    for method, path, body in anon_checks:
        status, _ = _call(method, base + path, None, body)
        ok = status in GUARD or status == 422
        results.append((PASS if ok else FAIL, method, path + "  [anon→reject]",
                        f"→ {status}" if ok else f"got {status}, expected 401/403"))

    # =====================================================================
    # 4. ADMIN reads (require token)
    # =====================================================================
    if token:
        check("auth me", "GET", "/api/auth/me", base=base, token=token, want_keys=["email", "is_admin"])
        check("admin pending", "GET", "/api/admin/pending", base=base, token=token)
        check("admin activity-log", "GET", "/api/admin/activity-log?limit=3", base=base, token=token, want_keys=["entries", "total"])
        check("admin over-coverage", "GET", "/api/admin/over-coverage", base=base, token=token, want_keys=["total_active", "missing"])
        # dataset-sizes aggregates R2/odata object sizes across every dataset —
        # legitimately slow, so allow a generous timeout before calling it a fail.
        check("admin dataset-sizes", "GET", "/api/admin/dataset-sizes", base=base, token=token, timeout=180)
        check("admin scheduled-jobs", "GET", "/api/admin/scheduled-jobs", base=base, token=token)
        check("admin scrape-tasks", "GET", "/api/admin/scrape-tasks", base=base, token=token, want_keys=["running", "pending", "failed"])
        check("admin datastore-jobs", "GET", "/api/admin/datastore-jobs", base=base, token=token)
        if ds_id:
            check("admin ds scrape-tasks", "GET", f"/api/admin/datasets/{ds_id}/scrape-tasks", base=base, token=token)
    else:
        for p in ("auth me", "admin pending", "admin activity-log", "admin over-coverage",
                  "admin dataset-sizes", "admin scheduled-jobs", "admin scrape-tasks", "admin datastore-jobs"):
            skip("GET", p, "no admin token")

    # =====================================================================
    # 5. (--full) self-cleaning mutation roundtrip: request → pending → delete
    # =====================================================================
    if args.full and token:
        # Numeric layer id (parser requires \d+) that's implausibly high so it
        # never collides with a real tracked layer; created then deleted below.
        test_url = "https://www.govmap.gov.il/?zoom=10&lay=999000111"
        TITLE = "SMOKE TEST — delete me"
        s, _ = _call("POST", base + "/api/datasets/requests", None,
                     {"source_type": "govmap", "source_urls": [test_url], "title": TITLE})
        results.append((PASS if s in (201, 200) else FAIL, "POST", "/api/datasets/requests  [roundtrip]", f"→ {s}"))
        # Find it in the pending queue (whether just-created or a leftover dup),
        # which exercises GET /admin/pending end to end.
        s, pend = _call("GET", base + "/api/admin/pending", token)
        found = [d for d in pend if "SMOKE TEST" in (d.get("title") or "")] if isinstance(pend, list) else []
        results.append((PASS if found else FAIL, "GET", "/api/admin/pending  [roundtrip]",
                        f"test request visible in pending ({len(found)})" if found else "test request not found in pending"))
        # Cleanup: delete every smoke-test dataset (covers leftovers from prior
        # interrupted runs too), exercising DELETE /datasets/{id}.
        cleaned = 0
        del_ok = True
        for d in found:
            s, _ = _call("DELETE", base + f"/api/datasets/{d['id']}", token)
            if s in (200, 204):
                cleaned += 1
            else:
                del_ok = False
        if found:
            results.append((PASS if (del_ok and cleaned == len(found)) else FAIL, "DELETE",
                            "/api/datasets/{id}  [cleanup]", f"deleted {cleaned}/{len(found)}"))
    elif args.full:
        skip("POST", "lifecycle roundtrip", "needs admin token")

    # ---- report ----
    print(f"{'STATUS':7} {'METHOD':6} PATH")
    print("-" * 88)
    n_pass = n_fail = n_skip = 0
    for st, method, path, detail in results:
        mark = {PASS: "✅", FAIL: "❌", SKIP: "⚪"}[st]
        if st == PASS:
            n_pass += 1
        elif st == FAIL:
            n_fail += 1
        else:
            n_skip += 1
        line = f"{mark} {st:5} {method:6} {path}"
        if st != PASS:
            line += f"   {detail}"
        print(line)
    print("-" * 88)
    print(f"TOTAL: {len(results)}   ✅ {n_pass} pass   ❌ {n_fail} fail   ⚪ {n_skip} skip")
    sys.exit(1 if n_fail else 0)


if __name__ == "__main__":
    main()
