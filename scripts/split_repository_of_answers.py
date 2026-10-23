#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Split gov.il's מאגר התשובות into one OVER dataset per "סוג מידע".

WHY
---
``repository-of-answers`` is one collector holding 15,517 items and 23,673
attachments. A full pass measured 23,412 s (6.5 h) of one worker, and it fails
often enough that the dataset's history has gaps — a single failure anywhere in
those six hours throws away the whole run.

The collector's own UI has a "סוג מידע" facet (``Info``), and the gov.il
DynamicCollector API accepts it as a query filter: the scraper already forwards
every non-pagination URL query param into ``QueryFilters`` (see
``govscraper/scrapers/govil/legacy_engine.py`` — the ``extra_filters`` block),
so ``?skip=0&Info=4`` really does scrape only הסכם התקשרות. Nothing in the
scraper needs to change; the split is entirely a matter of which URLs OVER
tracks.

WHAT THIS DOES
--------------
Creates five tracked datasets, one per Info value, each pointing at the same
collector with its own ``Info`` filter — then slows the original whole-register
dataset down to a rare cadence instead of retiring it (see RESIDUAL).

RESIDUAL — why the parent dataset stays
---------------------------------------
The five subsets do NOT cover the register. Measured over a full metadata walk
on 2026-09-07:

    Info=1  דו"ח התקשרויות      2,191 items   2,455 files
    Info=2  דוח                   194 items     164 files
    Info=3  היתר                    3 items       7 files
    Info=4  הסכם התקשרות        1,726 items   1,888 files
    Info=5  תשובה              11,144 items  18,833 files
    (none)                        305 items     375 files
    total                      15,517 items  23,673 files

305 items carry no ``Info`` at all and are reachable through no filter — the
API's ``Info=_none`` returns only 70 of them. So the only thing that covers
them is an unfiltered scrape, which is why the parent dataset keeps running,
just rarely. 33 items carry more than one ``Info`` and will appear in two
subsets; that is accepted.

Note also that Info=5 is 72% of the corpus, so it stays a heavy dataset
(~19k files). The win here is that four of five subsets become small and
reliable, and a failure in one no longer costs the other four.

USAGE
-----
Needs an OVER admin bearer token — the same JWT the admin UI holds; copy it
from ``localStorage.token`` in the browser on over.org.il.

    export OVER_ADMIN_TOKEN=eyJ...
    python scripts/split_repository_of_answers.py --dry-run
    python scripts/split_repository_of_answers.py

Creating a dataset as an admin makes it active AND queues its first poll
immediately, so a bare run starts five scrapes. ``--only`` stages that:

    python scripts/split_repository_of_answers.py --only 3,2,4,1
    python scripts/split_repository_of_answers.py --only 5

Re-running is safe: a subset whose URL is already tracked is reported and
skipped, not duplicated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

BASE_URL = "https://www.over.org.il"
COLLECTOR_URL = "https://www.gov.il/he/Departments/DynamicCollectors/repository-of-answers?skip=0"

# The parent dataset — the whole register, unfiltered. Kept as the residual
# safety net (see RESIDUAL above), moved off the weekly cadence.
PARENT_DATASET_ID = "82db2f91-9ff0-44f4-b842-df3c43f7185a"
PARENT_POLL_INTERVAL = 7_776_000  # 90 days

WEEKLY = 604_800

# Storage plan cloned from the parent: files mirrored to R2, full snapshots.
# ``download_files`` is the key that matters — without it the subsets would
# track the index CSV only and silently stop archiving the documents.
SCRAPER_CONFIG = {"download_files": True, "storage_backend": "r2"}

# Keys and labels come from the collector page's own ``InfoMultiChoiseValues``
# block, so they read the same as the facet in the UI. The counts are the
# 2026-09-07 measurement and are informational — they are printed so a run can
# be sanity-checked against what the source reports today.
SUBSETS = [
    {"info": 3, "label": "היתר", "items": 3, "files": 7},
    {"info": 2, "label": "דוח", "items": 194, "files": 164},
    {"info": 4, "label": "הסכם התקשרות", "items": 1_726, "files": 1_888},
    {"info": 1, "label": 'דו"ח התקשרויות', "items": 2_191, "files": 2_455},
    {"info": 5, "label": "תשובה", "items": 11_144, "files": 18_833},
]


def subset_url(info: int) -> str:
    """The collector URL for one Info value.

    ``skip=0`` is kept because the parent is stored with it and OVER's URL
    identity (app/services/url_identity.py) includes the sorted query string —
    keeping the shapes uniform makes the five datasets obviously siblings of
    the parent when read off the admin list.
    """
    return f"{COLLECTOR_URL}&Info={info}"


def subset_title(label: str) -> str:
    return f"מאגר התשובות — {label}"


def _request(method: str, path: str, token: str, payload: dict | None = None) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(body)
        except ValueError:
            return e.code, body


def _detail(body: dict | str) -> str:
    if isinstance(body, dict):
        return str(body.get("detail") or body)
    return str(body)[:300]


def create_subset(subset: dict, token: str, dry_run: bool) -> str:
    url = subset_url(subset["info"])
    title = subset_title(subset["label"])
    if dry_run:
        return f"DRY-RUN would create: {title}\n            {url}"

    status, body = _request("POST", "/api/datasets", token, {
        "source_type": "scraper",
        "source_url": url,
        "title": title,
        "scraper_config": dict(SCRAPER_CONFIG),
        "poll_interval": WEEKLY,
        "storage_mode": "full_snapshot",
    })
    if status == 201 and isinstance(body, dict):
        return f"created {body.get('id')}  {title}"
    # The duplicate check is by URL identity, so a re-run lands here rather
    # than making a second dataset for the same filter.
    if status == 400 and "already tracked" in _detail(body).lower():
        return f"exists (skipped)  {title}"
    return f"FAILED ({status}) {title}: {_detail(body)}"


def recadence_parent(token: str, dry_run: bool) -> str:
    if dry_run:
        return (f"DRY-RUN would set parent {PARENT_DATASET_ID} poll_interval="
                f"{PARENT_POLL_INTERVAL}s (90 days)")
    status, body = _request("PATCH", f"/api/datasets/{PARENT_DATASET_ID}", token, {
        "poll_interval": PARENT_POLL_INTERVAL,
    })
    if status == 200 and isinstance(body, dict):
        return (f"parent re-cadenced to {body.get('poll_interval')}s "
                f"({body.get('title')})")
    return f"FAILED ({status}) parent re-cadence: {_detail(body)}"


def main() -> int:
    global BASE_URL
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="print what would happen, change nothing")
    parser.add_argument("--only", default="",
                        help="comma-separated Info values to create this run, "
                             "e.g. '3,2,4' — lets you stage the five first "
                             "polls instead of starting them all at once")
    parser.add_argument("--skip-parent", action="store_true",
                        help="leave the parent dataset's cadence alone")
    parser.add_argument("--base-url", default=BASE_URL)
    args = parser.parse_args()

    BASE_URL = args.base_url.rstrip("/")

    token = os.environ.get("OVER_ADMIN_TOKEN", "").strip()
    if not token and not args.dry_run:
        print("OVER_ADMIN_TOKEN is not set. Copy the admin JWT from "
              "localStorage.token on over.org.il, or pass --dry-run.",
              file=sys.stderr)
        return 2

    wanted = None
    if args.only.strip():
        try:
            wanted = {int(v) for v in args.only.split(",") if v.strip()}
        except ValueError:
            print(f"--only must be comma-separated integers, got {args.only!r}",
                  file=sys.stderr)
            return 2

    chosen = [s for s in SUBSETS if wanted is None or s["info"] in wanted]
    if not chosen:
        print(f"--only {args.only} matched none of "
              f"{[s['info'] for s in SUBSETS]}", file=sys.stderr)
        return 2

    print(f"Splitting {COLLECTOR_URL}")
    print(f"  target: {BASE_URL}   subsets: {[s['info'] for s in chosen]}\n")
    for subset in chosen:
        print(f"  Info={subset['info']} {subset['label']} "
              f"(~{subset['items']:,} items / {subset['files']:,} files)")
        print(f"    {create_subset(subset, token, args.dry_run)}")

    if not args.skip_parent:
        print(f"\n  {recadence_parent(token, args.dry_run)}")
        print("    (the parent stays active because 305 items carry no "
              "'סוג מידע' and no filter reaches them)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
