#!/usr/bin/env python3
"""One-off backfill CLI: copy a CKAN dataset's version history from the ODATA
mirror onto R2, then repoint every version at the R2 objects.

Thin wrapper around ``app.services.r2_backfill`` (the same core the admin
endpoint ``POST /api/admin/datasets/{id}/migrate-r2`` uses). See that module
for the full rationale and mechanics.

DRY-RUN by default; ``--apply`` uploads+commits; ``--activate`` (with --apply)
also flips the dataset to the R2 backend and re-activates it. Requires
DATABASE_URL + S3_* env. Run from the repo root:

    python -m scripts.backfill_dataset_to_r2 <dataset_uuid> [--apply] [--activate]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import uuid as uuidlib

from app.database import async_session
from app.services.r2_backfill import backfill_dataset_to_r2

DEFAULT_DATASET = "81c9013d-a167-41f5-b71a-fcc8d6dd592d"


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dataset", nargs="?", default=DEFAULT_DATASET,
                    help="OVER tracked_dataset UUID")
    ap.add_argument("--apply", action="store_true",
                    help="upload to R2 and commit the DB rewrite (default: dry-run)")
    ap.add_argument("--activate", action="store_true",
                    help="with --apply: set storage_backend=r2 and is_active=true")
    args = ap.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== Backfill dataset {args.dataset} to R2 [{mode}] ===")
    async with async_session() as db:
        s = await backfill_dataset_to_r2(
            db, uuidlib.UUID(args.dataset),
            apply=args.apply, activate=args.activate,
        )

    if s.get("error"):
        print(f"ERROR: {s['error']}", file=sys.stderr)
        return 2

    print(f"Dataset: {s['title']!r}  source_type={s['source_type']}  "
          f"is_active={s['is_active']}  storage={s['storage_target']}")
    print(f"Versions: {s['versions']}  "
          f"unique ODATA resources: {s['unique_odata_resources']}")
    for p in s["plan"]:
        print(f"  {p['odata_id']}  {(p['format'] or '?'):5}  "
              f"size={p['size']}  -> {p['key']}")
    print(f"\nMigrated: {s['migrated']}  failed: {len(s['failed'])}  "
          f"repointed values: {s['repointed_values']}")
    if s["failed"]:
        print("FAILED (left on ODATA):")
        for f in s["failed"]:
            print(f"  - {f['id']}: {f['reason']}")
    print(f"activated={s['activated']}  committed={s['committed']}")
    if not args.apply:
        print("\nDRY-RUN — no uploads, no DB changes. Re-run with --apply.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
