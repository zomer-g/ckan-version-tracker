#!/usr/bin/env python3
"""Register the APPEND-set data.gov.il datasets as tracked append_only sources.

Thin CLI wrapper over app.services.append_registrar (the same logic the admin
endpoint POST /api/admin/register-append-datasets runs server-side). Idempotent:
skips a dataset that's already tracked. Registers flydata (keyless, windowed,
15-min) and the vehicle registry (keyed by mispar_rechev, daily).

Does NOT poll — registration is instant; the scheduler seeds on the next tick
(or pass --poll to seed inline; the vehicle seed streams ~4.1M rows and is slow).

Run where DATABASE_URL points at the target DB:
    python scripts/register_append_datasets.py [--poll]
"""
import argparse
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.database import async_session  # noqa: E402
from app.services.append_registrar import register_append_datasets  # noqa: E402


async def main(do_poll: bool) -> int:
    async with async_session() as db:
        results = await register_append_datasets(db)
    for r in results:
        print(f"{r['status'].upper():8} {r['ckan_id']} — {r.get('detail', '')}")

    if do_poll:
        from sqlalchemy import select
        from app.models.tracked_dataset import TrackedDataset
        from app.worker.poll_job import poll_dataset
        async with async_session() as db:
            for r in results:
                if r["status"] not in ("created", "skipped"):
                    continue
                row = (await db.execute(
                    select(TrackedDataset).where(TrackedDataset.ckan_id == r["ckan_id"])
                )).scalar_one_or_none()
                if row:
                    print(f"POLL     {r['ckan_id']} — seeding…")
                    await poll_dataset(str(row.id))
                    print(f"POLL     {r['ckan_id']} — done")

    return 0 if all(r["status"] != "error" for r in results) else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--poll", action="store_true",
        help="seed inline after registering (vehicle seed streams ~4.1M rows)",
    )
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.poll)))
