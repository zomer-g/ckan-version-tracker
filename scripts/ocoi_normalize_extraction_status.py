"""One-off: fold `extraction_status='completed'` back into `'extracted'`.

The migrated corpus carries `'extracted'` — that is what OCOI itself wrote and
what 2,917 of the documents hold. The worker ingestion path added in Phase 4a
wrote `'completed'` instead, so every document that arrived after the migration
got a second name for the same state. Nothing reads `'completed'`: the admin
counters ask for `'pending'`/`'failed'`, and the document filter offered
"הושלם" = `'completed'`, which therefore matched only the handful of new rows
and hid the whole migrated corpus behind an empty-looking result.

app/services/ocoi_ingest.py now writes `'extracted'`. This repairs the rows
written before that fix. It is idempotent — run it as many times as you like.

    python -m scripts.ocoi_normalize_extraction_status          # report only
    python -m scripts.ocoi_normalize_extraction_status --apply  # write

Needs OCOI_DATABASE_URL in the environment, the same variable the app uses.
"""
from __future__ import annotations

import argparse
import asyncio
import sys


async def main(apply: bool) -> int:
    from app.services import ocoi_db

    if not ocoi_db.is_configured():
        print("OCOI_DATABASE_URL is not set — nothing to do.", file=sys.stderr)
        return 2

    rows = await ocoi_db.fetch(
        "SELECT extraction_status, count(*) AS n FROM documents "
        "GROUP BY 1 ORDER BY n DESC")
    print("before:")
    for r in rows:
        print(f"  {r['extraction_status'] or '(null)':<12} {r['n']:>6}")

    stale = await ocoi_db.fetchval(
        "SELECT count(*) FROM documents WHERE extraction_status = 'completed'")
    stale = int(stale or 0)
    if not stale:
        print("\nnothing to normalise — no 'completed' rows.")
        return 0

    if not apply:
        print(f"\n{stale} row(s) would become 'extracted'. Re-run with --apply.")
        return 0

    await ocoi_db.execute(
        "UPDATE documents SET extraction_status = 'extracted' "
        "WHERE extraction_status = 'completed'")
    left = int(await ocoi_db.fetchval(
        "SELECT count(*) FROM documents WHERE extraction_status = 'completed'") or 0)
    print(f"\nnormalised {stale} row(s); 'completed' remaining: {left}")
    return 0 if left == 0 else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="write the change (default is a dry report)")
    sys.exit(asyncio.run(main(ap.parse_args().apply)))
