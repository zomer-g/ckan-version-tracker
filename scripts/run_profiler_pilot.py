#!/usr/bin/env python3
"""Run the table-profiler PILOT locally.

Profiles ~N tables spanning every source type in the system (public append_*,
odata, knesset, idx), stores each deterministic SQL profile in the append DB
(public.over_table_profiles), and — if a DeepSeek/Anthropic key is configured —
adds the LLM enrichment layer (Hebrew summary + per-column semantic type).

Requires the same environment the app uses:
    APPEND_DATABASE_URL           (read/write — to create + write profiles)
    APPEND_READONLY_DATABASE_URL  (optional — profiling reads fall back to r/w)
    DEEPSEEK_API_KEY / ANTHROPIC_API_KEY  (optional — enables --enrich)

Usage:
    python scripts/run_profiler_pilot.py               # 20 tables, enrich if key present
    python scripts/run_profiler_pilot.py --n 20 --no-enrich
    python scripts/run_profiler_pilot.py --force       # re-profile unchanged tables
    python scripts/run_profiler_pilot.py --table public append_xxated  # one table
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

# Ensure the repo root is importable when run as `python scripts/…`.
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import async_session  # noqa: E402
from app.services import append_store, table_profiler  # noqa: E402


async def _main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20, help="pilot size (default 20)")
    ap.add_argument("--no-enrich", dest="enrich", action="store_false",
                    help="skip the LLM enrichment layer")
    ap.add_argument("--force", action="store_true",
                    help="re-profile even if the table signature is unchanged")
    ap.add_argument("--table", nargs=2, metavar=("SCHEMA", "TABLE"),
                    help="profile ONE table instead of the pilot")
    args = ap.parse_args()

    if not append_store.is_configured():
        print("ERROR: APPEND_DATABASE_URL is not configured — cannot reach the append DB.",
              file=sys.stderr)
        return 2

    print(f"LLM provider: {table_profiler.llm_provider() or '(none — SQL-only)'}")
    await table_profiler.ensure_profile_table()

    async with async_session() as db:
        if args.table:
            schema, table = args.table
            localities = await table_profiler._load_locality_names(db)
            prof = await table_profiler.profile_table(schema, table, locality_names=localities)
            if args.enrich and table_profiler.llm_available():
                sample = await append_store.sample_rows(table, schema=schema, limit=5)
                enr = await table_profiler.enrich_profile(schema, table, db, sample_rows=sample.get("rows"))
                prof["_enrichment"] = enr
            print(json.dumps(prof, ensure_ascii=False, indent=2))
            return 0

        res = await table_profiler.run_pilot(db, n=args.n, enrich=args.enrich, force=args.force)

    print("\n=== PILOT SUMMARY ===")
    print(f"buckets covered : {', '.join(res['buckets'])}")
    print(f"profiled        : {len(res['profiled'])}")
    for t in res["profiled"]:
        print(f"   ✓ {t}")
    if res["skipped"]:
        print(f"skipped (unchanged): {len(res['skipped'])}")
    print(f"enriched (LLM)  : {res['enriched']}")
    if res["errors"]:
        print(f"errors          : {len(res['errors'])}")
        for e in res["errors"]:
            print(f"   ✗ {e['table']}: {e['error']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
