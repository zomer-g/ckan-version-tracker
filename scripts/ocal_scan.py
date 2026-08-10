#!/usr/bin/env python
"""Run the יומן לעם (Ocal) diary auto-import scan from a RESIDENTIAL IP.

WHY THIS EXISTS
    odata.org.il's raw file downloads (XLSX/ICAL) sit behind Cloudflare, which
    403s datacenter IPs — so the scan running on the Render web dyno can only
    import datastore-active CSVs (via the datastore-dump fallback) and fails on
    every XLSX diary. From a residential IP the raw download works for ALL
    formats, so run this on a machine that already hosts an OVER worker (it has
    the repo + a residential IP) via cron. It imports + enriches exactly like the
    scheduler (discover → gate → import → entity/cross-ref/matching → matview →
    logs the run in ocal.auto_import_logs with trigger='cron').

REQUIREMENTS (in the machine's .env or environment)
    OCAL_DATABASE_URL   the append DB, role ocal_app (same value as on Render)
    DEEPSEEK_API_KEY     (or ANTHROPIC_API_KEY) for LLM field-mapping of files
                         whose Hebrew headers the heuristic can't map

USAGE
    python scripts/ocal_scan.py [max_import]        # default 25 per run

CRON (every 6h; clears the backlog over the first day, then keeps up)
    0 */6 * * *  cd /path/to/ckan-versions && python scripts/ocal_scan.py 25 >> /var/log/ocal_scan.log 2>&1

Once this cron is live you can turn OFF the Render auto-scan (admin → יומן לעם →
אוטומציה → uncheck "ייבוא אוטומטי פעיל") so it stops erroring on blocked XLSX —
this residential run supersedes it.
"""
import asyncio
import os
import sys

# Make `app...` importable when run as `python scripts/ocal_scan.py` from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _main() -> int:
    from app.services import ocal_db, ocal_import

    if not ocal_db.is_configured():
        print("ERROR: OCAL_DATABASE_URL is not set — nothing to do.", file=sys.stderr)
        return 2

    try:
        cap = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    except ValueError:
        print(f"ERROR: max_import must be an integer, got {sys.argv[1]!r}", file=sys.stderr)
        return 2

    from app.config import settings
    provider = ("deepseek" if settings.deepseek_api_key
                else "anthropic" if settings.anthropic_api_key else None)
    print(f"ocal scan starting — max_import={cap}, llm_field_mapping={provider or 'OFF (heuristic only)'}")

    r = await ocal_import.scan_once(max_import=cap, trigger="cron")
    print(f"DONE: candidates={r.get('candidates')} imported={r.get('imported')} "
          f"skipped={r.get('skipped')} errors={r.get('errors')}")
    for res in (r.get("results") or []):
        print(f"  + {res.get('events_upserted')} events — {(res.get('title') or '')[:70]} "
              f"[map={res.get('map_method')}]")
    await ocal_db.close_pool()
    # Non-zero only when nothing imported AND everything errored (so cron surfaces it).
    return 1 if (r.get("errors") and not r.get("imported") and not r.get("skipped")) else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
