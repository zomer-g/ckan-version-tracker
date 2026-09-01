"""Export OCOI's six auth/billing tables before the Render database is deleted.

These are the ONLY tables that were never migrated. The port moved 14 data
tables into schema `ocoi` of OVER's append DB and deliberately left these
behind, because the append DB backs the PUBLIC SQL console and these carry
hashed OAuth codes, refresh tokens and Stripe customer ids:

    users, oauth_clients, oauth_authorization_codes, oauth_refresh_tokens,
    billing_accounts, usage_events

So they exist in exactly one place — the Render Postgres `ocoi-db`, which is on
the free plan and expires. Once that instance is gone they are gone, including
the un-billed `usage_events` rows (stripe_pushed_at IS NULL).

`pg_dump` is not installed on this machine (a missing DLL — the same reason the
Phase 0 migration was written against asyncpg), so this reads the rows directly
and writes both JSONL and a restore-ready SQL file per table.

    # PowerShell — paste the External Connection String from the Render
    # dashboard (ocoi-db -> Connect -> External). It is NOT stored anywhere.
    $env:OCOI_RENDER_DATABASE_URL = "postgresql://..."
    python -m scripts.ocoi_export_auth_tables --out backups/ocoi-auth

Verify the row counts it prints against the dashboard before deleting anything.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

TABLES = [
    "users",
    "oauth_clients",
    "oauth_authorization_codes",
    "oauth_refresh_tokens",
    "billing_accounts",
    "usage_events",
]


def _jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex()
    return v


def _sql_literal(v) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "'\\x" + bytes(v).hex() + "'::bytea"
    if isinstance(v, (dict, list)):
        v = json.dumps(v, ensure_ascii=False)
    elif isinstance(v, (datetime, date)):
        v = v.isoformat()
    return "'" + str(v).replace("'", "''") + "'"


async def main(out_dir: Path) -> int:
    dsn = os.environ.get("OCOI_RENDER_DATABASE_URL", "").strip()
    if not dsn:
        print("Set OCOI_RENDER_DATABASE_URL to ocoi-db's External Connection "
              "String (Render dashboard) and re-run.", file=sys.stderr)
        return 2

    import asyncpg

    out_dir.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(dsn=dsn, ssl="require")
    summary: dict[str, int] = {}
    try:
        for table in TABLES:
            exists = await conn.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", f"public.{table}")
            if not exists:
                print(f"  {table:<30} ABSENT — skipped")
                summary[table] = -1
                continue

            rows = await conn.fetch(f'SELECT * FROM "{table}"')
            summary[table] = len(rows)

            with (out_dir / f"{table}.jsonl").open("w", encoding="utf-8") as fh:
                for r in rows:
                    fh.write(json.dumps({k: _jsonable(v) for k, v in dict(r).items()},
                                        ensure_ascii=False) + "\n")

            with (out_dir / f"{table}.sql").open("w", encoding="utf-8") as fh:
                fh.write(f"-- {table}: {len(rows)} row(s)\n")
                if rows:
                    cols = list(rows[0].keys())
                    collist = ", ".join(f'"{c}"' for c in cols)
                    for r in rows:
                        vals = ", ".join(_sql_literal(r[c]) for c in cols)
                        fh.write(f'INSERT INTO "{table}" ({collist}) VALUES ({vals});\n')
            print(f"  {table:<30} {len(rows):>7} row(s)")
    finally:
        await conn.close()

    (out_dir / "MANIFEST.json").write_text(
        json.dumps({"tables": summary,
                    "note": "OCOI auth/billing tables, never migrated to OVER"},
                   ensure_ascii=False, indent=2), encoding="utf-8")
    total = sum(n for n in summary.values() if n > 0)
    print(f"\nwrote {total} row(s) across {len([n for n in summary.values() if n >= 0])} "
          f"table(s) to {out_dir}")
    print("Check these counts against the Render dashboard BEFORE deleting the database.")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default="backups/ocoi-auth",
                    help="directory to write into (default: backups/ocoi-auth)")
    sys.exit(asyncio.run(main(Path(ap.parse_args().out))))
