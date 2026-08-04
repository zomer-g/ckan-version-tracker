#!/usr/bin/env python3
"""Run SQL against the live גרסאות לעם /data console and print what it returns.

Why this exists: the console endpoint takes base64 (so Hebrew SQL survives the
trip), answers 307 on the apex domain (urllib drops the POST body on the
redirect), and enforces a 10-second statement timeout. Every one of those is a
five-minute detour when you rediscover it mid-task. This does the boring parts
and prints the timing, so a query that is drifting toward the timeout is visible
before it fails.

    python run_sql.py query.sql              # run a file
    echo "SELECT 1" | python run_sql.py      # or stdin, for trivial SQL only
    python run_sql.py query.sql --rows 20    # show more rows
    python run_sql.py query.sql --json out.json   # keep the full result
    python run_sql.py --catalog catalog.json      # cache the table catalog

Write the SQL to a FILE rather than echoing it. Hebrew and the double quotes
around Hebrew identifiers do not survive shell quoting intact, and the result is
a query that silently asks something you did not write (`over_settlement('פ"ת')`
came back NULL that way, and correct from a file).

Exit code is 1 when the query errors, so it can gate a script.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import sys
import time
import urllib.error
import urllib.request

URL = "https://www.over.org.il/api/tables/sql"
CATALOG_URL = "https://www.over.org.il/api/tables"
# Cloudflare in front of R2/the app answers 403 to a bare urllib user-agent.
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")


def run(sql: str, timeout: int = 300) -> dict:
    """POST the SQL and return the parsed result. Raises RuntimeError on a
    server-side SQL error, with the message the console itself would show."""
    body = json.dumps({"sql_b64": base64.b64encode(sql.encode()).decode()}).encode()
    req = urllib.request.Request(
        URL, data=body, headers={"Content-Type": "application/json", "User-Agent": UA})
    try:
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode())
    except urllib.error.HTTPError as e:
        detail = e.read().decode()
        try:
            detail = json.loads(detail).get("detail", detail)
        except ValueError:
            pass
        raise RuntimeError(f"HTTP {e.code}: {detail}") from None


def fetch_catalog(path: str) -> int:
    """Cache the whole-site table catalog to `path`. Grep it instead of asking
    the API per table — it is ~1.2 MB and holds every column plus its Hebrew
    alias."""
    req = urllib.request.Request(CATALOG_URL, headers={"User-Agent": UA})
    data = urllib.request.urlopen(req, timeout=300).read()
    with open(path, "wb") as fh:
        fh.write(data)
    return len(json.loads(data.decode())["tables"])


def main() -> int:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("file", nargs="?", help="file holding the SQL (default: stdin)")
    ap.add_argument("--rows", type=int, default=5, help="rows to print (default 5)")
    ap.add_argument("--width", type=int, default=300, help="chars per printed row")
    ap.add_argument("--json", metavar="PATH", help="write the full result here")
    ap.add_argument("--catalog", metavar="PATH",
                    help="fetch the table catalog to PATH and exit")
    args = ap.parse_args()

    if args.catalog:
        print(f"cached {fetch_catalog(args.catalog)} tables -> {args.catalog}")
        return 0

    sql = open(args.file, encoding="utf-8").read() if args.file else sys.stdin.read()
    if not sql.strip():
        print("no SQL given", file=sys.stderr)
        return 2

    t0 = time.time()
    try:
        res = run(sql)
    except RuntimeError as e:
        print(f"FAIL  {time.time() - t0:.1f}s  {e}")
        return 1

    rows = res.get("rows", [])
    took = time.time() - t0
    # Zero rows is a failed query until proven otherwise — flag it loudly rather
    # than letting an empty result read as an answer.
    print(f"{'OK   ' if rows else 'EMPTY'} {took:.1f}s  {len(rows)} rows"
          f"{' (truncated at the display cap)' if res.get('truncated') else ''}")
    print(f"cols: {res.get('columns')}")
    for r in rows[:args.rows]:
        print("   ", str(r)[:args.width])
    if took > 7:
        print("!! close to the 10s statement timeout — see the performance "
              "section in SKILL.md before handing this over")
    if args.json:
        json.dump(res, open(args.json, "w", encoding="utf-8"), ensure_ascii=False)
        print(f"full result -> {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
