#!/usr/bin/env python3
"""Standalone flights collector — the OVER flights pipeline, distilled.

Everything OVER does for מאגר הטיסות, with none of OVER: no tracked_datasets,
no VersionIndex, no storage plans, no admin UI. One file, one table, one loop.

    pip install httpx asyncpg
    export DATABASE_URL="postgresql://user:pass@host/db"
    python standalone_flights_collector.py            # one pass
    python standalone_flights_collector.py --loop     # every 15 min, forever

What it does, once per pass:
  1. Read the whole flight board via CKAN's datastore_search API (paged).
     NOT the file download — that URL is behind Google-OAuth and returns HTML.
  2. Hash each full row (SHA-256 over all column values).
  3. INSERT ... ON CONFLICT (row_hash) DO NOTHING into Postgres.

Result: every distinct STATE of every flight, each stamped with first_seen —
the moment it was first observed. Rows already seen are silently rejected by
the unique index, so re-reading an unchanged board is a cheap no-op.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import sys

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("flights")

# ── configuration ────────────────────────────────────────────────────────────
CKAN_BASE = "https://data.gov.il/api/3/action"
RESOURCE_ID = "e83f763b-b7d7-479e-b172-ae981ddc6de5"   # flydata, the only resource
TABLE = "flights"

PAGE_SIZE = 10_000          # rows per datastore_search call (memory ceiling)
PAGE_TIMEOUT = 120.0        # seconds per HTTP request
PAGE_MAX_RETRIES = 6        # transient timeouts / 5xx are retried, 4xx is not
FLUSH_EVERY = 5_000         # rows buffered before an INSERT
MAX_PARAMS = 30_000         # Postgres binds max 32767; stay under
POLL_INTERVAL = 900         # 15 min — matches the source's refresh cadence


# ── reading the source ───────────────────────────────────────────────────────
async def fetch_page(client: httpx.AsyncClient, offset: int) -> list[dict]:
    """One datastore_search page, with capped exponential backoff.

    A single slow page must not abort the pass, but a 4xx will never fix
    itself — so only timeouts, transport errors and 5xx are retried.
    """
    last: Exception | None = None
    for attempt in range(1, PAGE_MAX_RETRIES + 1):
        try:
            resp = await client.get(
                f"{CKAN_BASE}/datastore_search",
                params={"resource_id": RESOURCE_ID, "limit": PAGE_SIZE, "offset": offset},
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success"):
                raise RuntimeError(f"success=false: {data.get('error')}")
            records = data.get("result", {}).get("records") or []
            # `_id` is CKAN's synthetic row counter — it changes when the table
            # is rebuilt, so including it would make every row look new.
            return [{k: v for k, v in r.items() if k != "_id"} for r in records]
        except httpx.HTTPStatusError as e:
            if e.response.status_code < 500:
                raise
            last = e
        except (httpx.TimeoutException, httpx.TransportError, RuntimeError) as e:
            last = e
        if attempt < PAGE_MAX_RETRIES:
            backoff = min(30.0, 2.0 ** attempt)
            log.warning("retry %d/%d at offset=%d (%s) — backoff %.0fs",
                        attempt, PAGE_MAX_RETRIES, offset, type(last).__name__, backoff)
            await asyncio.sleep(backoff)
    raise RuntimeError(f"datastore_search failed at offset={offset}: {last!r}")


async def fetch_columns(client: httpx.AsyncClient) -> list[str]:
    """The resource's column names, from datastore_search's field metadata."""
    resp = await client.get(
        f"{CKAN_BASE}/datastore_search",
        params={"resource_id": RESOURCE_ID, "limit": 0},
    )
    resp.raise_for_status()
    fields = resp.json()["result"]["fields"]
    return [f["id"] for f in fields if f["id"] != "_id"]


# ── identity ─────────────────────────────────────────────────────────────────
def row_hash(row: dict, cols: list[str]) -> str:
    """SHA-256 over the row's values — the dedup identity.

    Order-independent (sort_keys) and None-safe (None and "" collapse), so the
    same flight state always hashes the same regardless of key order in the
    JSON the API happened to return.
    """
    canonical = json.dumps(
        {c: ("" if row.get(c) is None else str(row.get(c))) for c in cols},
        sort_keys=True, ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ── writing ──────────────────────────────────────────────────────────────────
async def ensure_table(conn, cols: list[str]) -> None:
    """Create the table, add any newly-appeared source column, ensure the index.

    Every value is stored as text: archival robustness over typing — a source
    that starts emitting "N/A" in a numeric column must not break the archive.
    The UNIQUE index on row_hash IS the dedup mechanism.
    """
    cols_sql = ", ".join(f'"{c}" text' for c in cols)
    await conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{TABLE}" ({cols_sql}, '
        f'"first_seen" timestamptz NOT NULL DEFAULT now(), "row_hash" text)'
    )
    for c in cols:  # schema drift: a new column at the source
        await conn.execute(f'ALTER TABLE "{TABLE}" ADD COLUMN IF NOT EXISTS "{c}" text')
    await conn.execute(
        f'CREATE UNIQUE INDEX IF NOT EXISTS "{TABLE}_row_hash_uq" ON "{TABLE}" ("row_hash")'
    )


def build_insert(cols: list[str], chunk: list[dict]) -> tuple[str, list]:
    """One multi-row INSERT ... ON CONFLICT DO NOTHING.

    Dedups WITHIN the chunk too: Postgres errors if a single statement carries
    the same conflict key twice, and a board can legitimately repeat a row.
    first_seen is inlined as now() so only rows that actually insert get
    stamped — a conflicting row keeps its original timestamp.
    """
    values, params, seen = [], [], set()
    p = 1
    for r in chunk:
        h = row_hash(r, cols)
        if h in seen:
            continue
        seen.add(h)
        placeholders = []
        for c in cols:
            v = r.get(c)
            params.append(None if v is None else str(v))
            placeholders.append(f"${p}")
            p += 1
        placeholders.append("now()")
        params.append(h)
        placeholders.append(f"${p}")
        p += 1
        values.append("(" + ",".join(placeholders) + ")")
    if not values:
        return "", []
    cols_sql = ",".join(f'"{c}"' for c in cols + ["first_seen", "row_hash"])
    return (
        f'INSERT INTO "{TABLE}" ({cols_sql}) VALUES {",".join(values)} '
        f'ON CONFLICT ("row_hash") DO NOTHING'
    ), params


async def append_rows(conn, cols: list[str], rows: list[dict]) -> int:
    """Insert rows in param-bounded chunks. Returns how many actually landed."""
    if not rows:
        return 0
    per_row = len(cols) + 1          # + row_hash; first_seen uses now(), no param
    size = max(1, MAX_PARAMS // per_row)
    inserted = 0
    for i in range(0, len(rows), size):
        sql, params = build_insert(cols, rows[i:i + size])
        if not sql:
            continue
        status = await conn.execute(sql, *params)   # "INSERT 0 42"
        try:
            inserted += int(status.split()[-1])
        except (ValueError, IndexError):
            pass
    return inserted


# ── the pass ─────────────────────────────────────────────────────────────────
async def collect_once(pool: asyncpg.Pool) -> dict:
    async with httpx.AsyncClient(timeout=PAGE_TIMEOUT) as client:
        cols = await fetch_columns(client)
        async with pool.acquire() as conn:
            await ensure_table(conn, cols)

            added = pages = scanned = 0
            pending: list[dict] = []
            offset = 0
            while True:
                batch = await fetch_page(client, offset)
                if not batch:
                    break
                pages += 1
                scanned += len(batch)
                offset += len(batch)
                pending.extend(batch)
                if len(pending) >= FLUSH_EVERY:
                    added += await append_rows(conn, cols, pending)
                    pending = []
            added += await append_rows(conn, cols, pending)

            total = await conn.fetchval(f'SELECT count(*) FROM "{TABLE}"')

    log.info("pass complete: scanned=%d added=%d total=%d pages=%d",
             scanned, added, total, pages)
    return {"scanned": scanned, "added": added, "total": total, "pages": pages}


async def main(loop_forever: bool) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        log.error("DATABASE_URL is not set")
        return 1
    # min_size=0 lets a serverless Postgres (Neon) scale its compute to zero
    # between passes — on a 15-min cadence that is most of the day.
    pool = await asyncpg.create_pool(dsn, min_size=0, max_size=4)
    try:
        while True:
            try:
                await collect_once(pool)
            except Exception:
                # One bad pass must never kill the loop: the next one re-reads
                # the whole board anyway, and ON CONFLICT makes that free.
                log.exception("pass failed")
                if not loop_forever:
                    return 1
            if not loop_forever:
                return 0
            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await pool.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--loop", action="store_true",
                    help=f"keep running, one pass every {POLL_INTERVAL}s")
    sys.exit(asyncio.run(main(ap.parse_args().loop)))
