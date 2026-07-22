"""Index-CSV → NEON mirror: the memory-bounded loader primitive.

Every scraper/govmap version carries one "נתוני הסורק" CSV on R2 — the index of
that version (GovMap feature tables including ``geometry_wkt``; FOI responses
including the ``attachment_filename``/``attachment_url`` file index). This module
loads ONE such CSV into ONE queryable table in the ``idx`` schema of the append
DB, so the /data SQL console can search *inside* the collections.

Scope: this is stage 0 of docs/neon-index-pilot/README.md §10 — the loader only.
Eligibility rules, the version-landed trigger, catalog wiring and the backfill
driver are stage 1 and deliberately NOT here yet.

Design decisions, all settled in the pilot (§10.1):

* **Latest version only.** The table is REPLACED on each sync, never appended
  to; version history stays in R2. That removes ``_row_hash`` and the ON CONFLICT
  dedup entirely — which is also why this path exists instead of reusing
  ``append_store.append_rows`` (measured 4.8–5.5x slower, plus a 1.04–1.12x
  storage premium for the unique index).
* **Every column is ``text``.** These CSVs have no reliable declared types, and
  the console lets users cast. Postgres TOAST-compresses the big geometry cells
  well enough that a table typically lands SMALLER than its source CSV (0.70x
  measured across 310 datasets).
* **Constant memory.** The object streams to a temp file, the CSV is parsed
  incrementally, and rows go out in fixed batches via ``COPY``. Nothing here ever
  holds the whole file — the largest index CSV in the corpus is 3.58 GB and the
  dyno has 512 MB.
* **Atomic swap.** Rows land in a staging table; a single transaction drops the
  old table and renames staging into place, so readers see either the previous
  version or the new one, never a partial load.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import tempfile

from app.services import append_store
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

SCHEMA = "idx"

# A COPY batch is bounded by BOTH row count and payload bytes, whichever trips
# first. Rows alone are not a memory bound: the pilot used 20k rows/batch on an
# unconstrained machine, but the largest layer in the corpus averages ~11 KB per
# row (3.58 GB over 329,182 rows), so 20k of those rows is ~220 MB in flight —
# and the dyno has 512 MB. Wide geometry rows therefore flush on the byte limit
# and narrow rows on the row limit.
COPY_BATCH_ROWS = 20_000
COPY_BATCH_BYTES = 32 * 1024 * 1024

# A single CSV cell can be enormous: one real dataset ("אינטרסים של מקורות") is
# 4 rows / 34 MB because each cell is a whole polygon. Python's default cap is
# 128 KB. Matches app/api/worker.py.
csv.field_size_limit(10**8)


def table_name(ds) -> str:
    """Stable, unique, ASCII table name for a dataset's index table.

    ``<sanitized ckan_name>_<id8>`` — the id suffix keeps two datasets that share
    a ckan_name apart. Clipped to Postgres' 63-byte identifier limit."""
    base = re.sub(r"[^a-z0-9_]+", "_", (ds.ckan_name or "").lower()).strip("_") or "ds"
    sid = str(ds.id).replace("-", "")[:8]
    return f"{base}"[:54].rstrip("_") + f"_{sid}"


def _qi(name: str) -> str:
    return append_store._qi(name)


def _qt(table: str, schema: str = SCHEMA) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def _staging_name(table: str) -> str:
    """Staging table name that stays inside the 63-byte identifier budget."""
    return append_store.clip_ident_bytes(table, 63 - len("__stg")) + "__stg"


def _iter_batches(path: str, columns: list[str], keep: list[int]):
    """Yield lists of positional row tuples, bounded by rows AND bytes.

    Flushes at ``COPY_BATCH_ROWS`` rows or ``COPY_BATCH_BYTES`` of accumulated
    cell text, whichever comes first — so peak memory stays flat whether the
    table is 40 narrow columns or one 10 KB geometry blob per row.

    Rows are normalised to exactly ``len(columns)`` values: short rows (a ragged
    CSV) are padded with None, long ones truncated, so COPY never rejects a row
    for arity. Each batch is dropped by the caller before the next is built."""
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header already consumed by the caller
        batch: list[tuple] = []
        nbytes = 0
        for row in reader:
            rec = tuple(row[i] if i < len(row) else None for i in keep)
            batch.append(rec)
            # len() on str is character count, close enough as a size proxy and
            # far cheaper than encoding every cell just to measure it.
            nbytes += sum(len(v) for v in rec if v is not None)
            if len(batch) >= COPY_BATCH_ROWS or nbytes >= COPY_BATCH_BYTES:
                yield batch
                batch = []
                nbytes = 0
        if batch:
            yield batch


async def load_index_csv(r2_value: str, table: str) -> dict:
    """Load one index CSV from R2 into ``idx.<table>``, replacing it atomically.

    ``r2_value`` is the ``r2:``-marked value straight out of
    ``version_index.resource_mappings["נתוני הסורק"]``.

    Returns ``{"table", "rows", "columns"}``. Raises on failure — the caller owns
    the retry/checkpoint policy (stage 1); a failure here leaves the PREVIOUS
    table untouched, because nothing is swapped until the load has finished.
    """
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured (APPEND_DATABASE_URL missing)")

    tmp: str | None = None
    try:
        fd, tmp = tempfile.mkstemp(suffix=".csv", prefix="idx-load-")
        os.close(fd)
        if not await storage_client.download_to_file(r2_value, tmp):
            raise RuntimeError(f"could not download {r2_value} from object storage")

        with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh), None)
        if not header:
            raise ValueError("index CSV is empty (no header row)")

        # 63-BYTE clip + dedup. Postgres truncates identifiers by bytes, and a
        # Hebrew letter is 2 of them, so distinct long headers sharing a prefix
        # would otherwise collapse onto each other.
        safe = append_store.safe_column_names(header)
        keep = [i for i, c in enumerate(safe) if c and c != "_id"]
        columns = [safe[i] for i in keep]
        if not columns:
            raise ValueError("index CSV has no usable columns")

        staging = _staging_name(table)
        defs = ", ".join(f"{_qi(c)} text" for c in columns)
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(SCHEMA)}")
            await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
            await conn.execute(f"CREATE TABLE {_qt(staging)} ({defs})")

            rows = 0
            try:
                for batch in _iter_batches(tmp, columns, keep):
                    await conn.copy_records_to_table(
                        staging, schema_name=SCHEMA, columns=columns, records=batch,
                    )
                    rows += len(batch)

                # Atomic cutover: readers see the old table or the new one.
                async with conn.transaction():
                    await conn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                    await conn.execute(
                        f"ALTER TABLE {_qt(staging)} RENAME TO {_qi(table)}")
            except BaseException:
                # Never leave a half-filled staging table behind to confuse the
                # next run or the catalog.
                try:
                    await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
                except Exception:  # noqa: BLE001 — cleanup is best-effort
                    logger.debug("idx: staging cleanup failed for %s", staging,
                                 exc_info=True)
                raise

            # Planner stats for the fresh table (cheap, and the /data console's
            # row estimates read reltuples).
            await conn.execute(f"ANALYZE {_qt(table)}")

        logger.info("idx mirror: loaded %s — %d rows, %d columns",
                    table, rows, len(columns))
        return {"table": table, "rows": rows, "columns": len(columns)}
    finally:
        if tmp:
            try:
                os.remove(tmp)
            except OSError:
                pass
