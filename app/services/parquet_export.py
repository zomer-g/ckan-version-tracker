"""Parquet mirrors of the large NEON tables.

A CSV is a row of text at a time. Parquet is the same rows stored column by
column, which changes three things for a table this size:

* **it compresses far better** — a column of dates or settlement codes is the
  same handful of values over and over, and dictionary encoding stores each one
  once. The real-estate corpus is 390 MB of CSV and about 44 MB gzipped; as
  Parquet it lands in the same range but, unlike a .gz, without having to be
  unpacked before anything can read it;
* **a reader can take one column** — "sum deal_amount by year" reads two
  columns out of fourteen instead of every byte of every row, and the per-block
  min/max lets it skip whole blocks that cannot match;
* **it is self-describing** — the column names and types travel inside the file,
  so pandas, DuckDB, QGIS and BigQuery open it without being told anything.

WHAT THIS DOES NOT DO: guess types. Every column of an append table is text,
because that is how the archive stores what a publisher published — and
deciding that ``deal_amount`` is a number means deciding what to do with the
row where it is not one. That is the publisher's call, not this file's, so the
Parquet is written all-string. The compression and the column pruning are
unaffected; only predicate pushdown on a numeric range is, and a reader who
wants it can cast on the way in. The CSV stays the primary download either way
— this is an extra form of the same rows, never a replacement.

Only tables over PARQUET_MIN_ROWS get one. Below that the file is small enough
that the CSV is simply easier for everyone, and the columnar win is noise.
"""
from __future__ import annotations

import logging
import os
import tempfile

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import append_store
from app.services.append_store import _qi
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

# The threshold a table has to cross to be worth a second copy. 500k is where
# the columnar read starts to dominate the cost of downloading at all.
PARQUET_MIN_ROWS = 500_000

# Rows pulled from Postgres, and written to Parquet, at a time. The point of
# both is that neither the table nor the file is ever held whole: memory stays
# flat whether the table is 600k rows or 5.6M.
_BATCH = 50_000

# Where the mirrors live in the bucket. Keyed by TABLE, not by version: an
# append table is cumulative, so there is one current Parquet of it, rebuilt
# when a new version lands rather than accumulating one file per version.
PARQUET_PREFIX = "parquet"


def parquet_key(table: str) -> str:
    return f"{PARQUET_PREFIX}/{table}.parquet"


def is_available() -> tuple[bool, str]:
    """Whether Parquet can be written at all, and why not when it cannot.

    pyarrow is an optional dependency here: everything else in this service is
    stdlib plus what the app already has, and a deployment that does not want
    the wheel should degrade to "no Parquet" rather than failing to boot.
    """
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except Exception as e:  # noqa: BLE001
        return False, f"pyarrow is not installed ({type(e).__name__})"
    if not storage_client.is_configured():
        return False, "object storage is not configured"
    return True, ""


async def eligible_tables(db: AsyncSession) -> list[dict]:
    """Every table big enough to deserve a Parquet mirror, with its dataset.

    Driven by the datasets' own tables rather than by a scan of the schema, so
    a derived index (the over_re_* crosswalk) or a leftover from a migration is
    never picked up: a mirror is offered for data someone PUBLISHED, and those
    have a dataset and a version behind them.
    """
    rows = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.status == "active")
    )).scalars().all()

    latest = await _latest_mappings(db, [d.id for d in rows])
    out: list[dict] = []
    seen: set[str] = set()
    for ds in rows:
        for entry in append_store.tables_from_mappings(ds, latest.get(ds.id)):
            table = entry.get("table")
            if not table or table in seen:
                continue
            seen.add(table)
            try:
                est = await append_store.table_count_estimate(table)
            except Exception:  # noqa: BLE001
                continue
            if est is None or est < PARQUET_MIN_ROWS:
                continue
            out.append({
                "table": table,
                "dataset_id": str(ds.id),
                "dataset_title": ds.title,
                "resource_name": entry.get("resource_name"),
                "est_rows": est,
            })
    out.sort(key=lambda r: -r["est_rows"])
    return out


async def _latest_mappings(db: AsyncSession, dataset_ids: list) -> dict:
    """{dataset_id: resource_mappings} for each dataset's newest version."""
    if not dataset_ids:
        return {}
    rows = (await db.execute(
        select(VersionIndex.tracked_dataset_id, VersionIndex.resource_mappings,
               VersionIndex.version_number)
        .where(VersionIndex.tracked_dataset_id.in_(dataset_ids))
        .order_by(VersionIndex.tracked_dataset_id,
                  VersionIndex.version_number.desc())
    )).all()
    out: dict = {}
    for ds_id, mappings, _ in rows:
        out.setdefault(ds_id, mappings)
    return out


async def build_table_parquet(table: str, *, min_rows: int = PARQUET_MIN_ROWS) -> dict:
    """Write one table to Parquet and upload it. Returns a summary dict.

    Streams both ends: rows arrive from a server-side cursor in batches and go
    straight into the writer, so a 5.6M-row table costs the same memory as a
    600k-row one. The file is built on disk and handed to boto3's managed
    upload, which streams it back out in parts.
    """
    ok, why = is_available()
    if not ok:
        return {"table": table, "error": why}

    import pyarrow as pa
    import pyarrow.parquet as pq

    cols = await append_store.user_columns(table)
    if not cols:
        return {"table": table, "error": "table not found or has no columns"}

    est = await append_store.table_count_estimate(table)
    if est is not None and 0 <= est < min_rows:
        return {"table": table, "skipped": f"only ~{est:,} rows (min {min_rows:,})"}

    schema = pa.schema([(c, pa.string()) for c in cols])
    select_list = ", ".join(_qi(c) for c in cols)
    sql = f"SELECT {select_list} FROM {_qi(table)}"

    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp_path = tmp.name
    tmp.close()
    rows_written = 0
    try:
        writer = pq.ParquetWriter(
            tmp_path, schema,
            # Dictionary encoding is what makes an all-string file small: a
            # settlement name or a date repeats across millions of rows and is
            # stored once. zstd over that beats gzip at a fraction of the CPU.
            compression="zstd",
            use_dictionary=True,
        )
        try:
            pool = await append_store.get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    batch: list[list] = [[] for _ in cols]
                    n_in_batch = 0
                    async for rec in conn.cursor(sql):
                        for i, c in enumerate(cols):
                            v = rec[c]
                            batch[i].append(None if v is None else str(v))
                        n_in_batch += 1
                        if n_in_batch >= _BATCH:
                            writer.write_table(pa.Table.from_arrays(
                                [pa.array(b, type=pa.string()) for b in batch],
                                schema=schema))
                            rows_written += n_in_batch
                            batch = [[] for _ in cols]
                            n_in_batch = 0
                    if n_in_batch:
                        writer.write_table(pa.Table.from_arrays(
                            [pa.array(b, type=pa.string()) for b in batch],
                            schema=schema))
                        rows_written += n_in_batch
        finally:
            writer.close()

        size = os.path.getsize(tmp_path)
        key = parquet_key(table)
        await storage_client.upload_object(
            key, file_path=tmp_path, content_type="application/vnd.apache.parquet")
        logger.info("parquet: %s → %s (%d rows, %d bytes)", table, key, rows_written, size)
        return {"table": table, "key": f"r2:{key}", "rows": rows_written,
                "bytes": size, "columns": len(cols)}
    except Exception as e:  # noqa: BLE001 — a mirror must never break a push
        logger.exception("parquet: %s failed", table)
        return {"table": table, "error": f"{type(e).__name__}: {e}"}
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


async def build_for_dataset(db: AsyncSession, ds, mappings: dict | None) -> list[dict]:
    """Rebuild the Parquet of every eligible table of ONE dataset.

    Called after a version lands, so the mirror reflects the table rather than
    whatever it held when it was last built. A table under the threshold is
    skipped silently — most datasets never produce one.
    """
    ok, why = is_available()
    if not ok:
        return [{"error": why}]
    out = []
    for entry in append_store.tables_from_mappings(ds, mappings):
        table = entry.get("table")
        if not table:
            continue
        try:
            est = await append_store.table_count_estimate(table)
        except Exception:  # noqa: BLE001
            continue
        if est is None or est < PARQUET_MIN_ROWS:
            continue
        out.append(await build_table_parquet(table))
    return out
