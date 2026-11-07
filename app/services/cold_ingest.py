"""New versions of a files-only dataset, straight into the cold tier.

A dataset in the cold tier (docs/history-phase4-plan.md) keeps its rows as
Parquet parts on R2 and an EMPTY table in SQL. Until now it could not take a new
version at all: a poll would have loaded rows into the empty table beside the
copy in R2, so the poll was refused. A dataset that is also FILES ONLY (storage
plan ``r2``, no ``archive_neon``) loads nothing into SQL, so its versions are
accepted again (history_export.cold_poll_gate) and this module carries each
version's tabular CSVs into the same Parquet history instead.

WHAT A VERSION WRITES
---------------------
Per table (one per tabular resource, named exactly as the SQL loader would
name it, see ``table_for``):

* a ``full`` part with the rows the version carried that were not open
  before, ``first_seen`` = the version's moment, ``last_seen`` NULL;
* a ``closures`` part (history_export.KIND_CLOSURES) with
  ``(row_hash, first_seen, last_seen)`` for every row that was open and that
  this version no longer carried, ``last_seen`` = the version's moment.

A row the version carried that was already open writes nothing. A version
identical to the last one therefore costs one read of the file and no new
object. Parts are never rewritten; history_query joins the closures onto the
full parts, so ``current`` and ``as_of`` answer as they would in SQL.

THE IDENTITY IS THE SQL LOADER'S
--------------------------------
A row's identity is ``row_hash``: the SHA-256 the keyless loader computes
(append_store.row_hash over the columns csv_stream_layout keeps, in the form
the table's column has: compact uuid or hex). The file is read with the same
two helpers the stream loader uses, so a row that did not change has the same
identity on both sides of the move to cold. If a file shares NOTHING with the
open rows of a table that has some, the identity is presumed broken, not the
source replaced wholesale, and the version is refused for that table: the
files are on R2 either way, and re-running after a fix is safe.

Closing follows the loader's rules too: only for a full pass the dataset may
close on (append_store.should_close, decided by the caller), and never past
append_store.close_guard.

The order is the safety, as in history_export: write locally, upload, read
each object BACK from R2 and compare its rows and ident hash with what was
computed, and only then record every part of the table in one transaction.
A failure deletes what was uploaded and records nothing.
"""
from __future__ import annotations

import asyncio
import csv as _csv
import logging
import os
import shutil
import sys
import tempfile
import uuid as _uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from app.config import settings
from app.services import append_store
from app.services import storage_client as storage
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

UTC = timezone.utc
FIRST_COL = "first_seen"
LAST_COL = append_store.VALIDITY_COL
HASH_COL = "row_hash"

# One ingest at a time: the open set of a big table is held in memory.
_lock = asyncio.Lock()


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


# ── which datasets, which resources, which tables ─────────────────────────────

def should_ingest(ds) -> bool:
    """A cold dataset that loads nothing into SQL. Pure."""
    from app.services import history_export
    from app.services.append_tables import is_append_archive

    return (history_export.history_tier_of(ds) == history_export.TIER_COLD
            and not is_append_archive(ds)
            and storage.dataset_stores_files(ds))


def is_tabular_key(value) -> bool:
    if not storage.is_storage_value(value):
        return False
    k = storage.key_of(value).lower()
    return k.endswith(".csv") or k.endswith(".csv.gz")


def tabular_resources(mappings: dict | None) -> list[tuple[str, str]]:
    """``[(resource name, r2 value)]`` of a version's named CSV resources, in
    mapping order. Underscore keys are aggregates (zips, geojson) and layouts."""
    out = []
    for name, value in (mappings or {}).items():
        if not isinstance(name, str) or name.startswith("_"):
            continue
        if is_tabular_key(value):
            out.append((name, value))
    return out


def table_for(ds, names: list[str]) -> dict[str, str]:
    """``{resource name: table}``, the names the SQL loader would use."""
    from app.api.worker import neon_per_resource

    per = neon_per_resource(getattr(ds, "scraper_config", None), names)
    return {n: (append_store.table_name_for_scraper_resource(ds, n) if per
                else append_store.table_name(ds)) for n in names}


def layout_mappings(layout: list[dict]) -> dict:
    """The keys the readers resolve a version's tables from (as the worker
    writes them): ``_append_tables`` for several tables, else ``append_table``."""
    if not layout:
        return {}
    if len({e["table"] for e in layout}) > 1:
        return {"_append_tables": layout}
    return {"append_table": layout[0]["table"]}


# ── keys ──────────────────────────────────────────────────────────────────────

def closures_part_key(dataset_id, table: str, month: date, part: int) -> str:
    from app.services.history_export import HISTORY_PREFIX

    return (f"{HISTORY_PREFIX}/{str(dataset_id)[:8]}/{table}/"
            f"closures_month={month:%Y-%m}/part-{int(part):04d}.parquet")


# ── the open set ──────────────────────────────────────────────────────────────

def open_rows(part_paths: list[str], closure_paths: list[str]) -> dict:
    """``{row_hash: first_seen}`` of the rows open after these parts: rows with
    ``last_seen`` NULL in the full parts (in order, a later instance of a hash
    wins), minus every ``(row_hash, first_seen)`` a closures part closed.
    Reads three columns, a row group at a time."""
    import pyarrow.parquet as pq

    stamps: dict = {}
    out: dict = {}
    for path in part_paths:
        f = pq.ParquetFile(path)
        names = f.schema_arrow.names
        if HASH_COL not in names:
            raise ValueError(f"{os.path.basename(path)} has no {HASH_COL}")
        cols = [HASH_COL, FIRST_COL] + ([LAST_COL] if LAST_COL in names else [])
        for batch in f.iter_batches(columns=cols, batch_size=100_000):
            hs = batch.column(0).to_pylist()
            fs = batch.column(1).to_pylist()
            ls = batch.column(2).to_pylist() if len(cols) == 3 else [None] * len(hs)
            for h, fs_, ls_ in zip(hs, fs, ls):
                if ls_ is not None or fs_ is None:
                    continue
                t = _utc(fs_)
                out[sys.intern(str(h))] = stamps.setdefault(t, t)
    for path in closure_paths:
        t = pq.read_table(path, columns=[HASH_COL, FIRST_COL])
        for h, fs_ in zip(t.column(0).to_pylist(), t.column(1).to_pylist()):
            h = str(h)
            if fs_ is not None and out.get(h) == _utc(fs_):
                del out[h]
    return out


# ── one file against the open set ─────────────────────────────────────────────

@dataclass
class FileDiff:
    cols: list[str] = field(default_factory=list)
    total: int = 0          # rows in the file
    matched: int = 0        # rows already open
    duplicates: int = 0     # rows repeated inside the file
    new_ids: list = field(default_factory=list)
    seen: set = field(default_factory=set)


def ident_of(row: dict, cols: list[str], *, exclude: tuple, as_uuid: bool) -> str:
    h = append_store.row_hash(row, cols, exclude=exclude)
    return str(append_store.compact_hash(h)) if as_uuid else h


def diff_file(fh, *, open_map: dict, writer, as_uuid: bool, exclude: tuple,
              ts: datetime, cols_out: list | None = None) -> FileDiff:
    """Stream a CSV (text handle) and write the rows not open before to
    ``writer`` (a history_export.PartWriter). Sync: runs on a thread."""
    reader = _csv.reader(fh)
    d = FileDiff()
    try:
        header = next(reader)
    except StopIteration:
        return d
    safe, keep, cols = append_store.csv_stream_layout(header)
    d.cols = cols
    if cols_out is not None:
        cols_out.extend(cols)
    if not cols:
        return d
    written: set = set()
    for row in reader:
        rec = append_store.csv_stream_row(row, safe, keep)
        d.total += 1
        ident = ident_of(rec, cols, exclude=exclude, as_uuid=as_uuid)
        if ident in open_map:
            if ident in d.seen:
                d.duplicates += 1
            else:
                d.matched += 1
                d.seen.add(ident)
            continue
        if ident in written:
            d.duplicates += 1
            continue
        written.add(ident)
        rec[FIRST_COL] = ts
        rec[LAST_COL] = None
        rec[HASH_COL] = ident
        writer.add(rec)
        d.new_ids.append(ident)
    return d


def identity_broken(*, open_count: int, total: int, matched: int) -> bool:
    """A file that shares no row with a table that has open rows: the identity
    changed (a parser or a column), not the whole source. Pure."""
    return open_count > 0 and total > 0 and matched == 0


def closing_ids(open_map: dict, seen: set) -> list[str]:
    return sorted(h for h in open_map if h not in seen)


def write_closures(ids: list[str], open_map: dict, *, ts: datetime, dirpath: str):
    """One local Parquet file of ``(row_hash, first_seen, last_seen)``, or None."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not ids:
        return None
    t = pa.timestamp("us", tz="UTC")
    schema = pa.schema([(HASH_COL, pa.string()), (FIRST_COL, t), (LAST_COL, t)])
    path = os.path.join(dirpath, f"closures-{_uuid.uuid4().hex}.parquet")
    table = pa.Table.from_arrays([pa.array(ids, pa.string()),
                                  pa.array([open_map[h] for h in ids], t),
                                  pa.array([ts] * len(ids), t)], schema=schema)
    pq.write_table(table, path, compression="zstd")
    return path


# ── one table ─────────────────────────────────────────────────────────────────

async def _hash_is_uuid(table: str) -> bool:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        return await append_store.hash_column_is_uuid(conn, table)


async def _hot_rows(table: str) -> int:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        return int(await conn.fetchval(
            f"SELECT count(*) FROM (SELECT 1 FROM {append_store._qi(table)} LIMIT 1) _t"))


async def ingest_table(ds, table: str, r2_value: str, *, ts: datetime, close: bool,
                       allow_shrink: bool = False, declared: bool = False) -> dict:
    """One resource of one version into ``table``'s cold history. Returns a
    summary; ``{"ok": False, "error"}`` on a refusal or a failure, after which
    nothing was recorded and nothing uploaded is left behind."""
    from app.services import history_export as hx
    from app.services import sampling_runs

    ts = _utc(ts)
    month = date(ts.year, ts.month, 1)
    tmpdir = tempfile.mkdtemp(prefix="coldin_")
    uploaded: list[str] = []
    try:
        parts = await hx._table_parts(table)
        data = [p for p in hx.data_parts(parts) if p.get("deleted_from_sql")]
        closures = [p for p in parts if p.get("deleted_from_sql")
                    and p.get("kind") == hx.KIND_CLOSURES]
        # Only parts whose rows are ONLY in R2 hold the table's state: a
        # phase-2 closed part left ``deleted_from_sql=false`` before the move
        # to cold has its rows inside the full parts too.

        # The file first: its header decides the SQL table's columns.
        src = os.path.join(tmpdir, "src.csv")
        if not await storage_client.download_to_file(r2_value, src):
            raise RuntimeError(f"could not read {storage.key_of(r2_value)} from R2")
        from app.api.worker import _open_maybe_gzip

        with _open_maybe_gzip(src) as fh:
            try:
                header = next(_csv.reader(fh))
            except StopIteration:
                return {"ok": True, "table": table, "rows": 0, "new": 0, "closed": 0,
                        "note": "empty file"}
        _safe, _keep, cols = append_store.csv_stream_layout(header)
        if not cols:
            return {"ok": True, "table": table, "rows": 0, "new": 0, "closed": 0,
                    "note": "no columns"}
        # The empty SQL table keeps the schema the catalog, /schema and the
        # autocomplete read; a new table or a new column is created here.
        await append_store.ensure_table(table, cols, key_col=None, keyless=True)
        if await _hot_rows(table):
            raise hx.ColdRefused(f"{table} holds rows in SQL: refusing to split it")
        as_uuid = await _hash_is_uuid(table)

        paths = []
        for i, p in enumerate(data + closures):
            path = os.path.join(tmpdir, f"in-{i:05d}.parquet")
            if not await storage_client.download_to_file(p["r2_key"], path):
                raise RuntimeError(f"could not read {p['r2_key']} from R2")
            paths.append(path)
        open_map = await asyncio.to_thread(open_rows, paths[:len(data)], paths[len(data):])
        for path in paths:
            os.remove(path)

        stamp = sampling_runs.stamp_column(ds)
        exclude = (stamp,) if stamp else ()
        writer = hx.PartWriter(cols, ident_col=HASH_COL, part_rows=settings.history_part_rows,
                               dirpath=tmpdir)

        def _run() -> FileDiff:
            try:
                with _open_maybe_gzip(src) as fh:
                    return diff_file(fh, open_map=open_map, writer=writer, as_uuid=as_uuid,
                                     exclude=exclude, ts=ts)
            except BaseException:
                writer.abort()
                raise

        d = await asyncio.to_thread(_run)
        local = writer.close()
        os.remove(src)
        if identity_broken(open_count=len(open_map), total=d.total, matched=d.matched):
            raise hx.ColdRefused(
                f"none of the file's {d.total:,} rows matches the {len(open_map):,} open rows "
                f"of {table}: the row identity is presumed changed, nothing written")

        closing: list[str] = []
        close_note = None
        if close and d.total:
            candidates = closing_ids(open_map, d.seen)
            close_note = append_store.close_guard(
                open_rows=len(open_map), closing=len(candidates),
                max_fraction=settings.append_close_max_fraction, allow_shrink=allow_shrink,
                min_pass_fraction=settings.history_close_min_pass_fraction, declared=declared)
            if close_note is None:
                closing = candidates
        cpath = await asyncio.to_thread(write_closures, closing, open_map, ts=ts, dirpath=tmpdir)

        start = max((int(p["part"]) for p in parts
                     if hx._as_date(p["closed_month"]) == month), default=0) + 1
        records: list[dict] = []
        n = start
        new_back: list[str] = []
        for lp in local:
            key = hx.full_part_key(ds.id, table, month, n)
            uploaded.append(key)
            await storage_client.upload_object(key, file_path=lp.path,
                                               content_type=hx.PARQUET_TYPE)
            ids, _l = await hx._read_back(key, tmpdir, HASH_COL)
            if len(ids) != lp.rows:
                raise RuntimeError(f"part {n}: rows differ (written {lp.rows}, in R2 {len(ids)})")
            new_back.extend(ids)
            records.append(_record(ds.id, table, month, n, key, lp.rows, lp.bytes,
                                   hx.content_hash(ids), hx.KIND_FULL,
                                   lp.first_seen_min, lp.first_seen_max, None, None))
            n += 1
        if hx.content_hash(new_back) != hx.content_hash(d.new_ids):
            raise RuntimeError("the new rows in R2 differ from the rows computed")
        if cpath:
            key = closures_part_key(ds.id, table, month, n)
            uploaded.append(key)
            await storage_client.upload_object(key, file_path=cpath,
                                               content_type=hx.PARQUET_TYPE)
            ids, _l = await hx._read_back(key, tmpdir, HASH_COL)
            if hx.content_hash(ids) != hx.content_hash(closing) or len(ids) != len(closing):
                raise RuntimeError("the closures in R2 differ from the closures computed")
            fmin = min(open_map[h] for h in closing)
            fmax = max(open_map[h] for h in closing)
            records.append(_record(ds.id, table, month, n, key, len(closing),
                                   os.path.getsize(cpath), hx.content_hash(ids),
                                   hx.KIND_CLOSURES, fmin, fmax, ts, ts))
        if records:
            await hx._record_parts(records)
        out = {"ok": True, "table": table, "rows": d.total, "matched": d.matched,
               "new": len(d.new_ids), "duplicates": d.duplicates, "closed": len(closing),
               "open_before": len(open_map), "parts": len(records)}
        if close_note:
            out["close_skipped"] = close_note
        logger.info("cold ingest: %s", out)
        return out
    except Exception as e:  # noqa: BLE001 — a refused table records nothing
        for key in uploaded:
            try:
                await storage_client.delete_object(key)
            except Exception:  # noqa: BLE001
                logger.warning("cold ingest: could not delete %s", key, exc_info=True)
        err = f"{type(e).__name__}: {e}"[:400]
        logger.warning("cold ingest: %s failed: %s", table, err)
        return {"ok": False, "table": table, "error": err}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _record(dataset_id, table, month, part, key, rows, size, chash, kind,
            fs_min, fs_max, ls_min, ls_max) -> dict:
    return {"id": str(_uuid.uuid4()), "dataset_id": str(dataset_id), "table_name": table,
            "closed_month": month, "part": part, "r2_key": key, "rows": int(rows),
            "bytes": int(size), "content_hash": chash,
            "first_seen_min": fs_min, "first_seen_max": fs_max,
            "last_seen_min": ls_min, "last_seen_max": ls_max,
            # The rows exist ONLY in R2: the SQL table is empty by design.
            "deleted_from_sql": True, "deleted_at": datetime.now(UTC), "kind": kind}


# ── one version ───────────────────────────────────────────────────────────────

async def ingest_version(dataset_id, version_number: int, *, close: bool = False,
                         allow_shrink: bool = False, declared: bool = False,
                         actor: str = "system") -> dict:
    """Every tabular resource of one version into the cold history, then the
    version's mapping gets the table layout (so every reader resolves the same
    tables) and the activity log one line. Never raises."""
    from sqlalchemy import select

    from app.database import async_session
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex
    from app.services import activity_log, history_export as hx

    async with _lock:
        try:
            async with async_session() as db:
                ds = await db.get(TrackedDataset, _uuid.UUID(str(dataset_id)))
                if ds is None:
                    return {"ok": False, "error": "dataset not found"}
                if not should_ingest(ds):
                    return {"ok": False, "error": "not a files-only dataset in the cold tier"}
                v = (await db.execute(select(VersionIndex).where(
                    VersionIndex.tracked_dataset_id == ds.id,
                    VersionIndex.version_number == int(version_number)))).scalar_one_or_none()
                if v is None:
                    return {"ok": False, "error": f"version {version_number} not found"}
                resources = tabular_resources(v.resource_mappings)
                tables = table_for(ds, [n for n, _v in resources])
                ts = v.detected_at
                db.expunge(ds)
            results = []
            layout = []
            per_table: dict[str, int] = {}
            for name, _value in resources:
                per_table[tables[name]] = per_table.get(tables[name], 0) + 1
            for name, value in resources:
                table = tables[name]
                # A table filled by several resources is never closed by one
                # of them (the worker's _bg_close rule).
                r = await ingest_table(ds, table, value, ts=ts,
                                       close=close and per_table[table] == 1,
                                       allow_shrink=allow_shrink, declared=declared)
                results.append({"resource": name, **r})
                if r.get("ok"):
                    layout.append({"resource": name, "table": table})
            if layout:
                async with async_session() as db:
                    v = (await db.execute(select(VersionIndex).where(
                        VersionIndex.tracked_dataset_id == ds.id,
                        VersionIndex.version_number == int(version_number)))).scalar_one()
                    v.resource_mappings = {**(v.resource_mappings or {}),
                                           **layout_mappings(layout)}
                    await db.commit()
                await hx._refresh_cold_cache()
            ok = all(r.get("ok") for r in results)
            new = sum(int(r.get("new") or 0) for r in results)
            closed = sum(int(r.get("closed") or 0) for r in results)
            msg = (f"גרסה {version_number} נכנסה לשכבה הקרה (Parquet): {new:,} שורות חדשות, "
                   f"{closed:,} שנסגרו, {len(results)} טבלאות")
            if not ok:
                msg += " — " + "; ".join(f"{r['table']}: {r['error']}"
                                         for r in results if not r.get("ok"))
            await activity_log.log_event(event="history_tier", dataset=ds,
                                         status="ok" if ok else "error",
                                         message=msg[:1900], actor=actor)
            return {"ok": ok, "version": int(version_number), "results": results}
        except Exception as e:  # noqa: BLE001 — a background job, never crash the app
            logger.exception("cold ingest: version %s of %s aborted", version_number, dataset_id)
            return {"ok": False, "error": f"{type(e).__name__}: {e}"[:400]}


_BG: set = set()


def schedule(dataset_id, version_number: int, **kw) -> None:
    """Run ingest_version off the request path (the push already answered)."""
    t = asyncio.create_task(ingest_version(dataset_id, version_number, **kw))
    _BG.add(t)
    t.add_done_callback(_BG.discard)
