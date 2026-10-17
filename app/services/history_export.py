"""Closed append rows to Parquet on R2: phase 2 of the history storage plan.

docs/history-phase2-plan.md is the contract; this is its implementation. Since
phase 1 every append row carries ``last_seen`` (NULL while it is in force, the
poll's start time once a full pass no longer returned it). Rows closed long ago
are the history that weighs on SQL. This job writes them out, one closing month
at a time, as immutable Parquet files:

    history/<dsid8>/<table>/closed_month=YYYY-MM/part-NNNN.parquet

and records each file in ``app.history_parts`` (models/history_part.py).

THE ORDER IS THE SAFETY
-----------------------
1. **Plan** (``plan`` / ``plan_table``): per table, the closed rows grouped by the
   UTC month of ``last_seen``, up to ``export_cutoff``, the start of the running
   month (with a few days' grace, so a long pass whose ``run_ts`` falls in the
   old month has finished closing before that month is written). A month the
   catalog already covers is exported again only for rows closed after its last
   part, as a NEW part. A table with fewer than HISTORY_MIN_ROWS closed rows is
   left alone, and pg_stats answers that question before any table is scanned.
2. **Write** (``PartWriter``): a server-side cursor ordered by
   ``(last_seen, ident)`` streams into Parquet parts of up to HISTORY_PART_ROWS
   rows (zstd, dictionary, 100k-row groups). Every source column is a string,
   like parquet_export; ``first_seen`` / ``last_seen`` are timestamp[us, UTC];
   ``row_hash`` is its hex. PostGIS ``geom`` is left out (``geometry_wkt``, when
   the source has it, is an ordinary text column and goes in).
3. **Verify** (``verify``): each part is uploaded, DOWNLOADED BACK from R2 (not
   the temp file), its ident column hashed, and that hash and row count compared
   with an independent SQL ``md5(string_agg(ident::text ORDER BY ident))`` over
   exactly the run of ``(last_seen, ident)`` the file says it holds. Any
   mismatch, or any failure, deletes every object uploaded for the month and
   records nothing: the parts of one month are recorded together or not at all.
4. **Record** the parts, ``deleted_from_sql=false``. Without a window that is
   where it ends: the rows are in both places.
5. **Delete** (``delete_exported``), only for a dataset with
   ``scraper_config.history_window_days`` and only rows whose ``last_seen`` is
   older than the window. The pairs ``(ident, last_seen)`` come from the
   verified file, read back from R2 and checked against its catalog hash; the
   DELETE is bounded by the month and matches those pairs, so nothing that is
   not in the file can go. Then ``VACUUM (ANALYZE)`` outside a transaction. A
   part becomes ``deleted_from_sql=true`` once every row of it is past the
   window; until then the next day's run carries on, and a crash halfway is
   simply resumed (the DELETE is idempotent).

No UNLOGGED and no staging table: xhostd refuses UNLOGGED, and the delete
passes its batch of pairs as two arrays to ``unnest`` instead.

Only ``storage_client`` touches R2. In-process state, one instance, like
index_migration: started by the daily job or from the admin storage tab.
"""
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import tempfile
import types
import uuid as _uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Mapping

from app.config import settings
from app.services import append_store
from app.services.append_store import _qi
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

UTC = timezone.utc
HISTORY_PREFIX = "history"
PARQUET_TYPE = "application/vnd.apache.parquet"

MIN_WINDOW_DAYS = 30
# Days after a month ends before it is exported (see export_cutoff).
GRACE_DAYS = 3
ROW_GROUP = 100_000
FETCH_BATCH = 50_000
# Pairs per DELETE statement.
HASH_BATCH = 10_000
STATEMENT_TIMEOUT_S = 3600

FIRST_COL = "first_seen"
LAST_COL = append_store.VALIDITY_COL
HASH_COL = "row_hash"
_INTERNAL_COLS = {FIRST_COL, LAST_COL, HASH_COL}
_GEOMETRY_UDTS = {"geometry", "geography"}


KIND_CLOSED = "closed"
KIND_FULL = "full"
TIER_COLD = "cold"
TIER_HOT = "hot"
# A dataset may go cold on its own when it is paused or polled this rarely.
COLD_MIN_POLL_S = 30 * 86400


class DeleteRefused(RuntimeError):
    """A delete that must not run: no verified catalog row, no window, a part
    already deleted, or a file in R2 that no longer matches its catalog row."""


# ── time ─────────────────────────────────────────────────────────────────────

def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _as_date(v) -> date:
    return v.date() if isinstance(v, datetime) else v


def month_dt(d: date) -> datetime:
    return datetime(d.year, d.month, 1, tzinfo=UTC)


def next_month(d: date) -> date:
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def export_cutoff(now: datetime) -> datetime:
    """Rows closed before this are exportable: the start of the running month,
    taken GRACE_DAYS ago. A pass's ``last_seen`` is its START time, and a long
    one (the vehicle registry) can still be closing rows stamped "Oct 31" on
    the morning of Nov 1; waiting a few days keeps a month whole when written."""
    m = _utc(now) - timedelta(days=GRACE_DAYS)
    return datetime(m.year, m.month, 1, tzinfo=UTC)


def delete_cutoff(now: datetime, window_days: int | None) -> datetime | None:
    return None if not window_days else _utc(now) - timedelta(days=int(window_days))


def validate_window(v) -> int | None:
    """None (export only, the default) or a whole number of days >= 30."""
    if v is None:
        return None
    n = int(v)
    if n < MIN_WINDOW_DAYS:
        raise ValueError(f"history_window_days must be at least {MIN_WINDOW_DAYS}")
    return n


def window_days_of(ds) -> int | None:
    """The dataset's window, or None. A value under the floor that reached the
    config some other way (the raw scraper_config_merge) counts as no window:
    a typo must never become a short window."""
    raw = (getattr(ds, "scraper_config", None) or {}).get("history_window_days")
    try:
        return validate_window(raw)
    except (TypeError, ValueError):
        return None


def part_key(dataset_id, table: str, month: date, part: int) -> str:
    return (f"{HISTORY_PREFIX}/{str(dataset_id)[:8]}/{table}/"
            f"closed_month={month:%Y-%m}/part-{int(part):04d}.parquet")


# ── the plan (pure) ──────────────────────────────────────────────────────────

@dataclass
class Job:
    dataset_id: str
    table: str
    month: date
    part: int                  # the first part number this export writes
    after: datetime | None     # export only rows closed after this (new part)
    rows: int                  # closed rows in the month (an upper bound)
    export: bool
    delete: bool
    window_days: int | None = None


def needs_scan(*, closed_estimate: int | None, has_parts: bool, min_rows: int) -> bool:
    """Whether the month scan of a table is worth running. The estimate is
    reltuples x (1 - null_frac of last_seen) from pg_stats: no table read. A
    table already in the catalog is always scanned (its months continue)."""
    if has_parts or closed_estimate is None:
        return True
    return closed_estimate >= min_rows


def plan_table(*, dataset_id, table: str, month_stats: list[dict], parts: list[dict],
               now: datetime, window_days: int | None, min_rows: int,
               closed_estimate: int | None = None) -> list[Job]:
    """The jobs for one table. Pure.

    ``month_stats``: ``[{month: date, rows, max_last_seen}]`` of the closed rows
    still in SQL; ``parts``: the table's catalog rows."""
    cutoff = export_cutoff(now)
    dcut = delete_cutoff(now, window_days)
    stats = {}
    for s in month_stats:
        m = _as_date(s["month"])
        if s.get("rows") and month_dt(next_month(m)) <= cutoff:
            stats[m] = s
    by_month: dict[date, list[dict]] = {}
    for p in parts:
        by_month.setdefault(_as_date(p["closed_month"]), []).append(p)
    if not parts and sum(int(s["rows"]) for s in stats.values()) < min_rows:
        return []

    jobs: list[Job] = []
    for m in sorted(set(stats) | set(by_month)):
        mparts = by_month.get(m, [])
        stat = stats.get(m)
        covered = max((p["last_seen_max"] for p in mparts if p.get("last_seen_max")),
                      default=None)
        deletable = dcut is not None and month_dt(m) < dcut
        new_rows = stat is not None and (
            covered is None or (stat.get("max_last_seen") is not None
                                and stat["max_last_seen"] > covered))
        pending = deletable and any(
            not p.get("deleted_from_sql") and p.get("last_seen_min") is not None
            and p["last_seen_min"] < dcut for p in mparts)
        if new_rows:
            jobs.append(Job(
                dataset_id=str(dataset_id), table=table, month=m,
                part=max((int(p["part"]) for p in mparts), default=0) + 1,
                after=covered, rows=int(stat["rows"]), export=True, delete=deletable,
                window_days=window_days))
        elif pending:
            jobs.append(Job(dataset_id=str(dataset_id), table=table, month=m, part=0,
                            after=None, rows=0, export=False, delete=True,
                            window_days=window_days))
    return jobs


# ── SQL builders (pure) ──────────────────────────────────────────────────────

def _ref(table: str) -> str:
    return f'"public".{_qi(table)}'


def month_stats_sql(table: str) -> str:
    """Closed rows per UTC month of ``last_seen``, before ``$1`` (the cutoff)."""
    return (
        f"SELECT (date_trunc('month', \"last_seen\", 'UTC') AT TIME ZONE 'UTC')::date AS month, "
        f'count(*) AS rows, max("last_seen") AS max_last_seen '
        f'FROM {_ref(table)} '
        f'WHERE "last_seen" IS NOT NULL AND "last_seen" < $1 '
        f"GROUP BY 1 ORDER BY 1")


def export_columns(rows: list[Mapping]) -> list[str]:
    """Source columns of a table in order, from information_schema rows
    ``{column_name, udt_name}``: bookkeeping and PostGIS geometry left out."""
    return [r["column_name"] for r in rows
            if r["column_name"] not in _INTERNAL_COLS
            and (r.get("udt_name") or "") not in _GEOMETRY_UDTS]


def export_select_sql(table: str, cols: list[str], *, ident_col: str, after: bool) -> str:
    """One month's closed rows in the order parts are cut: $1 month start,
    $2 month end, $3 (with ``after``) the last part's last_seen_max."""
    sel = [_qi(c) for c in cols] + [_qi(FIRST_COL), _qi(LAST_COL)]
    if ident_col == HASH_COL:
        sel.append(_qi(HASH_COL))
    where = '"last_seen" IS NOT NULL AND "last_seen" >= $1 AND "last_seen" < $2'
    if after:
        where += ' AND "last_seen" > $3'
    return (f"SELECT {', '.join(sel)} FROM {_ref(table)} WHERE {where} "
            f'ORDER BY "last_seen", {_qi(ident_col)}')


def content_hash_sql(table: str, *, ident_col: str = HASH_COL, ident_type: str = "uuid") -> str:
    """The SQL side of a part's hash: row count and md5 of the comma-joined
    idents in byte order (COLLATE "C" = Python's sort), over the run of
    ``(last_seen, ident)`` from ($3, $4) to ($5, $6) inside the month [$1, $2)."""
    i = _qi(ident_col)
    t = "uuid" if ident_type == "uuid" else "text"
    return (
        f"SELECT count(*) AS rows, "
        f"md5(coalesce(string_agg({i}::text, ',' ORDER BY {i}::text COLLATE \"C\"), '')) "
        f"AS content_hash FROM {_ref(table)} "
        f'WHERE "last_seen" IS NOT NULL AND "last_seen" >= $1 AND "last_seen" < $2 '
        f'AND ("last_seen", {i}) >= ($3::timestamptz, $4::{t}) '
        f'AND ("last_seen", {i}) <= ($5::timestamptz, $6::{t})')


def delete_sql(table: str, *, ident_col: str = HASH_COL, ident_type: str = "uuid") -> str:
    """Delete exactly the rows of a verified file: inside the month [$1, $2),
    older than the window ($3), and matching one of the file's
    ``(ident, last_seen)`` pairs ($4, $5 as arrays). Never by date alone."""
    t = "uuid" if ident_type == "uuid" else "text"
    return (
        f"DELETE FROM {_ref(table)} "
        f'WHERE "last_seen" IS NOT NULL AND "last_seen" >= $1 AND "last_seen" < $2 '
        f'AND "last_seen" < $3 '
        f'AND ({_qi(ident_col)}, "last_seen") IN '
        f"(SELECT * FROM unnest($4::{t}[], $5::timestamptz[]))")


def vacuum_sql(table: str) -> str:
    return f"VACUUM (ANALYZE) {_ref(table)}"


# ── content hash (pure) ──────────────────────────────────────────────────────

def content_hash(values: Iterable[str]) -> str:
    return hashlib.md5(",".join(sorted(values)).encode("utf-8")).hexdigest()


def _read_idents(path: str, ident_col: str):
    import pyarrow.parquet as pq

    t = pq.read_table(path, columns=[ident_col, LAST_COL])
    ids = [None if v is None else str(v) for v in t.column(ident_col).to_pylist()]
    lss = [None if v is None else _utc(v) for v in t.column(LAST_COL).to_pylist()]
    return ids, lss


def content_hash_of_parquet(path: str, ident_col: str = HASH_COL) -> dict:
    """``{rows, content_hash, first, last}`` of a Parquet part, where first/last
    are the ``(last_seen, ident)`` of its first and last row."""
    ids, lss = _read_idents(path, ident_col)
    n = len(ids)
    return {
        "rows": n,
        "content_hash": content_hash(ids),
        "first": (lss[0], ids[0]) if n else None,
        "last": (lss[-1], ids[-1]) if n else None,
    }


# ── the writer ───────────────────────────────────────────────────────────────

@dataclass
class LocalPart:
    path: str
    rows: int
    bytes: int
    first: tuple
    last: tuple
    first_seen_min: datetime | None
    first_seen_max: datetime | None
    last_seen_min: datetime | None
    last_seen_max: datetime | None


class PartWriter:
    """Rows in ``(last_seen, ident)`` order in, Parquet parts of up to
    ``part_rows`` out. Memory is one row group, whatever the month's size."""

    def __init__(self, cols: list[str], *, ident_col: str, part_rows: int, dirpath: str):
        import pyarrow as pa

        self.pa = pa
        self.cols = list(cols)
        self.ident_col = ident_col
        self.with_hash = ident_col == HASH_COL
        self.part_rows = max(1, int(part_rows))
        self.dirpath = dirpath
        ts = pa.timestamp("us", tz="UTC")
        fields = [(c, pa.string()) for c in self.cols] + [(FIRST_COL, ts), (LAST_COL, ts)]
        if self.with_hash:
            fields.append((HASH_COL, pa.string()))
        self.schema = pa.schema(fields)
        self.parts: list[LocalPart] = []
        self._writer = None

    def _open(self) -> None:
        import pyarrow.parquet as pq

        self._path = os.path.join(self.dirpath, f"part-{len(self.parts) + 1:04d}.parquet")
        self._writer = pq.ParquetWriter(self._path, self.schema, compression="zstd",
                                        use_dictionary=True)
        self._buf: list[list] = [[] for _ in self.schema.names]
        self._n = 0
        self._first = self._last = None
        self._fs_min = self._fs_max = self._ls_min = self._ls_max = None

    def add(self, rec) -> None:
        if self._writer is None:
            self._open()
        fs = rec[FIRST_COL]
        ls = rec[LAST_COL]
        fs = None if fs is None else _utc(fs)
        ls = None if ls is None else _utc(ls)  # NULL only in a full part
        for i, c in enumerate(self.cols):
            v = rec[c]
            self._buf[i].append(None if v is None else str(v))
        k = len(self.cols)
        self._buf[k].append(fs)
        self._buf[k + 1].append(ls)
        if self.with_hash:
            self._buf[k + 2].append(str(rec[HASH_COL]))
        key = (ls, str(rec[self.ident_col]))
        if self._first is None:
            self._first = key
        self._last = key
        if fs is not None:
            self._fs_min = fs if self._fs_min is None else min(self._fs_min, fs)
            self._fs_max = fs if self._fs_max is None else max(self._fs_max, fs)
        if ls is not None:
            self._ls_min = ls if self._ls_min is None else min(self._ls_min, ls)
            self._ls_max = ls if self._ls_max is None else max(self._ls_max, ls)
        self._n += 1
        if len(self._buf[0]) >= ROW_GROUP:
            self._flush()
        if self._n >= self.part_rows:
            self._finish()

    def _flush(self) -> None:
        if not self._buf[0]:
            return
        pa = self.pa
        arrays = [pa.array(b, type=f.type) for b, f in zip(self._buf, self.schema)]
        self._writer.write_table(pa.Table.from_arrays(arrays, schema=self.schema),
                                 row_group_size=ROW_GROUP)
        self._buf = [[] for _ in self.schema.names]

    def _finish(self) -> None:
        self._flush()
        self._writer.close()
        self._writer = None
        self.parts.append(LocalPart(
            path=self._path, rows=self._n, bytes=os.path.getsize(self._path),
            first=self._first, last=self._last,
            first_seen_min=self._fs_min, first_seen_max=self._fs_max,
            last_seen_min=self._ls_min, last_seen_max=self._ls_max))

    def close(self) -> list[LocalPart]:
        if self._writer is not None and self._n:
            self._finish()
        elif self._writer is not None:
            self.abort()
        return self.parts

    def abort(self) -> None:
        if self._writer is not None:
            try:
                self._writer.close()
            except Exception:  # noqa: BLE001
                pass
            self._writer = None


def write_parts(rows: Iterable, cols: list[str], *, ident_col: str, part_rows: int,
                dirpath: str) -> list[LocalPart]:
    w = PartWriter(cols, ident_col=ident_col, part_rows=part_rows, dirpath=dirpath)
    try:
        for r in rows:
            w.add(r)
    except BaseException:
        w.abort()
        raise
    return w.close()


# ── state ────────────────────────────────────────────────────────────────────

_state: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "current": None,
    "planned": 0,
    "done": 0,
    "exported_parts": 0,
    "exported_rows": 0,
    "deleted_rows": 0,
    "failed": [],
    "stop": False,
}
_audit: dict = {"ran_at": None, "checked": 0, "missing": []}


def status() -> dict:
    return {k: v for k, v in _state.items() if k != "stop"}


def request_stop() -> None:
    _state["stop"] = True


def audit_status() -> dict:
    return dict(_audit)


def missing_keys() -> set[str]:
    """Catalog keys the last weekly audit found absent from R2."""
    return set(_audit.get("missing") or [])


def is_available() -> tuple[bool, str]:
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except Exception as e:  # noqa: BLE001
        return False, f"pyarrow is not installed ({type(e).__name__})"
    if not storage_client.is_configured():
        return False, "object storage is not configured"
    return True, ""


# ── the database side ────────────────────────────────────────────────────────

async def _table_ident(conn, table: str) -> tuple[str, str] | None:
    """``(column, 'uuid'|'text')`` the table dedups its OPEN rows on, i.e. the
    column of its partial unique index; None when it has none (no validity)."""
    rows = await conn.fetch(
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename=$1", table)
    uniq = append_store.classify_unique_indexes([dict(r) for r in rows])
    col = next((u["column"] for u in uniq if u["partial"] and u["column"]), None)
    if not col:
        return None
    if col == HASH_COL and await append_store.hash_column_is_uuid(conn, table):
        return col, "uuid"
    return col, "text"


async def _closed_estimate(conn, table: str) -> int | None:
    """Closed rows of a table from the catalog alone, or None when unknown."""
    row = await conn.fetchrow(
        "SELECT c.reltuples::bigint AS n, s.null_frac "
        "FROM pg_class c JOIN pg_namespace ns ON ns.oid = c.relnamespace "
        "LEFT JOIN pg_stats s ON s.schemaname = ns.nspname AND s.tablename = c.relname "
        "AND s.attname = $2 "
        "WHERE ns.nspname = 'public' AND c.relname = $1", table, LAST_COL)
    if row is None:
        return 0
    n = row["n"]
    if n is None or n < 0:
        return None
    if row["null_frac"] is None:
        return int(n)  # no stats yet: the table size is an upper bound
    return int(n * (1.0 - float(row["null_frac"])))


async def _columns(conn, table: str) -> list[str]:
    rows = await conn.fetch(
        "SELECT column_name, udt_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position", table)
    return export_columns([dict(r) for r in rows])


async def _sql_part_hash(table: str, *, ident_col: str, ident_type: str,
                         month_start: datetime, month_end: datetime,
                         first: tuple, last: tuple) -> dict:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            content_hash_sql(table, ident_col=ident_col, ident_type=ident_type),
            month_start, month_end, first[0], first[1], last[0], last[1],
            timeout=STATEMENT_TIMEOUT_S)
    return {"rows": int(r["rows"]), "content_hash": r["content_hash"]}


async def _stream_rows(job: Job, cols: list[str], ident_col: str, writer: PartWriter,
                       month_start: datetime, month_end: datetime) -> None:
    sql = export_select_sql(job.table, cols, ident_col=ident_col, after=job.after is not None)
    params = [month_start, month_end] + ([job.after] if job.after is not None else [])
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_S}s'")
            async for rec in conn.cursor(sql, *params, prefetch=FETCH_BATCH):
                writer.add(rec)


# ── the catalog (app DB) ─────────────────────────────────────────────────────

_PART_FIELDS = ("id", "dataset_id", "table_name", "closed_month", "part", "r2_key",
                "rows", "bytes", "content_hash", "first_seen_min", "first_seen_max",
                "last_seen_min", "last_seen_max", "deleted_from_sql", "deleted_at",
                "created_at", "kind")


def _part_dict(p) -> dict:
    d = {k: getattr(p, k, None) for k in _PART_FIELDS}
    d["id"] = str(d["id"]) if d["id"] is not None else None
    d["dataset_id"] = str(d["dataset_id"]) if d["dataset_id"] is not None else None
    d["kind"] = d.get("kind") or KIND_CLOSED
    return d


async def _record_parts(parts: list[dict]) -> None:
    """Insert one month's verified parts in ONE transaction."""
    from app.database import async_session
    from app.models.history_part import HistoryPart

    async with async_session() as db:
        for p in parts:
            db.add(HistoryPart(
                id=_uuid.UUID(p["id"]), dataset_id=_uuid.UUID(str(p["dataset_id"])),
                **{k: p[k] for k in _PART_FIELDS
                   if k not in ("id", "dataset_id", "created_at")}))
        await db.commit()


async def _mark_deleted(part_id) -> None:
    from sqlalchemy import update

    from app.database import async_session
    from app.models.history_part import HistoryPart

    async with async_session() as db:
        await db.execute(update(HistoryPart)
                         .where(HistoryPart.id == _uuid.UUID(str(part_id)))
                         .values(deleted_from_sql=True, deleted_at=datetime.now(UTC)))
        await db.commit()


async def parts_for_dataset(db, dataset_id, table: str | None = None) -> list[dict]:
    from sqlalchemy import select

    from app.models.history_part import HistoryPart

    q = select(HistoryPart).where(HistoryPart.dataset_id == _uuid.UUID(str(dataset_id)))
    if table:
        q = q.where(HistoryPart.table_name == table)
    q = q.order_by(HistoryPart.table_name, HistoryPart.closed_month, HistoryPart.part)
    return [_part_dict(p) for p in (await db.execute(q)).scalars().all()]


async def _all_parts(db) -> list[dict]:
    from sqlalchemy import select

    from app.models.history_part import HistoryPart

    return [_part_dict(p) for p in (await db.execute(select(HistoryPart))).scalars().all()]


def _iso(v) -> str | None:
    return v.isoformat() if v is not None else None


def summarize(parts: list[dict]) -> dict:
    """The ``history`` block of /schema for a list of catalog rows."""
    mins = [p["last_seen_min"] for p in parts if p.get("last_seen_min")]
    maxs = [p["last_seen_max"] for p in parts if p.get("last_seen_max")]
    return {
        "parts": len(parts),
        "rows_in_r2": sum(int(p["rows"]) for p in parts),
        "rows_only_in_r2": sum(int(p["rows"]) for p in parts if p.get("deleted_from_sql")),
        "from": _iso(min(mins)) if mins else None,
        "to": _iso(max(maxs)) if maxs else None,
    }


async def summary_for(db, dataset_id, table: str) -> dict:
    return summarize(await parts_for_dataset(db, dataset_id, table))


async def datasets_with_deleted_parts(db, dataset_ids: list) -> set[str]:
    """Which of these datasets hold rows only in R2 (archive fidelity "tiered").
    Never raises: a missing catalog answers "none"."""
    if not dataset_ids:
        return set()
    try:
        from sqlalchemy import select

        from app.models.history_part import HistoryPart

        rows = (await db.execute(
            select(HistoryPart.dataset_id).distinct()
            .where(HistoryPart.dataset_id.in_(list(dataset_ids)),
                   HistoryPart.deleted_from_sql.is_(True)))).all()
        return {str(r[0]) for r in rows}
    except Exception:  # noqa: BLE001
        logger.debug("history export: deleted-parts lookup failed", exc_info=True)
        return set()


def public_file(p: dict, missing: set[str]) -> dict:
    return {
        "table": p["table_name"],
        "closed_month": f"{_as_date(p['closed_month']):%Y-%m}",
        "part": p["part"],
        "r2_key": p["r2_key"],
        "public_url": storage_client.public_url(p["r2_key"]),
        "rows": p["rows"],
        "bytes": p["bytes"],
        "content_hash": p["content_hash"],
        "first_seen_min": _iso(p.get("first_seen_min")),
        "first_seen_max": _iso(p.get("first_seen_max")),
        "last_seen_min": _iso(p.get("last_seen_min")),
        "last_seen_max": _iso(p.get("last_seen_max")),
        "deleted_from_sql": bool(p.get("deleted_from_sql")),
        "missing": p["r2_key"] in missing,
    }


async def admin_summary(db) -> dict:
    """The admin card: the job, the audit, and the catalog per dataset."""
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset

    parts = await _all_parts(db)
    by_ds: dict[str, dict] = {}
    for p in parts:
        d = by_ds.setdefault(p["dataset_id"], {"dataset_id": p["dataset_id"], "parts": 0,
                                               "rows_in_r2": 0, "rows_deleted": 0,
                                               "bytes": 0, "tables": set()})
        d["parts"] += 1
        d["rows_in_r2"] += int(p["rows"])
        d["bytes"] += int(p["bytes"])
        d["tables"].add(p["table_name"])
        if p.get("deleted_from_sql"):
            d["rows_deleted"] += int(p["rows"])
    titles = {}
    tiers = {}
    for ds_id, title, cfg in (await db.execute(
            select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.scraper_config)
    )).all():
        w = window_days_of(types.SimpleNamespace(scraper_config=cfg))
        tiers[str(ds_id)] = history_tier_of(types.SimpleNamespace(scraper_config=cfg))
        if w is not None or str(ds_id) in by_ds:
            titles[str(ds_id)] = (title, w)
            by_ds.setdefault(str(ds_id), {"dataset_id": str(ds_id), "parts": 0,
                                          "rows_in_r2": 0, "rows_deleted": 0,
                                          "bytes": 0, "tables": set()})
    datasets = []
    for k, d in by_ds.items():
        title, w = titles.get(k, (None, None))
        datasets.append({**d, "tables": sorted(d["tables"]), "title": title,
                         "history_window_days": w, "history_tier": tiers.get(k, TIER_HOT)})
    datasets.sort(key=lambda d: (-d["rows_in_r2"], d["title"] or ""))
    return {
        "enabled": settings.history_export_enabled,
        "min_rows": settings.history_min_rows,
        "part_rows": settings.history_part_rows,
        "hour_utc": settings.history_export_hour_utc,
        "available": is_available()[0],
        "job": status(),
        "audit": {**audit_status(), "missing_count": len(_audit.get("missing") or [])},
        "totals": summarize(parts) | {"bytes": sum(int(p["bytes"]) for p in parts)},
        "datasets": datasets,
        "tier_job": tier_status(),
    }


# ── plan / export / verify / delete ──────────────────────────────────────────

async def plan(db, *, only_dataset: str | None = None, now: datetime | None = None) -> list[Job]:
    """Every job for every active dataset's NEON tables (hidden ones are never
    published, so never exported to a public bucket)."""
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset
    from app.services.parquet_export import _latest_mappings

    now = now or datetime.now(UTC)
    q = select(TrackedDataset).where(TrackedDataset.status == "active")
    if only_dataset:
        q = q.where(TrackedDataset.id == _uuid.UUID(str(only_dataset)))
    datasets = (await db.execute(q)).scalars().all()
    latest = await _latest_mappings(db, [d.id for d in datasets])
    by_table: dict[str, list[dict]] = {}
    for p in await _all_parts(db):
        by_table.setdefault(p["table_name"], []).append(p)

    cutoff = export_cutoff(now)
    min_rows = settings.history_min_rows
    jobs: list[Job] = []
    seen: set[str] = set()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for ds in datasets:
            if history_tier_of(ds) == TIER_COLD:
                # Every row is already in R2 (a full part); the table is empty.
                continue
            window = window_days_of(ds)
            for entry in append_store.tables_from_mappings(ds, latest.get(ds.id)):
                table = entry.get("table")
                if not table or table in seen:
                    continue
                seen.add(table)
                tparts = by_table.get(table, [])
                try:
                    est = await _closed_estimate(conn, table)
                    if not needs_scan(closed_estimate=est, has_parts=bool(tparts),
                                      min_rows=min_rows):
                        continue
                    if await _table_ident(conn, table) is None:
                        continue
                    stats = [dict(r) for r in await conn.fetch(
                        month_stats_sql(table), cutoff, timeout=STATEMENT_TIMEOUT_S)]
                except Exception as e:  # noqa: BLE001 — one table must not stop the plan
                    logger.warning("history export: planning %s failed: %s", table, e)
                    continue
                jobs.extend(plan_table(
                    dataset_id=ds.id, table=table, month_stats=stats, parts=tparts,
                    now=now, window_days=window, min_rows=min_rows, closed_estimate=est))
    return jobs


async def verify(lp: LocalPart, *, key: str, table: str, ident_col: str, ident_type: str,
                 month_start: datetime, month_end: datetime,
                 tmpdir: str) -> tuple[bool, str, dict | None]:
    """Read the part back FROM R2 and compare it with SQL. ``(ok, why, back)``."""
    dest = os.path.join(tmpdir, "verify-" + os.path.basename(lp.path))
    try:
        if not await storage_client.download_to_file(key, dest):
            return False, "could not read the object back from R2", None
        back = content_hash_of_parquet(dest, ident_col)
    finally:
        try:
            os.remove(dest)
        except OSError:
            pass
    if back["rows"] != lp.rows or not back["rows"]:
        return False, f"rows differ (written {lp.rows}, in R2 {back['rows']})", back
    sql = await _sql_part_hash(table, ident_col=ident_col, ident_type=ident_type,
                               month_start=month_start, month_end=month_end,
                               first=back["first"], last=back["last"])
    if sql["rows"] != back["rows"]:
        return False, f"rows differ (SQL {sql['rows']}, R2 {back['rows']})", back
    if sql["content_hash"] != back["content_hash"]:
        return False, (f"content hash differs (SQL {sql['content_hash']}, "
                       f"R2 {back['content_hash']})"), back
    return True, "", back


async def export_month(job: Job, *, rows: Iterable | None = None, cols: list[str],
                       ident_col: str, ident_type: str, part_rows: int | None = None) -> dict:
    """Write, upload, verify and record one month of one table.

    ``rows`` is for tests; in production the rows stream from the table. All
    parts of the month are recorded together, or every uploaded object is
    deleted and nothing is recorded."""
    part_rows = part_rows or settings.history_part_rows
    ms, me = month_dt(job.month), month_dt(next_month(job.month))
    tmpdir = tempfile.mkdtemp(prefix="hx_")
    uploaded: list[str] = []
    try:
        writer = PartWriter(cols, ident_col=ident_col, part_rows=part_rows, dirpath=tmpdir)
        try:
            if rows is not None:
                for r in rows:
                    writer.add(r)
            else:
                await _stream_rows(job, cols, ident_col, writer, ms, me)
        except BaseException:
            writer.abort()
            raise
        local = writer.close()
        records: list[dict] = []
        for i, lp in enumerate(local):
            n = job.part + i
            key = part_key(job.dataset_id, job.table, job.month, n)
            uploaded.append(key)
            await storage_client.upload_object(key, file_path=lp.path, content_type=PARQUET_TYPE)
            ok, why, back = await verify(lp, key=key, table=job.table, ident_col=ident_col,
                                         ident_type=ident_type, month_start=ms,
                                         month_end=me, tmpdir=tmpdir)
            if not ok:
                raise RuntimeError(f"part {n}: {why}")
            records.append({
                "id": str(_uuid.uuid4()), "dataset_id": job.dataset_id,
                "table_name": job.table, "closed_month": job.month, "part": n,
                "r2_key": key, "rows": lp.rows, "bytes": lp.bytes,
                "content_hash": back["content_hash"],
                "first_seen_min": lp.first_seen_min, "first_seen_max": lp.first_seen_max,
                "last_seen_min": lp.last_seen_min, "last_seen_max": lp.last_seen_max,
                "deleted_from_sql": False, "deleted_at": None, "kind": KIND_CLOSED,
            })
        if records:
            await _record_parts(records)
            logger.info("history export: %s %s → %d parts, %d rows", job.table,
                        f"{job.month:%Y-%m}", len(records), sum(r["rows"] for r in records))
        return {"ok": True, "parts": records, "rows": sum(r["rows"] for r in records)}
    except Exception as e:  # noqa: BLE001 — a failed month leaves the table untouched
        for key in uploaded:
            try:
                await storage_client.delete_object(key)
            except Exception:  # noqa: BLE001
                logger.warning("history export: could not delete %s", key, exc_info=True)
        err = f"{type(e).__name__}: {e}"[:400]
        logger.warning("history export: %s %s failed: %s", job.table, f"{job.month:%Y-%m}", err)
        return {"ok": False, "error": err, "parts": []}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _rowcount(status: str) -> int:
    try:
        return int(str(status).rsplit(" ", 1)[-1])
    except (TypeError, ValueError):
        return 0


async def vacuum(table: str) -> None:
    """``VACUUM (ANALYZE)``: asyncpg runs a statement outside a transaction
    unless one is opened, and VACUUM refuses to run inside one. Not FULL: the
    space returns to the database for reuse, which is what stops the growth."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(vacuum_sql(table), timeout=STATEMENT_TIMEOUT_S)


async def delete_exported(part: dict | None, *, window_days: int | None,
                          now: datetime | None = None, vacuum_after: bool = True) -> dict:
    """Delete from SQL the rows of one verified part that are past the window."""
    if part is None:
        raise DeleteRefused("no catalog row for this part")
    if part.get("deleted_from_sql"):
        raise DeleteRefused("this part is already deleted from SQL")
    if window_days is None:
        raise DeleteRefused("the dataset has no history_window_days")
    try:
        window_days = validate_window(window_days)
    except (TypeError, ValueError) as e:
        raise DeleteRefused(str(e)) from e
    dcut = delete_cutoff(now or datetime.now(UTC), window_days)
    if part.get("last_seen_min") is None or part["last_seen_min"] >= dcut:
        return {"deleted": 0, "complete": False}

    table = part["table_name"]
    tmpdir = tempfile.mkdtemp(prefix="hx_")
    try:
        path = os.path.join(tmpdir, "part.parquet")
        if not await storage_client.download_to_file(part["r2_key"], path):
            raise DeleteRefused(f"{part['r2_key']} could not be read from R2")
        ident_col, ident_type = part.get("ident_col"), part.get("ident_type")
        if not ident_col:
            pool = await append_store.get_pool()
            async with pool.acquire() as conn:
                found = await _table_ident(conn, table)
            if not found:
                raise DeleteRefused(f"{table} has no partial dedup index")
            ident_col, ident_type = found
        back = content_hash_of_parquet(path, ident_col)
        if back["content_hash"] != part["content_hash"] or back["rows"] != int(part["rows"]):
            raise DeleteRefused(f"{part['r2_key']} no longer matches its catalog row")
        ids, lss = _read_idents(path, ident_col)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    pairs = [(i, ls) for i, ls in zip(ids, lss) if ls < dcut]
    month = _as_date(part["closed_month"])
    ms, me = month_dt(month), month_dt(next_month(month))
    sql = delete_sql(table, ident_col=ident_col, ident_type=ident_type)
    deleted = 0
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for s in range(0, len(pairs), HASH_BATCH):
            batch = pairs[s:s + HASH_BATCH]
            deleted += _rowcount(await conn.execute(
                sql, ms, me, dcut, [b[0] for b in batch], [b[1] for b in batch],
                timeout=STATEMENT_TIMEOUT_S))
        if deleted and vacuum_after:
            await conn.execute(vacuum_sql(table), timeout=STATEMENT_TIMEOUT_S)
    complete = part.get("last_seen_max") is not None and part["last_seen_max"] < dcut
    if complete:
        await _mark_deleted(part["id"])
    logger.info("history export: %s part %s deleted %d rows%s", table, part.get("part"),
                deleted, " (complete)" if complete else "")
    return {"deleted": deleted, "complete": complete}


async def run(*, only_dataset: str | None = None) -> None:
    """Plan and execute every job. Safe to re-run: done months are skipped."""
    from app.database import async_session

    if _state["running"]:
        return
    _state.update(running=True, stop=False, started_at=datetime.now(UTC).isoformat(),
                  finished_at=None, current=None, planned=0, done=0, exported_parts=0,
                  exported_rows=0, deleted_rows=0, failed=[])
    try:
        ok, why = is_available()
        if not ok:
            _state["failed"].append({"table": None, "error": why})
            return
        async with async_session() as db:
            jobs = await plan(db, only_dataset=only_dataset)
        _state["planned"] = len(jobs)
        pool = await append_store.get_pool()
        for job in jobs:
            if _state["stop"]:
                return
            label = f"{job.table} {job.month:%Y-%m}"
            _state["current"] = label
            try:
                async with pool.acquire() as conn:
                    ident = await _table_ident(conn, job.table)
                    cols = await _columns(conn, job.table) if job.export else []
                if ident is None:
                    raise RuntimeError("no partial dedup index")
                if job.export:
                    out = await export_month(job, cols=cols, ident_col=ident[0],
                                             ident_type=ident[1])
                    if not out["ok"]:
                        _state["failed"].append({"table": label, "error": out["error"]})
                        continue
                    _state["exported_parts"] += len(out["parts"])
                    _state["exported_rows"] += out["rows"]
                if job.delete:
                    async with async_session() as db:
                        parts = [p for p in await parts_for_dataset(db, job.dataset_id, job.table)
                                 if _as_date(p["closed_month"]) == job.month
                                 and not p["deleted_from_sql"]]
                    deleted = 0
                    for p in parts:
                        res = await delete_exported(
                            {**p, "ident_col": ident[0], "ident_type": ident[1]},
                            window_days=job.window_days, vacuum_after=False)
                        deleted += res["deleted"]
                    if deleted:
                        await vacuum(job.table)
                    _state["deleted_rows"] += deleted
                _state["done"] += 1
            except Exception as e:  # noqa: BLE001 — one job must not stop the rest
                logger.warning("history export: %s failed: %s", label, e)
                _state["failed"].append({"table": label, "error": f"{type(e).__name__}: {e}"[:400]})
    except Exception:  # noqa: BLE001 — surface in status, never crash the app
        logger.exception("history export: run aborted")
        _state["failed"].append({"table": _state["current"], "error": "aborted, see log"})
    finally:
        _state.update(running=False, current=None, finished_at=datetime.now(UTC).isoformat())
        logger.info("history export: finished, %d/%d jobs, %d parts, %d rows exported, "
                    "%d deleted, %d failed", _state["done"], _state["planned"],
                    _state["exported_parts"], _state["exported_rows"],
                    _state["deleted_rows"], len(_state["failed"]))


async def audit_history_parts() -> dict:
    """HEAD every catalog key; report the missing ones. Never edits the catalog."""
    from app.database import async_session

    async with async_session() as db:
        parts = await _all_parts(db)
    missing = []
    for p in parts:
        if await storage_client.object_size(p["r2_key"]) is None:
            missing.append(p["r2_key"])
    _audit.update(ran_at=datetime.now(UTC).isoformat(), checked=len(parts), missing=missing)
    if missing:
        logger.warning("history export: audit found %d of %d parts missing in R2",
                       len(missing), len(parts))
    return audit_status()


# ═════════════════════════════════════════════════════════════════════════════
# Phase 4: the cold tier (docs/history-phase4-plan.md)
# ═════════════════════════════════════════════════════════════════════════════
#
# A dataset nobody reads and nothing updates (the rain forecast: 60 tables, 62M
# OPEN rows) gains nothing from phase 2, which only moves closed rows. Moving it
# "cold" writes EVERY row of each table, open ones included, to ``full`` parts,
# verifies them the phase-2 way (read back from R2, count and md5 of the sorted
# idents against SQL), records them, deletes the rows by their ``(ident,
# last_seen)`` pairs NULL-safely, and leaves the table EMPTY with its schema, so
# the catalog, autocomplete and /schema keep working. Reads route to the
# DuckDB engine (history_query), which reads a cold table from its parts alone.
#
# The whole table step runs in ONE transaction holding SHARE ROW EXCLUSIVE on
# the table: nothing can write to it between the export and the delete, and a
# failure anywhere rolls the delete back. The catalog rows are written before
# the delete inside that transaction and marked deleted after the commit; a
# crash in between is repaired by the next call (recorded parts + empty table).
#
# ``rehydrate`` is the reverse: COPY the parts back, verify count and hash
# against the catalog in the same transaction, commit, and only then mark the
# parts ``deleted_from_sql=false`` and drop ``history_tier`` from the config. A
# poll of a cold dataset rehydrates first or is refused (api/worker._cold_guard,
# poll_job), because the loader would otherwise re-insert every open row.


class ColdRefused(RuntimeError):
    """A table or dataset that must not move between tiers (the reason says why)."""


def history_tier_of(ds) -> str:
    v = (getattr(ds, "scraper_config", None) or {}).get("history_tier")
    return TIER_COLD if v == TIER_COLD else TIER_HOT


def rehydrate_promised(ds) -> bool:
    """The admin said, when moving it, that the next poll may bring it back."""
    return (getattr(ds, "scraper_config", None) or {}).get("history_rehydrate_on_poll") is True


def cold_move_refusal(ds, *, confirm_rehydrate: bool) -> str | None:
    """Why ``ds`` may not move to the cold tier now, or None (plan section 2):
    it must be paused or polled more rarely than every 30 days, unless the
    admin promises that the next poll rehydrates it first."""
    if history_tier_of(ds) == TIER_COLD:
        return "המאגר כבר בשכבה הקרה"
    if confirm_rehydrate:
        return None
    if getattr(ds, "status", None) == "paused":
        return None
    if int(getattr(ds, "poll_interval", 0) or 0) > COLD_MIN_POLL_S:
        return None
    return ("המאגר פעיל ונדגם לפחות פעם בחודש: השהו אותו, או אשרו במפורש שה-poll "
            "הבא יחזיר אותו ל-SQL לפני הטעינה")


def config_after_rehydrate(cfg: dict | None) -> dict:
    return {k: v for k, v in (cfg or {}).items()
            if k not in ("history_tier", "history_rehydrate_on_poll")}


def full_part_key(dataset_id, table: str, month: date, part: int) -> str:
    return (f"{HISTORY_PREFIX}/{str(dataset_id)[:8]}/{table}/"
            f"full_month={month:%Y-%m}/part-{int(part):04d}.parquet")


def full_select_sql(table: str, cols: list[str], *, ident_col: str) -> str:
    """Every row of the table, open and closed, in a stable order."""
    sel = [_qi(c) for c in cols] + [_qi(FIRST_COL), _qi(LAST_COL)]
    if ident_col == HASH_COL:
        sel.append(_qi(HASH_COL))
    return (f"SELECT {', '.join(sel)} FROM {_ref(table)} "
            f'ORDER BY {_qi(ident_col)}, "last_seen" NULLS LAST')


def full_hash_sql(table: str, *, ident_col: str = HASH_COL) -> str:
    """Count and md5 of every ident of the table (COLLATE "C" = Python's sort)."""
    i = _qi(ident_col)
    return (f"SELECT count(*) AS rows, "
            f"md5(coalesce(string_agg({i}::text, ',' ORDER BY {i}::text COLLATE \"C\"), '')) "
            f"AS content_hash FROM {_ref(table)}")


def full_delete_sql(table: str, *, ident_col: str = HASH_COL, ident_type: str = "uuid") -> str:
    """Delete exactly the rows of verified full parts: by ``(ident, last_seen)``
    pairs, NULL-safe, since an open row's ``last_seen`` is NULL and ``NULL =
    NULL`` is never true."""
    t = "uuid" if ident_type == "uuid" else "text"
    return (f"DELETE FROM {_ref(table)} d "
            f"USING (SELECT * FROM unnest($1::{t}[], $2::timestamptz[])) AS p(i, l) "
            f'WHERE d.{_qi(ident_col)} = p.i AND d."last_seen" IS NOT DISTINCT FROM p.l')


class PgTable:
    """One append table inside one transaction holding SHARE ROW EXCLUSIVE:
    reads go on, nothing writes until commit. The interface export_full and
    rehydrate use (tests pass a fake with the same methods)."""

    def __init__(self, table: str):
        self.table = table
        self._types: dict[str, str] = {}

    async def __aenter__(self) -> "PgTable":
        pool = await append_store.get_pool()
        self._acq = pool.acquire()
        self.conn = await self._acq.__aenter__()
        self._tx = self.conn.transaction()
        await self._tx.start()
        await self.conn.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT_S}s'")
        await self.conn.execute(f"LOCK TABLE {_ref(self.table)} IN SHARE ROW EXCLUSIVE MODE")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        try:
            if exc_type is None:
                await self._tx.commit()
            else:
                await self._tx.rollback()
        finally:
            await self._acq.__aexit__(exc_type, exc, tb)
        return False

    async def describe(self) -> tuple[list[str], str, str]:
        rows = [dict(r) for r in await self.conn.fetch(
            "SELECT column_name, udt_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
            self.table)]
        self._types = {r["column_name"]: r["udt_name"] for r in rows}
        geo = [r["column_name"] for r in rows if r["udt_name"] in _GEOMETRY_UDTS]
        if geo:
            raise ColdRefused(f"{self.table} has PostGIS columns ({', '.join(geo)}), which "
                              "the Parquet parts do not carry: it cannot go cold")
        typed = [r["column_name"] for r in rows if r["column_name"] not in _INTERNAL_COLS
                 and r["udt_name"] not in ("text", "varchar")]
        if typed:
            raise ColdRefused(f"{self.table} has non-text source columns ({', '.join(typed)}): "
                              "the parts store strings and could not restore them")
        ident = await _table_ident(self.conn, self.table)
        if ident is None:
            raise ColdRefused(f"{self.table} has no partial dedup index")
        return export_columns(rows), ident[0], ident[1]

    async def count(self) -> int:
        return int(await self.conn.fetchval(f"SELECT count(*) FROM {_ref(self.table)}"))

    async def stream(self, cols: list[str], ident_col: str):
        async for rec in self.conn.cursor(full_select_sql(self.table, cols, ident_col=ident_col),
                                          prefetch=FETCH_BATCH):
            yield rec

    async def table_hash(self, ident_col: str, ident_type: str) -> dict:
        r = await self.conn.fetchrow(full_hash_sql(self.table, ident_col=ident_col))
        return {"rows": int(r["rows"]), "content_hash": r["content_hash"]}

    async def delete_pairs(self, ident_col: str, ident_type: str, pairs: list) -> int:
        sql = full_delete_sql(self.table, ident_col=ident_col, ident_type=ident_type)
        n = 0
        for s in range(0, len(pairs), HASH_BATCH):
            batch = pairs[s:s + HASH_BATCH]
            n += _rowcount(await self.conn.execute(sql, [b[0] for b in batch],
                                                   [b[1] for b in batch]))
        return n

    async def copy(self, cols: list[str], records: list[tuple]) -> None:
        conv = [(lambda v: v if v is None or isinstance(v, _uuid.UUID) else _uuid.UUID(str(v)))
                if self._types.get(c) == "uuid" else None for c in cols]
        if any(conv):
            records = [tuple(f(v) if f else v for f, v in zip(conv, r)) for r in records]
        await self.conn.copy_records_to_table(self.table, records=records, columns=cols,
                                              schema_name="public")


# ── the catalog side of the tiers ────────────────────────────────────────────

async def _table_parts(table: str) -> list[dict]:
    from sqlalchemy import select

    from app.database import async_session
    from app.models.history_part import HistoryPart

    async with async_session() as db:
        rows = (await db.execute(select(HistoryPart).where(HistoryPart.table_name == table)
                                 .order_by(HistoryPart.closed_month, HistoryPart.part))
                ).scalars().all()
        return [_part_dict(p) for p in rows]


async def _set_deleted(part_ids: list, value: bool) -> None:
    from sqlalchemy import update

    from app.database import async_session
    from app.models.history_part import HistoryPart

    if not part_ids:
        return
    async with async_session() as db:
        await db.execute(update(HistoryPart)
                         .where(HistoryPart.id.in_([_uuid.UUID(str(i)) for i in part_ids]))
                         .values(deleted_from_sql=value,
                                 deleted_at=datetime.now(UTC) if value else None))
        await db.commit()


def rehydrate_parts(parts: list[dict]) -> list[dict]:
    """The parts whose rows are ONLY in R2 (full or closed), in file order."""
    return sorted((p for p in parts if p.get("deleted_from_sql")),
                  key=lambda p: (_as_date(p["closed_month"]), int(p["part"]),
                                 p.get("kind") == KIND_FULL))


# ── export_full / rehydrate ───────────────────────────────────────────────────

async def _read_back(key: str, tmpdir: str, ident_col: str) -> tuple[list, list]:
    dest = os.path.join(tmpdir, "back-" + _uuid.uuid4().hex + ".parquet")
    try:
        if not await storage_client.download_to_file(key, dest):
            raise RuntimeError(f"could not read {key} back from R2")
        return _read_idents(dest, ident_col)
    finally:
        try:
            os.remove(dest)
        except OSError:
            pass


async def export_full(dataset_id, table: str, *, table_side=None, now: datetime | None = None,
                      part_rows: int | None = None) -> dict:
    """Move every row of ``table`` to ``full`` parts on R2 and empty the table.

    Returns ``{ok, parts, rows, deleted}`` or ``{ok: False, error}``. On a
    failure before the catalog write every uploaded object is deleted; the
    table is untouched either way (the transaction rolls back)."""
    now = now or datetime.now(UTC)
    month = date(now.year, now.month, 1)
    part_rows = part_rows or settings.history_part_rows
    side = table_side if table_side is not None else PgTable(table)
    tmpdir = tempfile.mkdtemp(prefix="hxf_")
    uploaded: list[str] = []
    records: list[dict] = []
    recorded = False
    deleted = 0
    try:
        async with side:
            cols, ident_col, ident_type = await side.describe()
            existing = await _table_parts(table)
            pending = [p for p in existing
                       if p.get("kind") == KIND_FULL and not p.get("deleted_from_sql")]
            if pending and await side.count() == 0:
                # A previous run committed its delete and died before the mark.
                records = pending
                recorded = True
            else:
                writer = PartWriter(cols, ident_col=ident_col, part_rows=part_rows, dirpath=tmpdir)
                try:
                    async for rec in side.stream(cols, ident_col):
                        writer.add(rec)
                except BaseException:
                    writer.abort()
                    raise
                local = writer.close()
                start = max((int(p["part"]) for p in existing
                             if _as_date(p["closed_month"]) == month), default=0) + 1
                all_ids: list[str] = []
                pairs: list[tuple] = []
                for i, lp in enumerate(local):
                    n = start + i
                    key = full_part_key(dataset_id, table, month, n)
                    uploaded.append(key)
                    await storage_client.upload_object(key, file_path=lp.path,
                                                       content_type=PARQUET_TYPE)
                    ids, lss = await _read_back(key, tmpdir, ident_col)
                    if len(ids) != lp.rows or not ids:
                        raise RuntimeError(f"part {n}: rows differ (written {lp.rows}, "
                                           f"in R2 {len(ids)})")
                    all_ids.extend(ids)
                    pairs.extend(zip(ids, lss))
                    records.append({
                        "id": str(_uuid.uuid4()), "dataset_id": str(dataset_id),
                        "table_name": table, "closed_month": month, "part": n,
                        "r2_key": key, "rows": lp.rows, "bytes": lp.bytes,
                        "content_hash": content_hash(ids),
                        "first_seen_min": lp.first_seen_min, "first_seen_max": lp.first_seen_max,
                        "last_seen_min": lp.last_seen_min, "last_seen_max": lp.last_seen_max,
                        "deleted_from_sql": False, "deleted_at": None, "kind": KIND_FULL,
                    })
                sql = await side.table_hash(ident_col, ident_type)
                if sql["rows"] != len(all_ids):
                    raise RuntimeError(f"rows differ (SQL {sql['rows']}, R2 {len(all_ids)})")
                if sql["content_hash"] != content_hash(all_ids):
                    raise RuntimeError(f"content hash differs (SQL {sql['content_hash']}, "
                                       f"R2 {content_hash(all_ids)})")
                if records:
                    await _record_parts(records)
                    recorded = True
                    deleted = await side.delete_pairs(ident_col, ident_type, pairs)
                    if deleted != len(pairs):
                        # Rolled back: the rows stay, the parts stay recorded as
                        # not deleted (in both places, as after a phase-2 export).
                        raise RuntimeError(f"deleted {deleted} of {len(pairs)} rows; rolled back")
        await _set_deleted([r["id"] for r in records], True)
        for r in records:
            r["deleted_from_sql"] = True
        rows = sum(int(r["rows"]) for r in records)
        if table_side is None and deleted:
            await vacuum(table)
        logger.info("history cold: %s → %d full parts, %d rows", table, len(records), rows)
        return {"ok": True, "parts": records, "rows": rows, "deleted": deleted}
    except Exception as e:  # noqa: BLE001 — a failed table stays hot
        if not recorded:
            for key in uploaded:
                try:
                    await storage_client.delete_object(key)
                except Exception:  # noqa: BLE001
                    logger.warning("history cold: could not delete %s", key, exc_info=True)
        err = f"{type(e).__name__}: {e}"[:400]
        logger.warning("history cold: %s failed: %s", table, err)
        return {"ok": False, "error": err, "parts": []}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


async def rehydrate(dataset_id, table: str, *, table_side=None) -> dict:
    """COPY the rows that are only in R2 back into the (empty) table, verify
    count and hash against the catalog in the same transaction, and only after
    the commit mark the parts ``deleted_from_sql=false``."""
    parts = rehydrate_parts(await _table_parts(table))
    if not parts:
        return {"ok": True, "rows": 0, "parts": 0}
    side = table_side if table_side is not None else PgTable(table)
    tmpdir = tempfile.mkdtemp(prefix="hxr_")
    try:
        import pyarrow.parquet as pq

        async with side:
            cols, ident_col, ident_type = await side.describe()
            if await side.count():
                raise ColdRefused(f"{table} is not empty: rehydrating would duplicate rows")
            known = set(cols) | {FIRST_COL, LAST_COL} | (
                {HASH_COL} if ident_col == HASH_COL else set())
            expected: list[str] = []
            for p in parts:
                path = os.path.join(tmpdir, "part.parquet")
                if not await storage_client.download_to_file(p["r2_key"], path):
                    raise RuntimeError(f"could not read {p['r2_key']} from R2")
                back = content_hash_of_parquet(path, ident_col)
                if back["rows"] != int(p["rows"]) or back["content_hash"] != p["content_hash"]:
                    raise RuntimeError(f"{p['r2_key']} no longer matches its catalog row")
                t = pq.read_table(path)
                extra = [c for c in t.column_names if c not in known]
                if extra:
                    raise ColdRefused(f"{p['r2_key']} has columns the table lacks: {extra}")
                names = list(t.column_names)
                for batch in t.to_batches(max_chunksize=FETCH_BATCH):
                    data = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
                    await side.copy(names, list(zip(*data)))
                ids, _l = _read_idents(path, ident_col)
                expected.extend(ids)
                os.remove(path)
            sql = await side.table_hash(ident_col, ident_type)
            if sql["rows"] != len(expected):
                raise RuntimeError(f"rows differ (loaded {sql['rows']}, catalog {len(expected)})")
            if sql["content_hash"] != content_hash(expected):
                raise RuntimeError("content hash differs after the load")
        await _set_deleted([p["id"] for p in parts], False)
        logger.info("history rehydrate: %s ← %d parts, %d rows", table, len(parts), len(expected))
        return {"ok": True, "rows": len(expected), "parts": len(parts)}
    except Exception as e:  # noqa: BLE001 — the transaction rolled back, catalog untouched
        err = f"{type(e).__name__}: {e}"[:400]
        logger.warning("history rehydrate: %s failed: %s", table, err)
        return {"ok": False, "error": err}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ── dataset-level moves (in-process job, one at a time) ──────────────────────

_tier: dict = {"running": False, "op": None, "dataset_id": None, "started_at": None,
               "finished_at": None, "current": None, "planned": 0, "done": 0,
               "rows": 0, "failed": [], "stop": False}


def tier_status() -> dict:
    return {k: v for k, v in _tier.items() if k != "stop"}


def request_tier_stop() -> None:
    _tier["stop"] = True


def _tier_start(op: str, dataset_id) -> None:
    if _tier["running"]:
        raise ColdRefused("another tier move is running")
    _tier.update(running=True, op=op, dataset_id=str(dataset_id), stop=False,
                 started_at=datetime.now(UTC).isoformat(), finished_at=None, current=None,
                 planned=0, done=0, rows=0, failed=[])


async def _refresh_cold_cache() -> None:
    try:
        from app.services import history_query

        await history_query.refresh_cold_tables()
    except Exception:  # noqa: BLE001
        logger.debug("history cold: cold-table cache refresh failed", exc_info=True)


async def _log(ds, message: str, status: str = "ok", actor: str | None = None) -> None:
    try:
        from app.services import activity_log

        await activity_log.log_event(event="history_tier", dataset=ds, status=status,
                                     message=message, actor=actor or "system")
    except Exception:  # noqa: BLE001
        pass


async def check_cold_move(dataset_id, *, confirm_rehydrate: bool) -> None:
    """Raise ColdRefused before a move is started (the endpoint's 409)."""
    from app.database import async_session
    from app.models.tracked_dataset import TrackedDataset

    async with async_session() as db:
        ds = await db.get(TrackedDataset, _uuid.UUID(str(dataset_id)))
    if ds is None:
        raise ColdRefused("dataset not found")
    why = cold_move_refusal(ds, confirm_rehydrate=confirm_rehydrate)
    if why:
        raise ColdRefused(why)


async def move_to_cold(dataset_id, *, confirm_rehydrate: bool = False,
                       actor: str | None = None) -> dict:
    """Every NEON table of the dataset to full parts. The tier is set FIRST, so
    no poll loads into a table being emptied; a table that fails stays hot and
    is read as one (the engine decides per table from the catalog)."""
    from app.database import async_session
    from app.models.tracked_dataset import TrackedDataset
    from app.services import append_tables

    _tier_start("cold", dataset_id)
    ds = None
    try:
        async with async_session() as db:
            ds = await db.get(TrackedDataset, _uuid.UUID(str(dataset_id)))
            if ds is None:
                raise ColdRefused("dataset not found")
            why = cold_move_refusal(ds, confirm_rehydrate=confirm_rehydrate)
            if why:
                raise ColdRefused(why)
            tables = [t["table"] for t in await append_tables.resolve_tables(ds, db)]
            cfg = dict(ds.scraper_config or {})
            cfg["history_tier"] = TIER_COLD
            if confirm_rehydrate:
                cfg["history_rehydrate_on_poll"] = True
            ds.scraper_config = cfg
            await db.commit()
        _tier["planned"] = len(tables)
        for table in tables:
            if _tier["stop"]:
                break
            _tier["current"] = table
            out = await export_full(dataset_id, table)
            if out["ok"]:
                _tier["done"] += 1
                _tier["rows"] += out["rows"]
            else:
                _tier["failed"].append({"table": table, "error": out["error"]})
            await _refresh_cold_cache()
        await _log(ds, f"הועבר לשכבה הקרה (R2): {_tier['done']}/{_tier['planned']} טבלאות, "
                       f"{_tier['rows']:,} שורות", "error" if _tier["failed"] else "ok", actor)
        return tier_status()
    except ColdRefused as e:
        _tier["failed"].append({"table": None, "error": str(e)})
        return tier_status()
    except Exception as e:  # noqa: BLE001 — surface in status, never crash the app
        logger.exception("history cold: move aborted")
        _tier["failed"].append({"table": _tier["current"], "error": f"{type(e).__name__}: {e}"[:400]})
        return tier_status()
    finally:
        _tier.update(running=False, current=None, finished_at=datetime.now(UTC).isoformat())


async def rehydrate_dataset(dataset_id, *, actor: str | None = None) -> dict:
    """Every cold table of the dataset back to SQL; the tier is cleared only
    when every table came back verified. ``{ok, tables, error?}``."""
    from sqlalchemy import select

    from app.database import async_session
    from app.models.history_part import HistoryPart
    from app.models.tracked_dataset import TrackedDataset

    async with async_session() as db:
        ds = await db.get(TrackedDataset, _uuid.UUID(str(dataset_id)))
        if ds is None:
            return {"ok": False, "error": "dataset not found", "tables": []}
        tables = sorted({r[0] for r in (await db.execute(
            select(HistoryPart.table_name).where(
                HistoryPart.dataset_id == ds.id,
                HistoryPart.deleted_from_sql.is_(True)))).all()})
    results = []
    for table in tables:
        _tier["current"] = table
        results.append({"table": table, **await rehydrate(dataset_id, table)})
    ok = all(r["ok"] for r in results)
    if ok:
        async with async_session() as db:
            row = await db.get(TrackedDataset, _uuid.UUID(str(dataset_id)))
            row.scraper_config = config_after_rehydrate(row.scraper_config)
            await db.commit()
    await _refresh_cold_cache()
    await _log(ds, f"הוחזר ל-SQL: {sum(int(r.get('rows') or 0) for r in results):,} שורות ב-"
                   f"{len(results)} טבלאות" + ("" if ok else " (נכשל חלקית)"),
               "ok" if ok else "error", actor)
    out = {"ok": ok, "tables": results}
    if not ok:
        out["error"] = "; ".join(f"{r['table']}: {r.get('error')}" for r in results if not r["ok"])
    return out


async def run_rehydrate(dataset_id, *, actor: str | None = None) -> dict:
    """``rehydrate_dataset`` under the tier job's state (the admin button)."""
    _tier_start("rehydrate", dataset_id)
    try:
        out = await rehydrate_dataset(dataset_id, actor=actor)
        _tier["planned"] = len(out["tables"])
        _tier["done"] = sum(1 for r in out["tables"] if r["ok"])
        _tier["rows"] = sum(int(r.get("rows") or 0) for r in out["tables"])
        _tier["failed"] = [{"table": r["table"], "error": r.get("error")}
                           for r in out["tables"] if not r["ok"]]
        if not out["tables"] and not out["ok"]:
            _tier["failed"] = [{"table": None, "error": out.get("error")}]
        return out
    except Exception as e:  # noqa: BLE001
        logger.exception("history rehydrate: aborted")
        _tier["failed"].append({"table": _tier["current"], "error": f"{type(e).__name__}: {e}"[:400]})
        return {"ok": False, "error": str(e), "tables": []}
    finally:
        _tier.update(running=False, current=None, finished_at=datetime.now(UTC).isoformat())


async def cold_poll_gate(ds) -> str | None:
    """Before any load into ``ds``: None when it may load (hot, or rehydrated
    just now because the admin promised it), else the refusal message."""
    if history_tier_of(ds) != TIER_COLD:
        return None
    if not rehydrate_promised(ds):
        return ("המאגר הועבר לשכבה הקרה (R2) והטבלאות שלו ריקות ב-SQL; טעינה הייתה מכפילה "
                "שורות. החזירו אותו ל-SQL מכרטיס המאגר באדמין ('החזר ל-SQL') לפני הדגימה.")
    out = await rehydrate_dataset(ds.id)
    if not out.get("ok"):
        return f"ההחזרה ל-SQL לפני הדגימה נכשלה: {out.get('error')}"
    try:
        ds.scraper_config = config_after_rehydrate(getattr(ds, "scraper_config", None))
    except Exception:  # noqa: BLE001 — a detached row; the DB already has it
        pass
    return None


# ── recommendations: cold candidates and windows ─────────────────────────────

STALE_DAYS = 180
QUIET_DAYS = 90
WINDOW_HINT_DAYS = 90
_CLOSED_SCAN_MIN = 10_000


def cold_candidates(rows: list[dict], *, now: datetime, stale_days: int = STALE_DAYS,
                    quiet_days: int = QUIET_DAYS) -> list[dict]:
    """Datasets nothing updated for ``stale_days`` and nobody read through the
    API for ``quiet_days``, biggest first. Pure."""
    now = _utc(now)
    out = []
    for r in rows:
        if r.get("tier") == TIER_COLD:
            continue
        lv, la = r.get("last_version_at"), r.get("last_access_at")
        if lv is not None and _utc(lv) > now - timedelta(days=stale_days):
            continue
        if la is not None and _utc(la) > now - timedelta(days=quiet_days):
            continue
        out.append({**r, "days_since_version": None if lv is None else (now - _utc(lv)).days,
                    "days_since_access": None if la is None else (now - _utc(la)).days})
    out.sort(key=lambda r: (-int(r.get("bytes") or 0), r.get("title") or ""))
    return out


def window_recommendation(*, table_rows: int, table_bytes: int, closed_old: int) -> dict:
    """What a window would free: the closed rows older than 90 days and their
    share of the table's bytes (a linear estimate). Pure."""
    per_row = (table_bytes / table_rows) if table_rows else 0
    return {"closed_over_90_days": int(closed_old), "est_bytes": int(closed_old * per_row)}


def validate_window_items(items: list[dict]) -> list[tuple]:
    """``[(dataset uuid, days | None)]`` or ValueError naming EVERY bad item, so
    a batch is applied whole or not at all."""
    out, errors, seen = [], [], set()
    for it in items or []:
        raw = (it or {}).get("dataset_id")
        try:
            uid = _uuid.UUID(str(raw))
        except (TypeError, ValueError):
            errors.append(f"{raw}: not a dataset id")
            continue
        if uid in seen:
            errors.append(f"{raw}: appears twice")
            continue
        seen.add(uid)
        try:
            out.append((uid, validate_window(it.get("days"))))
        except (TypeError, ValueError) as e:
            errors.append(f"{raw}: {e}")
    if errors:
        raise ValueError("; ".join(errors))
    if not out:
        raise ValueError("no items")
    return out


async def apply_windows(db, pairs: list[tuple]) -> list[dict]:
    """Write ``history_window_days`` for every pair in ONE commit, or raise
    before changing anything (an unknown dataset)."""
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset

    ids = [p[0] for p in pairs]
    found = {d.id: d for d in (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id.in_(ids)))).scalars().all()}
    missing = [str(i) for i in ids if i not in found]
    if missing:
        raise ValueError(f"unknown datasets: {', '.join(missing)}")
    out = []
    for uid, days in pairs:
        ds = found[uid]
        cfg = dict(ds.scraper_config or {})
        if days is None:
            cfg.pop("history_window_days", None)
        else:
            cfg["history_window_days"] = days
        ds.scraper_config = cfg
        out.append({"dataset_id": str(uid), "history_window_days": days})
    await db.commit()
    return out


async def candidates(db, *, now: datetime | None = None) -> dict:
    """The admin card: cold candidates and per-dataset window recommendations."""
    from sqlalchemy import func, select

    from app.models.api_access_log import ApiAccessLog
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex
    from app.services.parquet_export import _latest_mappings

    now = now or datetime.now(UTC)
    datasets = (await db.execute(select(TrackedDataset).where(
        TrackedDataset.status.in_(["active", "paused"])))).scalars().all()
    latest = await _latest_mappings(db, [d.id for d in datasets])
    tables_of: dict[str, list[str]] = {}
    for ds in datasets:
        ts = [e["table"] for e in append_store.tables_from_mappings(ds, latest.get(ds.id))
              if e.get("table")]
        if ts:
            tables_of[str(ds.id)] = ts
    ds_ids = [d.id for d in datasets if str(d.id) in tables_of]
    last_version = {str(k): v for k, v in (await db.execute(
        select(VersionIndex.tracked_dataset_id, func.max(VersionIndex.detected_at))
        .where(VersionIndex.tracked_dataset_id.in_(ds_ids))
        .group_by(VersionIndex.tracked_dataset_id))).all()}
    last_access: dict[str, datetime] = {}
    try:
        last_access = {str(k): v for k, v in (await db.execute(
            select(ApiAccessLog.target, func.max(ApiAccessLog.ts))
            .where(ApiAccessLog.target.in_([str(i) for i in ds_ids]))
            .group_by(ApiAccessLog.target))).all()}
    except Exception:  # noqa: BLE001 — no access log reads as "never read", stated in the card
        logger.debug("history candidates: access log unreadable", exc_info=True)
    all_tables = sorted({t for ts in tables_of.values() for t in ts})
    sizes: dict[str, tuple[int, int]] = {}
    closed_old: dict[str, int] = {}
    pool = await append_store.get_pool()
    hint_cut = _utc(now) - timedelta(days=WINDOW_HINT_DAYS)
    async with pool.acquire() as conn:
        for r in await conn.fetch(
                "SELECT c.relname, greatest(c.reltuples, 0)::bigint AS n, "
                "pg_total_relation_size(c.oid) AS b FROM pg_class c "
                "JOIN pg_namespace ns ON ns.oid = c.relnamespace "
                "WHERE ns.nspname = 'public' AND c.relkind = 'r' AND c.relname = ANY($1::text[])",
                all_tables):
            sizes[r["relname"]] = (int(r["n"]), int(r["b"]))
        for t in all_tables:
            try:
                est = await _closed_estimate(conn, t)
                if est is not None and est < _CLOSED_SCAN_MIN:
                    continue
                closed_old[t] = int(await conn.fetchval(
                    f'SELECT count(*) FROM {_ref(t)} WHERE "last_seen" < $1', hint_cut,
                    timeout=STATEMENT_TIMEOUT_S))
            except Exception:  # noqa: BLE001 — one table must not hide the rest
                logger.debug("history candidates: %s skipped", t, exc_info=True)
    rows, windows = [], []
    for ds in datasets:
        ts = tables_of.get(str(ds.id))
        if not ts:
            continue
        rows.append({"dataset_id": str(ds.id), "title": ds.title, "status": ds.status,
                     "poll_interval": ds.poll_interval, "tier": history_tier_of(ds),
                     "tables": len(ts),
                     "rows": sum(sizes.get(t, (0, 0))[0] for t in ts),
                     "bytes": sum(sizes.get(t, (0, 0))[1] for t in ts),
                     "last_version_at": last_version.get(str(ds.id)),
                     "last_access_at": last_access.get(str(ds.id)),
                     "may_move_now": cold_move_refusal(ds, confirm_rehydrate=False) is None})
        old = sum(closed_old.get(t, 0) for t in ts)
        if old and history_tier_of(ds) != TIER_COLD:
            rec = {"closed_over_90_days": 0, "est_bytes": 0}
            for t in ts:
                tr, tb = sizes.get(t, (0, 0))
                one = window_recommendation(table_rows=tr, table_bytes=tb,
                                            closed_old=closed_old.get(t, 0))
                rec = {k: rec[k] + one[k] for k in rec}
            windows.append({"dataset_id": str(ds.id), "title": ds.title,
                            "history_window_days": window_days_of(ds), **rec})
    windows.sort(key=lambda w: -w["est_bytes"])
    cands = cold_candidates(rows, now=now)
    for c in cands:
        for k in ("last_version_at", "last_access_at"):
            c[k] = _iso(c.get(k))
    return {"stale_days": STALE_DAYS, "quiet_days": QUIET_DAYS,
            "window_hint_days": WINDOW_HINT_DAYS, "candidates": cands, "windows": windows,
            "job": tier_status()}
