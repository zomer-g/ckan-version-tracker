"""Phase 2 of docs/history-storage-strategy.md: closed rows to Parquet on R2.

Written BEFORE the code (docs/history-phase2-plan.md section 5.2). There is no
Postgres and no R2 on this machine, so:

* the SQL is checked through its pure builders and through a recording fake
  connection that answers the few statements the service issues;
* Parquet is REAL: pyarrow writes files into a temp dir and the same files are
  read back, which is what proves both sides hash the same way;
* R2 is a fake object store swapped in for ``history_export.storage_client``
  (the module attribute, not the shared instance, so nothing leaks into the
  other suites that patch the real StorageClient).
"""
import asyncio
import hashlib
import os
import types
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from app.services import history_export as hx  # noqa: E402

UTC = timezone.utc
NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)
DSID = "6fb0e203-1111-4222-8333-444455556666"
TABLE = "append_flights_6fb0e203"


def _ts(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=UTC)


def _stat(month, rows, max_ls):
    return {"month": month, "rows": rows, "max_last_seen": max_ls}


def _part(month, part, *, last_seen_min, last_seen_max, deleted=False, rows=10, pid=None):
    return {
        "id": pid or str(uuid.uuid4()),
        "dataset_id": DSID,
        "table_name": TABLE,
        "closed_month": month,
        "part": part,
        "r2_key": hx.part_key(DSID, TABLE, month, part),
        "rows": rows,
        "bytes": 100,
        "content_hash": "x",
        "first_seen_min": None, "first_seen_max": None,
        "last_seen_min": last_seen_min, "last_seen_max": last_seen_max,
        "deleted_from_sql": deleted,
        "deleted_at": None,
    }


_STATS = [
    _stat(date(2026, 7, 1), 50_000, _ts(2026, 7, 30)),
    _stat(date(2026, 8, 1), 50_000, _ts(2026, 8, 30)),
    _stat(date(2026, 9, 1), 50_000, _ts(2026, 9, 30)),
]


def _plan(stats=_STATS, parts=(), window=None, min_rows=100_000, estimate=None):
    return hx.plan_table(
        dataset_id=DSID, table=TABLE, month_stats=list(stats), parts=list(parts),
        now=NOW, window_days=window, min_rows=min_rows, closed_estimate=estimate)


# ── 1. plan ───────────────────────────────────────────────────────────────────

def test_plan_exports_only_finished_months():
    """The month still running (October) is never exported: a monthly file is
    complete the moment it is written. The stats query is bounded by the
    cutoff, and the plan re-checks it rather than trusting its input."""
    stats = _STATS + [_stat(date(2026, 10, 1), 9_000, _ts(2026, 10, 15))]
    jobs = _plan(stats)
    assert [j.month for j in jobs] == [date(2026, 7, 1), date(2026, 8, 1), date(2026, 9, 1)]
    assert all(j.export and not j.delete for j in jobs), "no window = export only"
    assert all(j.part == 1 and j.after is None for j in jobs)


def test_export_cutoff_is_the_start_of_the_current_month():
    assert hx.export_cutoff(NOW) == _ts(2026, 10, 1)
    # A grace of a few days after a month ends lets a long pass whose run_ts
    # falls in the old month finish closing before that month is written.
    assert hx.export_cutoff(_ts(2026, 11, 1, 3)) == _ts(2026, 10, 1)
    assert hx.export_cutoff(_ts(2026, 11, 5, 3)) == _ts(2026, 11, 1)


def test_plan_with_a_60_day_window_deletes_only_old_months():
    jobs = {j.month: j for j in _plan(window=60)}
    assert jobs[date(2026, 7, 1)].delete is True
    assert jobs[date(2026, 8, 1)].delete is True
    assert jobs[date(2026, 9, 1)].delete is False


def test_plan_skips_a_month_the_catalog_already_covers():
    parts = [_part(date(2026, 8, 1), 1, last_seen_min=_ts(2026, 8, 2),
                   last_seen_max=_ts(2026, 8, 30))]
    months = [j.month for j in _plan(parts=parts)]
    assert date(2026, 8, 1) not in months
    assert months == [date(2026, 7, 1), date(2026, 9, 1)]


def test_plan_opens_a_new_part_for_rows_closed_after_the_last_one():
    parts = [_part(date(2026, 8, 1), 1, last_seen_min=_ts(2026, 8, 2),
                   last_seen_max=_ts(2026, 8, 20)),
             _part(date(2026, 8, 1), 2, last_seen_min=_ts(2026, 8, 20),
                   last_seen_max=_ts(2026, 8, 25))]
    aug = [j for j in _plan(parts=parts) if j.month == date(2026, 8, 1)]
    assert len(aug) == 1
    assert aug[0].export and aug[0].part == 3
    assert aug[0].after == _ts(2026, 8, 25)


def test_plan_resumes_a_pending_delete_on_an_exported_part():
    """A part exported without a window, once a window is set, is deleted
    without being written again (the file never changes)."""
    p = _part(date(2026, 7, 1), 1, last_seen_min=_ts(2026, 7, 2),
              last_seen_max=_ts(2026, 7, 30))
    stats = [_stat(date(2026, 7, 1), 50_000, _ts(2026, 7, 30))]
    jobs = _plan(stats=stats, parts=[p], window=60)
    assert len(jobs) == 1
    assert jobs[0].export is False and jobs[0].delete is True


# ── 2. the size floor ─────────────────────────────────────────────────────────

def test_a_small_table_is_not_planned():
    small = [_stat(date(2026, 7, 1), 30_000, _ts(2026, 7, 30)),
             _stat(date(2026, 8, 1), 30_000, _ts(2026, 8, 30))]
    assert _plan(stats=small) == []


def test_the_floor_does_not_strand_a_table_already_in_the_catalog():
    parts = [_part(date(2026, 7, 1), 1, last_seen_min=_ts(2026, 7, 2),
                   last_seen_max=_ts(2026, 7, 30), deleted=True)]
    small = [_stat(date(2026, 8, 1), 3_000, _ts(2026, 8, 30))]
    jobs = _plan(stats=small, parts=parts)
    assert [j.month for j in jobs] == [date(2026, 8, 1)]


def test_the_catalog_estimate_skips_the_scan():
    """pg_stats says how many rows are closed without reading the table; under
    the floor, the month scan is never run."""
    assert hx.needs_scan(closed_estimate=40_000, has_parts=False, min_rows=100_000) is False
    assert hx.needs_scan(closed_estimate=None, has_parts=False, min_rows=100_000) is True
    assert hx.needs_scan(closed_estimate=150_000, has_parts=False, min_rows=100_000) is True
    assert hx.needs_scan(closed_estimate=10, has_parts=True, min_rows=100_000) is True


# ── 3. no dependence on append_closes_rows ────────────────────────────────────

def test_a_dataset_that_does_not_close_has_nothing_to_plan():
    """append_closes_rows=false means no closed rows, so an empty month scan.
    The plan works from the rows alone, never from the flag."""
    assert _plan(stats=[]) == []
    ds = types.SimpleNamespace(scraper_config={"append_closes_rows": False,
                                               "history_window_days": 30})
    assert hx.window_days_of(ds) == 30


# ── 4. the content hash, both sides ───────────────────────────────────────────

def test_content_hash_sql_hashes_the_sorted_idents_in_the_part_range():
    sql = hx.content_hash_sql(TABLE, ident_col="row_hash", ident_type="uuid")
    assert "md5(" in sql and "string_agg(" in sql
    assert '"row_hash"::text' in sql
    assert 'ORDER BY "row_hash"::text COLLATE "C"' in sql
    assert '"last_seen" IS NOT NULL' in sql
    assert '"last_seen" >= $1' in sql and '"last_seen" < $2' in sql
    # The part is a contiguous run of (last_seen, ident), bounded on both ends.
    assert '("last_seen", "row_hash") >= ($3::timestamptz, $4::uuid)' in sql
    assert '("last_seen", "row_hash") <= ($5::timestamptz, $6::uuid)' in sql
    assert "count(*)" in sql


def test_content_hash_sql_quotes_a_text_ident():
    sql = hx.content_hash_sql('append_x"y', ident_col="מספר", ident_type="text")
    assert '"append_x""y"' in sql
    assert '"מספר"::text' in sql and "$4::text" in sql


def test_content_hash_is_md5_of_the_sorted_idents():
    ids = ["b", "a", "c"]
    assert hx.content_hash(ids) == hashlib.md5(b"a,b,c").hexdigest()
    assert hx.content_hash([]) == hashlib.md5(b"").hexdigest()


def _rows(n, *, month=(2026, 7), start_hour=0, cols=("שם", "עיר")):
    """n closed rows in one month, already in (last_seen, row_hash) order."""
    out = []
    for i in range(n):
        ls = datetime(month[0], month[1], 1 + (i // 4) % 27, (start_hour + i) % 24,
                      tzinfo=UTC)
        out.append({
            cols[0]: f"טיסה {i}", cols[1]: None if i % 5 == 0 else "תל אביב",
            "first_seen": ls - timedelta(days=3),
            "last_seen": ls,
            "row_hash": uuid.UUID(int=(i * 7919) % (2**128)),
        })
    out.sort(key=lambda r: (r["last_seen"], str(r["row_hash"])))
    return out


def test_content_hash_of_parquet_matches_the_python_hash(tmp_path):
    rows = _rows(7)
    parts = hx.write_parts(rows, ["שם", "עיר"], ident_col="row_hash",
                           part_rows=100, dirpath=str(tmp_path))
    assert len(parts) == 1
    got = hx.content_hash_of_parquet(parts[0].path, ident_col="row_hash")
    want = hashlib.md5(",".join(sorted(str(r["row_hash"]) for r in rows)).encode()).hexdigest()
    assert got["content_hash"] == want
    assert got["rows"] == 7
    assert got["first"] == (rows[0]["last_seen"], str(rows[0]["row_hash"]))
    assert got["last"] == (rows[-1]["last_seen"], str(rows[-1]["row_hash"]))


# ── 5. the writer ─────────────────────────────────────────────────────────────

def test_parquet_schema_is_strings_plus_validity_plus_hash(tmp_path):
    rows = _rows(4)
    (part,) = hx.write_parts(rows, ["שם", "עיר"], ident_col="row_hash",
                             part_rows=100, dirpath=str(tmp_path))
    t = pq.read_table(part.path)
    assert t.column_names == ["שם", "עיר", "first_seen", "last_seen", "row_hash"]
    assert t.schema.field("שם").type == pa.string()
    assert t.schema.field("row_hash").type == pa.string()
    for c in ("first_seen", "last_seen"):
        ty = t.schema.field(c).type
        assert pa.types.is_timestamp(ty) and ty.unit == "us" and ty.tz == "UTC"
    back = t.to_pylist()
    assert back[0]["last_seen"] == rows[0]["last_seen"]
    assert back[0]["עיר"] is None, "NULL stays NULL, not the string 'None'"
    md = pq.ParquetFile(part.path).metadata
    assert md.row_group(0).column(0).compression == "ZSTD"


def test_parts_split_every_part_rows(tmp_path):
    rows = _rows(8)
    parts = hx.write_parts(rows, ["שם", "עיר"], ident_col="row_hash",
                           part_rows=3, dirpath=str(tmp_path))
    assert [p.rows for p in parts] == [3, 3, 2]
    # Contiguous and in order: each part starts after the previous one ended.
    assert parts[0].last < parts[1].first and parts[1].last < parts[2].first
    assert parts[0].last_seen_min == rows[0]["last_seen"]
    assert parts[2].last_seen_max == rows[-1]["last_seen"]
    assert parts[0].first_seen_min == min(r["first_seen"] for r in rows[:3])
    assert all(os.path.getsize(p.path) == p.bytes for p in parts)


def test_a_keyed_table_keeps_its_key_as_the_ident(tmp_path):
    rows = [{"מספר": f"k{i}", "first_seen": _ts(2026, 7, 1), "last_seen": _ts(2026, 7, 2, i)}
            for i in range(3)]
    (part,) = hx.write_parts(rows, ["מספר"], ident_col="מספר", part_rows=10,
                             dirpath=str(tmp_path))
    t = pq.read_table(part.path)
    assert t.column_names == ["מספר", "first_seen", "last_seen"]
    got = hx.content_hash_of_parquet(part.path, ident_col="מספר")
    assert got["content_hash"] == hashlib.md5(b"k0,k1,k2").hexdigest()


def test_the_export_query_is_ordered_and_bounded():
    sql = hx.export_select_sql(TABLE, ["שם"], ident_col="row_hash", after=False)
    assert 'ORDER BY "last_seen", "row_hash"' in sql
    assert '"last_seen" >= $1 AND "last_seen" < $2' in sql
    assert "$3" not in sql
    assert '"row_hash"' in sql and '"first_seen"' in sql
    sql2 = hx.export_select_sql(TABLE, ["שם"], ident_col="row_hash", after=True)
    assert '"last_seen" > $3' in sql2


def test_month_stats_sql_groups_closed_rows_by_utc_month():
    sql = hx.month_stats_sql(TABLE)
    assert '"last_seen" IS NOT NULL' in sql and '"last_seen" < $1' in sql
    assert "'UTC'" in sql and "GROUP BY" in sql
    assert "count(*)" in sql and 'max("last_seen")' in sql


def test_the_key_layout():
    assert hx.part_key(DSID, TABLE, date(2026, 7, 1), 3) == (
        f"history/6fb0e203/{TABLE}/closed_month=2026-07/part-0003.parquet")


# ── 6. verify against what is IN R2 ───────────────────────────────────────────

class _FakeStore:
    """An object store in a dict of bytes. ``corrupt`` rewrites what a later
    download returns, the way a broken upload would."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.corrupt = None

    def is_configured(self):
        return True

    def public_url(self, key):
        return f"https://files.example/{key}"

    async def upload_object(self, key, *, file_content=None, file_path=None, content_type=None):
        with open(file_path, "rb") as f:
            self.objects[key] = f.read()
        return key

    async def download_to_file(self, key, dest):
        data = self.objects.get(key)
        if data is None:
            return False
        if self.corrupt:
            data = self.corrupt(key, data)
        with open(dest, "wb") as f:
            f.write(data)
        return True

    async def delete_object(self, key):
        self.deleted.append(key)
        self.objects.pop(key, None)

    async def object_size(self, key):
        d = self.objects.get(key)
        return None if d is None else len(d)


@pytest.fixture
def store(monkeypatch):
    s = _FakeStore()
    monkeypatch.setattr(hx, "storage_client", s)
    return s


def _job(month=date(2026, 7, 1), *, part=1, after=None, delete=False):
    return hx.Job(dataset_id=DSID, table=TABLE, month=month, part=part, after=after,
                  rows=0, export=True, delete=delete, window_days=None)


def _sql_hash_from(rows):
    """The SQL side, simulated from the same rows the table would hold."""
    async def fake(table, *, ident_col, ident_type, month_start, month_end, first, last):
        sel = [r for r in rows
               if month_start <= r["last_seen"] < month_end
               and first <= (r["last_seen"], str(r[ident_col])) <= last]
        return {"rows": len(sel), "content_hash": hx.content_hash(str(r[ident_col]) for r in sel)}
    return fake


def _export(monkeypatch, rows, *, sql_rows=None, part_rows=100):
    recorded = []

    async def fake_rec(parts):
        recorded.extend(parts)

    monkeypatch.setattr(hx, "_sql_part_hash", _sql_hash_from(sql_rows if sql_rows is not None else rows))
    monkeypatch.setattr(hx, "_record_parts", fake_rec)
    out = asyncio.run(hx.export_month(_job(), rows=rows, cols=["שם", "עיר"],
                                      ident_col="row_hash", ident_type="uuid",
                                      part_rows=part_rows))
    return out, recorded


def test_export_uploads_verifies_and_records(store, monkeypatch):
    rows = _rows(8)
    out, recorded = _export(monkeypatch, rows, part_rows=3)
    assert out["ok"] is True
    assert [p["part"] for p in recorded] == [1, 2, 3]
    assert [p["rows"] for p in recorded] == [3, 3, 2]
    assert all(p["deleted_from_sql"] is False for p in recorded)
    assert set(store.objects) == {p["r2_key"] for p in recorded}
    assert store.deleted == []
    for p in recorded:
        assert p["closed_month"] == date(2026, 7, 1)
        assert p["content_hash"] and p["bytes"] > 0


def test_a_hash_mismatch_deletes_the_object_and_records_nothing(store, monkeypatch):
    rows = _rows(6)
    sql_rows = list(rows)
    sql_rows[2] = {**rows[2], "row_hash": uuid.uuid4()}  # SQL holds a different row
    out, recorded = _export(monkeypatch, rows, sql_rows=sql_rows)
    assert out["ok"] is False and "hash" in out["error"]
    assert recorded == []
    assert store.deleted and store.objects == {}


def test_a_row_count_mismatch_deletes_the_object_and_records_nothing(store, monkeypatch):
    rows = _rows(6)
    extra = dict(rows[3])
    extra["row_hash"] = uuid.UUID(int=10**30)
    out, recorded = _export(monkeypatch, rows, sql_rows=rows + [extra])
    assert out["ok"] is False
    assert recorded == [] and store.objects == {}


def test_verification_reads_the_object_back_from_r2_not_the_temp_file(store, monkeypatch):
    """A file that was fine on disk and broken in the bucket must fail."""
    rows = _rows(5)

    def swap(key, data):
        import io
        buf = io.BytesIO()
        t = pq.read_table(io.BytesIO(data))
        t = t.set_column(t.column_names.index("row_hash"), "row_hash",
                         pa.array([str(uuid.uuid4()) for _ in range(t.num_rows)]))
        pq.write_table(t, buf)
        return buf.getvalue()

    store.corrupt = swap
    out, recorded = _export(monkeypatch, rows)
    assert out["ok"] is False and recorded == []
    assert store.objects == {}


def test_a_failure_in_a_later_part_undoes_the_whole_month(store, monkeypatch):
    """Parts of one month are recorded together or not at all, so the catalog
    never holds a month split in the middle of one closing timestamp."""
    rows = _rows(7)
    sql_rows = list(rows)
    sql_rows[6] = {**rows[6], "row_hash": uuid.uuid4()}
    out, recorded = _export(monkeypatch, rows, sql_rows=sql_rows, part_rows=3)
    assert out["ok"] is False
    assert recorded == [] and store.objects == {}
    assert len(store.deleted) == 3


def test_an_upload_failure_records_nothing(store, monkeypatch):
    async def boom(*a, **k):
        raise RuntimeError("R2 down")

    monkeypatch.setattr(store, "upload_object", boom)
    out, recorded = _export(monkeypatch, _rows(4))
    assert out["ok"] is False and recorded == []


def test_the_temp_files_are_removed(store, monkeypatch, tmp_path):
    monkeypatch.setattr(hx.tempfile, "tempdir", str(tmp_path))
    _export(monkeypatch, _rows(4))
    assert [p for p in os.listdir(tmp_path) if p.startswith("hx_")] == []


# ── 7. order: no delete without a verified catalog row and a window ──────────

class _Tx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.in_tx += 1

    async def __aexit__(self, *a):
        self.conn.in_tx -= 1


class _Conn:
    def __init__(self, rows):
        self.rows = rows  # the "table": list of dicts with row_hash/last_seen
        self.calls: list[tuple] = []
        self.in_tx = 0

    def transaction(self):
        return _Tx(self)

    async def execute(self, sql, *params, timeout=None):
        self.calls.append((sql, params, self.in_tx))
        if sql.lstrip().upper().startswith("DELETE"):
            ms, me, cutoff, ids, lss = params
            pairs = set(zip(ids, lss))
            before = len(self.rows)
            self.rows[:] = [r for r in self.rows
                            if not (r["last_seen"] is not None and ms <= r["last_seen"] < me
                                    and r["last_seen"] < cutoff
                                    and (str(r["row_hash"]), r["last_seen"]) in pairs)]
            return f"DELETE {before - len(self.rows)}"
        return "OK"


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        pool = self

        class _A:
            async def __aenter__(self_inner):
                return pool.conn

            async def __aexit__(self_inner, *a):
                return False

        return _A()


def _exported(store, monkeypatch, tmp_path, rows):
    """Export rows for real into the fake store and return the catalog row."""
    (lp,) = hx.write_parts(rows, ["שם", "עיר"], ident_col="row_hash",
                           part_rows=1000, dirpath=str(tmp_path))
    key = hx.part_key(DSID, TABLE, date(2026, 7, 1), 1)
    with open(lp.path, "rb") as f:
        store.objects[key] = f.read()
    return {
        "id": "p1", "dataset_id": DSID, "table_name": TABLE,
        "closed_month": date(2026, 7, 1), "part": 1, "r2_key": key, "rows": lp.rows,
        "bytes": lp.bytes, "content_hash": hx.content_hash_of_parquet(lp.path)["content_hash"],
        "last_seen_min": lp.last_seen_min, "last_seen_max": lp.last_seen_max,
        "deleted_from_sql": False, "ident_col": "row_hash", "ident_type": "uuid",
    }


@pytest.fixture
def table_conn(monkeypatch):
    holder = {}

    def make(rows):
        conn = _Conn(rows)
        holder["conn"] = conn

        async def pool():
            return _Pool(conn)

        monkeypatch.setattr(hx.append_store, "get_pool", pool)
        return conn

    marked = []

    async def fake_mark(part_id):
        marked.append(part_id)

    monkeypatch.setattr(hx, "_mark_deleted", fake_mark)
    holder["make"], holder["marked"] = make, marked
    return holder


def test_delete_refuses_without_a_catalog_row(store, table_conn):
    conn = table_conn["make"]([])
    with pytest.raises(hx.DeleteRefused):
        asyncio.run(hx.delete_exported(None, window_days=30, now=NOW))
    assert conn.calls == []


def test_delete_refuses_without_a_window(store, table_conn, tmp_path, monkeypatch):
    part = _exported(store, monkeypatch, tmp_path, _rows(4))
    conn = table_conn["make"](_rows(4))
    with pytest.raises(hx.DeleteRefused):
        asyncio.run(hx.delete_exported(part, window_days=None, now=NOW))
    assert conn.calls == [] and len(conn.rows) == 4


def test_delete_refuses_a_part_already_deleted(store, table_conn, tmp_path, monkeypatch):
    part = {**_exported(store, monkeypatch, tmp_path, _rows(4)), "deleted_from_sql": True}
    conn = table_conn["make"](_rows(4))
    with pytest.raises(hx.DeleteRefused):
        asyncio.run(hx.delete_exported(part, window_days=30, now=NOW))
    assert conn.calls == []


def test_delete_refuses_a_file_that_no_longer_matches_its_catalog_hash(
        store, table_conn, tmp_path, monkeypatch):
    part = {**_exported(store, monkeypatch, tmp_path, _rows(4)), "content_hash": "0" * 32}
    conn = table_conn["make"](_rows(4))
    with pytest.raises(hx.DeleteRefused):
        asyncio.run(hx.delete_exported(part, window_days=30, now=NOW))
    assert len(conn.rows) == 4


def test_window_validation():
    assert hx.validate_window(None) is None
    assert hx.validate_window(30) == 30
    for bad in (29, 0, -5):
        with pytest.raises(ValueError):
            hx.validate_window(bad)


# ── 8. the DELETE itself ──────────────────────────────────────────────────────

def test_delete_sql_is_by_month_and_by_ident_never_a_date_range_alone():
    sql = hx.delete_sql(TABLE, ident_col="row_hash", ident_type="uuid")
    assert sql.startswith("DELETE FROM")
    assert '"last_seen" >= $1' in sql and '"last_seen" < $2' in sql
    assert '"last_seen" < $3' in sql, "never inside the window"
    assert 'unnest($4::uuid[], $5::timestamptz[])' in sql
    assert '("row_hash", "last_seen") IN' in sql


def test_delete_removes_exactly_the_file_rows_then_vacuums_outside_a_transaction(
        store, table_conn, tmp_path, monkeypatch):
    rows = _rows(9)
    part = _exported(store, monkeypatch, tmp_path, rows)
    # The table also holds an OPEN row and a closed row of the same month that
    # is not in the file (closed after the export, same content as a file row).
    stray = {**rows[0], "last_seen": rows[0]["last_seen"] + timedelta(minutes=1)}
    open_row = {**rows[1], "last_seen": None}
    conn = table_conn["make"](list(rows) + [stray, open_row])
    monkeypatch.setattr(hx, "HASH_BATCH", 4)
    out = asyncio.run(hx.delete_exported(part, window_days=30, now=NOW))
    assert out["deleted"] == 9
    assert conn.rows == [stray, open_row]
    deletes = [c for c in conn.calls if c[0].startswith("DELETE")]
    assert len(deletes) == 3, "in batches of HASH_BATCH idents"
    vac = [c for c in conn.calls if "VACUUM" in c[0]]
    assert len(vac) == 1 and vac[0][0] == f'VACUUM (ANALYZE) "public"."{TABLE}"'
    assert vac[0][2] == 0, "VACUUM cannot run inside a transaction"
    assert conn.calls[-1][0] == vac[0][0], "after the deletes"
    assert table_conn["marked"] == ["p1"]


def test_delete_keeps_rows_still_inside_the_window(store, table_conn, tmp_path, monkeypatch):
    """A part that straddles the window boundary deletes its old rows and stays
    deleted_from_sql=false, so the next day's run finishes it."""
    rows = _rows(6)
    part = _exported(store, monkeypatch, tmp_path, rows)
    conn = table_conn["make"](list(rows))
    boundary = rows[3]["last_seen"]
    window = (NOW - boundary).days
    now = boundary + timedelta(days=window)  # cutoff lands exactly on rows[3]
    out = asyncio.run(hx.delete_exported(part, window_days=window, now=now))
    assert out["deleted"] == 3 and out["complete"] is False
    assert [r["row_hash"] for r in conn.rows] == [r["row_hash"] for r in rows[3:]]
    assert table_conn["marked"] == []


# ── 9. idempotence ────────────────────────────────────────────────────────────

def test_a_second_run_on_a_deleted_month_does_nothing():
    parts = [_part(date(2026, 7, 1), 1, last_seen_min=_ts(2026, 7, 2),
                   last_seen_max=_ts(2026, 7, 30), deleted=True)]
    stats = [_stat(date(2026, 7, 1), 0, None)]
    assert _plan(stats=stats, parts=parts, window=60, estimate=10**6) == []
    # And when the month has no rows left in SQL at all it is not in the stats.
    assert _plan(stats=[], parts=parts, window=60, estimate=10**6) == []


def test_delete_is_idempotent(store, table_conn, tmp_path, monkeypatch):
    rows = _rows(5)
    part = _exported(store, monkeypatch, tmp_path, rows)
    conn = table_conn["make"](list(rows))
    asyncio.run(hx.delete_exported(part, window_days=30, now=NOW))
    again = asyncio.run(hx.delete_exported(part, window_days=30, now=NOW))
    assert again["deleted"] == 0
    assert conn.rows == []
    # Nothing deleted the second time, so no second VACUUM either.
    assert len([c for c in conn.calls if "VACUUM" in c[0]]) == 1


# ── 10. the public API ────────────────────────────────────────────────────────

from fastapi import HTTPException  # noqa: E402

from app.api import append as api  # noqa: E402
from app.services import append_store, append_tables  # noqa: E402


class _R:
    def __init__(self, v):
        self.v = v

    def scalar_one_or_none(self):
        return self.v


class _DB:
    new = dirty = deleted = ()

    def __init__(self, ds):
        self.ds = ds

    def in_transaction(self):
        return False

    async def execute(self, q):
        return _R(self.ds)


def _ds(**kw):
    base = dict(id=uuid.UUID(DSID), title="לוח טיסות", status="active",
                storage_mode="append_only", scraper_config={"history_window_days": 30})
    base.update(kw)
    return types.SimpleNamespace(**base)


def _catalog():
    return [
        {**_part(date(2026, 7, 1), 1, last_seen_min=_ts(2026, 7, 2),
                 last_seen_max=_ts(2026, 7, 30), deleted=True, rows=40)},
        {**_part(date(2026, 8, 1), 1, last_seen_min=_ts(2026, 8, 2),
                 last_seen_max=_ts(2026, 8, 30), deleted=False, rows=60)},
        {**_part(date(2026, 8, 1), 1, last_seen_min=_ts(2026, 8, 2),
                 last_seen_max=_ts(2026, 8, 30), deleted=False, rows=5),
         "table_name": "append_flights_6fb0e203_other",
         "r2_key": hx.part_key(DSID, "append_flights_6fb0e203_other", date(2026, 8, 1), 1)},
    ]


@pytest.fixture
def catalog(monkeypatch, store):
    async def fake(db, dataset_id, table=None):
        return [p for p in _catalog() if table is None or p["table_name"] == table]

    monkeypatch.setattr(hx, "parts_for_dataset", fake)
    return store


def _files(ds, table=None):
    fn = getattr(api.archive_history_files, "__wrapped__", api.archive_history_files)
    req = types.SimpleNamespace(query_params={})
    return asyncio.run(fn(DSID, request=req, table=table, db=_DB(ds)))


def test_history_files_lists_the_catalog_with_public_urls(catalog):
    out = _files(_ds())
    assert len(out["files"]) == 3
    f = out["files"][0]
    assert f["public_url"] == f"https://files.example/{f['r2_key']}"
    assert f["closed_month"] == "2026-07"
    assert f["deleted_from_sql"] is True and f["missing"] is False
    assert out["rows_in_r2"] == 105 and out["rows_only_in_r2"] == 40


def test_history_files_filters_by_table(catalog):
    out = _files(_ds(), table="append_flights_6fb0e203_other")
    assert [f["rows"] for f in out["files"]] == [5]


def test_history_files_marks_a_key_the_audit_found_missing(catalog, monkeypatch):
    monkeypatch.setattr(hx, "missing_keys", lambda: {_catalog()[1]["r2_key"]})
    out = _files(_ds())
    assert [f["missing"] for f in out["files"]] == [False, True, False]


def test_history_files_404_for_a_dataset_without_neon(catalog, monkeypatch):
    async def none(db, dataset_id, table=None):
        return []

    monkeypatch.setattr(hx, "parts_for_dataset", none)
    ds = _ds(storage_mode="full_snapshot", scraper_config={})
    assert append_tables.is_append_archive(ds) is False
    with pytest.raises(HTTPException) as e:
        _files(ds)
    assert e.value.status_code == 404


def test_history_files_404_for_a_hidden_dataset(catalog):
    with pytest.raises(HTTPException) as e:
        _files(_ds(status="hidden"))
    assert e.value.status_code == 404


def test_history_files_404_for_an_unknown_dataset(catalog):
    with pytest.raises(HTTPException) as e:
        _files(None)
    assert e.value.status_code == 404


def test_schema_carries_the_history_block(monkeypatch, catalog):
    async def fake_resolve(dataset_id, db, selector=None):
        return _ds(), TABLE, [{"table": TABLE}]

    async def fake_cols(t):
        return ["שם", "first_seen"]

    async def fake_count(t):
        return 7

    async def fake_summary(db, dataset_id, table):
        return hx.summarize([p for p in _catalog() if p["table_name"] == table])

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    monkeypatch.setattr(hx, "summary_for", fake_summary)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}),
                         table=None, db=None))
    h = out["history"]
    assert h["parts"] == 2
    assert h["rows_in_r2"] == 100
    assert h["rows_only_in_r2"] == 40
    assert h["from"] == "2026-07-02T00:00:00+00:00"
    assert h["to"] == "2026-08-30T00:00:00+00:00"
    assert h["files_url"] == f"/api/append/{DSID}/history/files?table={TABLE}"
    # Phase 1's fields stay.
    assert out["supports_as_of"] is True and out["total"] == 7


def test_schema_without_a_catalog_still_answers(monkeypatch):
    """The existing /schema tests call it with db=None; the history block must
    never turn a schema read into a 500."""
    async def fake_resolve(dataset_id, db, selector=None):
        return _ds(), TABLE, [{"table": TABLE}]

    async def fake_cols(t):
        return ["a"]

    async def fake_count(t):
        return 1

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}),
                         table=None, db=None))
    assert "history" in out and out["total"] == 1


def test_summarize_an_empty_catalog():
    assert hx.summarize([]) == {"parts": 0, "rows_in_r2": 0, "rows_only_in_r2": 0,
                                "from": None, "to": None}


# ── 11. the window on the dataset update API ──────────────────────────────────

def test_update_request_window_validation():
    from app.api import datasets as dapi

    assert "history_window_days" in dapi.UpdateRequest.model_fields
    assert "history_window_days" in dapi.DatasetResponse.model_fields
    with pytest.raises(HTTPException) as e:
        dapi.apply_history_window({"a": 1}, 29)
    assert e.value.status_code == 400
    assert dapi.apply_history_window({"a": 1}, 30) == {"a": 1, "history_window_days": 30}
    assert dapi.apply_history_window({"a": 1, "history_window_days": 90}, None) == {"a": 1}
    assert dapi.apply_history_window({"history_window_days": 90}, None) is None
    # What the response reports is what the job will apply: never a raw value
    # that would fail the response model.
    assert dapi.history_window_of({"history_window_days": "abc"}) is None
    assert dapi.history_window_of({"history_window_days": 90}) == 90


def test_update_request_distinguishes_absent_from_null():
    from app.api import datasets as dapi

    absent = dapi.UpdateRequest()
    null = dapi.UpdateRequest(history_window_days=None)
    assert "history_window_days" not in absent.model_fields_set
    assert "history_window_days" in null.model_fields_set


def test_window_of_a_dataset():
    assert hx.window_days_of(types.SimpleNamespace(scraper_config=None)) is None
    assert hx.window_days_of(types.SimpleNamespace(scraper_config={"history_window_days": "45"})) == 45
    # A value under the floor that reached the config some other way (the raw
    # scraper_config_merge escape hatch) is treated as no window, never as 29.
    assert hx.window_days_of(types.SimpleNamespace(scraper_config={"history_window_days": 7})) is None


# ── 12. archive fidelity ──────────────────────────────────────────────────────

def test_fidelity_is_tiered_only_with_a_deleted_part():
    from app.services import archive_state

    kw = dict(has_versions=True, plan="neon", archives_neon=True, stores_files=False)
    m = {"append_table": TABLE}
    assert archive_state.describe(m, **kw)["fidelity"] == "rows"
    assert archive_state.describe(m, has_deleted_parts=False, **kw)["fidelity"] == "rows"
    assert archive_state.describe(m, has_deleted_parts=True, **kw)["fidelity"] == "tiered"
    assert archive_state.TIERED == "tiered"


# ── the model and the migration ───────────────────────────────────────────────

def test_the_model_matches_the_plan():
    from app.models import HistoryPart

    cols = set(HistoryPart.__table__.columns.keys())
    assert cols == {"id", "dataset_id", "table_name", "closed_month", "part", "r2_key",
                    "rows", "bytes", "content_hash", "first_seen_min", "first_seen_max",
                    "last_seen_min", "last_seen_max", "deleted_from_sql", "deleted_at",
                    "created_at", "kind"}  # kind: alembic 073 (phase 4)
    assert HistoryPart.__tablename__ == "history_parts"


def test_migration_072_creates_the_catalog_in_schema_app():
    path = os.path.join(os.path.dirname(__file__), "..", "alembic", "versions",
                        "072_history_parts.py")
    src = open(path, encoding="utf-8").read()
    assert 'revision = "072"' in src and 'down_revision = "071"' in src
    assert "app.history_parts" in src
    assert "ON DELETE RESTRICT" in src
    assert "UNIQUE (table_name, closed_month, part)" in src
    assert "UNLOGGED" not in src


# ── config + scheduler ────────────────────────────────────────────────────────

def test_config_defaults():
    from app.config import Settings

    s = Settings()
    assert s.history_export_enabled is False
    assert s.history_min_rows == 100_000
    assert s.history_part_rows == 500_000
    assert s.history_export_hour_utc == 3


def test_the_scheduler_registers_the_jobs_only_when_enabled():
    src = open(os.path.join(os.path.dirname(__file__), "..", "app", "worker",
                            "scheduler.py"), encoding="utf-8").read()
    assert "history_export_enabled" in src
    assert 'id="history_export"' in src and 'id="history_audit"' in src


# ── state ─────────────────────────────────────────────────────────────────────

def test_status_shape():
    s = hx.status()
    for k in ("running", "started_at", "finished_at", "current", "done", "failed"):
        assert k in s
    assert "stop" not in s

