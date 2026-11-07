"""Files-only datasets in the cold tier: new versions go to Parquet, not SQL.

app/services/cold_ingest.py writes each version's rows that were not open as a
``full`` part and the rows it no longer carried as a ``closures`` part;
history_query applies the closures; the poll gate lets such a dataset poll;
move_to_cold takes the tables of earlier versions too.
"""
import asyncio
import io
import types
import uuid
from datetime import date, datetime, timezone

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from app.config import settings  # noqa: E402
from app.services import append_store, cold_ingest as ci  # noqa: E402
from app.services import history_export as hx  # noqa: E402
from app.services import history_query as hq  # noqa: E402

UTC = timezone.utc
DSID = "a545e976-94b9-4dac-b501-a34cb198ae99"


def _ts(*a):
    return datetime(*a, tzinfo=UTC)


def _ds(cfg=None, **kw):
    base = dict(id=uuid.UUID(DSID), title="קדסטר", status="active", poll_interval=604800,
                source_type="scraper", source_url="https://data.gov.il/dataset/modern_cadaster",
                scraper_config=cfg, storage_mode="full_snapshot", ckan_name="modern_cadaster",
                is_active=True)
    base.update(kw)
    return types.SimpleNamespace(**base)


FILES_ONLY_COLD = {"history_tier": "cold", "storage_backend": "r2"}


# ── which datasets ────────────────────────────────────────────────────────────

def test_only_a_files_only_cold_dataset_is_ingested():
    assert ci.should_ingest(_ds(FILES_ONLY_COLD))
    assert not ci.should_ingest(_ds({"storage_backend": "r2"}))                 # hot
    assert not ci.should_ingest(_ds({**FILES_ONLY_COLD, "archive_neon": True}))  # loads SQL
    assert not ci.should_ingest(_ds({"history_tier": "cold", "storage_backend": "neon"}))
    assert not ci.should_ingest(_ds(FILES_ONLY_COLD, storage_mode="append_only"))


def test_a_files_only_cold_dataset_may_poll():
    assert asyncio.run(hx.cold_poll_gate(_ds(FILES_ONLY_COLD))) is None


def test_a_cold_dataset_that_loads_sql_is_still_refused():
    why = asyncio.run(hx.cold_poll_gate(_ds({"history_tier": "cold", "storage_backend": "r2",
                                             "archive_neon": True})))
    assert why and "קר" in why


def test_tabular_resources_are_the_named_csvs():
    m = {"שכבות מידע קדסטרי": "r2:datasets/x/v1/04d92acd_file.csv.gz",
         "דוגמת קובץ": "r2:datasets/x/v1/b738acfc_gdb.csv",
         "_geojson_0": "r2:datasets/x/v1/26fc0128_gdb.geojson.gz",
         "_zip_parts": ["r2:datasets/x/v1/a.zip"],
         "Domain.xlsx": "r2:datasets/x/v1/domain.xlsx",
         "odata": "some-ckan-id"}
    assert ci.tabular_resources(m) == [("שכבות מידע קדסטרי", m["שכבות מידע קדסטרי"]),
                                       ("דוגמת קובץ", m["דוגמת קובץ"])]


def test_tables_are_named_as_the_sql_loader_names_them():
    ds = _ds(FILES_ONLY_COLD)
    two = ci.table_for(ds, ["a", "b"])
    assert two == {"a": append_store.table_name_for_scraper_resource(ds, "a"),
                   "b": append_store.table_name_for_scraper_resource(ds, "b")}
    one = ci.table_for(ds, ["a"])
    assert one == {"a": append_store.table_name(ds)}
    ratchet = ci.table_for(_ds({**FILES_ONLY_COLD, "neon_tables_per_resource": True}), ["a"])
    assert ratchet == {"a": append_store.table_name_for_scraper_resource(ds, "a")}


def test_layout_mappings():
    assert ci.layout_mappings([]) == {}
    assert ci.layout_mappings([{"resource": "a", "table": "t1"}]) == {"append_table": "t1"}
    two = [{"resource": "a", "table": "t1"}, {"resource": "b", "table": "t2"}]
    assert ci.layout_mappings(two) == {"_append_tables": two}


# ── identity: the same as the SQL loader ─────────────────────────────────────

HEADER = ["Name", "ParcelNumber", "", "_id", "geometry_wkt"]


def test_the_identity_is_the_one_the_sql_loader_binds():
    safe, keep, cols = append_store.csv_stream_layout(HEADER)
    assert cols == ["Name", "ParcelNumber", "geometry_wkt"]
    rec = append_store.csv_stream_row(["חלקה", "12", "junk", "7", "POINT (1 2)"], safe, keep)
    bound: list = []
    append_store.build_insert("t", cols, [rec], key_col=None, keyless=True, hash_uuid=True,
                              idents_out=bound)
    assert ci.ident_of(rec, cols, exclude=(), as_uuid=True) == str(bound[0])
    hexed: list = []
    append_store.build_insert("t", cols, [rec], key_col=None, keyless=True, idents_out=hexed)
    assert ci.ident_of(rec, cols, exclude=(), as_uuid=False) == str(hexed[0])


def test_a_short_row_reads_as_empty_cells():
    safe, keep, _cols = append_store.csv_stream_layout(HEADER)
    assert append_store.csv_stream_row(["x"], safe, keep) == {
        "Name": "x", "ParcelNumber": "", "geometry_wkt": ""}


# ── the open set and the diff ─────────────────────────────────────────────────

def _full(path, rows, cols):
    t = pa.timestamp("us", tz="UTC")
    data = {c: pa.array([r[c] for r in rows], pa.string()) for c in cols}
    data["first_seen"] = pa.array([r["first_seen"] for r in rows], t)
    data["last_seen"] = pa.array([r.get("last_seen") for r in rows], t)
    data["row_hash"] = pa.array([r["row_hash"] for r in rows], pa.string())
    pq.write_table(pa.table(data), str(path))


def _closures(path, rows):
    t = pa.timestamp("us", tz="UTC")
    pq.write_table(pa.table({
        "row_hash": pa.array([r[0] for r in rows], pa.string()),
        "first_seen": pa.array([r[1] for r in rows], t),
        "last_seen": pa.array([r[2] for r in rows], t)}), str(path))


def test_open_rows_minus_closures_and_reopened(tmp_path):
    a = tmp_path / "a.parquet"
    _full(a, [{"pr": "1", "first_seen": _ts(2026, 11, 2), "row_hash": "h1"},
              {"pr": "2", "first_seen": _ts(2026, 11, 2), "row_hash": "h2"},
              {"pr": "3", "first_seen": _ts(2026, 10, 1), "last_seen": _ts(2026, 10, 9),
               "row_hash": "h3"}], ["pr"])
    b = tmp_path / "b.parquet"  # h2 reopened later
    _full(b, [{"pr": "2", "first_seen": _ts(2026, 12, 1), "row_hash": "h2"}], ["pr"])
    c = tmp_path / "c.parquet"  # closes h1 and the FIRST h2, not the reopened one
    _closures(c, [("h1", _ts(2026, 11, 2), _ts(2026, 11, 20)),
                  ("h2", _ts(2026, 11, 2), _ts(2026, 11, 20))])
    got = ci.open_rows([str(a), str(b)], [str(c)])
    assert got == {"h2": _ts(2026, 12, 1)}


class _Writer:
    def __init__(self):
        self.rows = []

    def add(self, rec):
        self.rows.append(dict(rec))


def _csv(rows):
    buf = io.StringIO()
    import csv
    w = csv.writer(buf)
    w.writerow(["Name", "ParcelNumber"])
    w.writerows(rows)
    buf.seek(0)
    return buf


def _id(name, num):
    return ci.ident_of({"Name": name, "ParcelNumber": num}, ["Name", "ParcelNumber"],
                       exclude=(), as_uuid=True)


def test_diff_writes_only_what_was_not_open():
    ts = _ts(2026, 11, 10)
    open_map = {_id("a", "1"): _ts(2026, 11, 3), _id("b", "2"): _ts(2026, 11, 3),
                _id("gone", "9"): _ts(2026, 11, 3)}
    w = _Writer()
    d = ci.diff_file(_csv([["a", "1"], ["b", "2"], ["c", "3"], ["c", "3"], ["a", "1"]]),
                     open_map=open_map, writer=w, as_uuid=True, exclude=(), ts=ts)
    assert (d.total, d.matched, d.duplicates, len(d.new_ids)) == (5, 2, 2, 1)
    assert [r["Name"] for r in w.rows] == ["c"]
    assert w.rows[0]["first_seen"] == ts and w.rows[0]["last_seen"] is None
    assert w.rows[0]["row_hash"] == _id("c", "3")
    assert ci.closing_ids(open_map, d.seen) == [_id("gone", "9")]


def test_an_identical_version_writes_nothing():
    open_map = {_id("a", "1"): _ts(2026, 11, 3)}
    w = _Writer()
    d = ci.diff_file(_csv([["a", "1"]]), open_map=open_map, writer=w, as_uuid=True,
                     exclude=(), ts=_ts(2026, 11, 10))
    assert w.rows == [] and d.matched == 1 and not ci.closing_ids(open_map, d.seen)


def test_a_file_that_shares_nothing_is_an_identity_break():
    assert ci.identity_broken(open_count=10, total=10, matched=0)
    assert not ci.identity_broken(open_count=10, total=10, matched=1)
    assert not ci.identity_broken(open_count=0, total=10, matched=0)   # first version
    assert not ci.identity_broken(open_count=10, total=0, matched=0)   # empty file


def test_the_stamp_column_is_left_out_of_the_identity():
    cols = ["Name", "sampled_at"]
    a = ci.ident_of({"Name": "x", "sampled_at": "1"}, cols, exclude=("sampled_at",), as_uuid=True)
    b = ci.ident_of({"Name": "x", "sampled_at": "2"}, cols, exclude=("sampled_at",), as_uuid=True)
    assert a == b


# ── the engine applies closures ───────────────────────────────────────────────

T = "append_modern_cadaster_a545e976_53b3e08a"


@pytest.fixture
def engine(monkeypatch, tmp_path):
    full = tmp_path / "full.parquet"
    _full(full, [{"pr": "1", "first_seen": _ts(2026, 11, 3), "row_hash": "h1"},
                 {"pr": "2", "first_seen": _ts(2026, 11, 3), "row_hash": "h2"},
                 {"pr": "old", "first_seen": _ts(2026, 10, 1), "last_seen": _ts(2026, 10, 2),
                  "row_hash": "h0"}], ["pr"])
    new = tmp_path / "new.parquet"
    _full(new, [{"pr": "3", "first_seen": _ts(2026, 11, 20), "row_hash": "h3"}], ["pr"])
    clo = tmp_path / "clo.parquet"
    _closures(clo, [("h2", _ts(2026, 11, 3), _ts(2026, 11, 20))])
    paths = {"k-full": full.as_posix(), "k-new": new.as_posix(), "k-clo": clo.as_posix()}

    def part(key, kind, n):
        return {"id": str(uuid.uuid4()), "dataset_id": DSID, "table_name": T, "kind": kind,
                "window_days": None, "closed_month": date(2026, 11, 1), "part": n,
                "r2_key": key, "rows": 1, "bytes": 1, "content_hash": "x",
                "first_seen_min": None, "first_seen_max": None, "last_seen_min": None,
                "last_seen_max": None, "deleted_from_sql": True, "deleted_at": None}

    data = [part("k-full", "full", 1), part("k-new", "full", 2)]
    closure_parts = [part("k-clo", "closures", 3)]

    async def fake_catalog(table, *, closures=False):
        if table != T:
            return []
        return [dict(p) for p in (closure_parts if closures else data)]

    def hot(conn):
        conn.execute("ATTACH ':memory:' AS hot")
        conn.execute('CREATE SCHEMA hot."public"')

    monkeypatch.setattr(hq, "_catalog_parts", fake_catalog)
    monkeypatch.setattr(hq, "_part_url", lambda key: paths[key])
    monkeypatch.setattr(hq, "_hot_backend", lambda: hq.MemoryHot(hot))
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    monkeypatch.setattr(settings, "history_query_memory_mb", 256)
    hq.reset_state()


def test_closures_close_rows_for_current_and_as_of(engine):
    now = asyncio.run(hq.run(f"SELECT pr FROM {T} ORDER BY pr", tables=[T], current=True))
    assert [r["pr"] for r in now["rows"]] == ["1", "3"]
    before = asyncio.run(hq.run(f"SELECT pr FROM {T} ORDER BY pr", tables=[T],
                                as_of=_ts(2026, 11, 10)))
    assert [r["pr"] for r in before["rows"]] == ["1", "2"]
    every = asyncio.run(hq.run(f"SELECT pr, last_seen FROM {T} ORDER BY pr", tables=[T]))
    assert [r["pr"] for r in every["rows"]] == ["1", "2", "3", "old"]
    closed = {r["pr"]: r["last_seen"] for r in every["rows"]}
    assert closed["2"] and closed["2"].startswith("2026-11-20") and closed["1"] is None


def test_a_closures_part_is_never_read_as_rows(engine):
    out = asyncio.run(hq.run(f"SELECT count(*) AS n FROM {T}", tables=[T]))
    assert out["rows"] == [{"n": 4}]


# ── rehydrate applies the closures ────────────────────────────────────────────

def test_apply_closures():
    closed = {("h2", _ts(2026, 11, 3)): _ts(2026, 11, 20)}
    got = hx.apply_closures(["h1", "h2", "h2"],
                            [_ts(2026, 11, 3), _ts(2026, 11, 3), _ts(2026, 12, 1)],
                            [None, None, None], closed)
    assert got == [None, _ts(2026, 11, 20), None]


def test_rehydrate_reads_data_parts_only():
    parts = [{"kind": "full", "deleted_from_sql": True, "closed_month": date(2026, 11, 1),
              "part": 1},
             {"kind": "closures", "deleted_from_sql": True, "closed_month": date(2026, 11, 1),
              "part": 2}]
    assert [p["kind"] for p in hx.rehydrate_parts(parts)] == ["full"]


def test_summaries_do_not_count_closures_as_rows():
    parts = [{"kind": "full", "rows": 10, "deleted_from_sql": True},
             {"kind": "closures", "rows": 3, "deleted_from_sql": True}]
    s = hx.summarize(parts)
    assert s["rows_in_r2"] == 10 and s["rows_only_in_r2"] == 10 and s["parts"] == 1


# ── move_to_cold takes every version's tables ─────────────────────────────────

class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _DB:
    def __init__(self, mappings):
        self.mappings = mappings

    async def execute(self, _q):
        return _Result([(m,) for m in self.mappings])


def test_resolve_all_tables_newest_first_with_the_old_single_table():
    from app.services import append_tables

    ds = _ds(FILES_ONLY_COLD)
    v3 = {"_append_tables": [{"resource": "a", "table": "t_a"},
                             {"resource": "b", "table": "t_b"}]}
    v2 = {"append_table": append_store.table_name(ds)}
    got = asyncio.run(append_tables.resolve_all_tables(ds, _DB([v3, v2, {}])))
    assert got == ["t_a", "t_b", append_store.table_name(ds)]


def test_the_push_schedules_the_ingest():
    import inspect

    from app.api import worker

    src = inspect.getsource(worker.push_version)
    assert "cold_ingest.should_ingest(ds)" in src and "cold_ingest.schedule(" in src


# ── ingest_table end to end (fake R2, fake catalog, fake SQL) ─────────────────

@pytest.fixture
def fake_r2(monkeypatch, tmp_path):
    import gzip
    import shutil as _sh

    store: dict = {}
    catalog: list = []
    created: list = []

    async def upload_object(key, *, file_path=None, file_content=None, content_type=None):
        dest = tmp_path / f"obj-{len(store)}"
        _sh.copyfile(file_path, dest)
        store[key] = str(dest)
        return key

    async def download_to_file(key_or_value, dest):
        key = key_or_value[3:] if key_or_value.startswith("r2:") else key_or_value
        if key not in store:
            return False
        _sh.copyfile(store[key], dest)
        return True

    async def delete_object(key):
        store.pop(key, None)

    monkeypatch.setattr(ci.storage_client, "upload_object", upload_object)
    monkeypatch.setattr(ci.storage_client, "download_to_file", download_to_file)
    monkeypatch.setattr(ci.storage_client, "delete_object", delete_object)
    monkeypatch.setattr(hx.storage_client, "download_to_file", download_to_file)

    async def table_parts(table):
        return [dict(p) for p in catalog if p["table_name"] == table]

    async def record_parts(parts):
        catalog.extend(dict(p) for p in parts)

    async def ensure_table(table, cols, **kw):
        created.append((table, list(cols)))

    async def zero(table):
        return 0

    async def is_uuid(table):
        return True

    monkeypatch.setattr(hx, "_table_parts", table_parts)
    monkeypatch.setattr(hx, "_record_parts", record_parts)
    monkeypatch.setattr(append_store, "ensure_table", ensure_table)
    monkeypatch.setattr(ci, "_hot_rows", zero)
    monkeypatch.setattr(ci, "_hash_is_uuid", is_uuid)
    monkeypatch.setattr(settings, "append_close_rows_enabled", True)

    def put_csv(key, rows):
        path = tmp_path / f"src-{len(store)}.csv.gz"
        import csv
        with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["Name", "ParcelNumber"])
            w.writerows(rows)
        store[key] = str(path)
        return "r2:" + key

    return types.SimpleNamespace(store=store, catalog=catalog, put_csv=put_csv, created=created)


def _ingest(ds, table, value, ts, **kw):
    return asyncio.run(ci.ingest_table(ds, table, value, ts=ts, **kw))


def test_ingest_first_then_unchanged_then_changed(fake_r2):
    ds = _ds(FILES_ONLY_COLD)
    t = "append_modern_cadaster_a545e976_53b3e08a"
    v1 = fake_r2.put_csv("datasets/x/v1/a.csv.gz", [["a", "1"], ["b", "2"], ["c", "3"]])
    out = _ingest(ds, t, v1, _ts(2026, 11, 3), close=True)
    assert out["ok"] and out["new"] == 3 and out["closed"] == 0 and out["parts"] == 1
    assert fake_r2.created == [(t, ["Name", "ParcelNumber"])]
    assert [p["kind"] for p in fake_r2.catalog] == ["full"]
    assert fake_r2.catalog[0]["deleted_from_sql"] is True

    same = fake_r2.put_csv("datasets/x/v2/a.csv.gz", [["c", "3"], ["a", "1"], ["b", "2"]])
    out = _ingest(ds, t, same, _ts(2026, 11, 10), close=True)
    assert out["ok"] and out["new"] == 0 and out["closed"] == 0 and out["parts"] == 0
    assert len(fake_r2.catalog) == 1

    changed = fake_r2.put_csv("datasets/x/v3/a.csv.gz", [["a", "1"], ["b", "2"], ["d", "4"]])
    out = _ingest(ds, t, changed, _ts(2026, 11, 20), close=True)
    assert out["ok"] and out["new"] == 1 and out["closed"] == 1 and out["parts"] == 2
    kinds = [(p["kind"], p["part"], p["rows"]) for p in fake_r2.catalog]
    assert kinds == [("full", 1, 3), ("full", 2, 1), ("closures", 3, 1)]
    clo = pq.read_table(fake_r2.store[fake_r2.catalog[2]["r2_key"]]).to_pylist()
    assert clo == [{"row_hash": _id("c", "3"), "first_seen": _ts(2026, 11, 3),
                    "last_seen": _ts(2026, 11, 20)}]
    # and again: idempotent
    out = _ingest(ds, t, changed, _ts(2026, 11, 20), close=True)
    assert out["ok"] and out["new"] == 0 and out["closed"] == 0


def test_ingest_without_close_keeps_rows_open(fake_r2):
    ds = _ds(FILES_ONLY_COLD)
    t = "tbl"
    _ingest(ds, t, fake_r2.put_csv("k1.csv.gz", [["a", "1"], ["b", "2"]]), _ts(2026, 11, 3),
            close=True)
    out = _ingest(ds, t, fake_r2.put_csv("k2.csv.gz", [["a", "1"]]), _ts(2026, 11, 4),
                  close=False)
    assert out["ok"] and out["closed"] == 0


def test_the_close_guard_refuses_a_mass_close(fake_r2):
    ds = _ds(FILES_ONLY_COLD)
    t = "tbl"
    _ingest(ds, t, fake_r2.put_csv("k1.csv.gz", [[str(i), "x"] for i in range(10)]),
            _ts(2026, 11, 3), close=True)
    out = _ingest(ds, t, fake_r2.put_csv("k2.csv.gz", [["0", "x"]]), _ts(2026, 11, 4),
                  close=True)
    assert out["ok"] and out["closed"] == 0 and out.get("close_skipped")


def test_a_file_sharing_nothing_is_refused_and_uploads_nothing(fake_r2):
    ds = _ds(FILES_ONLY_COLD)
    t = "tbl"
    _ingest(ds, t, fake_r2.put_csv("k1.csv.gz", [["a", "1"]]), _ts(2026, 11, 3), close=True)
    before = set(fake_r2.store)
    out = _ingest(ds, t, fake_r2.put_csv("k2.csv.gz", [["z", "9"]]), _ts(2026, 11, 4),
                  close=True)
    assert not out["ok"] and "identity" in out["error"]
    assert len(fake_r2.catalog) == 1
    assert set(fake_r2.store) - before == {"k2.csv.gz"}  # only the source it was given


def test_parts_from_a_sql_export_seed_the_open_set(fake_r2, tmp_path):
    """The move to cold wrote the table's rows (from SQL) as a full part: the
    next version's unchanged rows must match them."""
    ds = _ds(FILES_ONLY_COLD)
    t = "tbl"
    p = tmp_path / "exported.parquet"
    _full(p, [{"Name": "a", "ParcelNumber": "1", "first_seen": _ts(2026, 11, 3),
               "row_hash": _id("a", "1")}], ["Name", "ParcelNumber"])
    key = hx.full_part_key(DSID, t, date(2026, 11, 1), 1)
    fake_r2.store[key] = str(p)
    fake_r2.catalog.append({"id": str(uuid.uuid4()), "dataset_id": DSID, "table_name": t,
                            "closed_month": date(2026, 11, 1), "part": 1, "r2_key": key,
                            "rows": 1, "bytes": 1, "content_hash": "x", "kind": "full",
                            "deleted_from_sql": True})
    out = _ingest(ds, t, fake_r2.put_csv("k.csv.gz", [["a", "1"], ["b", "2"]]),
                  _ts(2026, 11, 20), close=True)
    assert out["ok"] and out["matched"] == 1 and out["new"] == 1 and out["closed"] == 0


# ── a derived geom does not keep a table hot; a stopped move resumes ─────────

def test_a_geom_derived_from_text_columns_may_go_cold():
    assert hx.geometry_rebuildable(["geom"], ["Name", "geometry_wkt", "geom"])
    assert hx.geometry_rebuildable(["geom"], ["Name", "lon", "lat", "geom"])
    assert not hx.geometry_rebuildable(["geom"], ["Name", "geom"])          # nothing to rebuild from
    assert not hx.geometry_rebuildable(["shape"], ["geometry_wkt", "shape"])  # not the derived column
    assert not hx.geometry_rebuildable(["geom", "area"], ["geometry_wkt", "geom", "area"])


def test_a_cold_dataset_may_resume_its_move():
    ds = _ds({"history_tier": "cold", "storage_backend": "r2"})
    assert hx.cold_move_refusal(ds, confirm_rehydrate=True)                 # "already cold"
    assert hx.cold_move_refusal(ds, confirm_rehydrate=True, resume=True) is None
    hot = _ds({"storage_backend": "r2"})
    assert hx.cold_move_refusal(hot, confirm_rehydrate=False, resume=True)  # active: still refused


class _DescribeConn:
    def __init__(self, cols):
        self.cols = cols

    async def fetch(self, _sql, _table):
        return [{"column_name": c, "udt_name": u} for c, u in self.cols]


def _describe(cols, monkeypatch):
    async def ident(conn, table):
        return ("row_hash", "uuid")

    monkeypatch.setattr(hx, "_table_ident", ident)
    t = hx.PgTable("append_modern_cadaster_a545e976")
    t.conn = _DescribeConn(cols)
    return t, asyncio.run(t.describe())


def test_describe_lets_the_cadaster_table_go_cold(monkeypatch):
    """The prod shape that was refused twice: text columns, geometry_wkt, the
    validity columns, a uuid row_hash and a derived PostGIS geom."""
    cols = [("Name", "text"), ("GlobalID", "text"), ("geometry_wkt", "text"),
            ("first_seen", "timestamptz"), ("last_seen", "timestamptz"),
            ("row_hash", "uuid"), ("geom", "geometry")]
    t, (export, ident, kind) = _describe(cols, monkeypatch)
    assert export == ["Name", "GlobalID", "geometry_wkt"]
    assert (ident, kind) == ("row_hash", "uuid") and t.geometry is True


def test_describe_still_refuses_a_geometry_nothing_rebuilds(monkeypatch):
    with pytest.raises(hx.ColdRefused):
        _describe([("Name", "text"), ("row_hash", "uuid"), ("geom", "geometry")], monkeypatch)
    with pytest.raises(hx.ColdRefused):
        _describe([("n", "int4"), ("geometry_wkt", "text"), ("row_hash", "uuid"),
                   ("geom", "geometry")], monkeypatch)
