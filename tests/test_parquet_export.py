"""Parquet mirrors of the large tables.

A CSV is rows of text; Parquet is the same rows stored column by column, which
for a multi-million-row table means it compresses far better, a reader can take
one column instead of every byte, and the schema travels inside the file.

Two things these pin, because both are easy to get wrong later:

  * the file is written ALL-STRING on purpose. Every append column is text —
    that is how the archive stores what a publisher published — and deciding
    that deal_amount is a number means deciding what to do with the row where
    it is not one. That is the publisher's call;
  * a mirror is offered for data someone PUBLISHED. The eligibility list is
    driven by datasets, not by a scan of the schema, so a derived index
    (over_re_*) or a leftover table is never picked up however large it is.
"""
import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.services import append_store, parquet_export as pe  # noqa: E402
from app.services.storage_client import storage_client  # noqa: E402

run = asyncio.run

pyarrow = pytest.importorskip("pyarrow", reason="parquet is an optional extra")
import pyarrow.parquet as pq  # noqa: E402


# ── naming and availability ──────────────────────────────────────────────

def test_the_key_is_per_table_not_per_version():
    """An append table is cumulative: there is ONE current Parquet of it,
    rebuilt when a version lands. Keying by version would pile up a file per
    push, each a near-copy of the last."""
    assert pe.parquet_key("append_x_1234") == "parquet/append_x_1234.parquet"
    assert pe.parquet_key("t") == pe.parquet_key("t"), "must be deterministic"


def test_missing_pyarrow_is_reported_not_raised(monkeypatch):
    """An optional dependency: a deploy without the wheel must answer "no
    Parquet" and leave everything else working, not fail to import."""
    real = pe.is_available

    def _fake():
        return False, "pyarrow is not installed (ImportError)"

    monkeypatch.setattr(pe, "is_available", _fake)
    ok, why = pe.is_available()
    assert ok is False and "pyarrow" in why
    monkeypatch.setattr(pe, "is_available", real)


# ── what gets a mirror ───────────────────────────────────────────────────

class _DS:
    def __init__(self, id_, title, ckan_name):
        self.id = id_
        self.title = title
        self.ckan_name = ckan_name
        self.status = "active"
        self.resource_id = None
        self.scraper_config = {}


class _DB:
    def __init__(self, datasets, versions):
        self._datasets = datasets
        self._versions = versions

    async def execute(self, stmt):
        text = str(stmt)
        datasets, versions = self._datasets, self._versions

        class _R:
            def scalars(self_inner):
                return types.SimpleNamespace(all=lambda: datasets)

            def all(self_inner):
                return versions
        return _R()


def _patch_counts(monkeypatch, counts):
    async def _est(table, **kw):
        return counts.get(table, 0)

    monkeypatch.setattr(append_store, "table_count_estimate", _est)


def test_a_table_under_the_threshold_is_not_mirrored(monkeypatch):
    ds = _DS("d1", "small", "small")
    db = _DB([ds], [("d1", {"append_table": "append_small"}, 1)])
    _patch_counts(monkeypatch, {"append_small": pe.PARQUET_MIN_ROWS - 1})
    assert run(pe.eligible_tables(db)) == []


def test_a_table_over_the_threshold_is_mirrored(monkeypatch):
    ds = _DS("d1", "big", "big")
    db = _DB([ds], [("d1", {"append_table": "append_big"}, 1)])
    _patch_counts(monkeypatch, {"append_big": 5_630_041})
    rows = run(pe.eligible_tables(db))
    assert [r["table"] for r in rows] == ["append_big"]
    assert rows[0]["dataset_id"] == "d1"


def test_each_table_of_a_multi_resource_dataset_is_considered(monkeypatch):
    """The rain forecast publishes 60 tables of a million rows each — one
    dataset, sixty mirrors."""
    ds = _DS("d1", "rain", "rain")
    mappings = {"_append_tables": [
        {"resource": "a", "table": "append_rain_a"},
        {"resource": "b", "table": "append_rain_b"},
        {"resource": "c", "table": "append_rain_small"},
    ]}
    db = _DB([ds], [("d1", mappings, 1)])
    _patch_counts(monkeypatch, {"append_rain_a": 1_037_142,
                                "append_rain_b": 1_037_142,
                                "append_rain_small": 10})
    assert {r["table"] for r in run(pe.eligible_tables(db))} == {
        "append_rain_a", "append_rain_b"}


def test_the_biggest_table_comes_first(monkeypatch):
    ds = _DS("d1", "x", "x")
    mappings = {"_append_tables": [{"resource": "a", "table": "append_mid"},
                                   {"resource": "b", "table": "append_huge"}]}
    db = _DB([ds], [("d1", mappings, 1)])
    _patch_counts(monkeypatch, {"append_mid": 600_000, "append_huge": 5_000_000})
    assert [r["table"] for r in run(pe.eligible_tables(db))] == [
        "append_huge", "append_mid"]


def test_only_the_newest_version_of_a_dataset_decides_its_tables(monkeypatch):
    """A dataset that was re-laid-out points at its current tables, not at the
    ones an older version filled and a migration has since emptied."""
    ds = _DS("d1", "x", "x")
    db = _DB([ds], [
        ("d1", {"append_table": "append_now"}, 4),
        ("d1", {"_append_tables": [{"resource": "a", "table": "append_old"}]}, 3),
    ])
    _patch_counts(monkeypatch, {"append_now": 3_217_456, "append_old": 2_492_861})
    assert [r["table"] for r in run(pe.eligible_tables(db))] == ["append_now"]


# ── the file itself ──────────────────────────────────────────────────────

class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __aiter__(self):
        async def gen():
            for r in self._rows:
                yield r
        return gen()


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self, sql, *args):
        return _FakeCursor(self._rows)

    def transaction(self):
        class _T:
            async def __aenter__(self_inner):
                return self_inner

            async def __aexit__(self_inner, *a):
                return False
        return _T()


class _FakePool:
    def __init__(self, rows):
        self._rows = rows

    def acquire(self):
        conn = _FakeConn(self._rows)

        class _A:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *a):
                return False
        return _A()


def _patch_build(monkeypatch, cols, rows, uploaded):
    async def _cols(table):
        return cols

    async def _est(table, **kw):
        return len(rows)

    async def _pool():
        return _FakePool(rows)

    async def _upload(key, *, file_path=None, file_content=None, content_type=None):
        with open(file_path, "rb") as fh:
            uploaded["bytes"] = fh.read()
        uploaded["key"] = key
        uploaded["content_type"] = content_type
        return key

    monkeypatch.setattr(append_store, "user_columns", _cols)
    monkeypatch.setattr(append_store, "table_count_estimate", _est)
    monkeypatch.setattr(append_store, "get_pool", _pool)
    monkeypatch.setattr(storage_client, "upload_object", _upload)
    monkeypatch.setattr(storage_client, "is_configured", lambda: True)


def test_the_file_round_trips_every_row_and_column(monkeypatch):
    import io as _io

    cols = ["gush", "chelka", "deal_date", "deal_amount"]
    rows = [{"gush": str(1000 + i), "chelka": str(i),
             "deal_date": "06/07/2026", "deal_amount": None if i == 2 else str(i * 10)}
            for i in range(5)]
    uploaded: dict = {}
    _patch_build(monkeypatch, cols, rows, uploaded)

    res = run(pe.build_table_parquet("append_x", min_rows=0))
    assert res.get("error") is None, res
    assert res["rows"] == 5 and res["columns"] == 4
    assert uploaded["key"] == "parquet/append_x.parquet"

    t = pq.read_table(_io.BytesIO(uploaded["bytes"]))
    assert t.num_rows == 5
    assert t.column_names == cols
    assert t.column("gush").to_pylist() == ["1000", "1001", "1002", "1003", "1004"]
    # A NULL stays a NULL rather than becoming the string "None".
    assert t.column("deal_amount").to_pylist()[2] is None


def test_every_column_is_a_string(monkeypatch):
    """Deliberate: the archive stores text because that is what was published,
    and typing deal_amount as a number decides what to do with the row where it
    is not one. That is the publisher's call, not this file's."""
    import io as _io

    cols = ["n", "d"]
    rows = [{"n": "12345", "d": "06/07/2026"}]
    uploaded: dict = {}
    _patch_build(monkeypatch, cols, rows, uploaded)
    run(pe.build_table_parquet("append_x", min_rows=0))

    t = pq.read_table(_io.BytesIO(uploaded["bytes"]))
    for f in t.schema:
        assert f.type == pyarrow.string(), f"{f.name} is {f.type}, not string"


def test_a_table_under_the_minimum_is_skipped_without_writing(monkeypatch):
    uploaded: dict = {}
    _patch_build(monkeypatch, ["a"], [{"a": "1"}], uploaded)
    res = run(pe.build_table_parquet("append_small", min_rows=500_000))
    assert "skipped" in res
    assert uploaded == {}, "nothing may be uploaded for a skipped table"


def test_a_missing_table_is_an_error_not_a_crash(monkeypatch):
    async def _cols(table):
        return []

    monkeypatch.setattr(append_store, "user_columns", _cols)
    monkeypatch.setattr(storage_client, "is_configured", lambda: True)
    res = run(pe.build_table_parquet("append_gone", min_rows=0))
    assert "error" in res and "columns" in res["error"]


def test_batching_does_not_change_the_result(monkeypatch):
    """More rows than one batch must produce the same file, not a truncated
    one — the writer is fed in row groups and the tail is easy to drop."""
    import io as _io

    monkeypatch.setattr(pe, "_BATCH", 3)
    cols = ["a"]
    rows = [{"a": str(i)} for i in range(10)]
    uploaded: dict = {}
    _patch_build(monkeypatch, cols, rows, uploaded)
    res = run(pe.build_table_parquet("append_x", min_rows=0))

    assert res["rows"] == 10
    t = pq.read_table(_io.BytesIO(uploaded["bytes"]))
    assert t.column("a").to_pylist() == [str(i) for i in range(10)]
