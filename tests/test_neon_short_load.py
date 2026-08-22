"""A NEON load that lands fewer rows than the version promises must say so.

The load is best-effort by design: a failure is logged, the version is published
anyway, and the reasoning is that the next poll refills the table. Nothing ever
compared the two numbers afterwards, so "partial" and "complete" looked
identical from every surface in the product.

גושים shape sat that way for a day — a version reporting 18,689 rows over a
table holding 11,578, with its own published GeoJSON containing 18,689 features
and not one duplicate among them. A third of the national block layer was
missing and the only symptom was a spatial join coming up two thirds short.
"""
import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import app.api.worker as worker  # noqa: E402
from app.services import append_store  # noqa: E402

run = asyncio.run


class _Rec:
    """Stands in for the TrackedDataset row the background loader updates."""
    def __init__(self):
        self.import_warning = None
        self.import_warning_at = None


def _patch(monkeypatch, *, held, loaded=None, rec=None, raises=None):
    async def _stream(table, path, delete_after=False, stamp_col=None):
        if raises:
            raise raises
        return held if loaded is None else loaded

    async def _count(table, **kw):
        return held

    monkeypatch.setattr(worker, "_neon_stream_load_file", _stream)
    # The shared helper reads the ESTIMATE — an exact count(*) of a million-row
    # table is the long step that must never sit on a request path.
    monkeypatch.setattr(append_store, "table_count_estimate", _count)
    monkeypatch.setattr(append_store, "table_count", _count)
    if rec is not None:
        monkeypatch.setattr(worker, "async_session", None, raising=False)
        _install_session(monkeypatch, rec)


def _install_session(monkeypatch, rec):
    class _DB:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def execute(self, *a, **k):
            return types.SimpleNamespace(scalar_one_or_none=lambda: rec)
        async def commit(self): pass
    import app.database as dbmod
    monkeypatch.setattr(dbmod, "async_session", lambda: _DB())


def test_a_short_load_is_written_onto_the_dataset(monkeypatch):
    rec = _Rec()
    _patch(monkeypatch, held=11578)
    _install_session(monkeypatch, rec)
    run(worker._neon_only_load_csv("append_x", "/tmp/x.csv", "גושים shape",
                                   ds_id="ds-1", expected=18689))
    assert rec.import_warning, "a third of the rows missing and nothing said so"
    assert "11,578" in rec.import_warning and "18,689" in rec.import_warning


def test_a_complete_load_says_nothing(monkeypatch):
    """The check must not cry wolf: an append table accumulates across versions
    and samples, so holding MORE than this version's count is the normal case."""
    rec = _Rec()
    _patch(monkeypatch, held=90000)
    _install_session(monkeypatch, rec)
    run(worker._neon_only_load_csv("append_x", "/tmp/x.csv", "נתוני הסורק",
                                   ds_id="ds-1", expected=18689))
    assert rec.import_warning is None


def test_a_load_that_threw_is_reported_even_if_the_count_looks_fine(monkeypatch):
    """A table already holding an earlier version's rows can pass the count test
    while THIS load put nothing in it."""
    rec = _Rec()
    _patch(monkeypatch, held=90000, raises=RuntimeError("connection reset"))
    _install_session(monkeypatch, rec)
    run(worker._neon_only_load_csv("append_x", "/tmp/x.csv", "נתוני הסורק",
                                   ds_id="ds-1", expected=18689))
    assert rec.import_warning


def test_the_check_is_skipped_when_there_is_nothing_to_compare(monkeypatch):
    """Callers that do not know the expected count (older paths) must keep
    working exactly as before rather than being flagged on every push."""
    rec = _Rec()
    _patch(monkeypatch, held=0)
    _install_session(monkeypatch, rec)
    run(worker._neon_only_load_csv("append_x", "/tmp/x.csv", "r", ds_id="ds-1",
                                   expected=0))
    run(worker._neon_only_load_csv("append_x", "/tmp/x.csv", "r", expected=99))
    assert rec.import_warning is None


def test_the_request_path_check_never_scans_the_table(monkeypatch):
    """push-version's own check runs while the request is open. An exact
    count(*) of the 1.1M-row parcel layer there is the long synchronous step
    that gets a task reclaimed mid-push — the failure this area keeps having.
    It must use the planner estimate, and it must tolerate the estimate being a
    few percent out rather than crying wolf on a healthy dataset."""
    import inspect
    src = inspect.getsource(worker.push_version)
    assert "table_count_estimate" in src
    assert "append_store.table_count(" not in src


def test_an_unanalysed_table_is_not_reported_as_empty():
    """reltuples is -1 before the first ANALYZE. Reading that as 0 would flag
    every freshly created table as having lost all its rows."""
    import inspect
    src = inspect.getsource(worker.push_version)
    assert "total < 0" in src


def test_every_loader_reports_a_short_load_not_just_two(monkeypatch):
    """The first version of this check covered two of the three loaders, and the
    one it missed is the one that failed. The parcel layer's version reported
    1,097,775 rows over a table holding 150,000 — a background load killed by an
    OOM eighty seconds in — and the dataset page said nothing, because its
    loader was the third.

    They now share one helper, so a fourth cannot quietly ship without it."""
    import inspect
    src = inspect.getsource(worker)
    for fn in ("_neon_only_load_csv", "_neon_stream_load_r2"):
        body = inspect.getsource(getattr(worker, fn))
        assert "_record_short_load" in body, f"{fn} can lose rows silently"
    # and push-version's own in-request check
    assert "_check_neon_landed" in inspect.getsource(worker.push_version)


def test_the_shared_check_flags_a_background_load_that_died(monkeypatch):
    rec = _Rec()

    async def _count(table, **kw):
        return 150000

    monkeypatch.setattr(append_store, "table_count_estimate", _count)
    _install_session(monkeypatch, rec)
    run(worker._record_short_load("append_shape_x", "ds-1", 1097775,
                                  res_name="חלקות shape"))
    assert rec.import_warning
    assert "150,000" in rec.import_warning and "1,097,775" in rec.import_warning


def test_the_shared_check_stays_quiet_when_the_table_is_fuller(monkeypatch):
    rec = _Rec()

    async def _count(table, **kw):
        return 2_000_000        # accumulated across versions — normal

    monkeypatch.setattr(append_store, "table_count_estimate", _count)
    _install_session(monkeypatch, rec)
    run(worker._record_short_load("append_shape_x", "ds-1", 1097775))
    assert rec.import_warning is None
