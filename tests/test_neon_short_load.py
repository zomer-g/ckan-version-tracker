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
    async def _stream(table, path, delete_after=False):
        if raises:
            raise raises
        return held if loaded is None else loaded

    async def _count(table, **kw):
        return held

    monkeypatch.setattr(worker, "_neon_stream_load_file", _stream)
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
