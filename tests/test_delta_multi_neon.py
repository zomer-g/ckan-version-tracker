"""Multi-resource NEON archive: per-resource tables + resumable checkpoint.

archive_multi_via_datastore_streaming streams each datastore-active resource of
a multi-resource archive_neon dataset into its OWN NEON table, checkpointing a
combined {done, cur, offset} in scraper_config after every durable flush so a
dyno recycle resumes the exact resource+offset instead of restarting. These
tests mock the append-store DB helpers and the datastore pager and pin: the
fresh full run (all tables, one version, checkpoint cleared), a resume that
skips a finished resource, and a mid-resource resume that continues from the
checkpointed offset.
"""
import asyncio
import os
import sys
import types
import uuid

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services import append_store  # noqa: E402
from app.services import delta_archiver as DA  # noqa: E402

RID_A = "d8b92642-bad4-43f4-b00f-ea24f0c0702b"
RID_B = "588dfec1-b95c-495b-a1f4-e1ca4278be5d"


def _ds(scraper_config=None):
    return types.SimpleNamespace(
        id=uuid.UUID("4ab75f12-01a0-4f1a-b980-41572b6776bd"),
        ckan_name="bus_rishui_bitzua_2021",
        scraper_config=scraper_config,
        last_polled_at=None,
        last_modified=None,
        last_error="prev",
    )


def _resources_info():
    fields = [{"id": "_id", "type": "int"}, {"id": "line", "type": "text"}]
    return [
        ({"id": RID_A, "name": "אוטובוסים 2026", "datastore_active": True}, {"fields": fields, "total": 3}),
        ({"id": RID_B, "name": "אוטובוסים 2025", "datastore_active": True}, {"fields": fields, "total": 3}),
    ]


class _FakeDB:
    def __init__(self):
        self.commits = 0
        self.added = []

    async def commit(self):
        self.commits += 1

    def add(self, obj):
        self.added.append(obj)


@pytest.fixture
def store(monkeypatch):
    """Mock append_store DB helpers + the datastore pager. Records calls."""
    calls = {"ensure": [], "append": [], "count": []}

    monkeypatch.setattr(append_store, "is_configured", lambda: True)

    async def ensure_table(table, cols, *, key_col, keyless):
        calls["ensure"].append((table, tuple(cols), key_col, keyless))

    async def append_rows(table, cols, rows, *, key_col, keyless, first_seen=None):
        calls["append"].append((table, len(rows)))
        return len(rows)

    async def table_count(table):
        calls["count"].append(table)
        return 3

    monkeypatch.setattr(append_store, "ensure_table", ensure_table)
    monkeypatch.setattr(append_store, "append_rows", append_rows)
    monkeypatch.setattr(append_store, "table_count", table_count)

    # Two pages per resource: offsets 2 then 3 (matches _stream_datastore_pages'
    # "next_offset AFTER this page" contract). Honors start_offset by skipping
    # pages whose end offset is already reached.
    PAGES = {
        RID_A: [(2, [{"line": "a1"}, {"line": "a2"}]), (3, [{"line": "a3"}])],
        RID_B: [(2, [{"line": "b1"}, {"line": "b2"}]), (3, [{"line": "b3"}])],
    }
    seen_start = {}

    async def fake_stream(rid, start_offset=0):
        seen_start[rid] = start_offset
        for end_off, batch in PAGES[rid]:
            if end_off <= start_offset:
                continue
            yield end_off, batch

    monkeypatch.setattr(DA, "_stream_datastore_pages", fake_stream)
    return calls, seen_start


def test_fresh_full_run_streams_all_to_per_resource_tables(store):
    calls, _ = store
    ds = _ds(scraper_config={"archive_neon": True, "storage_backend": "r2"})
    db = _FakeDB()
    ok = asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=ds, resources_info=_resources_info(),
        next_version=1, new_modified="2026-07-15", db=db,
    ))
    assert ok is True
    ta = append_store.table_name_for_resource(ds, RID_A)
    tb = append_store.table_name_for_resource(ds, RID_B)
    # Each resource got its own table, ensured keyless.
    assert {c[0] for c in calls["ensure"]} == {ta, tb}
    assert all(c[3] is True and c[2] is None for c in calls["ensure"])  # keyless, no key_col
    # All rows appended (3 each).
    assert sum(n for _, n in calls["append"]) == 6
    # Exactly one version recorded, tagged multi, mapping both tables.
    assert len(db.added) == 1
    v = db.added[0]
    assert v.change_summary["type"] == "append_db_multi"
    assert v.change_summary["rows_added"] == 6
    assert v.resource_mappings["_append_tables"] == {RID_A: ta, RID_B: tb}
    assert set(v.resource_mappings["_names"]) == {RID_A, RID_B}
    # Checkpoint cleared; _id stripped so 'line' is the only data col ensured.
    assert (ds.scraper_config or {}).get("neon_multi") is None
    assert ds.last_polled_at is not None
    assert ds.last_error is None
    assert calls["ensure"][0][1] == ("line",)


def test_resume_skips_finished_resource(store):
    calls, _ = store
    ds = _ds(scraper_config={
        "archive_neon": True,
        "neon_multi": {"done": [RID_A], "cur": None, "offset": 0},
    })
    db = _FakeDB()
    ok = asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=ds, resources_info=_resources_info(),
        next_version=1, new_modified="2026-07-15", db=db,
    ))
    assert ok is True
    tb = append_store.table_name_for_resource(ds, RID_B)
    # Only RID_B was streamed (RID_A already done).
    assert [c[0] for c in calls["ensure"]] == [tb]
    assert sum(n for _, n in calls["append"]) == 3
    assert len(db.added) == 1
    # Version still maps BOTH tables (RID_A's table name is deterministic).
    assert set(db.added[0].resource_mappings["_append_tables"]) == {RID_A, RID_B}


def test_mid_resource_resume_continues_from_checkpoint(store):
    calls, seen_start = store
    ds = _ds(scraper_config={
        "archive_neon": True,
        "neon_multi": {"done": [], "cur": RID_A, "offset": 2},
    })
    db = _FakeDB()
    ok = asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=ds, resources_info=_resources_info(),
        next_version=1, new_modified="2026-07-15", db=db,
    ))
    assert ok is True
    # RID_A resumed at offset 2 (page 1 skipped); RID_B fresh at 0.
    assert seen_start[RID_A] == 2
    assert seen_start[RID_B] == 0
    # RID_A contributed only its 2nd page (1 row); RID_B both (3).
    assert sum(n for _, n in calls["append"]) == 4


def test_declines_without_append_db(monkeypatch):
    monkeypatch.setattr(append_store, "is_configured", lambda: False)
    ds = _ds(scraper_config={"archive_neon": True})
    db = _FakeDB()
    ok = asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=ds, resources_info=_resources_info(),
        next_version=1, new_modified="x", db=db,
    ))
    assert ok is False
    assert db.added == []
