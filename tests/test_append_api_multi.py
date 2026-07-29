"""The public /api/append layer over MULTI-RESOURCE (append_db_multi) datasets.

Regression suite for a silent production failure: all 7 such datasets served as
completely empty across all 7 endpoints (schema 404, rows 0, CSV = a lone BOM)
while their rows sat in NEON — because delta_archiver writes
``{"_resource_ids", "_append_tables", "_names"}`` and append.py::_resolve looked
only for the single-table ``append_table``, then fell back to
``table_name(ds)``: a table that does not exist for these datasets.

The worst part was not the UI. /api/append/datastore_search backs MCP's
query_dataset_rows, so a model asking about "סניפי בנקים פיזיים" was told
``total: 0`` — and the only reasonable inference from that is "the dataset is
empty", not "the read failed". Verified against production before the fix:
append_branches_effd0eaf_2202bada held 1,400 rows and
append_branches_effd0eaf_6f3bda2a held 1,401; the write path was never broken.
"""
import asyncio
import types
import uuid

import pytest
from fastapi import HTTPException

from app.api import append as api
from app.services import append_store


def _ds(**kw):
    base = dict(
        id=uuid.UUID("effd0eaf-c6be-476b-8a44-7ff52fdb6ce2"),
        ckan_name="branches", title="סניפי בנקים פיזיים",
        organization="org", ckan_id="ckan-1", source_type="ckan",
        source_url=None, resource_id=None, storage_mode="append_only",
        scraper_config=None, tags=[], field_flags={},
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


# The real shape delta_archiver.py writes for a multi-resource NEON dataset.
_R1 = "2202bada-4baf-45f5-aa61-8c5bad9646d3"
_R2 = "6f3bda2a-8cde-4b86-a1c8-2761862b1224"
_MULTI = {
    "_resource_ids": [_R1, _R2],
    "_append_tables": {_R1: "append_branches_effd0eaf_2202bada",
                       _R2: "append_branches_effd0eaf_6f3bda2a"},
    "_names": {_R1: "סניפים", _R2: "עמדות"},
}


# ── the resolver ─────────────────────────────────────────────────────────────

def test_multi_resource_mappings_resolve_to_their_real_tables():
    """The bug, at its root: this returned the non-existent single-table name."""
    out = append_store.tables_from_mappings(_ds(), _MULTI)
    assert [t["table"] for t in out] == [
        "append_branches_effd0eaf_2202bada",
        "append_branches_effd0eaf_6f3bda2a",
    ]
    assert [t["resource_name"] for t in out] == ["סניפים", "עמדות"]


def test_table_order_comes_from_resource_ids_not_the_jsonb_dict():
    """Postgres does not preserve jsonb object key order (keys are stored sorted
    by length then bytes), so iterating _append_tables would make "the dataset's
    first table" depend on how the resource UUIDs happen to sort. Callers default
    to the first entry, so this order is user-visible."""
    shuffled = {**_MULTI, "_append_tables": {
        _R2: "append_branches_effd0eaf_6f3bda2a",
        _R1: "append_branches_effd0eaf_2202bada",
    }}
    out = append_store.tables_from_mappings(_ds(), shuffled)
    assert out[0]["resource_id"] == _R1, "must follow _resource_ids"


def test_a_table_missing_from_resource_ids_is_still_served():
    """_resource_ids is the ordering hint, not the whitelist — a mapping entry it
    omits must not silently disappear."""
    partial = {"_resource_ids": [_R1], "_append_tables": _MULTI["_append_tables"],
               "_names": _MULTI["_names"]}
    out = append_store.tables_from_mappings(_ds(), partial)
    assert len(out) == 2 and out[0]["resource_id"] == _R1


def test_single_table_datasets_are_unchanged():
    """The 10 single-table append datasets work in production today; this is the
    regression guard that matters most."""
    out = append_store.tables_from_mappings(_ds(), {"append_table": "append_x_abc"})
    assert [t["table"] for t in out] == ["append_x_abc"]


def test_no_mapping_falls_back_to_the_deterministic_name():
    ds = _ds()
    out = append_store.tables_from_mappings(ds, None)
    assert [t["table"] for t in out] == [append_store.table_name(ds)]


# ── choosing among them ──────────────────────────────────────────────────────

def _tables():
    return append_store.tables_from_mappings(_ds(), _MULTI)


def test_default_is_the_first_resource():
    assert api._pick(_tables(), None)["resource_id"] == _R1


@pytest.mark.parametrize("selector", [
    "append_branches_effd0eaf_6f3bda2a",   # physical table name
    _R2,                                   # resource id (CKAN habit)
    "עמדות",                                # resource name (from /schema or the UI)
])
def test_a_table_can_be_selected_three_ways(selector):
    assert api._pick(_tables(), selector)["resource_id"] == _R2


def test_an_unknown_selector_404s_and_lists_what_exists():
    """Serving a DIFFERENT table than the one asked for is how this class of bug
    survives unnoticed — so it must fail, and say what is available."""
    with pytest.raises(HTTPException) as e:
        api._pick(_tables(), "no_such_table")
    assert e.value.status_code == 404
    assert len(e.value.detail["available"]) == 2
    assert {t["resource_name"] for t in e.value.detail["available"]} == {"סניפים", "עמדות"}


def test_the_selector_is_never_treated_as_a_column_filter():
    """/rows and /download.csv turn unknown query params into per-column ILIKEs.
    An unreserved `table` param would filter on a column named "table" instead of
    selecting a table — and silently return nothing."""
    for p in ("table", "resource", "resource_id"):
        assert p in api._RESERVED


# ── the download filename ────────────────────────────────────────────────────

def test_a_hebrew_filename_does_not_break_the_header():
    """Caught in production, not by these tests: naming multi-table CSVs after
    their resource put "סניפים - עברית" in Content-Disposition, header values
    must be latin-1 encodable, and every such download 500ed."""
    h = api._content_disposition("branches_סניפים - עברית_append.csv")
    h.encode("latin-1")                      # what the server does; must not raise
    assert "filename*=UTF-8''" in h          # the real name still travels
    assert 'filename="branches_' in h        # and an ASCII fallback exists


def test_the_ascii_fallback_cannot_inject_a_header():
    """It lands inside a quoted header string, so a stray quote or newline would
    end the value and start something else."""
    h = api._content_disposition('a"b\r\nX-Evil: 1.csv')
    assert '"' not in h.split("filename*=")[0].split('filename="')[1].rstrip('"; ')
    assert "\r" not in h and "\n" not in h


def test_an_all_non_ascii_name_still_yields_a_filename():
    h = api._content_disposition("סניפים.csv")
    assert 'filename="' in h and 'filename=""' not in h


# ── _resolve: the endpoints' single entry point ──────────────────────────────

class _Result:
    def __init__(self, scalar=None, rows=None):
        self._scalar, self._rows = scalar, rows or []

    def scalar_one_or_none(self):
        return self._scalar

    def all(self):
        return self._rows


class _DB:
    """Answers _resolve's two queries in order: the dataset, then the versions'
    resource_mappings newest-first."""

    def __init__(self, ds, mapping_rows):
        self.ds, self.mapping_rows, self.n = ds, mapping_rows, 0

    async def execute(self, q):
        self.n += 1
        if self.n == 1:
            return _Result(scalar=self.ds)
        return _Result(rows=[(m,) for m in self.mapping_rows])


@pytest.fixture(autouse=True)
def _configured(monkeypatch):
    monkeypatch.setattr(append_store, "is_configured", lambda: True)


def _resolve(mapping_rows, selector=None, ds=None):
    ds = ds or _ds()
    return asyncio.run(api._resolve(str(ds.id), _DB(ds, mapping_rows), selector))


def test_resolve_finds_multi_resource_tables():
    """Before the fix this returned table_name(ds) — a table that does not exist
    — and every endpoint downstream reported an empty dataset."""
    _, table, tables = _resolve([_MULTI])
    assert table == "append_branches_effd0eaf_2202bada"
    assert len(tables) == 2


def test_resolve_honours_the_selector():
    _, table, _t = _resolve([_MULTI], selector="עמדות")
    assert table == "append_branches_effd0eaf_6f3bda2a"


def test_resolve_still_prefers_a_newer_single_table_version():
    """Versions arrive newest-first and the first NEON mapping wins, whichever
    shape it is."""
    _, table, tables = _resolve([{"append_table": "append_new"}, _MULTI])
    assert table == "append_new" and len(tables) == 1


def test_resolve_skips_versions_with_no_neon_mapping():
    """A dataset can have R2-only versions on top of its NEON ones."""
    _, table, _t = _resolve([{"r2:file": "x"}, {}, _MULTI])
    assert table == "append_branches_effd0eaf_2202bada"


def test_resolve_rejects_a_dataset_that_is_not_an_append_archive():
    ds = _ds(storage_mode="full_snapshot")
    with pytest.raises(HTTPException) as e:
        _resolve([], ds=ds)
    assert e.value.status_code == 409


# ── one resolver, structurally ───────────────────────────────────────────────

class _MappingsDB:
    """resolve_tables issues exactly ONE query (the mappings), unlike _resolve."""

    def __init__(self, mapping_rows):
        self.mapping_rows = mapping_rows

    async def execute(self, q):
        return _Result(rows=[(m,) for m in self.mapping_rows])


def test_the_shared_resolver_reads_the_newest_neon_mapping():
    from app.services import append_tables
    got = asyncio.run(append_tables.resolve_tables(
        _ds(), _MappingsDB([{"r2:x": "y"}, _MULTI])))
    assert [t["resource_id"] for t in got] == [_R1, _R2]


def test_the_shared_resolver_falls_back_when_no_version_has_a_mapping():
    from app.services import append_tables
    ds = _ds()
    got = asyncio.run(append_tables.resolve_tables(ds, _MappingsDB([])))
    assert [t["table"] for t in got] == [append_store.table_name(ds)]


def test_the_mcp_does_not_guess_the_table_name_itself():
    """Source-level guard for the failure mode that outlived the API fix.

    query_dataset_rows called append_store.table_name(d) directly and never read
    resource_mappings, so fixing /api/append did nothing for it — the MCP does
    not go through the HTTP endpoint it advertises as query_url. A model was
    told `total: 0` for a dataset holding 2,806 rows, and "empty dataset" is the
    only sane reading of that. Mocked tests cannot see this; matching the source
    can."""
    import re as _re
    src = open("app/mcp/server.py", encoding="utf-8").read()
    body = src.split("async def _tool_query_dataset_rows")[1].split("\nasync def ")[0]
    assert not _re.search(r"append_store\.table_name\(", body), \
        "query_dataset_rows must resolve through append_tables.resolve_tables"
    assert "append_tables.resolve_tables" in body
