"""Unit tests for the /data catalog + console SQL guards (no DB needed).

Covers the pure helpers: the search_path whitelist and read-only SQL validator
in append_store, and the dataset→table resolution / source-url / file-link logic
in data_catalog.
"""
import os
import sys
import types
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.services import append_store as A  # noqa: E402
from app.services import data_catalog as C  # noqa: E402


# ── search_path whitelist ────────────────────────────────────────────────────

def test_safe_search_path_quotes_valid_schemas():
    assert A._safe_search_path("public, knesset") == '"public", "knesset"'
    assert A._safe_search_path("public") == '"public"'


@pytest.mark.parametrize("bad", [
    "public; DROP TABLE x",
    "public, pg_catalog; select 1",
    "public--",
    "",
    "1bad",
    "knesset, ",  # trailing empty is fine, but a bad token:
])
def test_safe_search_path_rejects_injection(bad):
    # "knesset, " actually normalises to just knesset (trailing empty dropped),
    # so exclude that one from the reject expectation.
    if bad.strip(", ") and all(
        __import__("re").fullmatch(r"[a-z_][a-z0-9_]*", p.strip())
        for p in bad.split(",") if p.strip()
    ):
        pytest.skip("valid after trimming")
    with pytest.raises(ValueError):
        A._safe_search_path(bad)


# ── read-only SQL validator (shared by run_readonly_sql + iter_sql_csv) ───────

def test_validate_readonly_sql_accepts_select_and_with():
    assert A.validate_readonly_sql("SELECT 1").lower().startswith("select")
    assert A.validate_readonly_sql("  WITH t AS (SELECT 1) SELECT * FROM t ;")


@pytest.mark.parametrize("sql", [
    "",
    "SELECT 1; SELECT 2",             # multiple statements
    "UPDATE t SET x = 1",             # write
    "DROP TABLE t",                   # DDL
    "INSERT INTO t VALUES (1)",       # write
    "delete from t",                  # write (case-insensitive)
])
def test_validate_readonly_sql_rejects(sql):
    with pytest.raises(ValueError):
        A.validate_readonly_sql(sql)


# ── data_catalog: dataset → physical table resolution ────────────────────────

def _ds(**kw):
    base = dict(
        id=uuid.uuid4(), ckan_name="my_dataset", title="מאגר לדוגמה",
        organization="org", ckan_id="ckan-1", source_type="ckan",
        source_url=None, resource_id=None, storage_mode="full_snapshot",
        scraper_config=None, tags=[],
    )
    base.update(kw)
    return types.SimpleNamespace(**base)


def test_tables_of_single_from_mapping():
    ds = _ds()
    out = C._tables_of(ds, {"append_table": "append_my_dataset_abc123"})
    assert out == [{"table": "append_my_dataset_abc123", "resource_name": None}]


def test_tables_of_multi_resource():
    ds = _ds()
    maps = {
        "_append_tables": {"rid1": "append_ds_a_1", "rid2": "append_ds_a_2"},
        "_names": {"rid1": "2023", "rid2": "2024"},
    }
    out = sorted(C._tables_of(ds, maps), key=lambda r: r["table"])
    assert out == [
        {"table": "append_ds_a_1", "resource_name": "2023"},
        {"table": "append_ds_a_2", "resource_name": "2024"},
    ]


def test_tables_of_falls_back_to_deterministic_name():
    ds = _ds()
    out = C._tables_of(ds, {})
    assert len(out) == 1
    assert out[0]["table"] == A.table_name(ds)
    assert out[0]["resource_name"] is None


# ── source url + file links ──────────────────────────────────────────────────

def test_source_url_ckan_builds_datagovil_link():
    ds = _ds(source_type="ckan", organization="mot", ckan_name="bus")
    assert C._source_url(ds) == "https://data.gov.il/he/datasets/mot/bus"


def test_source_url_scraper_uses_source_url():
    ds = _ds(source_type="scraper", source_url="https://example.gov.il/x")
    assert C._source_url(ds) == "https://example.gov.il/x"


def test_files_of_skips_bookkeeping_keys():
    vid = uuid.uuid4()
    maps = {
        "קובץ ראשי": "res-123",
        "_resource_ids": ["res-123"],
        "_hashes": {"res-123": "abc"},
        "metadata": "meta-1",
    }
    files = C._files_of(vid, maps)
    assert files == [{"name": "קובץ ראשי", "url": f"/api/versions/{vid}/download/קובץ ראשי"}]


def test_ds_record_appends_resource_name_to_title():
    ds = _ds(title="תחבורה")
    rec = C._ds_record(ds, "append_x_1", "2024", None, 5, [{"name": "a", "type": "text"}])
    assert rec["title"] == "תחבורה — 2024"
    assert rec["schema"] == "public" and rec["kind"] == "dataset"
    assert rec["archive_url"] == f"/archive/{ds.id}"
