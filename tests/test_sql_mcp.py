"""Unit tests for the dedicated whole-site SQL MCP (/data/mcp). No DB needed.

The catalog and the SQL execution path are stubbed; what is under test is the
MCP surface itself — the tool registry, JSON-RPC dispatch, the catalog search
(including matching on COLUMN names), the unknown-table gate that keeps a
caller-supplied name from reaching a schema-qualified query, and that run_sql
goes through the read-only console path with the console search_path.
"""
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.mcp import sql_server as S  # noqa: E402
from app.services import append_store as A  # noqa: E402
from app.services import data_catalog as C  # noqa: E402

CATALOG = [
    {
        "table": "append_vehicles_1234", "schema": "public", "kind": "dataset",
        "title": "רכבים פרטיים", "organization": "משרד התחבורה",
        "source_type": "ckan", "dataset_id": "11111111-1111-1111-1111-111111111111",
        "source_url": "https://data.gov.il/x", "tags": ["תחבורה"], "est_rows": 4_100_000,
        "columns": [{"name": "mispar_rechev", "type": "text"},
                    {"name": "tozeret_nm", "type": "text", "alias": "יצרן"}],
    },
    {
        "table": "kns_bill", "schema": "knesset", "kind": "knesset",
        "title": "הצעות חוק", "organization": "הכנסת", "source_type": "knesset",
        "source_url": "https://knesset.gov.il", "tags": [], "est_rows": 90_000,
        "columns": [{"name": "billid", "type": "int"}, {"name": "name", "type": "text"}],
    },
]


class _Req:
    """Minimal stand-in for a Starlette Request — base_url() reads headers only."""
    headers = {"host": "over.org.il", "x-forwarded-proto": "https"}

    class url:  # noqa: D106
        scheme = "https"
        netloc = "over.org.il"


def _call(tool, args=None):
    async def _go():
        return await S._IMPL[tool](_Req(), None, None, args or {})
    return asyncio.run(_go())


@pytest.fixture(autouse=True)
def _stub_catalog(monkeypatch):
    async def build_catalog(db, **kw):
        return CATALOG
    monkeypatch.setattr(C, "build_catalog", build_catalog)
    monkeypatch.setattr(A, "is_configured", lambda: True)


# ── tool registry / dispatch ────────────────────────────────────────────────

def test_every_declared_tool_is_implemented():
    declared = {t["name"] for t in S.TOOLS}
    assert declared == set(S._IMPL)


def test_tools_have_object_input_schemas():
    for t in S.TOOLS:
        assert t["inputSchema"]["type"] == "object"
        assert t["description"].strip()


def test_initialize_and_tools_list():
    async def _go():
        init = await S.handle_message(_Req(), None, None, None,
                                      {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                                       "params": {"protocolVersion": "2025-06-18"}})
        listed = await S.handle_message(_Req(), None, None, None,
                                        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
        note = await S.handle_message(_Req(), None, None, None,
                                      {"jsonrpc": "2.0", "method": "notifications/initialized"})
        return init, listed, note
    init, listed, note = asyncio.run(_go())
    assert init["result"]["serverInfo"]["name"] == "over-sql-mcp"
    assert "run_sql" in init["result"]["instructions"]
    assert {t["name"] for t in listed["result"]["tools"]} == set(S._IMPL)
    assert note is None  # notifications get no response


def test_unknown_method_is_rpc_error():
    resp = asyncio.run(S.handle_message(_Req(), None, None, None,
                                        {"jsonrpc": "2.0", "id": 9, "method": "nope"}))
    assert resp["error"]["code"] == -32601


# ── list_tables / list_schemas ──────────────────────────────────────────────

def test_list_tables_matches_on_column_name_not_just_title():
    # "mispar_rechev" appears in no title — only as a column.
    out, n = _call("list_tables", {"q": "mispar_rechev"})
    assert n == 1
    assert out["tables"][0]["table"] == "append_vehicles_1234"
    assert out["tables"][0]["matched_columns"] == ["mispar_rechev"]


def test_list_tables_matches_hebrew_column_alias():
    out, _ = _call("list_tables", {"q": "יצרן"})
    assert [t["table"] for t in out["tables"]] == ["append_vehicles_1234"]


def test_list_tables_filters_by_schema_and_orders_by_size():
    out, _ = _call("list_tables", {"schema": "knesset"})
    assert [t["table"] for t in out["tables"]] == ["kns_bill"]
    every, _ = _call("list_tables", {})
    assert [t["table"] for t in every["tables"]] == ["append_vehicles_1234", "kns_bill"]


def test_list_tables_omits_columns_unless_asked():
    out, _ = _call("list_tables", {"q": "רכבים"})
    assert "columns" not in out["tables"][0]
    out, _ = _call("list_tables", {"q": "רכבים", "include_columns": True})
    assert [c["name"] for c in out["tables"][0]["columns"]] == ["mispar_rechev", "tozeret_nm"]


def test_list_schemas_counts_tables_per_schema():
    out, _ = _call("list_schemas", {})
    by = {s["schema"]: s for s in out["schemas"]}
    assert by["public"]["tables"] == 1 and by["knesset"]["tables"] == 1
    assert out["search_path"] == C.CONSOLE_SEARCH_PATH


# ── the unknown-table gate ──────────────────────────────────────────────────

@pytest.mark.parametrize("tool", ["describe_schema", "get_table", "get_table_profile"])
def test_unknown_table_is_rejected_before_any_query(tool):
    with pytest.raises(ValueError) as e:
        _call(tool, {"table": "pg_shadow"})
    assert "לא נמצאה טבלה" in str(e.value)


def test_unknown_table_error_suggests_near_misses():
    with pytest.raises(ValueError) as e:
        _call("get_table", {"table": "vehicles"})
    assert "append_vehicles_1234" in str(e.value)


# ── describe_schema ─────────────────────────────────────────────────────────

def test_describe_schema_one_table_uses_that_tables_schema(monkeypatch):
    seen = {}

    async def schema_text(table, *, title=None, schema="public"):
        seen.update(table=table, schema=schema)
        return "CREATE TABLE ...;"
    monkeypatch.setattr(A, "schema_text", schema_text)
    out, _ = _call("describe_schema", {"table": "append_vehicles_1234"})
    assert seen == {"table": "append_vehicles_1234", "schema": "public"}
    assert out["search_path"] == C.CONSOLE_SEARCH_PATH


def test_describe_schema_rejects_unknown_schema():
    with pytest.raises(ValueError):
        _call("describe_schema", {"schema": "pg_catalog"})


def test_describe_schema_whole_catalog(monkeypatch):
    async def schema_text_all(db, *, schema=None):
        return f"-- ddl for {schema or 'all'}"
    monkeypatch.setattr(C, "schema_text_all", schema_text_all)
    out, _ = _call("describe_schema", {})
    assert out["schema"] == "all"


# ── run_sql ─────────────────────────────────────────────────────────────────

def test_run_sql_uses_readonly_path_with_console_search_path(monkeypatch):
    seen = {}

    async def run_readonly_sql(sql, *, table=None, search_path=None,
                               max_rows=1000, timeout_ms=10000):
        seen.update(sql=sql, search_path=search_path, max_rows=max_rows)
        return {"columns": ["n"], "rows": [{"n": 1}], "truncated": False, "row_count": 1}
    monkeypatch.setattr(A, "run_readonly_sql", run_readonly_sql)
    out, n = _call("run_sql", {"sql": "SELECT 1 AS n"})
    assert n == 1
    assert seen["search_path"] == C.CONSOLE_SEARCH_PATH
    assert seen["max_rows"] == 200  # default


def test_run_sql_max_rows_is_capped(monkeypatch):
    seen = {}

    async def run_readonly_sql(sql, **kw):
        seen.update(kw)
        return {"rows": [], "row_count": 0, "truncated": False}
    monkeypatch.setattr(A, "run_readonly_sql", run_readonly_sql)
    _call("run_sql", {"sql": "SELECT 1", "max_rows": 999_999})
    assert seen["max_rows"] == S.MAX_SQL_ROWS


def test_run_sql_flags_truncation(monkeypatch):
    async def run_readonly_sql(sql, **kw):
        return {"rows": [{"a": 1}], "row_count": 1, "truncated": True}
    monkeypatch.setattr(A, "run_readonly_sql", run_readonly_sql)
    out, _ = _call("run_sql", {"sql": "SELECT 1", "max_rows": 1})
    assert "נקטעה" in out["note"]


@pytest.mark.parametrize("sql", [
    "UPDATE append_vehicles_1234 SET x = 1",
    "SELECT 1; DROP TABLE kns_bill",
    "DELETE FROM kns_bill",
])
def test_run_sql_rejects_writes(sql):
    # The real validator runs — no monkeypatch — so a write never reaches the DB.
    with pytest.raises(ValueError):
        _call("run_sql", {"sql": sql})


# ── get_table ───────────────────────────────────────────────────────────────

def test_get_table_absolutizes_links(monkeypatch):
    async def table_detail(table, db):
        return {**CATALOG[0], "row_count": 4_100_000, "sample": [{"mispar_rechev": "1"}],
                "files": [{"name": "csv", "url": "/api/versions/abc/download/csv"}],
                "versions_url": "/versions/11111111-1111-1111-1111-111111111111",
                "field_flags": {"internal": True}}
    monkeypatch.setattr(C, "table_detail", table_detail)
    out, n = _call("get_table", {"table": "append_vehicles_1234"})
    assert n == 1
    assert out["files"][0]["url"].startswith("https://over.org.il/api/versions/")
    assert out["versions_url"].startswith("https://over.org.il/versions/")
    assert "field_flags" not in out


# ── get_table_profile ───────────────────────────────────────────────────────

def test_profile_missing_is_a_clear_answer_not_an_error(monkeypatch):
    from app.services import table_profiler

    async def get_profile(schema, table):
        return None
    monkeypatch.setattr(table_profiler, "get_profile", get_profile)
    out, n = _call("get_table_profile", {"table": "kns_bill"})
    assert out["profiled"] is False and n == 0


def test_profile_is_looked_up_in_the_tables_own_schema(monkeypatch):
    from app.services import table_profiler
    seen = {}

    async def get_profile(schema, table):
        seen.update(schema=schema, table=table)
        return {"table": table, "columns": []}
    monkeypatch.setattr(table_profiler, "get_profile", get_profile)
    _call("get_table_profile", {"table": "kns_bill"})
    assert seen == {"schema": "knesset", "table": "kns_bill"}
