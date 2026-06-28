"""Unit tests for the append-store pure helpers (no DB needed)."""
import os
import sys
import types

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.config import settings  # noqa: E402
from app.services import append_store as A  # noqa: E402


def test_dsn_strips_libpq_params_and_dialect_suffix():
    old = settings.append_database_url
    try:
        settings.append_database_url = (
            "postgresql+asyncpg://u:pw@ep-x.us-east-1.aws.neon.tech/neondb"
            "?sslmode=require&channel_binding=require"
        )
        dsn = A._dsn()
        assert dsn.startswith("postgresql://")          # dialect suffix dropped
        assert "sslmode" not in dsn                       # libpq-only param dropped
        assert "channel_binding" not in dsn
        assert "ep-x.us-east-1.aws.neon.tech/neondb" in dsn
        assert "u:pw@" in dsn
    finally:
        settings.append_database_url = old


def test_is_configured_reflects_setting():
    old = settings.append_database_url
    try:
        settings.append_database_url = ""
        assert A.is_configured() is False
        settings.append_database_url = "postgresql://x/y"
        assert A.is_configured() is True
    finally:
        settings.append_database_url = old


def _ds(ckan_name, id_):
    return types.SimpleNamespace(ckan_name=ckan_name, id=id_)


def test_table_name_is_stable_unique_and_in_limit():
    ds = _ds("private-and-commercial-vehicles", "e437ab0b-c247-4d35-b2c4-79c2d19dbabd")
    t = A.table_name(ds)
    assert t == A.table_name(ds)                # stable
    assert t.startswith("append_private_and_commercial_vehicles")
    assert t.endswith("_e437ab0b")              # id suffix for uniqueness
    assert len(t) <= 63                          # Postgres identifier limit
    # Distinct datasets that share a ckan_name don't collide (id suffix differs).
    assert A.table_name(ds) != A.table_name(_ds("private-and-commercial-vehicles", "ffffffff-0000-0000-0000-000000000000"))


def test_row_hash_is_order_independent_and_none_safe():
    a = A.row_hash({"b": "2", "a": "1", "c": None}, ["a", "b", "c"])
    b = A.row_hash({"a": "1", "b": "2", "c": None}, ["a", "b", "c"])
    assert a == b                                # key order doesn't matter
    # None and "" collapse to the same identity (matches version_detector).
    assert A.row_hash({"a": None}, ["a"]) == A.row_hash({"a": ""}, ["a"])


def test_build_insert_keyless_dedups_chunk_and_targets_row_hash():
    cols = ["CHOPER", "CHFLTN"]
    chunk = [
        {"CHOPER": "LY", "CHFLTN": "1"},
        {"CHOPER": "LY", "CHFLTN": "1"},   # dup within chunk → collapses
        {"CHOPER": "LY", "CHFLTN": "2"},
    ]
    sql, params = A.build_insert("t", cols, chunk, key_col=None, keyless=True)
    assert "ON CONFLICT (\"row_hash\")" in sql
    assert "now()" in sql
    assert sql.count("),(") == 1               # 2 unique rows → 2 value tuples
    # 2 rows × (2 cols + 1 row_hash) params = 6
    assert len(params) == 6


def test_build_insert_keyed_targets_key_column():
    cols = ["mispar_rechev", "baalut"]
    chunk = [
        {"mispar_rechev": "1", "baalut": "פרטי"},
        {"mispar_rechev": "1", "baalut": "מסחרי"},  # same key → collapses
        {"mispar_rechev": "2", "baalut": "פרטי"},
    ]
    sql, params = A.build_insert("t", cols, chunk, key_col="mispar_rechev", keyless=False)
    assert 'ON CONFLICT ("mispar_rechev")' in sql
    assert sql.count("),(") == 1               # 2 unique keys
    assert len(params) == 4                      # 2 rows × 2 cols, no row_hash


def test_build_insert_empty_chunk():
    sql, params = A.build_insert("t", ["a"], [], key_col=None, keyless=True)
    assert sql == "" and params == []


def test_content_hash_expr_is_deterministic_sql_over_all_cols():
    cols = ["mispar_rechev", "baalut"]
    no_alias = A._content_hash_expr(cols)
    aliased = A._content_hash_expr(cols, alias="s")
    # md5 over coalesced columns in order, separated by a control char.
    assert no_alias.startswith("md5(concat_ws(chr(31),")
    assert 'coalesce("mispar_rechev"::text' in no_alias
    assert 'coalesce("baalut"::text' in no_alias
    # The aliased form (staging diff) references the alias; the bare form
    # (backfill of existing rows) doesn't — but both hash the same columns in
    # the same order, so an unchanged row hashes identically either way.
    assert 'coalesce(s."mispar_rechev"::text' in aliased
    assert no_alias == aliased.replace("s.", "")


def test_chunk_size_keeps_params_under_ceiling():
    # 24-col keyed vehicle rows
    n = A.chunk_size_for(24, keyless=False)
    assert n * 24 <= 30000
    # 18-col keyless flights rows (+1 hash param/row)
    n2 = A.chunk_size_for(18, keyless=True)
    assert n2 * (18 + 1) <= 30000
