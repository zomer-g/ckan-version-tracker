"""Compact dedup hashes (uuid) and moving a dataset's rows to files."""
import asyncio
import uuid

from app.services import append_store, hash_compaction, sql_offload

SHA = "ab" * 32  # a 64-hex digest


def test_compact_hash_is_first_128_bits():
    assert append_store.compact_hash(SHA) == uuid.UUID(SHA[:32])


def test_build_insert_binds_text_or_uuid_by_table_type():
    rows = [{"a": "1"}]
    _, text_params = append_store.build_insert("t", ["a"], rows, key_col=None, keyless=True)
    _, uuid_params = append_store.build_insert("t", ["a"], rows, key_col=None, keyless=True,
                                               hash_uuid=True)
    assert isinstance(text_params[-1], str) and len(text_params[-1]) == 64
    assert uuid_params[-1] == uuid.UUID(text_params[-1][:32])


def test_same_row_same_identity_in_both_forms():
    # What the compaction's USING left(row_hash, 32)::uuid produces from an old
    # row must equal what the writer now binds for the same row.
    row = {"x": "7", "y": "שלום"}
    old = append_store.row_hash(row, ["x", "y"])
    assert uuid.UUID(old[:32]) == append_store.compact_hash(append_store.row_hash(row, ["x", "y"]))


def test_content_hash_expr_casts_only_when_asked():
    plain = append_store._content_hash_expr(["a"])
    assert plain.startswith("md5(concat_ws(") and not plain.endswith("::uuid")
    assert append_store._content_hash_expr(["a"], as_uuid=True) == plain + "::uuid"


class _Conn:
    def __init__(self, typ):
        self.typ, self.calls = typ, 0

    async def fetchval(self, *_a):
        self.calls += 1
        return self.typ


def test_hash_type_cache_keeps_only_uuid():
    t = f"t_{uuid.uuid4().hex[:6]}"
    text = _Conn("text")
    assert asyncio.run(append_store.hash_column_is_uuid(text, t)) is False
    assert asyncio.run(append_store.hash_column_is_uuid(text, t)) is False
    assert text.calls == 2  # text is re-asked every time: it may be converted
    u = _Conn("uuid")
    assert asyncio.run(append_store.hash_column_is_uuid(u, t)) is True
    assert asyncio.run(append_store.hash_column_is_uuid(u, t)) is True
    assert u.calls == 1


def test_alter_sql():
    assert hash_compaction.alter_sql("idx", 'we"ird', "_row_hash") == (
        'ALTER TABLE "idx"."we""ird" ALTER COLUMN "_row_hash" TYPE uuid '
        'USING left("_row_hash", 32)::uuid')


def test_export_query_drops_internal_columns():
    q = sql_offload.export_query("append_x", ["year", "pr", "first_seen", "row_hash", "geom"])
    assert q == 'SELECT "year", "pr", "first_seen" FROM public."append_x"'


def test_repoint_multi_resource_dict():
    m = {"_resource_ids": ["r1", "r2"], "_names": {"r1": "a", "r2": "b"},
         "_append_tables": {"r1": "append_a", "r2": "append_b"}}
    moved = {"append_a": {"value": "r2:k/a.csv", "resource_id": "r1", "name": "a (all)"}}
    out = sql_offload.repoint_mappings(m, moved)
    assert out["r1"] == "r2:k/a.csv" and out["_names"]["r1"] == "a (all)"
    assert out["_append_tables"] == {"r2": "append_b"}  # untouched table stays
    both = {**moved, "append_b": {"value": "r2:k/b.csv", "resource_id": "r2", "name": "b"}}
    out = sql_offload.repoint_mappings(m, both)
    assert "_append_tables" not in out and out["r2"] == "r2:k/b.csv"
    assert m["_append_tables"] == {"r1": "append_a", "r2": "append_b"}  # input not mutated


def test_repoint_single_table_and_list_shape():
    out = sql_offload.repoint_mappings(
        {"append_table": "append_s", "_resource_ids": ["rid"]},
        {"append_s": {"value": "r2:k/s.csv", "resource_id": "rid", "name": "s"}})
    assert "append_table" not in out and out["rid"] == "r2:k/s.csv"
    out = sql_offload.repoint_mappings(
        {"_append_tables": [{"resource": "docs", "table": "append_d"}]},
        {"append_d": {"value": "r2:k/d.csv", "resource_id": None, "name": "docs"}})
    assert "_append_tables" not in out
    rid = [k for k in out["_resource_ids"]][0]
    assert out[rid] == "r2:k/d.csv" and out["_names"][rid] == "docs"
