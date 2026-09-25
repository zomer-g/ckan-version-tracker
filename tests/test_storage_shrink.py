"""Compact dedup hashes (uuid)."""
import asyncio
import uuid

from app.services import append_store, hash_compaction

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
