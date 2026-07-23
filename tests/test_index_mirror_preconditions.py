"""Stage-0 preconditions for the index → NEON mirror
(docs/neon-index-pilot/README.md §10.2).

These guard the two failures that actually killed loads during the pilot, plus
the two operational backstops the rollout depends on.
"""
import asyncio

import pytest

from app.services import append_store, data_catalog, index_mirror


# ── 10.2.1 — identifiers are safe at 63 BYTES, not 63 characters ─────────────

def test_clip_ident_bytes_never_splits_a_character():
    # 40 Hebrew letters = 80 bytes; the clip must land on a character boundary.
    name = "א" * 40
    out = append_store.clip_ident_bytes(name)
    assert len(out.encode("utf-8")) <= 63
    assert out == "א" * 31          # 62 bytes — 32 letters would be 64
    out.encode("utf-8").decode("utf-8")  # still valid UTF-8


def test_clip_ident_bytes_passes_short_names_through():
    assert append_store.clip_ident_bytes("objectId") == "objectId"
    assert append_store.clip_ident_bytes("נפה") == "נפה"


def test_long_hebrew_headers_sharing_a_prefix_stay_distinct():
    """The exact failure from the pilot: two real column names from the 2008
    census GovMap layer. Both are >63 bytes and share a long prefix, so a
    character-based clip (or none at all — Postgres clips server-side) collapses
    them into one identifier and CREATE TABLE dies with DuplicateColumnError."""
    a = "אחוז בני 15 ומעלה שעבדו בשנת 2008 בעלי השכלה על תיכונית"
    b = "אחוז בני 15 ומעלה שעבדו בשנת 2008 בעלי תואר אקדמי"
    assert len(a.encode("utf-8")) > 63 and len(b.encode("utf-8")) > 63
    # Precondition for the test to be meaningful: they DO collide when clipped.
    assert append_store.clip_ident_bytes(a) == append_store.clip_ident_bytes(b)

    out = append_store.safe_column_names([a, b])
    assert len(set(out)) == 2, "colliding headers must be disambiguated"
    for name in out:
        assert len(name.encode("utf-8")) <= 63


def test_safe_column_names_keeps_order_and_fills_blanks():
    out = append_store.safe_column_names(["objectId", "", None, "נפה"])
    assert out[0] == "objectId" and out[3] == "נפה"
    assert out[1] == "col_2" and out[2] == "col_3"
    assert len(out) == 4


def test_safe_column_names_dedups_plain_duplicates():
    out = append_store.safe_column_names(["a", "a", "A"])
    assert len(set(n.lower() for n in out)) == 3


def test_blank_headers_stay_dropped_in_the_existing_neon_path():
    """Regression guard for the wiring in worker._neon_stream_load_r2: blank
    headers were dropped before the 63-byte fix, and must stay dropped.
    Materialising them as col_N would insert into a column that tables created
    earlier do not have."""
    header = ["objectId", "", "נפה", None]
    safe = append_store.safe_column_names(header)
    keep = [i for i, raw in enumerate(header)
            if (raw or "").strip() and safe[i] != "_id"]
    assert [safe[i] for i in keep] == ["objectId", "נפה"]


def test_safe_column_names_suffix_fits_in_the_byte_budget():
    """A disambiguating suffix must not push the name back over 63 bytes."""
    headers = ["א" * 40] * 12
    out = append_store.safe_column_names(headers)
    assert len(set(out)) == 12
    for name in out:
        assert len(name.encode("utf-8")) <= 63


# ── 10.2.2 — build_catalog is cached ─────────────────────────────────────────

def test_catalog_cache_serves_the_second_call_without_rebuilding(monkeypatch):
    calls = {"n": 0}

    async def fake_build(db):
        calls["n"] += 1
        return [{"table": "t1"}]

    monkeypatch.setattr(data_catalog, "_build_catalog_uncached", fake_build)
    data_catalog.invalidate_catalog_cache()

    async def go():
        first = await data_catalog.build_catalog(db=None)
        second = await data_catalog.build_catalog(db=None)
        return first, second

    first, second = asyncio.run(go())
    assert calls["n"] == 1, "second call must be served from cache"
    assert first == second == [{"table": "t1"}]


def test_invalidate_forces_a_rebuild(monkeypatch):
    calls = {"n": 0}

    async def fake_build(db):
        calls["n"] += 1
        return [{"table": f"t{calls['n']}"}]

    monkeypatch.setattr(data_catalog, "_build_catalog_uncached", fake_build)
    data_catalog.invalidate_catalog_cache()

    async def go():
        await data_catalog.build_catalog(db=None)
        data_catalog.invalidate_catalog_cache()
        return await data_catalog.build_catalog(db=None)

    out = asyncio.run(go())
    assert calls["n"] == 2
    assert out == [{"table": "t2"}]


def test_use_cache_false_always_rebuilds(monkeypatch):
    calls = {"n": 0}

    async def fake_build(db):
        calls["n"] += 1
        return []

    monkeypatch.setattr(data_catalog, "_build_catalog_uncached", fake_build)
    data_catalog.invalidate_catalog_cache()

    async def go():
        await data_catalog.build_catalog(db=None)
        await data_catalog.build_catalog(db=None, use_cache=False)

    asyncio.run(go())
    assert calls["n"] == 2


def test_concurrent_cold_reads_rebuild_only_once(monkeypatch):
    """A cold cache under concurrent /data loads must not fan out into N full
    rebuilds — that is exactly the ~3s × N stampede the cache exists to stop."""
    calls = {"n": 0}

    async def fake_build(db):
        calls["n"] += 1
        await asyncio.sleep(0.02)
        return [{"table": "t"}]

    monkeypatch.setattr(data_catalog, "_build_catalog_uncached", fake_build)
    data_catalog.invalidate_catalog_cache()

    async def go():
        await asyncio.gather(*(data_catalog.build_catalog(db=None) for _ in range(8)))

    asyncio.run(go())
    assert calls["n"] == 1


# ── 10.2.3 — read-only console pool carries a timeout backstop ────────────────

def test_readonly_pool_declares_a_statement_timeout(monkeypatch):
    captured = {}

    class _FakePool:
        pass

    async def fake_create_pool(**kw):
        captured.update(kw)
        return _FakePool()

    monkeypatch.setattr(append_store.settings, "append_readonly_database_url",
                        "postgresql://ro:pw@ep-x.aws.neon.tech/neondb")
    monkeypatch.setattr(append_store.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(append_store, "_ro_pool", None)

    asyncio.run(append_store.get_readonly_pool())
    assert captured["server_settings"]["statement_timeout"] == "30s"
    monkeypatch.setattr(append_store, "_ro_pool", None)


def test_readwrite_pool_has_no_statement_timeout(monkeypatch):
    """The backfill's COPY legitimately runs for minutes — capping the RW pool
    would break it."""
    captured = {}

    async def fake_create_pool(**kw):
        captured.update(kw)
        return object()

    monkeypatch.setattr(append_store.settings, "append_database_url",
                        "postgresql://u:pw@ep-x.aws.neon.tech/neondb")
    monkeypatch.setattr(append_store.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(append_store, "_pool", None)

    asyncio.run(append_store.get_pool())
    assert "server_settings" not in captured
    monkeypatch.setattr(append_store, "_pool", None)


# ── 10.2.4 — the loader is streaming / memory-bounded ────────────────────────

def test_csv_field_limit_covers_the_34mb_single_cell_dataset():
    import csv as _csv
    assert _csv.field_size_limit() >= 10**8


def test_iter_batches_is_bounded_and_pads_ragged_rows(tmp_path):
    src = tmp_path / "idx.csv"
    rows = 45_000
    with open(src, "w", encoding="utf-8", newline="") as fh:
        w = _csv_writer(fh)
        w.writerow(["a", "b", "c"])
        for i in range(rows):
            w.writerow([i, "x"] if i % 1000 == 0 else [i, "x", "y"])  # ragged

    batches = list(index_mirror._iter_batches(str(src), ["a", "b", "c"], [0, 1, 2]))
    assert sum(len(b) for b in batches) == rows
    assert max(len(b) for b in batches) == index_mirror.COPY_BATCH_ROWS
    for b in batches:
        for rec in b:
            assert len(rec) == 3          # short rows padded, never rejected
    # A short row's missing trailing cell becomes None.
    assert batches[0][0][2] is None


def _csv_writer(fh):
    import csv as _csv
    return _csv.writer(fh)


def test_iter_batches_flushes_on_bytes_for_wide_rows(tmp_path):
    """The real memory bound. The biggest layer in the corpus averages ~11 KB
    per row, so a row-count-only limit would put ~220 MB in flight per batch on
    a 512 MB dyno. Wide rows must flush on the byte budget instead."""
    src = tmp_path / "wide.csv"
    cell = "P" * 200_000                      # 200 KB per row
    n = 400                                   # 80 MB total — spans >1 batch
    with open(src, "w", encoding="utf-8", newline="") as fh:
        w = _csv_writer(fh)
        w.writerow(["geometry_wkt"])
        for _ in range(n):
            w.writerow([cell])

    batches = list(index_mirror._iter_batches(str(src), ["geometry_wkt"], [0]))
    assert sum(len(b) for b in batches) == n
    assert len(batches) > 1, "byte budget must force more than one flush"
    for b in batches:
        assert len(b) < index_mirror.COPY_BATCH_ROWS   # rows never hit the cap
        payload = sum(len(v) for rec in b for v in rec if v is not None)
        # One row may straddle the limit, so allow a single row of overshoot.
        assert payload <= index_mirror.COPY_BATCH_BYTES + len(cell)


def test_staging_name_stays_within_the_identifier_budget():
    long_table = "a" * 63
    stg = index_mirror._staging_name(long_table)
    assert len(stg.encode("utf-8")) <= 63
    assert stg != long_table and stg.endswith("__stg")


def test_table_name_is_ascii_unique_and_within_limit():
    class _DS:
        ckan_name = "מאגר-בעברית עם רווחים"
        id = "304e43d5-c419-43bd-8b46-f31a4da0c075"

    name = index_mirror.table_name(_DS())
    assert name.isascii() and len(name.encode("utf-8")) <= 63
    assert name.endswith("304e43d5")

    class _Other(_DS):
        id = "82db2f91-9ff0-44f4-b842-df3c43f7185a"

    assert index_mirror.table_name(_Other()) != name


@pytest.mark.parametrize("bad", ["", None])
def test_load_index_csv_requires_configuration(monkeypatch, bad):
    monkeypatch.setattr(append_store.settings, "append_database_url", bad)
    with pytest.raises(RuntimeError, match="append DB is not configured"):
        asyncio.run(index_mirror.load_index_csv("r2:some/key", "t"))


# ── copy-to-AI schema covers the WHOLE catalog, not just knesset ─────────────

def _fake_catalog(monkeypatch, recs):
    async def fake(db, use_cache=True):
        return recs
    monkeypatch.setattr(data_catalog, "build_catalog", fake)


def test_schema_text_covers_every_schema(monkeypatch):
    """The button is labelled 'copy schema to AI'; if it emits only one schema,
    an assistant writes confident SQL against tables it was never shown."""
    _fake_catalog(monkeypatch, [
        {"schema": "public", "table": "append_x", "title": "מאגר",
         "columns": [{"name": "a", "type": "text"}]},
        {"schema": "knesset", "table": "kns_bill", "title": "הצעות חוק",
         "columns": [{"name": "id", "type": "integer"}]},
        {"schema": "idx", "table": "govmap_1_a", "title": "שכבה",
         "columns": [{"name": "objectId", "type": "text"},
                     {"name": "נפה", "type": "text"}]},
    ])
    out = asyncio.run(data_catalog.schema_text_all(db=None))
    assert "public.append_x" in out
    assert "knesset.kns_bill" in out
    assert "idx." in out and "govmap_1_a" in out
    # identifiers needing quotes are shown quoted, ready to paste
    assert '"objectId"' in out and '"נפה"' in out
    # bare lowercase names stay bare
    assert "(a text)" in out
    assert data_catalog.CONSOLE_SEARCH_PATH in out


def test_schema_text_can_be_narrowed_to_one_schema(monkeypatch):
    _fake_catalog(monkeypatch, [
        {"schema": "public", "table": "append_x", "title": "",
         "columns": [{"name": "a", "type": "text"}]},
        {"schema": "idx", "table": "govmap_1_a", "title": "",
         "columns": [{"name": "b", "type": "text"}]},
    ])
    out = asyncio.run(data_catalog.schema_text_all(db=None, schema="idx"))
    assert "govmap_1_a" in out and "append_x" not in out


def test_schema_text_is_one_line_per_table(monkeypatch):
    """Compact by design — the catalog is hundreds of tables and grows with
    every mirrored collection."""
    _fake_catalog(monkeypatch, [
        {"schema": "idx", "table": f"t{i}", "title": "",
         "columns": [{"name": "a", "type": "text"}, {"name": "b", "type": "text"}]}
        for i in range(20)
    ])
    out = asyncio.run(data_catalog.schema_text_all(db=None))
    assert out.count("CREATE TABLE") == 20
    assert len([l for l in out.splitlines() if l.startswith("CREATE TABLE")]) == 20


def test_schema_text_skips_tables_without_columns(monkeypatch):
    _fake_catalog(monkeypatch, [
        {"schema": "idx", "table": "empty", "title": "", "columns": []},
        {"schema": "idx", "table": "ok", "title": "",
         "columns": [{"name": "a", "type": "text"}]},
    ])
    out = asyncio.run(data_catalog.schema_text_all(db=None))
    assert "idx.ok" in out and "idx.empty" not in out
