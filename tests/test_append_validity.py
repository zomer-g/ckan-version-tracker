"""Row validity in the append archive: ``last_seen`` and "the dataset as of X".

Phase 1 of docs/history-storage-strategy.md (plan: docs/history-phase1-plan.md,
section 4.2). Every append table gets ``last_seen timestamptz NULL``: NULL means
the row is still in force, a timestamp means a FULL pass of the source no longer
returned it. The dedup index becomes partial (``WHERE last_seen IS NULL``), so a
closed row that comes back identical opens a NEW validity interval instead of
being swallowed by the old one.

No live Postgres here (none on this machine, and the suite must not need one):
everything below is a pure SQL builder or a monkeypatched call.
"""
import asyncio
import inspect
import types
import uuid
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.api import append as api
from app.config import settings
from app.services import append_store


def _row(**kw):
    return dict(kw)


# ── 1. build_insert: the partial conflict clause ──────────────────────────────

def test_partial_open_keyless_do_nothing():
    sql, _ = append_store.build_insert(
        "t", ["a", "b"], [_row(a="1", b="2")], key_col=None, keyless=True,
        partial_open=True)
    assert sql.endswith('ON CONFLICT ("row_hash") WHERE "last_seen" IS NULL DO NOTHING')


def test_partial_open_keeps_the_stamp_refresh():
    sql, _ = append_store.build_insert(
        "t", ["a", "sampled"], [_row(a="1", sampled="2026-10-15")],
        key_col=None, keyless=True, stamp_col="sampled", partial_open=True)
    assert sql.endswith(
        'ON CONFLICT ("row_hash") WHERE "last_seen" IS NULL '
        'DO UPDATE SET "sampled" = EXCLUDED."sampled"')


@pytest.mark.parametrize("stamp", [None, "sampled"])
def test_without_partial_open_the_clause_is_unchanged(stamp):
    """Regression: a table whose index has not been migrated yet must get the
    exact statement it got before this phase, byte for byte."""
    rows = [_row(a="1", sampled="x"), _row(a="2", sampled="y")]
    before = append_store.build_insert(
        "t", ["a", "sampled"], rows, key_col=None, keyless=True, stamp_col=stamp)
    explicit = append_store.build_insert(
        "t", ["a", "sampled"], rows, key_col=None, keyless=True, stamp_col=stamp,
        partial_open=False)
    assert before == explicit
    tail = ('DO UPDATE SET "sampled" = EXCLUDED."sampled"' if stamp else "DO NOTHING")
    assert before[0].endswith(f'ON CONFLICT ("row_hash") {tail}')
    assert "last_seen" not in before[0]


# ── 2. keyed tables: the same, on the key ─────────────────────────────────────

def test_partial_open_on_a_keyed_table_targets_the_key():
    sql, _ = append_store.build_insert(
        "t", ["mispar", "b"], [_row(mispar="7", b="x")], key_col="mispar",
        keyless=False, partial_open=True)
    assert sql.endswith('ON CONFLICT ("mispar") WHERE "last_seen" IS NULL DO NOTHING')


def test_keyed_without_partial_is_unchanged():
    sql, _ = append_store.build_insert(
        "t", ["mispar", "b"], [_row(mispar="7", b="x")], key_col="mispar", keyless=False)
    assert sql.endswith('ON CONFLICT ("mispar") DO NOTHING')


def test_build_insert_reports_the_identities_it_used():
    """The pass tracker is fed from the same identities the INSERT used, so a
    row can never be counted as "seen" under a different hash than it was
    stored with. Duplicates inside a chunk are reported once."""
    out: list = []
    rows = [_row(a="1"), _row(a="1"), _row(a="2")]
    append_store.build_insert("t", ["a"], rows, key_col=None, keyless=True,
                              hash_uuid=True, idents_out=out)
    assert out == [append_store.compact_hash(append_store.row_hash(r, ["a"]))
                   for r in (rows[0], rows[2])]
    keyed: list = []
    append_store.build_insert("t", ["k"], [_row(k="9"), _row(k=None)], key_col="k",
                              keyless=False, idents_out=keyed)
    # A NULL key is stored as NULL and can never match anything; it is not an
    # identity a pass can vouch for.
    assert keyed == ["9"]


# ── 3. validity_where ─────────────────────────────────────────────────────────

def test_as_of_is_one_parameter_used_twice():
    ts = datetime(2026, 10, 1, tzinfo=timezone.utc)
    sql, params = append_store.validity_where(as_of=ts, start_param=3)
    assert sql == '"first_seen" <= $3 AND ("last_seen" IS NULL OR "last_seen" > $3)'
    assert params == [ts]


def test_current_is_the_open_rows():
    sql, params = append_store.validity_where(current=True)
    assert sql == '"last_seen" IS NULL'
    assert params == []


def test_no_validity_param_is_no_condition():
    assert append_store.validity_where() == ("", [])


def test_a_table_without_last_seen_treats_every_row_as_open():
    """A table the poll has not touched since the deploy has no last_seen yet.
    Every row in it is, by definition, still in force."""
    ts = datetime(2026, 10, 1, tzinfo=timezone.utc)
    assert append_store.validity_where(current=True, has_last_seen=False) == ("", [])
    assert append_store.validity_where(as_of=ts, has_last_seen=False) == (
        '"first_seen" <= $1', [ts])


def test_as_of_and_current_together_is_an_error():
    with pytest.raises(ValueError):
        append_store.validity_where(as_of=datetime.now(timezone.utc), current=True)
    with pytest.raises(HTTPException) as e:
        api._validity("2026-10-01", True)
    assert e.value.status_code == 400


# ── 4. parsing as_of ──────────────────────────────────────────────────────────

def test_a_bare_date_is_the_end_of_that_day_utc():
    assert append_store.parse_as_of("2026-10-01") == datetime(
        2026, 10, 1, 23, 59, 59, 999000, tzinfo=timezone.utc)


def test_a_timestamp_keeps_its_time_and_a_naive_one_is_utc():
    assert append_store.parse_as_of("2026-10-01T08:30:00+03:00") == datetime(
        2026, 10, 1, 5, 30, tzinfo=timezone.utc)
    assert append_store.parse_as_of("2026-10-01T08:30:00") == datetime(
        2026, 10, 1, 8, 30, tzinfo=timezone.utc)
    assert append_store.parse_as_of("2026-10-01T08:30:00Z") == datetime(
        2026, 10, 1, 8, 30, tzinfo=timezone.utc)


@pytest.mark.parametrize("bad", ["yesterday", "2026-13-01", "01/10/2026", "", "  "])
def test_an_invalid_as_of_is_a_400(bad):
    with pytest.raises(HTTPException) as e:
        api._validity(bad, False)
    assert e.value.status_code == 400


def test_the_api_helper_passes_through_the_valid_cases():
    assert api._validity(None, False) == (None, False)
    assert api._validity(None, True) == (None, True)
    ts, cur = api._validity("2026-10-01", False)
    assert ts.date().isoformat() == "2026-10-01" and cur is False


def test_validity_params_are_reserved_not_column_filters():
    for p in ("as_of", "current"):
        assert p in api._RESERVED


# ── 5. PassTracker.close_sql ──────────────────────────────────────────────────

def _tracker(**kw):
    base = dict(ident_col="row_hash", ident_type="uuid", token="abc123")
    base.update(kw)
    return append_store.PassTracker("append_x_1", **base)


def test_close_sql_is_one_batched_update_with_one_run_ts():
    sql = _tracker().close_sql(batch=50000)
    assert sql.startswith('UPDATE "append_x_1" SET "last_seen" = $1 WHERE ctid IN (')
    assert "LIMIT 50000" in sql
    # The same placeholder everywhere: one moment for the whole pass.
    assert "$2" not in sql
    assert '"last_seen" IS NULL' in sql


def test_close_sql_anti_joins_the_pass_table_not_a_value_list():
    """The pass identities live in a table, never inlined: a 6.4M-row pass as a
    literal list is not a statement Postgres would accept. NOT EXISTS rather
    than the plan's NOT IN: NOT IN over a subquery larger than work_mem
    degrades to a per-row rescan, and a single NULL identity would make it
    close nothing at all."""
    t = _tracker()
    sql = t.close_sql()
    assert 'NOT EXISTS (SELECT 1 FROM "_loader"."pass_abc123" p' in sql
    assert 'p."ident" = o."row_hash"' in sql
    assert "ANY(" not in sql and "IN (SELECT ident" not in sql
    assert t.pass_table == '"_loader"."pass_abc123"'


def test_the_pass_table_is_a_plain_table_typed_like_the_identity():
    # Plain, not UNLOGGED: xhostd's Postgres refuses UNLOGGED tables.
    ddl = " ".join(_tracker().create_sql())
    assert 'CREATE SCHEMA IF NOT EXISTS "_loader"' in ddl
    assert 'CREATE TABLE "_loader"."pass_abc123" ("ident" uuid)' in ddl
    assert "UNLOGGED" not in ddl
    keyed = " ".join(_tracker(ident_col="mispar", ident_type="text").create_sql())
    assert '("ident" text)' in keyed
    assert 'o."mispar"' in _tracker(ident_col="mispar", ident_type="text").close_sql()


def test_count_sql_measures_what_would_close():
    sql = _tracker().count_sql()
    assert 'FROM "append_x_1" o WHERE o."last_seen" IS NULL' in sql
    assert "NOT EXISTS" in sql


def test_a_hostile_token_cannot_reach_the_sql():
    with pytest.raises(ValueError):
        append_store.PassTracker("t", ident_col="row_hash", token='x"; DROP TABLE y; --')


def test_the_close_guard_refuses_a_mass_close():
    """Same spirit as the push shrink guard: a source that returned half of
    itself is far more likely broken than purged."""
    g = append_store.close_guard
    assert g(open_rows=1000, closing=400, max_fraction=0.5) is None
    assert g(open_rows=1000, closing=501, max_fraction=0.5) is not None
    assert g(open_rows=1000, closing=900, max_fraction=0.5, allow_shrink=True) is None
    assert g(open_rows=0, closing=0, max_fraction=0.5) is None


# ── 6. should_close: every arm ────────────────────────────────────────────────

def _ds(cfg=None):
    return types.SimpleNamespace(scraper_config=cfg)


@pytest.fixture
def closing_on(monkeypatch):
    monkeypatch.setattr(settings, "append_close_rows_enabled", True)


def test_the_global_flag_defaults_off():
    assert settings.append_close_rows_enabled is False
    assert settings.append_close_max_fraction == 0.5


def test_should_close_needs_the_global_flag(monkeypatch):
    monkeypatch.setattr(settings, "append_close_rows_enabled", False)
    assert append_store.should_close(_ds()) is False


def test_should_close_all_conditions_met(closing_on):
    assert append_store.should_close(_ds()) is True
    assert append_store.should_close(_ds({"append_closes_rows": True})) is True


def test_should_close_respects_the_dataset_opt_out(closing_on):
    assert append_store.should_close(_ds({"append_closes_rows": False})) is False


def test_should_close_not_on_an_incomplete_pass(closing_on):
    assert append_store.should_close(_ds(), completed=False) is False


def test_should_close_not_on_a_resumed_scan(closing_on):
    assert append_store.should_close(_ds(), start_offset=5000) is False


def test_should_close_not_on_a_partial_run(closing_on):
    assert append_store.should_close(_ds(), partial_run=True) is False


def test_should_close_not_on_an_incremental_archive(closing_on):
    """An archive-mode dataset pushes only what is NEW each run (JDA, ממ״מ):
    every row it did not resend would be closed."""
    assert append_store.should_close(_ds({"archive": True, "archive_type": "mmm"})) is False


# ── 7. the historical backfill never closes ───────────────────────────────────

def test_r2_backfill_never_opens_a_pass(monkeypatch):
    from app.services import r2_backfill

    calls: list[dict] = []

    async def fake_bytes(_self, _key):
        return "a,b\n1,2\n3,4\n".encode("utf-8")

    async def fake_ensure(*a, **kw):
        return None

    async def fake_append(table, cols, rows, **kw):
        calls.append(kw)
        return len(rows)

    async def boom(*a, **kw):
        raise AssertionError("r2_backfill must never begin a closing pass")

    # On the CLASS, as tests/test_symbology_download.py does: undoing a patch on
    # the instance leaves a bound method in its __dict__ that shadows every
    # later class-level patch.
    monkeypatch.setattr(type(r2_backfill.storage_client), "get_object_bytes", fake_bytes)
    monkeypatch.setattr(append_store, "ensure_table", fake_ensure)
    monkeypatch.setattr(append_store, "append_rows", fake_append)
    monkeypatch.setattr(append_store, "begin_pass", boom)
    seen, inserted, err = asyncio.run(r2_backfill._stream_r2_csv_to_neon(
        "append_t", "r2:x", first_seen=datetime(2025, 1, 1, tzinfo=timezone.utc)))
    assert (seen, inserted, err) == (2, 2, None)
    assert calls and all(kw.get("tracker") is None for kw in calls)
    assert "begin_pass" not in inspect.getsource(r2_backfill)


# ── 9. /schema advertises the validity columns ────────────────────────────────

def test_schema_publishes_last_seen_and_as_of(monkeypatch):
    ds = types.SimpleNamespace(
        id=uuid.uuid4(), title="t", scraper_config=None, ckan_name="t")

    async def fake_resolve(dataset_id, db, selector=None):
        return ds, "append_t", [{"table": "append_t", "resource_id": None,
                                 "resource_name": None}]

    async def fake_cols(_t):
        return ["a", "first_seen"]

    async def fake_count(_t):
        return 3

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    req = types.SimpleNamespace(query_params={})
    out = asyncio.run(fn("x", request=req, table=None, db=None))
    assert out["last_seen_column"] == "last_seen"
    assert out["supports_as_of"] is True
    # Nothing that was there before is gone.
    for k in ("first_seen_column", "total", "columns", "item_key", "supports_latest"):
        assert k in out


# ── the read path: identical SQL when no validity param is given ──────────────

class _Conn:
    def __init__(self, cols):
        self.cols = cols
        self.sql: list[str] = []
        self.params: list = []

    async def fetch(self, sql, *params):
        self.sql.append(sql)
        self.params.append(params)
        if "information_schema.columns" in sql:
            return [{"column_name": c} for c in self.cols]
        return []

    async def fetchval(self, sql, *params):
        self.sql.append(sql)
        self.params.append(params)
        return 0


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *exc):
                return False
        return _Ctx()


def _run_query(monkeypatch, cols, **kw):
    conn = _Conn(cols)

    async def get_pool():
        return _Pool(conn)

    monkeypatch.setattr(append_store, "get_pool", get_pool)
    out = asyncio.run(append_store.query("append_t", **kw))
    return out, conn


_COLS = ["a", "first_seen", "row_hash", "last_seen"]


def test_query_without_validity_is_the_old_statement(monkeypatch):
    out, conn = _run_query(monkeypatch, _COLS, q="x")
    assert out["columns"] == ["a", "first_seen"], "last_seen stays out of the default view"
    count_sql = conn.sql[1]
    assert count_sql == (
        'SELECT count(*) FROM "append_t" WHERE '
        '("a"::text ILIKE $1 OR "first_seen"::text ILIKE $1)')


def test_query_current_filters_inside_the_source(monkeypatch):
    out, conn = _run_query(monkeypatch, _COLS, q="x", current=True)
    assert out["columns"] == ["a", "first_seen", "last_seen"]
    count_sql = conn.sql[1]
    assert '(SELECT * FROM "append_t" WHERE "last_seen" IS NULL) AS "append_t"' in count_sql
    assert "ILIKE $1" in count_sql


def test_query_as_of_numbers_its_params_first(monkeypatch):
    ts = datetime(2026, 10, 1, tzinfo=timezone.utc)
    _out, conn = _run_query(monkeypatch, _COLS, q="x", as_of=ts)
    count_sql = conn.sql[1]
    assert '"first_seen" <= $1 AND ("last_seen" IS NULL OR "last_seen" > $1)' in count_sql
    assert "ILIKE $2" in count_sql
    assert conn.params[1] == (ts, "%x%")


def test_the_worker_cache_csv_never_carries_last_seen(monkeypatch):
    """iter_csv_open rebuilds a worker's cache of what it published, and the
    worker hashes those columns. A new column there would give every row a new
    identity and double the table on the next run."""
    conn = _Conn(_COLS)

    async def get_pool():
        return _Pool(conn)

    monkeypatch.setattr(append_store, "get_pool", get_pool)

    async def head():
        gen = append_store.iter_csv_open("append_t")
        return await gen.__anext__()

    first = asyncio.run(head()).decode("utf-8-sig")
    assert first.strip() == "a,first_seen"


# ── 10. the index migration plan ──────────────────────────────────────────────

def _ix(table, name, column, partial=False, unique=True):
    where = " WHERE (last_seen IS NULL)" if partial else ""
    uq = "UNIQUE " if unique else ""
    return {"table": table, "indexname": name,
            "indexdef": f"CREATE {uq}INDEX {name} ON public.{table} USING btree ({column}){where}",
            "bytes": 100}


def test_migration_plan_lists_tables_with_a_full_unique_index():
    from app.services import index_migration
    rows = [
        _ix("append_a", "append_a_uq", "row_hash"),
        _ix("append_b", "append_b_hash_uq", "row_hash"),
        _ix("append_c", "append_c_uq", '"mispar rechev"'),
    ]
    got = index_migration.plan(rows)
    assert [(p["table"], p["column"], p["old_index"]) for p in got] == [
        ("append_a", "row_hash", "append_a_uq"),
        ("append_b", "row_hash", "append_b_hash_uq"),
        ("append_c", "mispar rechev", "append_c_uq"),
    ]


def test_migration_plan_is_idempotent():
    from app.services import index_migration
    rows = [
        _ix("append_a", "append_a_open_uq", "row_hash", partial=True),
        # converted, but the old index not dropped yet (a stop between the two
        # steps): still listed, so the next run finishes the drop
        _ix("append_b", "append_b_open_uq", "row_hash", partial=True),
        _ix("append_b", "append_b_uq", "row_hash"),
        # an ordinary non-unique index is never a candidate
        _ix("append_d", "append_d_geom_gix", "geom", unique=False),
    ]
    got = index_migration.plan(rows)
    assert [(p["table"], p["partial_exists"]) for p in got] == [("append_b", True)]
    # a table that is fully converted does not appear
    assert index_migration.plan([rows[0]]) == []


def test_the_migration_builds_concurrently_and_then_drops():
    from app.services import index_migration
    create = index_migration.create_sql("append_a", "append_a_open_uq", "row_hash")
    assert create == ('CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "append_a_open_uq" '
                      'ON "public"."append_a" ("row_hash") WHERE "last_seen" IS NULL')
    assert index_migration.drop_sql("append_a_uq") == (
        'DROP INDEX CONCURRENTLY IF EXISTS "public"."append_a_uq"')


def test_classify_reads_the_predicate_not_the_name():
    """A long table name gets a hashed index name (append_store._index_name),
    so "ends in open_uq" is not a reliable test; the predicate is."""
    got = append_store.classify_unique_indexes([
        _ix("t", "t_xxxxxxxx", "row_hash", partial=True),
        _ix("t", "t_uq", "row_hash"),
    ])
    assert [(g["name"], g["column"], g["partial"]) for g in got] == [
        ("t_xxxxxxxx", "row_hash", True), ("t_uq", "row_hash", False)]


# ── the loaders: a full pass closes, anything else does not ───────────────────

class _FakeTracker:
    def __init__(self, table):
        self.table = table
        self.added: list = []
        self.closed_at = None
        self.abandoned = False

    async def close_missing(self, run_ts, **kw):
        self.closed_at = run_ts
        return {"closed": 1, "open_rows": 3}

    async def abandon(self):
        self.abandoned = True


def _multi_store(monkeypatch, *, fail_rid=None):
    from app.services import delta_archiver as DA

    rid_a, rid_b = "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"
    calls = {"append_kw": [], "trackers": {}}
    monkeypatch.setattr(append_store, "is_configured", lambda: True)

    async def ensure_table(*a, **kw):
        return None

    async def append_rows(table, cols, rows, **kw):
        calls["append_kw"].append(kw)
        return len(rows)

    async def table_count(_t):
        return 3

    async def begin_pass(table, *, ident_col="row_hash"):
        tr = _FakeTracker(table)
        calls["trackers"][table] = tr
        return tr

    async def stream(rid, start_offset=0):
        yield 2, [{"line": rid[:1]}]
        if rid == fail_rid:
            raise RuntimeError("datastore blew up")

    monkeypatch.setattr(append_store, "ensure_table", ensure_table)
    monkeypatch.setattr(append_store, "append_rows", append_rows)
    monkeypatch.setattr(append_store, "table_count", table_count)
    monkeypatch.setattr(append_store, "begin_pass", begin_pass)
    monkeypatch.setattr(DA, "_stream_datastore_pages", stream)
    fields = [{"id": "line", "type": "text"}]
    info = [({"id": r, "name": r[:1]}, {"fields": fields}) for r in (rid_a, rid_b)]
    return DA, info, calls, (rid_a, rid_b)


class _DB:
    def __init__(self):
        self.added = []

    async def commit(self):
        pass

    def add(self, o):
        self.added.append(o)


def _multi_ds(cfg=None):
    return types.SimpleNamespace(id=uuid.UUID("4ab75f12-01a0-4f1a-b980-41572b6776bd"),
                                 ckan_name="multi", scraper_config=cfg,
                                 last_polled_at=None, last_modified=None, last_error=None)


def test_closing_off_passes_no_tracker_and_records_nothing_new(monkeypatch):
    monkeypatch.setattr(settings, "append_close_rows_enabled", False)
    DA, info, calls, _ = _multi_store(monkeypatch)
    db = _DB()
    assert asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=_multi_ds(), resources_info=info, next_version=1, new_modified="m", db=db))
    assert calls["trackers"] == {}
    assert all("tracker" not in kw for kw in calls["append_kw"])
    assert "closed" not in db.added[0].change_summary


def test_a_full_multi_pass_closes_each_table_with_one_moment(closing_on, monkeypatch):
    DA, info, calls, _ = _multi_store(monkeypatch)
    db = _DB()
    assert asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=_multi_ds(), resources_info=info, next_version=1, new_modified="m", db=db))
    trs = list(calls["trackers"].values())
    assert len(trs) == 2 and all(t.closed_at is not None for t in trs)
    assert trs[0].closed_at == trs[1].closed_at, "one run_ts for the whole poll"
    assert all(kw.get("tracker") is not None for kw in calls["append_kw"])
    assert len(db.added[0].change_summary["closed"]) == 2


def test_an_aborted_pass_closes_nothing(closing_on, monkeypatch):
    DA, info, calls, (rid_a, _b) = _multi_store(monkeypatch, fail_rid="aaaaaaaa-0000-0000-0000-000000000001")
    db = _DB()
    assert not asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=_multi_ds(), resources_info=info, next_version=1, new_modified="m", db=db))
    (tr,) = calls["trackers"].values()
    assert tr.abandoned and tr.closed_at is None


def test_a_resumed_resource_does_not_open_a_pass(closing_on, monkeypatch):
    DA, info, calls, (rid_a, rid_b) = _multi_store(monkeypatch)
    ds = _multi_ds({"neon_multi": {"done": [], "cur": rid_a, "offset": 1}})
    asyncio.run(DA.archive_multi_via_datastore_streaming(
        ds=ds, resources_info=info, next_version=1, new_modified="m", db=_DB()))
    tables = set(calls["trackers"])
    assert append_store.table_name_for_resource(ds, rid_a) not in tables
    assert append_store.table_name_for_resource(ds, rid_b) in tables


def test_the_stream_loader_closes_only_after_the_whole_file(closing_on, monkeypatch, tmp_path):
    from app.api import worker

    trackers: list = []

    async def ensure_table(*a, **kw):
        return None

    async def append_rows(table, cols, rows, **kw):
        assert kw.get("tracker") is trackers[0]
        return len(rows)

    async def begin_pass(table, *, ident_col="row_hash"):
        trackers.append(_FakeTracker(table))
        return trackers[-1]

    async def fill_geometry(*a, **kw):
        return {}

    monkeypatch.setattr(append_store, "ensure_table", ensure_table)
    monkeypatch.setattr(append_store, "append_rows", append_rows)
    monkeypatch.setattr(append_store, "begin_pass", begin_pass)
    monkeypatch.setattr(append_store, "fill_geometry", fill_geometry)
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    ts = datetime(2026, 10, 15, tzinfo=timezone.utc)
    n = asyncio.run(worker._neon_stream_load_file(
        "append_t", str(p), close={"run_ts": ts, "allow_shrink": False}))
    assert n == 2 and trackers[0].closed_at == ts and not trackers[0].abandoned
    # without `close` no pass is opened at all
    trackers.clear()

    async def append_plain(table, cols, rows, **kw):
        assert "tracker" not in kw
        return len(rows)

    monkeypatch.setattr(append_store, "append_rows", append_plain)
    asyncio.run(worker._neon_stream_load_file("append_t", str(p)))
    assert trackers == []


def test_push_version_closes_only_full_single_batch_runs():
    """The worker path's gate, read off the source: a multi-batch run (any
    batch, including the last) and a skipped version never close."""
    from app.api import worker
    src = inspect.getsource(worker.push_version)
    gate = src.split("_close_ok = _archive_neon and append_store.should_close(")[1][:300]
    for flag in ("body.partial_run", "body.more_batches", "body.consolidate_dedup_key",
                 "body.skip_version"):
        assert flag in gate
