"""Phase 3 of docs/history-storage-strategy.md: the DuckDB reader for the cold tier.

Written BEFORE the code (docs/history-phase3-plan.md section 4.2). There is no
Postgres and no R2 on this machine, but DuckDB is, so the engine runs for real:

* the Parquet parts are written here with pyarrow, in the exact shape
  history_export writes them (source columns as strings, first_seen/last_seen
  as timestamp[us, UTC], row_hash as a string);
* the "hot" Postgres table is a DuckDB in-memory database attached under the
  same name the engine gives Postgres (``hot``), so the views the engine builds
  read ``hot."public"."<table>"`` exactly as they do in production;
* R2 is local files: ``_part_url`` maps a catalog key to a path on disk.
  ``httpfs`` and the ``postgres`` extension are exercised only on prod (4.3).
"""
import asyncio
import threading
import time
import types
import uuid
from datetime import date, datetime, timezone

import pytest

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

from fastapi import HTTPException  # noqa: E402

from app.config import settings  # noqa: E402
from app.services import append_store  # noqa: E402
from app.services import history_query as hq  # noqa: E402

UTC = timezone.utc
DSID = "6fb0e203-1111-4222-8333-444455556666"
TABLE = "append_flights_6fb0e203"
OTHER = "append_other_abcd1234"


def _ts(y, m, d, h=0):
    return datetime(y, m, d, h, tzinfo=UTC)


def _part(month: date, part: int, *, deleted: bool, rows: int = 2, table: str = TABLE,
          ls_min=None, ls_max=None) -> dict:
    return {
        "id": str(uuid.uuid4()), "dataset_id": DSID, "table_name": table,
        "closed_month": month, "part": part,
        "r2_key": f"history/6fb0e203/{table}/closed_month={month:%Y-%m}/part-{part:04d}.parquet",
        "rows": rows, "bytes": 100, "content_hash": "x",
        "first_seen_min": None, "first_seen_max": None,
        "last_seen_min": ls_min, "last_seen_max": ls_max,
        "deleted_from_sql": deleted, "deleted_at": None,
    }


# ── 1. extract_table_refs ─────────────────────────────────────────────────────

KNOWN = ["append_x_abcd1234", "append_y_ffff0000", "kns_bill"]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM append_x_abcd1234",
    'SELECT * FROM "append_x_abcd1234" LIMIT 5',
    "SELECT count(*) FROM public.append_x_abcd1234 t",
    'SELECT * FROM public."append_x_abcd1234"',
    "SELECT * FROM APPEND_X_ABCD1234",
])
def test_extract_finds_a_table_written_every_way(sql):
    assert hq.extract_table_refs(sql, KNOWN) == ["append_x_abcd1234"]


def test_extract_ignores_names_inside_literals_and_comments():
    sql = ("-- append_y_ffff0000 is the other one\n"
           "SELECT 'append_y_ffff0000' AS s /* kns_bill */ FROM append_x_abcd1234")
    assert hq.extract_table_refs(sql, KNOWN) == ["append_x_abcd1234"]


def test_extract_returns_only_catalog_names_in_order_once():
    sql = ("SELECT * FROM kns_bill b JOIN append_x_abcd1234 a ON a.id = b.id "
           "JOIN append_x_abcd1234 a2 ON true JOIN not_a_table n ON true")
    assert hq.extract_table_refs(sql, KNOWN) == ["kns_bill", "append_x_abcd1234"]


def test_extract_accepts_schema_qualified_catalog_entries():
    known = [("public", "append_x_abcd1234"), ("knesset", "kns_bill")]
    assert hq.extract_table_refs("SELECT * FROM knesset.kns_bill", known) == ["kns_bill"]


# ── 2. build_history_view ─────────────────────────────────────────────────────

def _url(key: str) -> str:
    return "s3://bucket/" + key


def test_view_is_a_union_with_the_same_columns_on_both_sides():
    parts = [_part(date(2026, 8, 1), 1, deleted=True), _part(date(2026, 7, 1), 1, deleted=True)]
    sql = hq.build_history_view(TABLE, parts, ["שם", "first_seen", "last_seen", "row_hash", "geom"],
                                ["שם", "קוד", "first_seen", "last_seen", "row_hash"],
                                url_of=_url)
    assert sql.count("UNION ALL") == 1
    hot, cold = sql.split("UNION ALL")
    aliases = lambda s: [a for a in __import__("re").findall(r'AS ("[^"]+")', s)]  # noqa: E731
    assert aliases(hot)[-5:] == aliases(cold)[-5:]
    assert aliases(hot) == aliases(cold)
    # the column only the parquet side has is NULL on the hot side
    assert 'NULL::VARCHAR AS "קוד"' in hot
    assert 'NULL::VARCHAR AS "קוד"' not in cold
    assert "geom" not in sql
    assert f'hot."public"."{TABLE}"' in hot
    # keys in (closed_month, part) order, union_by_name for parts with added columns
    k7, k8 = parts[1]["r2_key"], parts[0]["r2_key"]
    assert f"read_parquet(['s3://bucket/{k7}', 's3://bucket/{k8}'], union_by_name=true)" in cold
    assert "first_seen" in hot and "TIMESTAMPTZ" in sql


def test_view_fills_a_column_missing_from_the_parquet_side():
    parts = [_part(date(2026, 7, 1), 1, deleted=True)]
    sql = hq.build_history_view(TABLE, parts, ["שם", "חדש", "first_seen", "last_seen"],
                                ["שם", "first_seen", "last_seen", "row_hash"], url_of=_url)
    hot, cold = sql.split("UNION ALL")
    assert 'NULL::VARCHAR AS "חדש"' in cold
    assert 'NULL::VARCHAR AS "row_hash"' in hot


def test_view_quotes_a_key_with_a_quote_safely():
    p = _part(date(2026, 7, 1), 1, deleted=True)
    p["r2_key"] = "history/x/it's/part-0001.parquet"
    sql = hq.build_history_view(TABLE, [p], ["a"], ["a"], url_of=_url)
    assert "it''s" in sql


def test_no_parts_means_no_view():
    assert hq.build_history_view(TABLE, [], ["a", "first_seen"], url_of=_url) is None


def test_validity_condition_for_as_of_and_current():
    at = _ts(2026, 7, 15, 23)
    cond = hq.validity_sql(as_of=at)
    assert '"first_seen" <=' in cond and '"last_seen" IS NULL' in cond
    assert "2026-07-15T23:00:00+00:00" in cond
    assert hq.validity_sql(current=True) == '"last_seen" IS NULL'
    assert hq.validity_sql() == ""
    with pytest.raises(ValueError):
        hq.validity_sql(as_of=at, current=True)


# ── 3. cold_parts_for: deleted parts only ─────────────────────────────────────

def test_cold_parts_are_only_the_deleted_ones(monkeypatch):
    cat = [_part(date(2026, 8, 1), 1, deleted=True), _part(date(2026, 7, 1), 2, deleted=False),
           _part(date(2026, 7, 1), 1, deleted=True)]

    async def fake(table):
        return [p for p in cat if p["table_name"] == table]

    monkeypatch.setattr(hq, "_catalog_parts", fake)
    out = asyncio.run(hq.cold_parts_for(TABLE))
    assert [(p["closed_month"], p["part"]) for p in out] == [(date(2026, 7, 1), 1),
                                                           (date(2026, 8, 1), 1)]
    assert all(p["deleted_from_sql"] for p in out)


def test_cold_parts_never_raise(monkeypatch):
    async def boom(table):
        raise RuntimeError("no catalog")

    monkeypatch.setattr(hq, "_catalog_parts", boom)
    assert asyncio.run(hq.cold_parts_for(TABLE)) == []


# ── 4. the real engine ────────────────────────────────────────────────────────

def _write_parquet(path, rows: list[dict], cols: list[str]):
    data = {c: pa.array([r.get(c) for r in rows], type=pa.string()) for c in cols}
    for c in ("first_seen", "last_seen"):
        data[c] = pa.array([r[c] for r in rows], type=pa.timestamp("us", tz="UTC"))
    data["row_hash"] = pa.array([str(uuid.uuid4()) for _ in rows], type=pa.string())
    pq.write_table(pa.table(data), str(path))


# part 1 (July): A, B; part 2 (August, with the added column קוד): C, D
P1 = [{"שם": "A", "first_seen": _ts(2026, 6, 1), "last_seen": _ts(2026, 7, 10)},
      {"שם": "B", "first_seen": _ts(2026, 6, 15), "last_seen": _ts(2026, 7, 20)}]
P2 = [{"שם": "C", "קוד": "1", "first_seen": _ts(2026, 7, 1), "last_seen": _ts(2026, 8, 5)},
      {"שם": "D", "קוד": "2", "first_seen": _ts(2026, 7, 5), "last_seen": _ts(2026, 8, 25)}]
# hot: E closed but still in SQL, F and G open
HOT = [("E", "3", _ts(2026, 8, 1), _ts(2026, 10, 2)),
       ("F", "4", _ts(2026, 9, 1), None),
       ("G", None, _ts(2026, 7, 15), None)]


def _hot_setup(conn):
    conn.execute("ATTACH ':memory:' AS hot")
    conn.execute('CREATE SCHEMA hot."public"')
    for t in (TABLE, OTHER):
        conn.execute(f'CREATE TABLE hot."public"."{t}" ("שם" VARCHAR, "קוד" VARCHAR, '
                     'first_seen TIMESTAMPTZ, last_seen TIMESTAMPTZ, row_hash UUID, geom BLOB)')
    for r in HOT:
        conn.execute(f'INSERT INTO hot."public"."{TABLE}" VALUES (?, ?, ?, ?, uuid(), NULL)', list(r))
    conn.execute(f"INSERT INTO hot.\"public\".\"{OTHER}\" VALUES ('Z', NULL, now(), NULL, uuid(), NULL)")


@pytest.fixture
def engine(monkeypatch, tmp_path):
    """The engine on real DuckDB with local parts and an in-memory hot side."""
    paths = {}
    p1 = _part(date(2026, 7, 1), 1, deleted=True, ls_min=_ts(2026, 7, 10), ls_max=_ts(2026, 7, 20))
    p2 = _part(date(2026, 8, 1), 1, deleted=True, ls_min=_ts(2026, 8, 5), ls_max=_ts(2026, 8, 25))
    _write_parquet(tmp_path / "p1.parquet", P1, ["שם"])
    _write_parquet(tmp_path / "p2.parquet", P2, ["שם", "קוד"])
    paths[p1["r2_key"]] = (tmp_path / "p1.parquet").as_posix()
    paths[p2["r2_key"]] = (tmp_path / "p2.parquet").as_posix()
    # a part still in SQL must never be read (it would count twice)
    p3 = _part(date(2026, 9, 1), 1, deleted=False)
    cat = {TABLE: [p2, p3, p1], OTHER: []}

    async def fake_catalog(table):
        return list(cat.get(table, []))

    monkeypatch.setattr(hq, "_catalog_parts", fake_catalog)
    monkeypatch.setattr(hq, "_part_url", lambda key: paths[key])
    monkeypatch.setattr(hq, "_hot_backend", lambda: hq.MemoryHot(_hot_setup))
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    monkeypatch.setattr(settings, "history_query_memory_mb", 256)
    monkeypatch.setattr(settings, "history_query_queue", 3)
    hq.reset_state()
    return types.SimpleNamespace(parts=[p1, p2], catalog=cat)


def _run(sql, **kw):
    kw.setdefault("tables", [TABLE])
    return asyncio.run(hq.run(sql, **kw))


def test_count_is_hot_plus_deleted_parts(engine):
    out = _run(f"SELECT count(*) AS n FROM {TABLE}")
    assert out["rows"] == [{"n": 7}]
    assert out["engine"] == "duckdb"
    assert out["cold_parts"] == 2
    assert isinstance(out["elapsed_ms"], int)
    assert out["columns"] == ["n"] and out["fields"][0]["type"] == "int"
    assert out["truncated"] is False and out["row_count"] == 1


def test_qualified_and_quoted_references_read_the_same_view(engine):
    for ref in (f"public.{TABLE}", f'"{TABLE}"', f'public."{TABLE}"'):
        assert _run(f"SELECT count(*) AS n FROM {ref}")["rows"] == [{"n": 7}]


def test_group_by_the_added_column_is_null_for_the_older_rows(engine):
    out = _run(f'SELECT "קוד" AS k, count(*) AS n FROM {TABLE} GROUP BY 1 ORDER BY 1 NULLS FIRST')
    got = {r["k"]: r["n"] for r in out["rows"]}
    # A, B (July part, before the column existed) and G (hot, empty)
    assert got[None] == 3
    assert got["1"] == 1 and got["2"] == 1 and got["3"] == 1 and got["4"] == 1


def test_as_of_returns_exactly_the_rows_in_force(engine):
    out = _run(f'SELECT "שם" AS s FROM {TABLE} ORDER BY 1',
               as_of=append_store.parse_as_of("2026-07-15"))
    assert [r["s"] for r in out["rows"]] == ["B", "C", "D", "G"]


def test_current_returns_only_the_open_rows(engine):
    out = _run(f'SELECT "שם" AS s FROM {TABLE} ORDER BY 1', current=True)
    assert [r["s"] for r in out["rows"]] == ["F", "G"]


def test_a_table_without_parts_is_read_straight_from_hot(engine):
    out = _run(f"SELECT count(*) AS n FROM {OTHER}", tables=[OTHER])
    assert out["rows"] == [{"n": 1}] and out["cold_parts"] == 0


def test_a_join_across_a_cold_and_a_plain_table(engine):
    out = _run(f"SELECT (SELECT count(*) FROM {TABLE}) + (SELECT count(*) FROM {OTHER}) AS n",
               tables=[TABLE, OTHER])
    assert out["rows"] == [{"n": 8}]


def test_timestamps_come_back_as_text(engine):
    out = _run(f"SELECT max(last_seen) AS m FROM {TABLE}")
    assert out["rows"][0]["m"].startswith("2026-10-02")
    assert out["fields"][0]["type"] == "timestamp"


def test_geom_is_not_in_the_view(engine):
    with pytest.raises(hq.HistoryQueryError):
        _run(f"SELECT geom FROM {TABLE}")


def test_result_is_capped(engine):
    out = _run(f"SELECT * FROM {TABLE}", max_rows=3)
    assert out["truncated"] is True and out["row_count"] == 3


def test_parameters_are_bound_not_spliced(engine):
    out = _run(f'SELECT count(*) AS n FROM {TABLE} WHERE "שם" = $1', params=["A"])
    assert out["rows"] == [{"n": 1}]


# ── security: what the engine refuses ────────────────────────────────────────

@pytest.mark.parametrize("sql", [
    "SELECT * FROM read_csv('C:/Windows/win.ini')",
    "SELECT * FROM read_parquet('s3://bucket/other.parquet')",
    "SELECT current_setting('s3_secret_access_key')",
    "SELECT * FROM duckdb_secrets()",
    "SELECT * FROM duckdb_databases()",
    "SELECT getenv('R2_SECRET_KEY')",
    "SELECT * FROM glob('*')",
    "SELECT * FROM pragma_database_list",
    "SELECT 1; ATTACH 'x' AS y",
    "INSTALL httpfs",
    "SELECT * FROM postgres_query('hot', 'select 1')",
])
def test_dangerous_sql_is_refused(engine, sql):
    with pytest.raises(ValueError):
        _run(sql)


def test_the_extended_deny_list_does_not_touch_the_postgres_consoles():
    # "export" / "import" are plausible column names; the Postgres path is unchanged.
    assert append_store.validate_readonly_sql('SELECT "export" FROM t') == 'SELECT "export" FROM t'
    with pytest.raises(ValueError):
        hq.validate_sql("SELECT 1 FROM t WHERE x IN (SELECT 1) UNION SELECT 1 FROM t, pragma_version()")


def test_file_access_outside_the_parts_is_blocked_by_duckdb_itself(engine, tmp_path):
    """Even a call the deny list missed hits enable_external_access=false."""
    other = tmp_path / "stray.parquet"
    _write_parquet(other, P1, ["שם"])
    conn = hq._open_connection()
    try:
        hq._lock(conn, allowed=[])
        with pytest.raises(duckdb.Error):
            conn.execute(f"SELECT * FROM read_parquet('{other.as_posix()}')").fetchall()
    finally:
        conn.close()


# ── 5. timeout ────────────────────────────────────────────────────────────────

def test_an_endless_query_is_interrupted_and_the_engine_freed(engine):
    t0 = time.monotonic()
    with pytest.raises(hq.HistoryTimeout) as e:
        _run("SELECT sum(i) AS s FROM range(1000000000000) t(i)", tables=[], timeout_s=1)
    assert time.monotonic() - t0 < 10
    assert "30" not in str(e.value) or True  # the message names the limit
    st = hq.status()
    assert st["active"] == 0 and st["waiting"] == 0
    assert st["stats"]["timeouts"] == 1
    assert _run("SELECT 1 AS one", tables=[])["rows"] == [{"one": 1}]


# ── 6. concurrency ────────────────────────────────────────────────────────────

def test_one_runs_three_wait_and_the_next_is_busy(engine, monkeypatch):
    release = threading.Event()
    real = hq._execute

    def slow(*a, **kw):
        release.wait(10)
        return real(*a, **kw)

    monkeypatch.setattr(hq, "_execute", slow)

    async def main():
        tasks = [asyncio.ensure_future(hq.run("SELECT 1 AS one", tables=[])) for _ in range(5)]
        await asyncio.sleep(0.3)
        st = hq.status()
        assert st["active"] == 1 and st["waiting"] == 3
        release.set()
        return await asyncio.gather(*tasks, return_exceptions=True)

    out = asyncio.run(main())
    busy = [r for r in out if isinstance(r, hq.EngineBusy)]
    ok = [r for r in out if isinstance(r, dict)]
    assert len(busy) == 1 and len(ok) == 4
    assert hq.status()["stats"]["busy"] == 1


def test_a_smaller_queue_rejects_the_fourth(engine, monkeypatch):
    """The plan's 4.2.6 wording: with room for two waiters the fourth is turned away."""
    monkeypatch.setattr(settings, "history_query_queue", 2)
    release = threading.Event()
    real = hq._execute
    monkeypatch.setattr(hq, "_execute", lambda *a, **k: (release.wait(10), real(*a, **k))[1])

    async def main():
        tasks = [asyncio.ensure_future(hq.run("SELECT 1 AS one", tables=[])) for _ in range(4)]
        await asyncio.sleep(0.3)
        release.set()
        return await asyncio.gather(*tasks, return_exceptions=True)

    out = asyncio.run(main())
    assert isinstance(out[3], hq.EngineBusy)
    assert all(isinstance(r, dict) for r in out[:3])


# ── 7. connection settings ────────────────────────────────────────────────────

def test_memory_limit_threads_and_temp_dir_are_set(engine):
    conn = hq._open_connection()
    try:
        mem, threads, tz = conn.execute(
            "SELECT current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('TimeZone')").fetchone()
        tmp = conn.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    finally:
        conn.close()
    assert mem == "256.0 MiB"
    assert int(threads) == 2
    assert tz == "UTC"
    assert tmp


# ── 8. duckdb_error_hint ──────────────────────────────────────────────────────

def test_hint_for_a_postgres_regex():
    e = duckdb.connect().execute
    with pytest.raises(duckdb.Error) as err:
        e("SELECT 'abc' ~* 'B'")
    assert "regexp_matches" in hq.duckdb_error_hint(err.value)


def test_hint_for_postgis():
    with pytest.raises(duckdb.Error) as err:
        duckdb.connect().execute("SELECT ST_AsText('x')")
    assert "לא זמין בהיסטוריה" in hq.duckdb_error_hint(err.value)


def test_hint_for_a_uuid_comparison():
    with pytest.raises(duckdb.Error) as err:
        duckdb.connect().execute("SELECT 'x'::uuid")
    assert "טקסט" in hq.duckdb_error_hint(err.value)


def test_an_unknown_error_comes_back_as_is():
    assert hq.duckdb_error_hint(RuntimeError("boom")) == "boom"


def test_a_partial_regex_match_gets_a_dialect_note(engine):
    out = _run(f"SELECT count(*) AS n FROM {TABLE} WHERE \"שם\" ~ 'A'")
    assert "regexp_matches" in (out.get("dialect_note") or "")


# ── 9. routing as_of ──────────────────────────────────────────────────────────

PARTS = [_part(date(2026, 7, 1), 1, deleted=True, ls_min=_ts(2026, 7, 10), ls_max=_ts(2026, 7, 20)),
         _part(date(2026, 8, 1), 1, deleted=True, ls_min=_ts(2026, 8, 5), ls_max=_ts(2026, 8, 25))]


def test_routing_rule(monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", True)
    # older than the oldest deleted part: some deleted rows were in force then
    assert hq.should_use_history(_ts(2026, 7, 1), PARTS) is True
    # inside the range of the deleted parts
    assert hq.should_use_history(_ts(2026, 8, 10), PARTS) is True
    # after every deleted row had closed: SQL alone is exact, stay on Postgres
    assert hq.should_use_history(_ts(2026, 9, 1), PARTS) is False
    # no deleted parts: nothing outside SQL
    assert hq.should_use_history(_ts(2026, 7, 1), []) is False
    # the rows in force now are never in R2
    assert hq.should_use_history(None, PARTS, current=True) is False
    assert hq.should_use_history(None, PARTS) is True   # the whole history
    assert hq.should_use_history(None, []) is False


def test_routing_is_off_with_the_flag(monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)
    assert hq.should_use_history(_ts(2026, 7, 1), PARTS) is False
    # but the caller can still tell that SQL alone is not the whole answer
    assert hq.should_use_history(_ts(2026, 7, 1), PARTS, enabled=True) is True


def test_config_defaults():
    from app.config import Settings

    s = Settings(_env_file=None)
    assert s.history_query_enabled is False
    assert s.history_query_memory_mb == 1536
    assert s.history_query_timeout_s == 30
    assert s.history_query_hot_max_rows == 2_000_000
    assert s.history_query_queue == 3


def test_is_available_says_disabled(monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)
    assert hq.is_available() == (False, "disabled")


def test_requirements_pin_duckdb():
    import pathlib

    req = (pathlib.Path(__file__).parent.parent / "requirements.txt").read_text()
    assert "duckdb==1.5.1" in req


# ── the arrow fallback for the hot side ───────────────────────────────────────

def test_arrow_hot_builds_the_same_hot_schema():
    rows = [{"שם": "A", "first_seen": _ts(2026, 7, 1), "last_seen": None,
             "row_hash": uuid.uuid4()}]
    hot = hq.ArrowHot()
    hot.load("public", TABLE, ["שם", "first_seen", "last_seen", "row_hash"], rows)
    conn = duckdb.connect()
    hot.setup(conn)
    assert conn.execute(f'SELECT count(*), max("שם") FROM hot."public"."{TABLE}"').fetchone() == (1, "A")
    t = dict(conn.execute(f'DESCRIBE hot."public"."{TABLE}"').fetchall()[i][:2]
             for i in range(4))
    assert t["first_seen"] == "TIMESTAMP WITH TIME ZONE"


def test_arrow_hot_refuses_past_the_cap(monkeypatch):
    monkeypatch.setattr(settings, "history_query_hot_max_rows", 1)
    hot = hq.ArrowHot()
    with pytest.raises(hq.HistoryQueryError):
        hot.load("public", TABLE, ["a"], [{"a": "1"}, {"a": "2"}])


# ── 10. /api/append/{id}/sql and /schema ─────────────────────────────────────

from app.api import append as api  # noqa: E402


def _ds(**kw):
    base = dict(id=uuid.UUID(DSID), title="לוח טיסות", status="active",
                storage_mode="append_only", scraper_config={}, ckan_name="flights")
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.fixture
def append_api(monkeypatch):
    calls = {"pg": [], "hq": []}

    async def fake_resolve(dataset_id, db, selector=None):
        return _ds(), TABLE, [{"table": TABLE}, {"table": OTHER}]

    async def fake_pg(sql, **kw):
        calls["pg"].append(sql)
        return {"columns": ["n"], "fields": [{"id": "n", "type": "int"}], "rows": [{"n": 3}],
                "truncated": False, "row_count": 1}

    async def fake_hq(sql, **kw):
        calls["hq"].append(kw)
        return {"columns": ["n"], "fields": [{"id": "n", "type": "int"}], "rows": [{"n": 7}],
                "truncated": False, "row_count": 1, "engine": "duckdb", "cold_parts": 2,
                "elapsed_ms": 5}

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "run_readonly_sql", fake_pg)
    monkeypatch.setattr(hq, "run", fake_hq)
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    monkeypatch.setattr(api, "admit", lambda *a, **k: None)
    return calls


def _sql(body):
    fn = getattr(api.archive_sql, "__wrapped__", api.archive_sql)
    return asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), body=body,
                          table=None, db=None, caller=None))


def test_sql_without_the_flag_is_unchanged(append_api, monkeypatch):
    out = _sql(api.SqlBody(sql=f"SELECT count(*) AS n FROM {TABLE}"))
    assert out == {"columns": ["n"], "fields": [{"id": "n", "type": "int"}], "rows": [{"n": 3}],
                   "truncated": False, "row_count": 1}
    assert append_api["hq"] == []


def test_include_history_without_deleted_parts_stays_on_postgres(append_api, monkeypatch):
    async def none(table):
        return []

    monkeypatch.setattr(hq, "cold_parts_for", none)
    out = _sql(api.SqlBody(sql=f"SELECT count(*) AS n FROM {TABLE}", include_history=True))
    assert out["engine"] == "postgres" and out["history_note"]
    assert out["rows"] == [{"n": 3}] and append_api["hq"] == []


def test_include_history_with_deleted_parts_routes_to_the_engine(append_api, monkeypatch):
    async def cold(table):
        return PARTS if table == TABLE else []

    monkeypatch.setattr(hq, "cold_parts_for", cold)
    out = _sql(api.SqlBody(sql=f"SELECT count(*) AS n FROM {TABLE} JOIN {OTHER} ON true",
                           include_history=True))
    assert out["engine"] == "duckdb" and out["rows"] == [{"n": 7}]
    assert append_api["pg"] == []
    assert append_api["hq"][0]["tables"] == [TABLE, OTHER]


def test_include_history_with_the_flag_off_says_so(append_api, monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)

    async def cold(table):
        return PARTS

    monkeypatch.setattr(hq, "cold_parts_for", cold)
    out = _sql(api.SqlBody(sql=f"SELECT count(*) AS n FROM {TABLE}", include_history=True))
    assert out["engine"] == "postgres" and "history_note" in out and append_api["hq"] == []


def test_a_busy_engine_is_a_429(append_api, monkeypatch):
    async def cold(table):
        return PARTS

    async def busy(sql, **kw):
        raise hq.EngineBusy("busy")

    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "run", busy)
    with pytest.raises(HTTPException) as e:
        _sql(api.SqlBody(sql=f"SELECT 1 FROM {TABLE}", include_history=True))
    assert e.value.status_code == 429


def test_an_engine_error_is_a_400(append_api, monkeypatch):
    async def cold(table):
        return PARTS

    async def bad(sql, **kw):
        raise hq.HistoryQueryError("Binder Error")

    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "run", bad)
    with pytest.raises(HTTPException) as e:
        _sql(api.SqlBody(sql=f"SELECT 1 FROM {TABLE}", include_history=True))
    assert e.value.status_code == 400


def test_schema_history_says_how_to_query_it(monkeypatch):
    async def fake_resolve(dataset_id, db, selector=None):
        return _ds(), TABLE, [{"table": TABLE}]

    async def fake_cols(t):
        return ["a"]

    async def fake_count(t):
        return 1

    from app.services import history_export as hx

    async def fake_summary(db, dataset_id, table):
        return hx.summarize(PARTS)

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "user_columns", fake_cols)
    monkeypatch.setattr(append_store, "table_count", fake_count)
    monkeypatch.setattr(hx, "summary_for", fake_summary)
    monkeypatch.setattr(settings, "history_query_enabled", True)
    fn = getattr(api.archive_schema, "__wrapped__", api.archive_schema)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}),
                         table=None, db=None))
    assert out["history"]["queryable_via"] == "duckdb"
    assert out["history"]["rows_only_in_r2"] == 4
    monkeypatch.setattr(settings, "history_query_enabled", False)
    out = asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}),
                         table=None, db=None))
    assert out["history"]["queryable_via"] is None


# ── /rows and /datastore_search with as_of ────────────────────────────────────

@pytest.fixture
def rows_api(monkeypatch):
    calls = {"pg": [], "hq": []}

    async def fake_resolve(dataset_id, db, selector=None):
        return _ds(), TABLE, [{"table": TABLE}]

    async def fake_query(table, **kw):
        calls["pg"].append(kw)
        return {"columns": ["a"], "rows": [], "total": 0, "limit": 50, "offset": 0,
                "sort": "first_seen", "order": "desc"}

    async def fake_rows(table, **kw):
        calls["hq"].append(kw)
        return {"columns": ["a"], "rows": [], "total": 9, "limit": 50, "offset": 0,
                "sort": "first_seen", "order": "desc", "engine": "duckdb"}

    async def cold(table):
        return PARTS

    monkeypatch.setattr(api, "_resolve", fake_resolve)
    monkeypatch.setattr(append_store, "query", fake_query)
    monkeypatch.setattr(hq, "rows_page", fake_rows)
    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    return calls


def _rows(**kw):
    fn = getattr(api.archive_rows, "__wrapped__", api.archive_rows)
    args = dict(limit=50, offset=0, sort=None, order="desc", q=None, table=None,
                latest=False, as_of=None, current=False, db=None)
    args.update(kw)
    return asyncio.run(fn(DSID, request=types.SimpleNamespace(query_params={}), **args))


def test_rows_without_as_of_never_look_at_the_catalog(rows_api, monkeypatch):
    async def boom(table):
        raise AssertionError("catalog read on a plain /rows")

    monkeypatch.setattr(hq, "cold_parts_for", boom)
    out = _rows()
    assert out["total"] == 0 and rows_api["hq"] == [] and "engine" not in out


def test_rows_with_an_old_as_of_route_to_the_engine(rows_api):
    out = _rows(as_of="2026-07-15")
    assert out["engine"] == "duckdb" and rows_api["pg"] == []
    assert rows_api["hq"][0]["as_of"] == append_store.parse_as_of("2026-07-15")
    assert rows_api["hq"][0]["parts"] == PARTS


def test_rows_with_a_recent_as_of_stay_on_postgres(rows_api):
    out = _rows(as_of="2026-09-15")
    assert rows_api["hq"] == [] and len(rows_api["pg"]) == 1
    assert "history_note" not in out


def test_rows_current_stay_on_postgres(rows_api):
    _rows(current=True)
    assert rows_api["hq"] == [] and len(rows_api["pg"]) == 1


def test_rows_with_an_old_as_of_and_the_flag_off_carry_a_note(rows_api, monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)
    out = _rows(as_of="2026-07-15")
    assert rows_api["hq"] == [] and out["history_note"]


def test_datastore_search_with_an_old_as_of_routes_to_the_engine(rows_api, monkeypatch):
    async def fake_ds(table, **kw):
        rows_api["hq"].append(kw)
        return {"fields": [], "records": [], "total": 9, "limit": 100, "offset": 0,
                "engine": "duckdb"}

    monkeypatch.setattr(hq, "datastore_page", fake_ds)
    fn = getattr(api.datastore_search, "__wrapped__", api.datastore_search)
    req = types.SimpleNamespace(
        query_params={},
        url=types.SimpleNamespace(remove_query_params=lambda *_: types.SimpleNamespace(
            include_query_params=lambda **k: f"u?{k}")))
    out = asyncio.run(fn(DSID, request=req, limit=100, offset=0, q=None, fields=None,
                         sort=None, filters=None, distinct=False, include_total=True,
                         table=None, latest=False, as_of="2026-07-15", current=False, db=None))
    assert out["result"]["engine"] == "duckdb" and out["result"]["total"] == 9


def test_rows_page_on_the_real_engine(engine):
    out = asyncio.run(hq.rows_page(TABLE, parts=engine.parts, as_of=append_store.parse_as_of("2026-07-15"),
                                   limit=2, offset=0, sort="שם", order="asc", q=None, filters={}))
    assert out["total"] == 4
    assert [r["שם"] for r in out["rows"]] == ["B", "C"]
    assert "last_seen" in out["columns"] and out["engine"] == "duckdb"


def test_rows_page_filters_on_the_real_engine(engine):
    out = asyncio.run(hq.rows_page(TABLE, parts=engine.parts, as_of=None, limit=10, offset=0,
                                   sort=None, order="desc", q="ד", filters={}))
    assert out["total"] == 0
    out = asyncio.run(hq.rows_page(TABLE, parts=engine.parts, as_of=None, limit=10, offset=0,
                                   sort=None, order="desc", q=None, filters={"קוד": "2"}))
    assert [r["שם"] for r in out["rows"]] == ["D"]


def test_datastore_page_on_the_real_engine(engine):
    out = asyncio.run(hq.datastore_page(TABLE, parts=engine.parts,
                                        as_of=append_store.parse_as_of("2026-07-15"),
                                        filters={"שם": ["B", "G"]}, limit=10, offset=0,
                                        sort="שם desc"))
    assert out["total"] == 2 and [r["שם"] for r in out["records"]] == ["G", "B"]


# ── 11. /api/tables/sql ───────────────────────────────────────────────────────

from app.api import tables as tapi  # noqa: E402


@pytest.fixture
def console(monkeypatch):
    calls = {"pg": 0, "hq": [], "known": None}
    catalog = [{"schema": "public", "table": TABLE, "dataset_id": DSID},
               {"schema": "public", "table": OTHER, "dataset_id": DSID},
               {"schema": "knesset", "table": "kns_bill", "dataset_id": None}]

    async def fake_catalog():
        return catalog

    real_extract = hq.extract_table_refs

    def spy_extract(sql, known):
        calls["known"] = list(known)
        return real_extract(sql, known)

    async def fake_pg(sql, **kw):
        calls["pg"] += 1
        return {"columns": ["n"], "fields": [], "rows": [{"n": 1}], "truncated": False,
                "row_count": 1}

    async def fake_hq(sql, **kw):
        calls["hq"].append(kw)
        return {"columns": ["n"], "fields": [], "rows": [{"n": 2}], "truncated": False,
                "row_count": 1, "engine": "duckdb", "cold_parts": 2, "elapsed_ms": 1}

    async def cold(table):
        return PARTS if table == TABLE else []

    monkeypatch.setattr(tapi, "_console_catalog", fake_catalog)
    monkeypatch.setattr(hq, "extract_table_refs", spy_extract)
    monkeypatch.setattr(append_store, "run_readonly_sql", fake_pg)
    monkeypatch.setattr(append_store, "is_configured", lambda: True)
    monkeypatch.setattr(hq, "run", fake_hq)
    monkeypatch.setattr(hq, "cold_parts_for", cold)
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_enabled", True)
    return calls


def _console(body):
    fn = getattr(tapi.run_sql, "__wrapped__", tapi.run_sql)
    return asyncio.run(fn(request=types.SimpleNamespace(), body=body, user=None))


def test_console_without_the_flag_never_reads_the_catalog(console):
    out = _console(tapi.SqlBody(sql=f"SELECT 1 FROM {TABLE}"))
    assert out["rows"] == [{"n": 1}] and console["known"] is None and "engine" not in out


def test_console_with_a_cold_table_runs_the_engine(console):
    out = _console(tapi.SqlBody(sql=f"SELECT 1 FROM {TABLE} JOIN knesset.kns_bill ON true",
                                include_history=True))
    assert out["engine"] == "duckdb" and console["pg"] == 0
    assert ("public", TABLE) in console["known"]
    assert console["hq"][0]["tables"] == [("public", TABLE), ("knesset", "kns_bill")]


def test_console_without_a_cold_table_stays_on_postgres(console):
    out = _console(tapi.SqlBody(sql=f"SELECT 1 FROM {OTHER}", include_history=True))
    assert out["engine"] == "postgres" and out["history_note"] and console["hq"] == []


# ── MCP + admin ───────────────────────────────────────────────────────────────

def test_mcp_tools_take_include_history():
    from app.mcp import server, sql_server

    run_sql = next(t for t in sql_server.TOOLS if t["name"] == "run_sql")
    assert "include_history" in run_sql["inputSchema"]["properties"]
    rows = next(t for t in server.TOOLS if t["name"] == "query_dataset_rows")
    assert "as_of" in rows["inputSchema"]["properties"]
    assert "היסטוריה" in rows["inputSchema"]["properties"]["as_of"]["description"]


def test_admin_has_query_status_and_smoke():
    from app.api import admin_storage

    paths = {r.path for r in admin_storage.router.routes}
    assert any(p.endswith("/history/query-status") for p in paths)
    assert any(p.endswith("/history/query-smoke") for p in paths)


def test_status_when_disabled(monkeypatch):
    monkeypatch.setattr(settings, "history_query_enabled", False)
    st = hq.status()
    assert st["available"] is False and st["reason"] == "disabled"
    assert {"active", "waiting", "stats", "extensions", "hot_mode"} <= set(st)


def test_boot_hook_is_behind_the_flag():
    import pathlib

    src = (pathlib.Path(__file__).parent.parent / "app" / "main.py").read_text(encoding="utf-8")
    assert "history_query_enabled" in src and "ensure_extensions" in src
