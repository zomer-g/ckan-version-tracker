"""Prices on the cold tier: ended states leave SQL by their own end date.

The prices source publishes STATES and never closes a row by absence
(``append_closes_rows: false``); a state's end arrives as its stamp column
(``last_seen_date``) filled in place. history_export.close_stamped_states turns
that stamp into ``last_seen`` for a dataset with a window, so the ordinary
phase-2 export moves the ended states to Parquet and deletes them from SQL, and
prices_query.price_history reads them back from R2 through the history engine.

No Postgres here: the SQL is checked as text and through a recording
connection; the R2 side runs on real DuckDB over local Parquet files, as in
test_history_query.
"""
import asyncio
import types
import uuid
from datetime import date, datetime, timezone

import pytest

from app.config import settings
from app.services import append_store, history_export, prices_query, sampling_runs

UTC = timezone.utc
DSID = "2e0a7e7b-2bbb-4bbe-8e13-d87c90bca124"
TABLE = "append_prices_chain_f240d9c5_2e0a7e7b_ad8971b7"
PRICES_CFG = {"kind": "prices", "append_closes_rows": False,
              "append_stamp_column": "last_seen_date"}


def _ds(cfg=None, **kw):
    base = dict(id=uuid.UUID(DSID), title="שקיפות מחירים — קרפור", status="active",
                source_type="scraper", source_url="https://prices.example/carrefour",
                scraper_config=cfg, storage_mode="append_only", is_active=True)
    base.update(kw)
    return types.SimpleNamespace(**base)


@pytest.fixture(autouse=True)
def no_manifest(monkeypatch):
    from app.services import source_registry

    monkeypatch.setattr(source_registry, "cached_manifests", lambda: [])
    monkeypatch.setattr(source_registry, "match_manifests", lambda url, mans: None)


# ── which datasets close by their stamp ──────────────────────────────────────

def test_the_prices_stamp_is_a_state_stamp():
    assert sampling_runs.state_stamp_column(_ds(PRICES_CFG)) == "last_seen_date"


def test_a_sampled_source_has_no_state_stamp():
    cfg = {**PRICES_CFG, "sampling": {"item_key": "id", "sample_column": "sampled_at"}}
    assert sampling_runs.state_stamp_column(_ds(cfg)) is None


def test_only_a_state_source_with_a_window_closes_by_its_stamp():
    assert not history_export.closes_by_stamp(_ds(PRICES_CFG))
    assert history_export.closes_by_stamp(_ds({**PRICES_CFG, "history_window_days": 30}))
    # a window under the floor is no window
    assert not history_export.closes_by_stamp(_ds({**PRICES_CFG, "history_window_days": 7}))
    assert not history_export.closes_by_stamp(_ds({"history_window_days": 30}))


def test_reopen_refuses_a_dataset_closed_by_its_stamp():
    assert append_store.reopen_refusal(_ds(PRICES_CFG)) is None
    why = append_store.reopen_refusal(_ds({**PRICES_CFG, "history_window_days": 30}))
    assert why and "stamp" in why


# ── the cutoff and the bound ─────────────────────────────────────────────────

def test_cutoff_is_the_earlier_of_export_and_window():
    now = datetime(2026, 11, 20, 10, tzinfo=UTC)
    # export cutoff 2026-11-01; window 30 days -> 2026-10-21 10:00
    assert history_export.stamp_close_cutoff(now, 30) == datetime(2026, 10, 21, 10, tzinfo=UTC)
    # window 90 days is earlier than the month
    assert history_export.stamp_close_cutoff(now, 90) == datetime(2026, 8, 22, 10, tzinfo=UTC)
    assert history_export.stamp_close_cutoff(now, None) is None


def test_bound_closes_exactly_the_states_whose_end_is_before_the_cutoff():
    # last_seen = day after the stamp, 00:00 UTC; closed iff that < cutoff
    mid = datetime(2026, 11, 1, tzinfo=UTC)
    assert history_export.stamp_bound(mid) == "2026-10-31"     # 10-30 -> 10-31 < 11-01
    noon = datetime(2026, 10, 21, 10, tzinfo=UTC)
    assert history_export.stamp_bound(noon) == "2026-10-21"    # 10-20 -> 10-21 00:00 < 10:00
    for stamp, cutoff in (("2026-10-30", mid), ("2026-10-20", noon)):
        d = date.fromisoformat(stamp)
        last_seen = datetime(d.year, d.month, d.day, tzinfo=UTC) + history_export.timedelta(days=1)
        assert last_seen < cutoff and stamp < history_export.stamp_bound(cutoff)
    d = date(2026, 10, 31)
    assert not datetime(2026, 11, 1, tzinfo=UTC) < mid
    assert not d.isoformat() < history_export.stamp_bound(mid)


def test_close_sql_touches_only_open_rows_with_a_valid_date_stamp():
    sql = history_export.close_by_stamp_sql(TABLE, "last_seen_date", batch=1000)
    assert sql.startswith(f'UPDATE "public"."{TABLE}" SET "last_seen" = ')
    assert "\"last_seen_date\"::date + 1" in sql and "AT TIME ZONE 'UTC'" in sql
    assert '"last_seen" IS NULL' in sql
    assert "'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'" in sql
    assert "pg_input_is_valid(\"last_seen_date\", 'date')" in sql
    assert '"last_seen_date" < $1' in sql and "LIMIT 1000" in sql


# ── the job step ─────────────────────────────────────────────────────────────

class _Conn:
    def __init__(self, closes):
        self.sql = []
        self._closes = list(closes)

    async def execute(self, sql, *args, **kw):
        self.sql.append((sql, args))
        if sql.startswith("UPDATE"):
            return f"UPDATE {self._closes.pop(0) if self._closes else 0}"
        return "ANALYZE"


def _pool(conn):
    class _Acq:
        async def __aenter__(self):
            return conn

        async def __aexit__(self, *a):
            return False

    async def get_pool():
        return types.SimpleNamespace(acquire=lambda: _Acq())
    return get_pool


class _DB:
    def __init__(self, datasets):
        self._ds = datasets

    async def execute(self, q):
        ds = self._ds
        return types.SimpleNamespace(scalars=lambda: types.SimpleNamespace(all=lambda: ds))


def _wire(monkeypatch, conn, tables, columns):
    from app.services import parquet_export

    monkeypatch.setattr(append_store, "get_pool", _pool(conn))

    async def latest(db, ids):
        return {}
    monkeypatch.setattr(parquet_export, "_latest_mappings", latest)
    monkeypatch.setattr(append_store, "tables_from_mappings",
                        lambda ds, m: [{"table": t} for t in tables])

    async def cols(c, table):
        return columns[table]
    monkeypatch.setattr(history_export, "_columns", cols)

    async def ident(c, table):
        return ("row_hash", "uuid")
    monkeypatch.setattr(history_export, "_table_ident", ident)


def test_the_step_closes_in_batches_and_analyzes(monkeypatch):
    conn = _Conn([50_000, 20_000])
    cov = TABLE.replace("ad8971b7", "eab86bdc")
    _wire(monkeypatch, conn, [TABLE, cov],
          {TABLE: ["store_id", "last_seen_date"], cov: ["store_id", "snapshot_date"]})
    ds = _ds({**PRICES_CFG, "history_window_days": 30})
    now = datetime(2026, 11, 20, 10, tzinfo=UTC)
    out = asyncio.run(history_export.close_stamped_states(_DB([ds]), now=now))
    assert out == {TABLE: 70_000}  # the coverage table has no stamp: skipped
    updates = [a for s, a in conn.sql if s.startswith("UPDATE")]
    assert len(updates) == 3 and all(a == ("2026-10-21",) for a in updates)
    assert conn.sql[-1][0].startswith("ANALYZE")


def test_the_step_skips_a_dataset_without_a_window(monkeypatch):
    conn = _Conn([5])
    _wire(monkeypatch, conn, [TABLE], {TABLE: ["last_seen_date"]})
    out = asyncio.run(history_export.close_stamped_states(_DB([_ds(PRICES_CFG)])))
    assert out == {} and conn.sql == []


def test_the_step_skips_a_cold_dataset(monkeypatch):
    conn = _Conn([5])
    _wire(monkeypatch, conn, [TABLE], {TABLE: ["last_seen_date"]})
    ds = _ds({**PRICES_CFG, "history_window_days": 30, "history_tier": "cold"})
    assert asyncio.run(history_export.close_stamped_states(_DB([ds]))) == {}


def test_a_failing_table_is_reported_not_raised(monkeypatch):
    class Boom(_Conn):
        async def execute(self, sql, *a, **kw):
            raise RuntimeError("canceling statement")
    _wire(monkeypatch, Boom([]), [TABLE], {TABLE: ["last_seen_date"]})
    ds = _ds({**PRICES_CFG, "history_window_days": 30})
    out = asyncio.run(history_export.close_stamped_states(_DB([ds])))
    assert out[TABLE].startswith("error: RuntimeError")


# ── price history reads R2 ───────────────────────────────────────────────────

def _state(chain, store, first, price, last=""):
    return {"chain": chain, "chain_name": chain, "sub_chain_id": "1", "store_id": store,
            "item_price": price, "unit_of_measure_price": price, "first_seen_date": first,
            "last_seen_date": last, "price_update_time": first + " 08:00"}


def test_merge_drops_a_state_already_read_from_sql_and_sorts_newest_first():
    hot = [_state("c", "1", "2026-10-20", "9.90"), _state("c", "1", "2026-09-01", "8.90", "2026-09-30")]
    cold = [_state("c", "1", "2026-09-01", "8.90", "2026-09-30"), _state("c", "2", "2026-08-01", "7.90", "2026-08-31")]
    out = prices_query.merge_states(hot, cold)
    assert [r["first_seen_date"] for r in out] == ["2026-10-20", "2026-09-01", "2026-08-01"]


def test_cold_is_skipped_only_when_sql_fills_the_page_with_newer_states():
    parts = {TABLE: [{"last_seen_max": datetime(2026, 9, 1, tzinfo=UTC)}]}
    hot = [_state("c", "1", "2026-10-0%d" % i, "1") for i in range(9, 0, -1)]
    assert prices_query.cold_can_matter(hot[:3], 5, parts)       # page not full
    assert not prices_query.cold_can_matter(hot, 5, parts)      # 5th newest began 10-05 > 09-01
    old = [_state("c", "1", "2026-08-0%d" % i, "1") for i in range(9, 0, -1)]
    assert prices_query.cold_can_matter(old, 5, parts)


def test_cold_sql_matches_bare_codes_and_store_as_literals():
    sql = prices_query.cold_states_sql([{"chain": "c'x", "name": "קרפור", "table": TABLE}],
                                       ["1028", "7290000066134"], "117", 200)
    assert f'FROM public."{TABLE}"' in sql
    assert "ltrim(item_code, '0') IN ('1028', '7290000066134')" in sql
    assert "store_id = '117'" in sql and "'c''x' AS chain" in sql and "LIMIT 201" in sql


duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _write_part(path, states):
    cols = ["chain_id", "sub_chain_id", "store_id", "item_code", "item_price",
            "unit_of_measure_price", "allow_discount", "item_status", "price_update_time",
            "first_seen_date", "last_seen_date"]
    data = {c: pa.array([s.get(c, "") for s in states], type=pa.string()) for c in cols}
    for c in ("first_seen", "last_seen"):
        data[c] = pa.array([datetime(2026, 9, 1, tzinfo=UTC)] * len(states),
                           type=pa.timestamp("us", tz="UTC"))
    data["row_hash"] = pa.array([uuid.uuid4().hex for _ in states], type=pa.string())
    pq.write_table(pa.table(data), str(path))


@pytest.fixture
def cold_engine(monkeypatch, tmp_path):
    from app.services import history_query as hq

    key = f"history/2e0a7e7b/{TABLE}/closed_month=2026-09/part-0001.parquet"
    _write_part(tmp_path / "p.parquet", [
        {"sub_chain_id": "1", "store_id": "117", "item_code": "0001028", "item_price": "8.90",
         "first_seen_date": "2026-09-01", "last_seen_date": "2026-09-14",
         "price_update_time": "2026-09-01 08:00"},
        {"sub_chain_id": "1", "store_id": "117", "item_code": "555", "item_price": "1.00",
         "first_seen_date": "2026-09-02", "last_seen_date": "2026-09-03"},
    ])
    part = {"id": str(uuid.uuid4()), "dataset_id": DSID, "table_name": TABLE,
            "closed_month": date(2026, 9, 1), "part": 1, "r2_key": key, "rows": 2,
            "bytes": 1, "content_hash": "x", "last_seen_min": datetime(2026, 9, 2, tzinfo=UTC),
            "last_seen_max": datetime(2026, 9, 15, tzinfo=UTC), "deleted_from_sql": True}

    async def catalog(tables):
        return {TABLE: [part]} if TABLE in tables else {}
    monkeypatch.setattr(hq, "_catalog_parts_many", catalog)
    monkeypatch.setattr(hq, "_part_url", lambda k: (tmp_path / "p.parquet").as_posix())
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_memory_mb", 256)
    monkeypatch.setattr(settings, "history_query_queue", 3)
    hq.reset_state()
    return hq


def _wire_prices(monkeypatch, hot_rows):
    entries = [{"chain": "f240d9c5", "name": "קרפור", "dataset_id": DSID,
                "tables": {"prices": TABLE}}]

    async def require(db, chains_filter, role="prices"):
        return entries
    monkeypatch.setattr(prices_query, "_require", require)

    async def fetch(sql, params):
        return [dict(r) for r in hot_rows]
    monkeypatch.setattr(prices_query, "_fetch", fetch)


def test_price_history_adds_the_states_that_left_sql(cold_engine, monkeypatch):
    _wire_prices(monkeypatch, [_state("f240d9c5", "117", "2026-10-15", "9.90")])
    out = asyncio.run(prices_query.price_history(None, item_code="1028"))
    assert [(s["first_seen_date"], s["item_price"]) for s in out["states"]] == [
        ("2026-10-15", "9.90"), ("2026-09-01", "8.90")]
    cold = out["states"][1]
    assert cold["chain_name"] == "קרפור" and cold["last_seen_date"] == "2026-09-14"
    assert out["history"]["cold_parts"] == 1
    assert [t["date"] for t in out["timeline"]] == ["2026-09-01", "2026-10-15"]


def test_price_history_without_parts_is_sql_alone(cold_engine, monkeypatch):
    async def none(tables):
        return {}
    monkeypatch.setattr(cold_engine, "_catalog_parts_many", none)
    _wire_prices(monkeypatch, [_state("f240d9c5", "117", "2026-10-15", "9.90")])
    out = asyncio.run(prices_query.price_history(None, item_code="1028"))
    assert len(out["states"]) == 1 and out["history"] == {}


def test_price_history_survives_an_engine_failure(cold_engine, monkeypatch):
    monkeypatch.setattr(cold_engine, "is_available", lambda: (False, "off"))
    _wire_prices(monkeypatch, [_state("f240d9c5", "117", "2026-10-15", "9.90")])
    out = asyncio.run(prices_query.price_history(None, item_code="1028"))
    assert len(out["states"]) == 1 and "off" in out["history"]["error"]
    assert "rows" not in out["history"]


def test_a_failing_close_step_does_not_stop_the_export(monkeypatch):
    import app.database as database

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    monkeypatch.setattr(database, "async_session", lambda: _Session())
    monkeypatch.setattr(history_export, "is_available", lambda: (True, None))

    async def boom(db, **kw):
        raise RuntimeError("no catalog")
    monkeypatch.setattr(history_export, "close_stamped_states", boom)
    planned = []

    async def plan(db, **kw):
        planned.append(kw)
        return []
    monkeypatch.setattr(history_export, "plan", plan)
    asyncio.run(history_export.run())
    st = history_export.status()
    assert planned and any("no catalog" in f["error"] for f in st["failed"])
