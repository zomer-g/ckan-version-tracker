"""The unified price views with the states that left SQL (prices_history).

Three layers, as in the module:

* the filters a query guarantees, read with sqlglot (pure);
* the DuckDB read of the parts and the Postgres SQL that re-injects them (pure);
* end to end: a real Postgres (PRICES_PG_TEST_URL, a throwaway database the
  test fills itself) holds a chain's hot tables and the views prices_unified
  builds; a real Parquet part on disk holds its ended states; the console
  path and the /api/prices/table path must answer a day before the window
  with the states that are only in R2, once each.
"""
import asyncio
import os
import types
import uuid
from datetime import date, datetime, timezone

import pytest

from app.services import prices_history as ph
from app.services import prices_unified as pu

V = "prices_market"


def _eq(sql, view=V):
    return ph.extract_filters(sql, view)


# ── 1. filters ───────────────────────────────────────────────────────────────

def test_the_canonical_day_query_pushes_its_item():
    sql = """WITH d AS (SELECT '2026-10-20'::text AS day),
    p AS MATERIALIZED (SELECT m.chain_name, m.store_id, m.item_price FROM prices_market m, d
      WHERE m.item_code_bare = '7290004131074' AND m.first_seen_date <= d.day
        AND (m.last_seen_date IS NULL OR m.last_seen_date >= d.day))
    SELECT chain_name, count(*) FROM p GROUP BY 1"""
    f = _eq(sql)
    assert f["eq"] == {"item_code_bare": ["7290004131074"]}
    assert f["last_seen_from"] is None and f["first_seen_to"] is None  # d.day is not a literal


def test_literal_days_and_in_lists_are_pushed():
    f = _eq("SELECT * FROM prices_market WHERE item_code_bare IN ('1', '2') AND store_id = 117 "
            "AND first_seen_date <= '2026-09-25' "
            "AND (last_seen_date IS NULL OR last_seen_date >= '2026-09-25')")
    assert f["eq"] == {"item_code_bare": ["1", "2"], "store_id": ["117"]}
    assert f["last_seen_from"] == "2026-09-25" and f["first_seen_to"] == "2026-09-25"


def test_a_filter_inside_an_or_is_not_pushed():
    f = _eq("SELECT * FROM prices_market WHERE item_code_bare = '1' OR chain_name = 'קרפור'")
    assert f["eq"] == {}


def test_a_filter_in_the_select_list_is_not_pushed():
    f = _eq("SELECT count(*) FILTER (WHERE store_id = '1'), "
            "sum(CASE WHEN chain = 'x' THEN 1 END) FROM prices_market WHERE item_code_bare = '5'")
    assert f["eq"] == {"item_code_bare": ["5"]}


def test_two_conditions_on_one_column_keep_their_common_values():
    f = _eq("SELECT * FROM prices_market WHERE item_code_bare IN ('1','2') AND item_code_bare = '2'")
    assert f["eq"] == {"item_code_bare": ["2"]}
    f = _eq("SELECT * FROM prices_market WHERE item_code_bare = '1' AND item_code_bare = '2'")
    assert f["eq"] == {"item_code_bare": [ph.NOTHING_MATCHES]}


def test_every_reference_must_guarantee_a_filter():
    two = ("SELECT * FROM prices_market a JOIN prices_market b ON a.store_id = b.store_id "
           "WHERE a.item_code_bare = '1' AND b.item_code_bare = '2'")
    # each reference has its own condition, but only through its alias: both are
    # sources of one SELECT, so the union of their items is what must be read
    assert _eq(two)["eq"] == {"item_code_bare": ["1", "2"]}
    one_bare = ("SELECT * FROM prices_market a WHERE a.item_code_bare = '1' "
                "UNION ALL SELECT * FROM prices_market")
    assert _eq(one_bare)["eq"] == {}


def test_an_outer_joined_view_is_read_whole():
    sql = ("SELECT * FROM prices_stores s LEFT JOIN prices_market m ON m.store_id = s.store_id "
           "WHERE m.item_code_bare = '1'")
    assert _eq(sql)["eq"] == {}


def test_an_unqualified_column_with_two_sources_is_ambiguous():
    sql = "SELECT * FROM prices_market m, other o WHERE store_id = '1' AND m.item_code_bare = '9'"
    assert _eq(sql)["eq"] == {"item_code_bare": ["9"]}


def test_another_tables_column_is_not_the_views():
    sql = ("SELECT * FROM prices_market m JOIN prices_stores s ON s.store_id = m.store_id "
           "WHERE s.city = 'חיפה' AND m.item_code_bare = '9'")
    assert _eq(sql)["eq"] == {"item_code_bare": ["9"]}


def test_unparsable_sql_pushes_nothing():
    assert _eq("SELECT FROM WHERE (((")["eq"] == {}


def test_views_in_finds_only_the_unified_views():
    assert ph.views_in([("public", V), ("knesset", V), "prices_stores", ("public", "x")]) == [
        V, "prices_stores"]


# ── 2. the read and the re-injection ─────────────────────────────────────────

E1 = {"chain": "carrefour:carrefour", "name": "קרפור", "dataset_id": "d1",
      "tables": {"prices": "append_p1", "stores": "append_s1"}}
E2 = {"chain": "shufersal:shufersal", "name": "שופרסל", "dataset_id": "d2",
      "tables": {"prices": "append_p2"}}


def test_entries_are_narrowed_by_the_chain_columns():
    assert ph.select_entries([E1, E2], V, {"eq": {}}) == [E1, E2]
    assert ph.select_entries([E1, E2], V, {"eq": {"chain_name": ["שופרסל"]}}) == [E2]
    assert ph.select_entries([E1, E2], V, {"eq": {"chain": ["carrefour:carrefour"]}}) == [E1]
    assert ph.select_entries([E1, E2], "prices_stores", {"eq": {}}) == [E1]


def test_cold_where_pushes_text_columns_codes_and_days():
    cols = {"store_id", "item_code", "item_price", "first_seen_date", "last_seen_date"}
    w = ph.cold_where(V, {"eq": {"item_code_bare": ["0001028"], "store_id": ["117"],
                                 "item_price": ["9.9"], "city": ["חיפה"]},
                          "last_seen_from": "2026-09-25", "first_seen_to": "2026-09-25"}, cols)
    assert "ltrim(item_code, '0') IN ('1028')" in w
    assert '"store_id" IN (\'117\')' in w
    assert "last_seen_date >= '2026-09-25'" in w and "first_seen_date <= '2026-09-25'" in w
    assert not any("item_price" in x or "city" in x for x in w)


def test_cold_read_is_one_statement_by_name():
    sql = ph.cold_read_sql({"append_p1": ["a = 1"], "append_p2": []})
    assert "SELECT 'append_p1' AS __table, * FROM public.\"append_p1\" WHERE a = 1" in sql
    assert "UNION ALL BY NAME" in sql and sql.endswith('FROM public."append_p2"')


def test_values_source_escapes_and_types_every_value():
    src = ph.values_source("append_p1", ["item_code", "store_id"],
                           [{"item_code": "a'b", "store_id": None, "row_hash": "h",
                             "last_seen": "2026-09-15 00:00:00+00"}], anti_join=True)
    assert "('a''b'::text, NULL::text, 'h'::text, '2026-09-15 00:00:00+00'::text)" in src
    assert 'AS c("item_code", "store_id", "row_hash", "last_seen")' in src
    assert 'NOT EXISTS (SELECT 1 FROM public."append_p1" h' in src


def test_branch_reads_the_given_relation_instead_of_the_table():
    columns = {"append_p1": {"store_id", "item_code", "item_price", "first_seen_date",
                             "last_seen_date"}}
    sql = pu._branch(V, {**E1, "tables": {"prices": "append_p1"}}, columns, main_from="(X)")
    assert sql.endswith("FROM (X) m") and "append_p1" not in sql


def test_unqualify_and_wrap():
    sql = 'SELECT * FROM public.prices_market JOIN "public"."prices_market" x ON true'
    assert ph.unqualify(sql, [V]) == "SELECT * FROM prices_market JOIN prices_market x ON true"
    assert ph.unqualify("SELECT * FROM public.prices_market_old", [V]).endswith("prices_market_old")
    w = ph.wrap("SELECT 1", ["a AS (SELECT 2)"])
    assert w.startswith("WITH a AS (SELECT 2)") and w.endswith("SELECT 1\n) _with_history")


# ── 3. end to end on Postgres + DuckDB ───────────────────────────────────────

PG_URL = os.environ.get("PRICES_PG_TEST_URL")
asyncpg = pytest.importorskip("asyncpg")
duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq_ = pytest.importorskip("pyarrow.parquet")
needs_pg = pytest.mark.skipif(not PG_URL, reason="PRICES_PG_TEST_URL is not set")

UTC = timezone.utc
P, S, PR, CV = "append_prices_chain_t_p", "append_prices_chain_t_s", "append_prices_chain_t_pr", "append_prices_chain_t_cv"
ENTRY = {"chain": "carrefour:carrefour", "name": "קרפור", "dataset_id": "2e0a7e7b",
         "tables": {"prices": P, "stores": S, "products": PR, "coverage": CV}}
PCOLS = ["chain_id", "sub_chain_id", "store_id", "item_code", "item_price",
         "unit_of_measure_price", "allow_discount", "item_status", "price_update_time",
         "first_seen_date", "last_seen_date"]
H_SQL = str(uuid.uuid4())   # an ended state still in SQL AND in a partial part
H_R2 = str(uuid.uuid4())    # an ended state only in R2


def _state(price, first, last, store="117"):
    return {"chain_id": "729", "sub_chain_id": "1", "store_id": store, "item_code": "0001028",
            "item_price": price, "unit_of_measure_price": price, "allow_discount": "1",
            "item_status": "", "price_update_time": first + " 08:00",
            "first_seen_date": first, "last_seen_date": last}


async def _setup(conn):
    await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public")
    await conn.execute("CREATE TABLE public.over_settlements (code int, name text)")
    await conn.execute("INSERT INTO public.over_settlements VALUES (4000, 'חיפה')")
    await conn.execute("CREATE FUNCTION public.over_settlement_code(text) RETURNS int "
                       "LANGUAGE sql IMMUTABLE AS $$ SELECT CASE WHEN $1 = 'חיפה' THEN 4000 END $$")
    cols = ", ".join(f'"{c}" text' for c in PCOLS)
    await conn.execute(f'CREATE TABLE {P} ({cols}, first_seen timestamptz, last_seen timestamptz, row_hash uuid)')
    await conn.execute(f"CREATE TABLE {S} (store_id text, sub_chain_id text, store_name text, "
                       "address text, city text, first_seen_date text, last_seen_date text)")
    await conn.execute(f"INSERT INTO {S} VALUES ('117', '1', 'קרפור חיפה', 'הרצל 1', 'חיפה', '2026-09-01', '')")
    await conn.execute(f"CREATE TABLE {PR} (item_code text, item_name text, manufacturer_name text, "
                       "quantity text, unit_qty text, unit_of_measure text, is_weighted text, "
                       "first_seen_date text, last_seen_date text)")
    await conn.execute(f"INSERT INTO {PR} VALUES ('1028', 'חלב 3%', 'תנובה', '1', 'ליטר', 'ליטר', '0', '2026-09-01', '')")
    await conn.execute(f"CREATE TABLE {CV} (store_id text, snapshot_date text)")
    await conn.execute(f"INSERT INTO {CV} VALUES ('117', '2026-10-28')")
    ins = (f'INSERT INTO {P} ({", ".join(chr(34) + c + chr(34) for c in PCOLS)}, first_seen, last_seen, row_hash) '
           f"VALUES ({', '.join('$' + str(i + 1) for i in range(len(PCOLS)))}, now(), $12, $13)")
    open_ = _state("7.35", "2026-10-01", "")
    ended_in_sql = _state("6.90", "2026-09-15", "2026-09-30")
    await conn.execute(ins, *[open_[c] for c in PCOLS], None, uuid.uuid4())
    await conn.execute(ins, *[ended_in_sql[c] for c in PCOLS],
                       datetime(2026, 10, 1, tzinfo=UTC), uuid.UUID(H_SQL))
    entries = [ENTRY]
    pool_cols = {}
    for r in await conn.fetch("SELECT table_name, column_name FROM information_schema.columns "
                              "WHERE table_schema = 'public'"):
        pool_cols.setdefault(r["table_name"], set()).add(r["column_name"])
    for view in pu.VIEWS:
        body = pu.view_sql(view, entries, pool_cols)
        if body:
            await conn.execute(f'CREATE VIEW public."{view}" AS {body}')
    return pool_cols


def _write_part(path):
    rows = [_state("5.90", "2026-09-01", "2026-09-14"), _state("6.90", "2026-09-15", "2026-09-30")]
    data = {c: pa.array([r[c] for r in rows], type=pa.string()) for c in PCOLS}
    data["first_seen"] = pa.array([datetime(2026, 9, 1, tzinfo=UTC)] * 2, type=pa.timestamp("us", tz="UTC"))
    data["last_seen"] = pa.array([datetime(2026, 9, 15, tzinfo=UTC), datetime(2026, 10, 1, tzinfo=UTC)],
                                 type=pa.timestamp("us", tz="UTC"))
    data["row_hash"] = pa.array([H_R2, H_SQL], type=pa.string())
    pq_.write_table(pa.table(data), str(path))


@pytest.fixture
def world(monkeypatch, tmp_path):
    from app.config import settings
    from app.services import history_query as hq
    from app.services import prices_query

    async def setup():
        conn = await asyncpg.connect(PG_URL)
        try:
            return await _setup(conn)
        finally:
            await conn.close()
    cols = asyncio.run(setup())
    _write_part(tmp_path / "p.parquet")
    part = {"id": "x", "dataset_id": "d", "table_name": P, "closed_month": date(2026, 9, 1),
            "part": 1, "r2_key": "k", "rows": 2, "kind": "closed", "deleted_from_sql": False,
            "partial": True, "last_seen_min": datetime(2026, 9, 15, tzinfo=UTC),
            "last_seen_max": datetime(2026, 10, 1, tzinfo=UTC)}

    async def cold_parts_of(tables):
        return {P: [part]} if P in tables else {}

    async def entries():
        return [ENTRY]

    async def columns(tables):
        return {t: c for t, c in cols.items() if t in tables}

    async def run_sql(sql):
        conn = await asyncpg.connect(PG_URL)
        try:
            rows = await conn.fetch(sql)
        finally:
            await conn.close()
        return {"rows": [dict(r) for r in rows]}

    async def fetch(sql, params):
        conn = await asyncpg.connect(PG_URL)
        try:
            return [dict(r) for r in await conn.fetch(sql, *params)]
        finally:
            await conn.close()

    async def noop(db):
        return None

    async def chain_keys(db, wanted):
        return wanted

    monkeypatch.setattr(hq, "cold_parts_of", cold_parts_of)
    monkeypatch.setattr(hq, "_part_url", lambda k: (tmp_path / "p.parquet").as_posix())
    monkeypatch.setattr(hq, "is_available", lambda: (True, ""))
    monkeypatch.setattr(settings, "history_query_memory_mb", 256)
    monkeypatch.setattr(ph, "_entries", entries)
    monkeypatch.setattr(ph, "_columns", columns)
    monkeypatch.setattr(prices_query, "_fetch", fetch)
    monkeypatch.setattr(pu, "ensure_views_safely", noop)
    monkeypatch.setattr(pu, "_chain_keys", chain_keys)
    hq.reset_state()
    return types.SimpleNamespace(run_sql=run_sql)


DAY_SQL = """SELECT m.chain_name, m.store_name, m.city, m.city_code, m.item_name,
       m.item_price::text AS price, m.first_seen_date, m.last_seen_date
FROM public.prices_market m
WHERE m.item_code_bare = '1028' AND m.first_seen_date <= '{d}'
  AND (m.last_seen_date IS NULL OR m.last_seen_date >= '{d}')"""


@needs_pg
def test_console_a_day_only_in_r2_comes_back_with_its_store_and_product(world):
    res = asyncio.run(ph.run_with_history(DAY_SQL.format(d="2026-09-10"), views=[V],
                                          run_sql=world.run_sql))
    assert res["engine"] == "postgres+r2"
    assert [(r["price"], r["store_name"], r["city"], r["city_code"], r["item_name"])
            for r in res["rows"]] == [("5.90", "קרפור חיפה", "חיפה", 4000, "חלב 3%")]


@needs_pg
def test_console_a_state_still_in_sql_is_counted_once(world):
    sql = ("SELECT first_seen_date, count(*) AS n FROM prices_market "
           "WHERE item_code_bare = '1028' GROUP BY 1 ORDER BY 1")
    res = asyncio.run(ph.run_with_history(sql, views=[V], run_sql=world.run_sql))
    assert [(r["first_seen_date"], r["n"]) for r in res["rows"]] == [
        ("2026-09-01", 1), ("2026-09-15", 1), ("2026-10-01", 1)]


@needs_pg
def test_console_a_day_in_sql_is_unchanged(world):
    res = asyncio.run(ph.run_with_history(DAY_SQL.format(d="2026-10-15"), views=[V],
                                          run_sql=world.run_sql))
    assert [r["price"] for r in res["rows"]] == ["7.35"]


@needs_pg
def test_console_a_query_of_its_own_with_ctes_and_aggregates(world):
    sql = """WITH d AS (SELECT '2026-09-10'::text AS day)
SELECT chain_name, min(item_price) AS lo,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY item_price) AS med
FROM prices_market m, d WHERE m.item_code_bare = '1028' AND m.first_seen_date <= d.day
  AND (m.last_seen_date IS NULL OR m.last_seen_date >= d.day) GROUP BY 1"""
    res = asyncio.run(ph.run_with_history(sql, views=[V], run_sql=world.run_sql))
    assert [(r["chain_name"], float(r["lo"])) for r in res["rows"]] == [("קרפור", 5.9)]


@needs_pg
def test_api_table_on_a_past_day_includes_r2(world):
    out = asyncio.run(pu.query(None, V, filters={"item_code": "1028"}, on_date="2026-09-10"))
    assert [r["item_price"] for r in out["rows"]] == [5.9]
    assert out["history"]["cold_rows"] == 1  # on_date reached the R2 read too
    now = asyncio.run(pu.query(None, V, filters={"item_code": "1028"}))
    assert [r["item_price"] for r in now["rows"]] == [7.35] and now["history"] == {}


@needs_pg
def test_too_broad_a_read_is_refused(world, monkeypatch):
    monkeypatch.setattr(ph, "COLD_MAX_ROWS", 1)
    with pytest.raises(ph.TooBroad):
        asyncio.run(ph.cold_cte(V, {"eq": {}}, max_rows=1))


# ── 4. routing from the console and MCP ──────────────────────────────────────

def test_route_sql_sends_a_price_view_to_the_injection(monkeypatch):
    from app.services import history_query as hq

    seen = {}

    async def fake(sql, *, views, run_sql):
        seen["views"] = views
        return {"engine": "postgres+r2"}
    monkeypatch.setattr(ph, "run_with_history", fake)

    async def pg():
        raise AssertionError("must not run the original statement")

    async def pg_sql(s):
        return {}

    res = asyncio.run(hq.route_sql("SELECT * FROM prices_market", tables=[("public", V)],
                                   run_pg=pg, run_pg_sql=pg_sql))
    assert res["engine"] == "postgres+r2" and seen["views"] == [V]


def test_route_sql_refuses_a_price_view_with_another_cold_table(monkeypatch):
    from app.services import history_query as hq

    async def parts(name):
        return [{"x": 1}] if name == "append_rain" else []
    monkeypatch.setattr(hq, "cold_parts_for", parts)

    async def pg_sql(s):
        return {}

    with pytest.raises(hq.HistoryQueryError):
        asyncio.run(hq.route_sql("SELECT 1", tables=[("public", V), ("public", "append_rain")],
                                 run_pg=pg_sql, run_pg_sql=pg_sql))


def test_without_a_runner_route_sql_keeps_its_old_path(monkeypatch):
    from app.services import history_query as hq

    async def parts(name):
        return []
    monkeypatch.setattr(hq, "cold_parts_for", parts)
    monkeypatch.setattr(hq, "union_branches", lambda n: _none())

    async def pg():
        return {"rows": []}

    res = asyncio.run(hq.route_sql("SELECT 1", tables=[("public", V)], run_pg=pg))
    assert res["engine"] == "postgres"


async def _none():
    return None


@needs_pg
def test_api_ended_states_only_include_r2(world):
    out = asyncio.run(pu.query(None, V, filters={"item_code": "1028", "is_current": "false"},
                               order="first_seen_date"))
    assert [r["first_seen_date"] for r in out["rows"]] == ["2026-09-01", "2026-09-15"]
