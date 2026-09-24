"""Tests for the uniform price tables (app/services/prices_unified.py).

~33 chains, six tables each, become six views with one schema. What can go
wrong quietly:

* a chain missing a table or a column drops out of the union, or breaks it
  (UNION ALL needs the same column types in every branch);
* a filter that cannot be pushed into the branches (a function around the
  column) turns an indexed lookup into a scan of ~16M price states;
* the /data console SQL handed back differs from what was executed;
* an unbounded query on prices_market reaching the database at all.
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services import prices_query as pq  # noqa: E402
from app.services import prices_unified as pu  # noqa: E402


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


FULL = {
    "prices": {"chain_id", "sub_chain_id", "store_id", "item_code", "item_price",
               "unit_of_measure_price", "allow_discount", "item_status",
               "price_update_time", "first_seen_date", "last_seen_date"},
    "products": {"item_code", "item_type", "item_name", "manufacturer_name",
                 "manufacture_country", "manufacturer_item_description", "unit_qty",
                 "quantity", "unit_of_measure", "is_weighted", "qty_in_package",
                 "first_seen_date", "last_seen_date"},
    "stores": {"sub_chain_id", "store_id", "chain_name", "sub_chain_name", "store_type",
               "store_name", "address", "city", "zip_code", "first_seen_date",
               "last_seen_date"},
    "coverage": {"sub_chain_id", "store_id", "snapshot_date", "price_file",
                 "price_file_time", "price_items", "promo_file", "promo_file_time",
                 "promo_count"},
}


def _entry(key, roles, name="רשת"):
    return {"chain": key, "name": name, "account": key, "dataset_id": f"id-{key}",
            "tables": {r: f"t_{key}_{r}" for r in roles}}


def _columns(entries, drop=None):
    out = {}
    for e in entries:
        for role, table in e["tables"].items():
            out[table] = set(FULL.get(role, set())) - set((drop or {}).get(table, ()))
    return out


def test_every_branch_has_the_same_columns_in_the_same_order():
    entries = [_entry("a", ["prices", "products", "stores", "coverage"]),
               _entry("b", ["prices"])]                      # no stores, products, coverage
    cols = _columns(entries, drop={"t_a_prices": {"allow_discount"}})
    sql = pu.view_sql("prices_market", entries, cols)
    a, b = sql.split("\nUNION ALL\n")
    assert "NULL::text AS \"allow_discount\"" in a          # missing column → NULL
    assert "NULL::int AS \"city_code\"" in b                 # no stores → typed NULL
    assert "NULL::text AS \"store_name\"" in b
    assert "LEFT JOIN LATERAL" not in b
    for branch in (a, b):
        assert branch.index('"item_code_bare"') < branch.index('"item_price"') \
            < branch.index('"is_current"') < branch.index('"as_of"')


def test_a_chain_without_the_role_table_is_left_out():
    entries = [_entry("a", ["prices", "stores"]), _entry("b", ["prices"])]
    sql = pu.view_sql("prices_stores", entries, _columns(entries))
    assert "UNION ALL" not in sql and "'a'::text AS chain" in sql
    assert pu.view_sql("prices_promotions", entries, _columns(entries)) is None


def test_bare_item_code_is_the_index_expression():
    """prices_query indexes ltrim("item_code", '0'); the view must expose that
    exact expression, or a lookup through the view scans every chain."""
    entries = [_entry("a", ["prices", "products"])]
    sql = pu.view_sql("prices_market", entries, _columns(entries))
    assert "ltrim(m.item_code, '0') AS \"item_code_bare\"" in sql
    assert pq._BARE == "ltrim(\"item_code\", '0')"


def test_prices_are_cast_only_when_numeric():
    entries = [_entry("a", ["prices"])]
    sql = pu.view_sql("prices_market", entries, _columns(entries))
    assert "CASE WHEN m.\"item_price\" ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN" in sql


def test_chain_name_is_the_published_hebrew_name():
    entries = [_entry("cerberus:ramilevi", ["prices"], name="רמי לוי שיווק השקמה")]
    sql = pu.view_sql("prices_market", entries, _columns(entries))
    assert "'רמי לוי שיווק השקמה'::text AS chain_name" in sql


def test_signature_moves_with_tables_and_columns():
    entries = [_entry("a", ["prices", "stores"])]
    base = pu.signature(entries, _columns(entries))
    assert pu.signature(entries, _columns(entries)) == base
    more = entries + [_entry("b", ["prices"])]
    assert pu.signature(more, _columns(more)) != base
    assert pu.signature(entries, _columns(entries, drop={"t_a_prices": {"item_status"}})) != base


def test_describe_lists_every_view_and_types():
    d = {t["name"]: t for t in pu.describe()}
    assert set(d) == set(pu.VIEWS)
    market = {c["name"]: c["type"] for c in d["prices_market"]["columns"]}
    assert market["item_price"] == "numeric"
    assert market["is_current"] == "boolean"
    assert market["city_code"] == "integer"
    assert list(market)[:3] == ["chain", "chain_name", "dataset_id"]


def test_render_inlines_params_for_the_console():
    q = pu.Query()
    sql = f"SELECT 1 WHERE a = ANY({q.p(['x', "o'b"])}::text[]) AND b = {q.p(4000)}"
    for i in range(9):
        q.p(i)
    sql += f" AND c = {q.p('last')}"
    rendered = q.render(sql)
    assert "ARRAY['x', 'o''b']" in rendered
    assert "b = 4000" in rendered
    assert "c = 'last'" in rendered                          # $11, not $1 + "1"


# --------------------------------------------------------------------------
# query() — the SQL it sends
# --------------------------------------------------------------------------

@pytest.fixture
def captured(monkeypatch):
    sent = {}

    async def no_views(db):
        return None

    async def fake_fetch(sql, params):
        sent["sql"], sent["params"] = sql, params
        return [{"chain": "a", "item_price": pu.Decimal("3.90")}]

    async def fake_chains(db, refresh=False):
        return [{"chain": "cerberus:ramilevi", "name": "רמי לוי", "account": "RamiLevi",
                 "dataset_id": "x",
                 "tables": {"prices": "t"}}]

    async def fake_city(city):
        return 4000

    monkeypatch.setattr(pu, "ensure_views_safely", no_views)
    monkeypatch.setattr(pq, "_fetch", fake_fetch)
    monkeypatch.setattr(pq, "chains", fake_chains)
    monkeypatch.setattr(pq, "city_code", fake_city)
    return sent


def test_market_needs_a_selective_filter(captured):
    with pytest.raises(ValueError):
        run(pu.query(None, "prices_market", filters={"chain": "ramilevi"}))
    with pytest.raises(ValueError):                           # store without its chain
        run(pu.query(None, "prices_market", filters={"store_id": "19"}))
    assert "sql" not in captured


def test_market_query_is_pushdown_friendly(captured):
    out = run(pu.query(None, "prices_market",
                       filters={"item_code": "007290000066134", "chain": "ramilevi"},
                       city="חיפה", limit=10))
    sql = captured["sql"]
    assert "item_code_bare = ANY($1::text[])" in sql
    assert captured["params"][0] == ["7290000066134"]
    assert captured["params"][1] == "cerberus:ramilevi"        # account → key
    assert "is_current" in sql and "city_code = $" in sql
    assert sql.endswith("LIMIT 11 OFFSET 0")                   # one extra = has_more
    assert out["rows"][0]["item_price"] == 3.9                 # Decimal → float
    assert "LIMIT 10" in out["console_sql"] and "$" not in out["console_sql"]


def test_store_id_matches_both_paddings(captured):
    run(pu.query(None, "prices_stores", filters={"store_id": "019"}))
    assert "store_id = ANY($1::text[])" in captured["sql"]
    assert captured["params"][0] == ["019", "19"]


def test_on_date_replaces_current(captured):
    run(pu.query(None, "prices_market", filters={"item_code": "1"}, on_date="2026-09-01"))
    sql = captured["sql"]
    assert "first_seen_date <= $2" in sql and "last_seen_date >= $2" in sql
    assert "is_current" not in sql.split("WHERE", 1)[1]


def test_unknown_columns_and_orders_are_refused(captured):
    with pytest.raises(ValueError):
        run(pu.query(None, "prices_stores", filters={"nope": "1"}))
    with pytest.raises(ValueError):
        run(pu.query(None, "prices_stores", order="store_name; drop"))
    with pytest.raises(ValueError):
        run(pu.query(None, "no_such_table"))


def test_coverage_has_no_state_filter(captured):
    run(pu.query(None, "prices_coverage", filters={"store_id": "1"}))
    assert "is_current" not in captured["sql"]


def test_text_search_on_stores(captured):
    run(pu.query(None, "prices_stores", q="קניון עזריאלי"))
    sql = captured["sql"]
    assert sql.count("ILIKE") == 6                               # 2 terms × 3 columns
