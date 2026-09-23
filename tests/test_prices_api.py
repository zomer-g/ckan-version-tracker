"""Tests for the retail-prices query layer, REST API and MCP.

The prices source is ~30 datasets (one per retailer) that this layer turns into
one market. What can go wrong quietly:

* a resource name that drifts from the worker's (``מחירים`` …) — the chain's
  table is never found, and the chain silently drops out of every comparison;
* a barcode written with or without leading zeros matching in one chain only;
* a city filter that keeps stores of another town, or drops a chain that
  writes its cities as names instead of codes;
* a basket ranking that calls a store cheapest because it lacks half the items;
* "nothing published yet" reading as "no such product".

Plus the two platform changes the source needs: the append loader's stamp
column (``append_stamp_column``), and companions beyond eight.
"""
from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.mcp import prices_server as ps  # noqa: E402
from app.services import prices_query as pq  # noqa: E402
from app.services import sampling_runs  # noqa: E402


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# --------------------------------------------------------------------------
# Cross-repo contract
# --------------------------------------------------------------------------

def test_resource_names_match_the_workers():
    """Pinned against govscraper/scrapers/prices/_engine.py (T_PRICES …). A
    rename there starts a new, empty table; this is the tripwire."""
    assert pq.RESOURCES == {
        "prices": "מחירים", "products": "מוצרים", "promos": "מבצעים",
        "promo_items": "פריטי מבצעים", "stores": "סניפים", "coverage": "כיסוי יומי",
    }


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------

def test_bare_codes_strip_any_padding():
    assert pq.bare_codes("7290000066134") == ["7290000066134"]
    assert pq.bare_codes(["0001334277", "1334277", "0000001334277"]) == ["1334277"]
    assert pq.bare_codes("") == []
    assert pq.bare_codes("abc") == ["abc"]
    assert pq._item_match("p", 1) == "ltrim(p.item_code, '0') = ANY($1::text[])"


def test_city_filter_by_code_and_by_name_fallback():
    rows = [
        {"city_code": 4000, "store_name": "חיפה מרכז", "address": "", "city_name": "חיפה"},
        {"city_code": 5000, "store_name": "תל אביב", "address": "", "city_name": "תל אביב"},
        # City Market: no code, the town is in the store name.
        {"city_code": None, "store_name": "סיטי מרקט חיפה, הנביאים 3", "address": "",
         "city_name": None},
        {"city_code": None, "store_name": "סיטי מרקט אשקלון", "address": "", "city_name": None},
    ]
    kept = pq._city_filter(rows, "חיפה", 4000)
    assert [r["store_name"] for r in kept] == ["חיפה מרכז", "סיטי מרקט חיפה, הנביאים 3"]
    assert pq._city_filter(rows, None, None) == rows


def test_chain_name_is_the_title_after_the_dash():
    ds = SimpleNamespace(title="שקיפות מחירים — רמי לוי", scraper_config={"chain": "x"})
    assert pq.chain_name_of(ds) == "רמי לוי"


# --------------------------------------------------------------------------
# Comparisons (query layer stubbed)
# --------------------------------------------------------------------------

ENTRIES = [
    {"chain": "cerberus:ramilevi", "name": "רמי לוי", "dataset_id": "d1",
     "tables": {"prices": "t1", "stores": "s1"}},
    {"chain": "shufersal:shufersal", "name": "שופרסל", "dataset_id": "d2",
     "tables": {"prices": "t2", "stores": "s2"}},
]


def _price(chain, store, code, price, city=4000, name="במבה"):
    return {"chain": chain, "chain_name": "רמי לוי" if "rami" in chain else "שופרסל",
            "item_code": code, "item_name": name, "sub_chain_id": "1", "store_id": store,
            "item_price": price, "unit_of_measure_price": "", "first_seen_date": "2026-09-20",
            "price_update_time": "", "as_of": "2026-09-23", "store_name": f"סניף {store}",
            "address": "", "city_name": "חיפה", "city_code": city}


@pytest.fixture
def stubbed(monkeypatch):
    rows: list[dict] = []

    async def fake_require(db, chains_filter, role="prices"):
        return ENTRIES

    async def fake_current(entries, codes, *a, **k):
        return [r for r in rows if r["item_code"].lstrip("0") in codes]

    async def fake_city_code(city):
        return {"חיפה": 4000}.get(city)

    monkeypatch.setattr(pq, "_require", fake_require)
    monkeypatch.setattr(pq, "_current_prices", fake_current)
    monkeypatch.setattr(pq, "city_code", fake_city_code)
    return rows


def test_compare_prices_per_chain_stats(stubbed):
    stubbed += [
        _price("cerberus:ramilevi", "1", "7290000066134", "4.90"),
        _price("cerberus:ramilevi", "2", "7290000066134", "5.50"),
        _price("shufersal:shufersal", "7", "7290000066134", "6.20"),
        _price("shufersal:shufersal", "8", "7290000066134", "5.90", city=5000),
    ]
    out = run(pq.compare_prices(None, item_codes=["7290000066134", "123"], city="חיפה"))
    [item] = out["items"]
    assert [c["chain"] for c in item["chains"]] == ["cerberus:ramilevi", "shufersal:shufersal"]
    rami = item["chains"][0]
    assert (rami["stores"], rami["min_price"], rami["max_price"]) == (2, 4.9, 5.5)
    # The Tel Aviv store is outside the city filter.
    assert item["chains"][1]["stores"] == 1
    assert item["cheapest_stores"][0]["store_id"] == "1"
    assert out["not_found"] == ["123"]


def test_barcode_with_leading_zeros_matches_the_padded_spelling(stubbed):
    stubbed.append(_price("cerberus:ramilevi", "1", "0001334277", "3.00"))
    out = run(pq.compare_prices(None, item_codes=["1334277"]))
    assert out["items"] and out["not_found"] == []


def test_basket_ranks_completeness_before_total(stubbed):
    a, b = "7290000000001", "7290000000002"
    stubbed += [
        # Store 1 carries both items; store 9 is cheaper only because it lacks one.
        _price("cerberus:ramilevi", "1", a, "10.00"),
        _price("cerberus:ramilevi", "1", b, "10.00"),
        _price("shufersal:shufersal", "9", a, "5.00"),
    ]
    out = run(pq.compare_basket(None, basket=[{"item_code": a, "quantity": 2},
                                              {"item_code": b}], city="חיפה"))
    first = out["stores"][0]
    assert (first["store_id"], first["items_found"], first["total"]) == ("1", 2, 30.0)
    assert out["stores"][1]["items_missing"] == [b]


def test_basket_needs_a_city_or_chains(stubbed):
    with pytest.raises(ValueError):
        run(pq.compare_basket(None, basket=[{"item_code": "1"}]))


# --------------------------------------------------------------------------
# MCP
# --------------------------------------------------------------------------

def _user():
    return SimpleNamespace(id=None, client_id=None)


def test_mcp_lists_its_tools():
    msg = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    out = run(ps.handle_message(None, None, _user(), None, msg))
    names = {t["name"] for t in out["result"]["tools"]}
    assert names == set(ps._IMPL)
    assert {"compare_prices", "compare_basket", "search_products"} <= names


def test_mcp_initialize_carries_instructions():
    msg = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    out = run(ps.handle_message(None, None, _user(), None, msg))
    assert "item_code" in out["result"]["instructions"]


def test_not_collected_yet_is_said_plainly(monkeypatch):
    async def nothing(db, chains_filter, role="prices"):
        raise pq.NotCollectedYet("טרם נאסף")

    async def no_log(**kw):
        return None

    monkeypatch.setattr(pq, "_require", nothing)
    monkeypatch.setattr(ps, "log_usage", no_log)
    msg = {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
           "params": {"name": "compare_prices", "arguments": {"item_codes": ["1"]}}}
    out = run(ps.handle_message(None, None, _user(), None, msg))
    assert out["result"]["isError"] is True
    assert "טרם נאסף" in out["result"]["content"][0]["text"]


# --------------------------------------------------------------------------
# Platform changes the source relies on
# --------------------------------------------------------------------------

def test_stamp_column_from_scraper_config(monkeypatch):
    monkeypatch.setattr(sampling_runs, "sampling_spec", lambda ds: None)
    ds = SimpleNamespace(scraper_config={"append_stamp_column": "last_seen_date"})
    assert sampling_runs.stamp_column(ds) == "last_seen_date"
    assert sampling_runs.stamp_column(SimpleNamespace(scraper_config={})) is None


def test_sampling_spec_wins_over_the_config_stamp(monkeypatch):
    monkeypatch.setattr(sampling_runs, "sampling_spec",
                        lambda ds: {"item_key": "k", "sample_column": "sampled_at"})
    ds = SimpleNamespace(scraper_config={"append_stamp_column": "last_seen_date"})
    assert sampling_runs.stamp_column(ds) == "sampled_at"


def test_closing_row_updates_only_the_stamp():
    """The conflict clause that lets a closed state land on its open row."""
    from app.services import append_store

    cols = ["item_code", "item_price", "first_seen_date", "last_seen_date"]
    opened = {"item_code": "a", "item_price": "1", "first_seen_date": "d1",
              "last_seen_date": ""}
    closed = {**opened, "last_seen_date": "d2"}
    assert append_store.row_hash(opened, cols, exclude=("last_seen_date",)) == \
        append_store.row_hash(closed, cols, exclude=("last_seen_date",))
    sql, _ = append_store.build_insert("t", cols, [closed], key_col=None, keyless=True,
                                       stamp_col="last_seen_date")
    assert 'DO UPDATE SET "last_seen_date" = EXCLUDED."last_seen_date"' in sql


def test_a_manifest_may_open_many_companions():
    from app.services import source_registry as sr

    manifest = {
        "manifest_version": 1, "id": "manycompanions", "label_he": "x", "label_en": "x",
        "site_url": "https://example.org/",
        "badge": {"bg": "#fff", "fg": "#000", "accent": "#123"},
        "url_patterns": [{"regex": r"^https?://example\.org/$",
                          "companions": [f"https://example.org/c{i}" for i in range(33)]}],
    }
    assert len(sr.validate_manifest(manifest).url_patterns[0].companions) == 33
