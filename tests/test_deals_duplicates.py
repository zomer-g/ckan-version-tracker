"""Execute the deal readers on small fixtures without a running Postgres.

SQLite supports the null-safe comparisons used for duplicate detection. The
adapter removes PostgreSQL's text/int casts for the shared parcel reader only.
No filtering or deduplication is implemented by the test double.
"""
import asyncio
import os
import re
import sqlite3

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services import deals_query, nadlan_index, nadlan_query


ROW = {
    "settlement_code": "7900", "settlement": "פתח תקווה", "gush": "6319",
    "chelka": "225", "sub_chelka": "007", "deal_date": "23/04/2025",
    "deal_amount": "2450000", "declared_amount": "2450000",
    "deal_nature": "דירה בבית קומות", "portion": "1.000", "year_built": "1998",
    "asset_area": "104", "room_num": "4",
}


@pytest.fixture
def register(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.create_function("btrim", 1, lambda v: v.strip() if v is not None else None)
    conn.execute("ATTACH DATABASE ':memory:' AS public")
    columns = ", ".join(f'"{c}" TEXT' for c in ROW)
    conn.execute(f"CREATE TABLE public.deals ({columns})")

    async def source():
        return "public", "deals"

    async def fetch(sql, *args, **kwargs):
        sql = re.sub(r"::(?:text|int)\b", "", sql)
        params = {str(i): value for i, value in enumerate(args, 1)}
        return [dict(r) for r in conn.execute(sql, params)]

    async def addresses(pairs):
        return {}

    monkeypatch.setattr(nadlan_index, "deals_table", source)
    monkeypatch.setattr(deals_query, "_fetch", fetch)
    monkeypatch.setattr(nadlan_query, "_fetch", fetch)
    monkeypatch.setattr(deals_query, "addresses_for", addresses)
    deals_query.invalidate_cache()
    yield conn
    conn.close()


def insert(conn, *rows):
    columns = ", ".join(ROW)
    placeholders = ", ".join("?" for _ in ROW)
    conn.executemany(f"INSERT INTO public.deals ({columns}) VALUES ({placeholders})",
                     [[row[c] for c in ROW] for row in rows])


def search(**filters):
    return asyncio.run(deals_query.search(filters))


@pytest.mark.parametrize("missing_code", ["", None, "   "])
def test_search_counts_a_code_missing_copy_once(register, missing_code):
    insert(register, ROW, ROW | {"settlement_code": missing_code})
    result = search(settlement="פתח תקווה")
    assert result["total"] == 1
    assert len(result["data"]) == 1
    assert result["data"][0]["settlement_code"] == "7900"
    # This is a read filter. Both original rows remain in the archive.
    assert register.execute("SELECT count(*) FROM public.deals").fetchone()[0] == 2


def test_a_code_missing_row_without_a_copy_is_kept(register):
    insert(register, ROW | {"settlement_code": ""})
    result = search()
    assert result["total"] == 1
    assert result["data"][0]["settlement_code"] is None


@pytest.mark.parametrize("column, other", [
    ("settlement", "חיפה"), ("gush", "6320"), ("chelka", "226"),
    ("sub_chelka", "008"), ("deal_date", "24/04/2025"),
    ("deal_amount", "5008000"), ("declared_amount", "8445555"),
    ("deal_nature", "דירת גן"), ("portion", "0.500"),
    ("year_built", "1999"), ("asset_area", "105"), ("room_num", "5"),
])
def test_any_other_difference_preserves_both_rows(register, column, other):
    insert(register, ROW, ROW | {"settlement_code": "", column: other})
    assert search()["total"] == 2


def test_matching_null_source_fields_still_identify_the_copy(register):
    row = ROW | {"room_num": None, "year_built": None}
    insert(register, row, row | {"settlement_code": ""})
    assert search()["total"] == 1


def test_a_null_and_an_empty_source_value_are_not_assumed_equal(register):
    insert(register, ROW | {"room_num": None},
           ROW | {"settlement_code": "", "room_num": ""})
    assert search()["total"] == 2


def test_two_nonempty_settlement_codes_are_not_deduplicated(register):
    insert(register, ROW, ROW | {"settlement_code": "4000"})
    assert search()["total"] == 2


def test_split_amounts_remain_separate(register):
    row = ROW | {"deal_amount": "5008000", "declared_amount": "8445555"}
    remainder = row | {"deal_amount": "3437555"}
    insert(register, row, remainder,
           row | {"settlement_code": ""}, remainder | {"settlement_code": ""})
    result = search()
    assert result["total"] == 2
    assert sorted(r["amount"] for r in result["data"]) == [3437555, 5008000]


def test_deduplication_happens_before_paging(register):
    insert(register, ROW, ROW | {"settlement_code": ""},
           ROW | {"deal_date": "22/04/2025", "sub_chelka": "008"})
    result = asyncio.run(deals_query.search({}, limit=1, offset=1))
    assert result["total"] == 2
    assert result["data"][0]["sub_parcel"] == "008"


def test_console_link_returns_the_same_rows_as_search(register):
    insert(register, ROW, ROW | {"settlement_code": ""})
    result = search(settlement="פתח תקווה")
    rows = [dict(r) for r in register.execute(result["console_sql"])]
    assert len(rows) == 1
    assert rows[0]["settlement_code"] == "7900"


def test_shared_parcel_reader_deduplicates_its_rows_and_total(register):
    insert(register, ROW, ROW | {"settlement_code": ""})
    rows, total = asyncio.run(nadlan_query.parcel_deals(6319, 225, sub_parcel="007"))
    assert total == 1
    assert len(rows) == 1
    assert rows[0]["settlement_code"] == 7900
