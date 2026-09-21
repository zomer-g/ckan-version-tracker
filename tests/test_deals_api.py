"""Route- and filter-level tests for /api/deals — the מיסוי מקרקעין register.

No Postgres: ``deals_query._fetch`` is monkeypatched, which is enough to
exercise the limiter, the validation layer, the envelope and — the part with the
most room to go wrong — the SQL the filter builds. What is pinned here:

  * every route is rate-limited,
  * an unset filter contributes NOTHING to the WHERE clause (a `$n IS NULL OR …`
    would defeat the very index this project adds),
  * ordering and date ranges go through the same immutable expression the index
    is built on, so the two cannot drift apart,
  * the count is capped rather than exact, and says so,
  * the caveats travel with the data.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.deals import router as deals_router
from app.rate_limit import limiter
from app.services import deals_query, nadlan_index, nadlan_query
from app.services.nadlan_index import DEAL_SORT_KEY

# The corpus is DISCOVERED, not named (nadlan_index.find_deals_table), so a test
# that stubs only _fetch would still reach the database to ask where it is.
_DEALS_TABLE = ("public", "append_taxes_nadlan_full_f41fb496_fd06f5ae")

_ROW = {
    "settlement_code": "7900", "settlement": "פתח תקווה", "gush": "6319",
    "chelka": "225", "sub_chelka": "007", "deal_date": "23/04/2025",
    "deal_amount": "2450000", "declared_amount": "2450000",
    "deal_nature": "דירה בבית קומות", "portion": "1.000", "year_built": "1998",
    "asset_area": "104", "room_num": "4",
}


@pytest.fixture
def seen(monkeypatch):
    """Capture every statement the router causes, with its parameters."""
    captured: list[tuple[str, tuple]] = []

    async def _fetch(sql, *args, **kw):
        captured.append((sql, args, kw))
        if " count(*) AS n " in sql:
            return [{"n": 42}]
        if "btrim(settlement) AS settlement" in sql:
            return [{"settlement": "פתח תקווה", "settlement_code": "7900",
                     "deals": 3, "last_deal": "20250423"}]
        if "GROUP BY" in sql and "substr(deal_date, 7, 4)" in sql:
            return [{"year": "2025", "deals": 3, "median_amount": 2_450_000,
                     "median_area": 104}]
        if "GROUP BY" in sql:
            return [{"nature": "דירה בבית קומות", "deals": 3,
                     "median_amount": 2_450_000}]
        return [_ROW]

    async def _table():
        return _DEALS_TABLE

    monkeypatch.setattr(deals_query, "_fetch", _fetch)
    monkeypatch.setattr(nadlan_index, "deals_table", _table)
    deals_query.invalidate_cache()
    return captured


@pytest.fixture
def client(monkeypatch, seen):
    async def _ready():
        return True

    monkeypatch.setattr(deals_query, "is_ready", _ready)
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(deals_router)
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _sql(seen, needle: str) -> str:
    matches = [s for s, _, _ in seen if needle in s]
    assert matches, f"no statement containing {needle!r}"
    return matches[0]


# ── invariants ────────────────────────────────────────────────────────────────
def test_every_route_has_a_rate_limit():
    unlimited = [
        f"{sorted(r.methods)} {r.path}"
        for r in deals_router.routes if isinstance(r, APIRoute)
        if not limiter._route_limits.get(f"{r.endpoint.__module__}.{r.endpoint.__name__}")
    ]
    assert unlimited == [], f"endpoints missing @limiter.limit: {unlimited}"


def test_503_until_the_register_has_been_scraped(monkeypatch):
    async def _not_ready():
        return False

    monkeypatch.setattr(deals_query, "is_ready", _not_ready)
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(deals_router)
    limiter.reset()
    assert TestClient(app, raise_server_exceptions=False) \
        .get("/api/deals/search").status_code == 503


def test_the_data_is_not_presented_as_processed(client):
    """נדל"ן לעם derives a crosswalk; this project passes one publisher's rows
    through. Claiming otherwise would be the wrong kind of honest."""
    body = client.get("/api/deals/search").json()
    assert body["processed"] is False
    assert len(body["caveats"]) == 4
    assert any("תת-גוש" in c for c in body["caveats"])
    assert any("חציון" in c for c in body["caveats"])
    assert any("portion" in c for c in body["caveats"])


def test_price_per_sqm_is_normalised_by_the_share_sold():
    """The area is the whole asset's and the amount pays for the share only, so
    half a flat must price per sqm like a whole one, not at half of it."""
    from app.services.nadlan_query import _deal_row
    half = _deal_row({"deal_amount": "1000000", "asset_area": "80", "portion": "0.500"})
    assert half["portion_fraction"] == 0.5
    assert half["price_per_sqm"] == 12500
    assert half["price_per_sqm_normalized"] == 25000
    # A share rounded to 0.000, or no area, gets no figure rather than a crash.
    assert _deal_row({"deal_amount": "5", "asset_area": "80",
                      "portion": "0.000"})["price_per_sqm_normalized"] is None
    assert _deal_row({"deal_amount": "5", "asset_area": "0",
                      "portion": "1.000"})["price_per_sqm_normalized"] is None


# ── the filter ────────────────────────────────────────────────────────────────
def test_an_unset_filter_is_absent_from_the_sql(client, seen):
    """Not `$n IS NULL OR settlement = $n` — that form cannot use an index."""
    client.get("/api/deals/search")
    sql = _sql(seen, "ORDER BY")
    assert "WHERE true" in sql
    assert "settlement" not in sql.split("WHERE")[1].split("ORDER BY")[0]


def test_each_filter_becomes_a_parameter_never_an_inlined_value(client, seen):
    client.get("/api/deals/search?settlement=פתח תקווה&gush=6319&helka=225"
               "&nature=דירה בבית קומות&min_amount=1000000")
    sql, args = next((s, a) for s, a, _ in seen if "ORDER BY" in s)
    assert "פתח תקווה" not in sql and "6319" not in sql
    assert "פתח תקווה" in args and "6319" in args and 1_000_000 in args


def test_a_date_range_is_compared_on_the_indexed_expression(client, seen):
    """``to_date`` is only STABLE, so neither an expression index nor a correct
    ordering is available through it — both go through the same ``substr``."""
    client.get("/api/deals/search?date_from=2020-01-01&date_to=2024-12-31")
    sql, args = next((s, a) for s, a, _ in seen if "ORDER BY" in s)
    assert sql.count(DEAL_SORT_KEY) >= 3           # two bounds + the ORDER BY
    assert "20200101" in args and "20241231" in args
    assert "to_date(" not in sql


def test_the_sub_parcel_is_zero_padded_to_the_published_form(client, seen):
    """The register writes תת-חלקה as three digits ('007'); a caller typing 7
    must not silently match nothing."""
    client.get("/api/deals/search?sub_parcel=7")
    _, args = next((s, a) for s, a, _ in seen if "ORDER BY" in s)
    assert "007" in args


def test_an_empty_amount_cannot_crash_the_sort(client, seen):
    """Every numeric column parses today (measured on all 3.84 M rows), but a
    publisher change must degrade, not 500."""
    client.get("/api/deals/search?sort=amount_desc&min_amount=1")
    sql = _sql(seen, "ORDER BY")
    assert "nullif(deal_amount, '')::bigint" in sql
    assert "deal_amount::bigint" not in sql


def test_the_chart_and_the_table_describe_the_same_rows(client, seen):
    """/series and /breakdown take the identical filter, so a chart can never
    summarise a different population than the listing beside it."""
    q = "settlement=פתח תקווה&nature=דירה בבית קומות&date_from=2020-01-01"
    client.get(f"/api/deals/search?{q}")
    table_args = next(a for s, a, _ in seen if "ORDER BY" in s and "GROUP BY" not in s)
    seen.clear()
    client.get(f"/api/deals/series?{q}")
    series_args = next(a for s, a, _ in seen if "GROUP BY" in s)
    assert table_args == series_args


# ── paging and counting ───────────────────────────────────────────────────────
def test_the_total_is_capped_and_says_so(client, monkeypatch, seen):
    async def _fetch(sql, *args, **kw):
        if " count(*) AS n " in sql:
            return [{"n": deals_query.COUNT_CAP + 1}]
        return [_ROW]

    monkeypatch.setattr(deals_query, "_fetch", _fetch)
    body = client.get("/api/deals/search").json()
    assert body["total"] == deals_query.COUNT_CAP
    assert body["total_capped"] is True


def test_the_count_query_stops_at_the_cap(client, seen):
    client.get("/api/deals/search")
    assert f"LIMIT {deals_query.COUNT_CAP + 1}" in _sql(seen, " count(*) AS n ")


def test_a_row_comes_back_typed_and_linked_to_its_source(client):
    body = client.get("/api/deals/search").json()
    row = body["data"][0]
    assert row["date"] == "2025-04-23" and row["date_src"] == "23/04/2025"
    assert row["amount"] == 2_450_000 and row["rooms"] == 4
    assert row["gush"] == "6319" and row["sub_parcel"] == "007"
    assert body["row_url"].startswith("https://www.over.org.il/data?q=")


# ── validation ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("url", [
    "/api/deals/search?limit=9999",              # unbounded page
    "/api/deals/search?sort=amount",             # not a sort mode
    "/api/deals/search?date_from=2020",          # not a date
    "/api/deals/search?gush=abc",                # not a gush
    "/api/deals/parcel/abc/225",                 # not a gush
])
def test_bad_input_is_refused_before_the_database(client, url):
    assert client.get(url).status_code == 422


# ── the parcel view, shared with נדל"ן לעם ────────────────────────────────────
def test_the_parcel_route_reuses_the_nadlan_reader(client, monkeypatch):
    """The same list the property page shows, from the same function, so the
    two surfaces cannot disagree about one parcel's history."""
    async def _deals(gush, helka, limit=50, offset=0, sub_parcel=None):
        assert (gush, helka, sub_parcel) == (6319, 225, "007")
        return [{"date": "2025-04-23", "amount": 2_450_000}], 143

    monkeypatch.setattr(nadlan_query, "parcel_deals", _deals)
    body = client.get("/api/deals/parcel/6319/225?sub_parcel=7").json()
    assert body["total"] == 143 and body["data"][0]["amount"] == 2_450_000


# ── the cached pick lists ─────────────────────────────────────────────────────
def test_the_settlement_list_is_cached(client, seen):
    """A whole-table aggregate over 3.84 M rows that only moves when the scraper
    appends — it must not run once per page load."""
    assert client.get("/api/deals/settlements").status_code == 200
    body = client.get("/api/deals/settlements").json()
    assert body["count"] == 1 and body["data"][0]["last_deal"] == "2025-04-23"
    assert len([s for s, _, _ in seen if "GROUP BY" in s]) == 1


def test_the_settlement_list_is_grouped_by_name_not_code(client, seen):
    """674,340 rows carry an empty settlement_code and only 2,171 lack a name,
    so the name is the key and the code is the optional extra."""
    client.get("/api/deals/settlements")
    sql = _sql(seen, "GROUP BY")
    assert "btrim(settlement) AS settlement" in sql
    assert "GROUP BY 1" in sql


def test_the_cached_whole_table_aggregates_get_a_longer_ceiling(client, seen):
    """stats() counts DISTINCT (gush||'-'||chelka) over 3.84 M rows and came in
    just over the 8s browse budget — it 500'd on the first production deploy.
    It is cached, so it runs a few times an hour, and the browse budget stays
    tight: a browse that needs longer is a missing index."""
    client.get("/api/deals/stats")
    stats_kw = next(kw for s, _, kw in seen if "max(scraped_at)" in s)
    assert stats_kw["timeout_ms"] == deals_query._AGGREGATE_TIMEOUT_MS

    seen.clear()
    client.get("/api/deals/search?settlement=חיפה")
    browse_kw = next(kw for s, _, kw in seen if "ORDER BY" in s)
    assert browse_kw == {}, "the browse keeps the tight default"
