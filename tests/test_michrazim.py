"""מכרזי רמ"י — the land-tenders register as one corpus.

No Postgres: table discovery and ``michrazim._fetch`` are stubbed. Pinned here:

  * each table of the register is found by its column signature, and no table
    of the register matches two roles — the columns are the ones the scraper
    publishes (govil-scraper, michrazim/_engine.py *_COLUMNS);
  * the SQL quotes values (a settlement like ג'ת), keeps lots current
    (last_seen IS NULL), keys a lot on (tender, lot), and joins the tenders
    table only when it is loaded;
  * a filter that needs the tenders table says so instead of answering empty;
  * the per-sqm rule is in the SQL, and amount_per_unit is derived only from
    lot-price lots;
  * the parcel cube of נדל"ן לעם and the routes and MCP tools are wired to it.
"""
import os
import re

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.services import michrazim, nadlan_layers

# The scraper's published columns, verbatim.
TENDER_COLUMNS = [
    "מזהה מכרז", "מספר מכרז", "סוג מכרז", "סטטוס", "סטטוס בסיסי", "מרחב",
    "יישוב", "סמל יישוב", "שכונה", "ייעוד", "יחידות דיור", "תאריך פרסום",
    "תאריך פתיחה", "מועד אחרון להגשת הצעות", "תאריך ועדת מכרזים",
    "תאריך הגרלה", "תאריך סיור", "קישור למכרז", "geometry_wkt",
]
LOT_COLUMNS = [
    "מזהה מכרז", "מספר מכרז", "מזהה מתחם", "שם מתחם", 'שטח (מ"ר)',
    "שטח בנייה", 'קיבולת (יח"ד)', "הוצאות פיתוח", "מחיר סף", "מחיר שומה",
    "סכום ערבות", "שם זוכה", "סכום זכייה", "תמורה מופחתת", "מחיר מקסימום",
    "מספר הצעות", "גוש/חלקה", "תוכניות ומגרשים", "מספר מגרש (מפה)", "שטח (מפה)",
    'יח"ד (מפה)', "חוזה (מפה)", "סטטוס מכרז", "יישוב", "קישור למכרז",
    "גאומטריה (רשת ישראל החדשה)", "geometry_wkt",
]
LOT_BID_COLUMNS = ["מזהה מכרז", "מספר מכרז", "מזהה מתחם", "שם מתחם", "מזהה הצעה",
                   "סכום הצעה", "תוספת לתמורה", "מצב ההצעה", "קוד מצב ההצעה"]
TENDER_BID_COLUMNS = ["מזהה מכרז", "מספר מכרז", "מזהה מציע", "מזהה הצעה", "סכום הצעה",
                      "מספר קדימות", "מספר המתנה", "תיאור קדימות", "תיאור המתנה",
                      "הצעה פסולה", "יחידות דיור", "מספר שותפים",
                      "מספרי זהות של השותפים", "ShutafimMezaheIshiyut"]
PARCEL_COLUMNS = ["מזהה מכרז", "מספר מכרז", "מזהה מתחם", "שם מתחם", "גוש", "חלקה",
                  "מזהה רשומה"]
PLAN_COLUMNS = ["מזהה מכרז", "מספר מכרז", "מזהה מתחם", "שם מתחם", "תוכנית", "מגרש",
                "מזהה רשומה"]
DOCUMENT_COLUMNS = ["מפתח מסמך", "מזהה מכרז", "מספר מכרז", "מזהה מסמך", "סוג פרסום",
                    "תיאור", "שם קובץ", "סוג קובץ", "גודל (בתים)", "תאריך עדכון",
                    "מהות מסמך", "קישור", "קישור למכרז"]

P = "append_michrazim_register_33ed4e63_b252fc31_"
_ARCHIVE = ["first_seen", "last_seen"]
TABLES = {
    P[:-1]: TENDER_COLUMNS + _ARCHIVE,
    P + "d849f194": LOT_COLUMNS + _ARCHIVE + ["geom"],
    P + "abbf6b55": LOT_BID_COLUMNS + _ARCHIVE,
    P + "6a9219cd": TENDER_BID_COLUMNS + _ARCHIVE,
    P + "8302b80d": PARCEL_COLUMNS + _ARCHIVE,
    P + "ba6004b3": PLAN_COLUMNS + _ARCHIVE,
    P + "19163596": DOCUMENT_COLUMNS + _ARCHIVE,
}
ROLE_OF = {P[:-1]: "tenders", P + "d849f194": "lots", P + "abbf6b55": "lot_bids",
           P + "6a9219cd": "tender_bids", P + "8302b80d": "parcels",
           P + "ba6004b3": "plans", P + "19163596": "documents"}


def _tb(without=()):
    return {ROLE_OF[t]: ("public", t, cols) for t, cols in TABLES.items()
            if ROLE_OF[t] not in without}


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    michrazim.invalidate_cache()
    nadlan_layers.invalidate_caches()

    async def _no_catalog():
        return None

    monkeypatch.setattr(michrazim, "_catalog_tables", _no_catalog)
    yield
    michrazim.invalidate_cache()


def _stub_tables(monkeypatch, tables=TABLES):
    async def _cols(schema):
        return {t: [{"name": c} for c in cols] for t, cols in tables.items()} \
            if schema == "public" else {}

    async def _est(table, schema="public"):
        return 1000

    monkeypatch.setattr(michrazim.append_store, "schema_table_columns", _cols)
    monkeypatch.setattr(michrazim.append_store, "table_count_estimate", _est)


# ── finding the tables ────────────────────────────────────────────────────────
def test_every_table_matches_exactly_one_role():
    for table, cols in TABLES.items():
        hits = [r for r, sig in michrazim.SIGNATURES.items() if sig <= set(cols)]
        assert hits == [ROLE_OF[table]], (table, hits)


def test_the_larger_of_two_candidates_wins():
    stale = "append_old_copy_aaaaaaaa_d849f194"
    found = michrazim.match_roles(
        {P + "d849f194": LOT_COLUMNS, stale: LOT_COLUMNS},
        {P + "d849f194": 51_752, stale: 10})
    assert found["lots"][0] == P + "d849f194"


@pytest.mark.asyncio
async def test_tables_are_found_by_signature_not_name(monkeypatch):
    _stub_tables(monkeypatch, {"append_anything_12345678": LOT_COLUMNS + _ARCHIVE,
                               "append_unrelated_x": ["a", "b"]})
    tb = await michrazim.tables()
    assert set(tb) == {"lots"}
    assert tb["lots"][1] == "append_anything_12345678"
    assert await michrazim.is_ready()


@pytest.mark.asyncio
async def test_after_the_split_each_table_answers_from_its_own_dataset(monkeypatch):
    """The first version held every table in one dataset (b252fc31). After the
    split each table is a dataset of its own; while both exist the per-table
    dataset answers, and a table the catalog no longer lists (a superseded
    version's) never does."""
    old_lots, new_lots = P + "d849f194", "append_michrazim_lots_1a2b3c4d_aaaaaaaa"
    stale_plans = P + "ba6004b3"
    _stub_tables(monkeypatch, {old_lots: LOT_COLUMNS + _ARCHIVE,
                               new_lots: LOT_COLUMNS + _ARCHIVE,
                               P + "8302b80d": PARCEL_COLUMNS + _ARCHIVE,
                               stale_plans: PLAN_COLUMNS + _ARCHIVE})

    async def _catalog():
        return {old_lots: (michrazim.DATASET_ID, 2), P + "8302b80d": (michrazim.DATASET_ID, 2),
                new_lots: ("1a2b3c4d-0000-4000-8000-000000000000", 1)}

    monkeypatch.setattr(michrazim, "_catalog_tables", _catalog)
    tb = await michrazim.tables()
    assert tb["lots"][1] == new_lots
    assert "plans" not in tb                      # not in any current version
    assert michrazim.dataset_of("lots").startswith("1a2b3c4d")
    assert michrazim.dataset_of("parcels") == michrazim.DATASET_ID
    links = await michrazim.dataset_links()
    assert links["lots"].endswith("/versions/1a2b3c4d-0000-4000-8000-000000000000")


# ── SQL ───────────────────────────────────────────────────────────────────────
def test_values_are_quoted_and_lots_are_current():
    sql, _ = michrazim.lots_sql({"settlement": "ג'ת", "year_from": 2015, "gush": 20993,
                                 "helka": 41}, _tb(), limit=10, offset=0, sort="year_desc")
    assert "'ג''ת'" in sql                       # the quote is doubled
    assert "l.last_seen IS NULL" in sql and "p.last_seen IS NULL" in sql
    # a lot is (tender, lot): a re-marketed lot shares its lot id
    assert 'p."מזהה מכרז" = l."מזהה מכרז" AND p."מזהה מתחם" = l."מזהה מתחם"' in sql
    assert "p.\"גוש\" = '20993' AND p.\"חלקה\" = '41'" in sql
    assert "public.over_settlement_code(l.\"יישוב\") = public.over_settlement_code('ג''ת')" in sql
    assert "left(l.\"מזהה מכרז\", 4) >= '2015'" in sql


def test_numbers_are_parsed_defensively():
    sql, _ = michrazim.lots_sql({}, _tb(), limit=1, offset=0, sort="year_desc")
    # never a bare ::numeric — '1,2' and '[12' are in the register
    assert not re.search(r'"\s*::numeric', sql.replace('btrim(l."', ""))
    assert "~ '^-?[0-9]+(\\.[0-9]+)?$'" in sql


def test_the_tenders_table_is_joined_only_when_loaded():
    with_t, notes_t = michrazim.lots_sql({}, _tb(), limit=1, offset=0, sort="year_desc")
    without, notes = michrazim.lots_sql({}, _tb(without=("tenders",)), limit=1, offset=0,
                                        sort="year_desc")
    assert "LEFT JOIN" in with_t and 'AS tender_type' in with_t and not notes_t
    assert "LEFT JOIN" not in without and "tender_type" not in without
    assert notes and "טבלת המכרזים" in notes[0]


def test_a_tender_type_filter_without_the_table_explains_itself():
    with pytest.raises(ValueError, match="טבלת המכרזים"):
        michrazim.where_clause({"tender_type": "מחיר למשתכן"}, _tb(without=("tenders",)))
    raw, _, _ = michrazim.where_clause({"tender_type": "מחיר למשתכן"}, _tb())
    assert "t.\"סוג מכרז\" = 'מחיר למשתכן'" in raw


def test_amount_basis_rule_and_per_unit_only_for_lot_prices():
    assert f"winning_amount < {michrazim.PER_SQM_MAX}" in michrazim.AMOUNT_BASIS_SQL
    assert "coalesce(units, 0) >= 2 OR max_price IS NOT NULL" in michrazim.AMOUNT_BASIS_SQL
    assert "NOT LIKE 'אין הצעות%'" in michrazim.AMOUNT_BASIS_SQL
    assert "amount_basis = 'lot_total' AND units > 0" in michrazim.DERIVED_SQL
    with pytest.raises(ValueError):
        michrazim.where_clause({"amount_basis": "x"}, _tb())


def test_series_takes_medians_and_separates_the_two_kinds_of_amount():
    sql, _ = michrazim.series_sql({"won_only": True}, _tb())
    assert "avg(" not in sql.lower()
    assert "median_amount_per_unit" in sql and "median_sqm_price" in sql
    assert "FILTER (WHERE amount_basis = 'per_sqm_suspected')" in sql


def test_index_names_fit_postgres():
    specs = michrazim.index_specs(_tb())
    assert {s[2] for s in specs} and all(len(s[2].encode()) <= 63 for s in specs)
    assert len({s[2] for s in specs}) == len(specs)


@pytest.mark.asyncio
async def test_tender_never_returns_bidder_id_numbers(monkeypatch):
    _stub_tables(monkeypatch)
    seen = []

    async def _fetch(sql):
        seen.append(sql)
        if "FROM y" in sql and "count(*)" in sql:
            return [{"n": 1}]
        if "FROM y" in sql:
            return [{"tender_id": "20240134", "lot_id": "1", "units": 104}]
        return []

    monkeypatch.setattr(michrazim, "_fetch", _fetch)
    out = await michrazim.tender(20240134)
    assert out["lots"][0]["units"] == 104
    bids_sql = next(s for s in seen if P + "6a9219cd" in s)
    for col in michrazim.PERSONAL_COLUMNS:
        assert f'"{col}"' not in bids_sql


# ── נדל"ן לעם cube ────────────────────────────────────────────────────────────
def test_the_cube_is_registered_with_its_grain():
    c = nadlan_layers.cube(nadlan_layers.TENDERS_ID)
    assert c and c["grain"] == "gush_helka" and c["datasets"] == [michrazim.DATASET_ID]


def test_a_lot_becomes_a_labelled_feature_with_a_link():
    f = nadlan_layers.tender_lot_feature({
        "tender_number": "134/2024", "year": "2024", "units": 104,
        "winning_amount": 3183131, "amount_per_unit": 30607, "winner": "א.ש. אלפא",
        "tender_url": "https://apps.land.gov.il/MichrazimSite/#/michraz/20240134",
        "amount_basis": "lot_total", "max_price": None})
    by = {x["key"]: x for x in f["fields"]}
    assert by["winning_amount"]["value"] == "3,183,131"
    assert by["year"]["value"] == "2024"
    assert by["tender_url"]["url"].endswith("/20240134")
    assert "max_price" not in by and "amount_basis" not in by

    sq = nadlan_layers.tender_lot_feature({"winning_amount": 8326, "units": 218,
                                           "amount_basis": "per_sqm_suspected"})
    assert sq["fields"][0]["key"] == "amount_basis"


def _nadlan_app():
    from app.api.nadlan import router
    from app.rate_limit import limiter
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def test_the_parcel_route_serves_the_tenders_cube(monkeypatch):
    from app.api import nadlan as api
    from app.services import nadlan_query

    async def _ready():
        return True

    async def _parcel(g, h, s):
        return {"parcel_key": "20993-0-41", "gush": 20993, "parcel": 41}

    async def _cube(parcel):
        return {"available": True, "features": [], "count": 0, "notes": ["n"]}

    monkeypatch.setattr(nadlan_query, "is_ready", _ready)
    monkeypatch.setattr(api.nadlan_layers, "parcel_row", _parcel)
    monkeypatch.setattr(api.nadlan_layers, "tenders_cube", _cube)
    r = _nadlan_app().get("/api/nadlan/parcel/20993/41/layer/rmi_tender_lots")
    assert r.status_code == 200, r.text
    assert r.json()["cube"]["grain"] == "gush_helka" and r.json()["notes"] == ["n"]


# ── REST + MCP ────────────────────────────────────────────────────────────────
def _app():
    from app.api.michrazim import router
    from app.rate_limit import limiter
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def test_routes_answer_503_before_the_register_is_loaded(monkeypatch):
    _stub_tables(monkeypatch, {})
    r = _app().get("/api/michrazim/lots")
    assert r.status_code == 503


def test_lots_route_passes_the_filter_and_maps_a_bad_one_to_422(monkeypatch):
    _stub_tables(monkeypatch, {t: c for t, c in TABLES.items() if ROLE_OF[t] != "tenders"})
    seen = {}

    async def _search(f, limit, offset, sort):
        seen.update(f=f, limit=limit, sort=sort)
        if f.get("tender_type"):
            raise ValueError("הסינון לפי סוג מכרז דורש את טבלת המכרזים")
        return {"data": [], "total": 0, "total_capped": False, "limit": limit,
                "offset": offset, "console_sql": "SELECT 1", "row_url": "u", "notes": []}

    monkeypatch.setattr(michrazim, "search_lots", _search)
    c = _app()
    r = c.get("/api/michrazim/lots", params={"settlement": "שהם", "won_only": "true",
                                             "sort": "per_unit_desc", "limit": 5})
    assert r.status_code == 200, r.text
    assert seen["f"] == {"settlement": "שהם", "won_only": True}
    assert r.json()["caveats"] == michrazim.CAVEATS and r.json()["processed"] is False
    assert c.get("/api/michrazim/lots", params={"tender_type": "x"}).status_code == 422
    assert c.get("/api/michrazim/lots", params={"sort": "drop table"}).status_code == 422


def test_mcp_tools_are_declared_and_implemented():
    from app.mcp import deals_server, nadlan_server
    for srv in (deals_server, nadlan_server):
        assert sorted(srv._IMPL) == sorted(t["name"] for t in srv.TOOLS)
    names = {t["name"] for t in deals_server.TOOLS}
    assert {"search_tender_lots", "tender_price_series", "get_tender",
            "parcel_tenders", "list_tender_settlements"} <= names
    assert "parcel_tenders" in {t["name"] for t in nadlan_server.TOOLS}
    assert "amount_basis" in deals_server.SERVER_INSTRUCTIONS


def test_mcp_filters_drop_unset_and_false():
    from app.mcp.deals_server import _tender_filters
    assert _tender_filters({"settlement": "שהם", "won_only": False, "gush": None,
                            "year_from": 2020, "junk": 1}) == {"settlement": "שהם",
                                                               "year_from": 2020}
