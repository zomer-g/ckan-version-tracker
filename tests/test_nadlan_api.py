"""Route-level tests for /api/nadlan — the four ways into one answer.

No Postgres: app.services.nadlan_query is monkeypatched, which is enough to
exercise the limiter, the validation layer and the envelope shape. What is
pinned here:

  * every route is rate-limited (a future endpoint added without a decorator
    fails CI rather than shipping unlimited over a 4.58 GB table),
  * the input bounds hold — an unbounded radius or a malformed zip must be
    refused before it reaches the database,
  * the 503-before-build contract,
  * and the caveats travel WITH the data, so a caller cannot show the crosswalk
    without the coverage limits that qualify it.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.nadlan import router as nadlan_router
from app.rate_limit import limiter
from app.services import nadlan_index, nadlan_query


_PARCEL = {
    "parcel_key": "6319-0-225", "gush": 6319, "gush_suffix": 0, "parcel": 225,
    "gp_key": "6319-225", "gp_ambiguous": False, "settlement_code": 7900,
    "locality_name": "פתח תקווה", "reg_mun_name": None, "county_name": None,
    "region_name": None, "legal_area": 1234.0, "status_text": "מוסדר",
    "lat": 32.0789, "lon": 34.9171,
}


@pytest.fixture
def client(monkeypatch):
    async def _ready():
        return True

    monkeypatch.setattr(nadlan_query, "is_ready", _ready)
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(nadlan_router)
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _stub_lookup(monkeypatch, parcels=(_PARCEL,), addresses=()):
    # Only the DB layer is stubbed — property_envelope() itself runs for real,
    # so the envelope shape, the source deep-links and the confidence downgrade
    # are genuinely exercised rather than mocked away.
    async def _fetch(sql, *args):
        return []

    async def _deals_table():
        # The corpus is discovered, not named: without this the envelope would
        # reach the database to ask where it lives.
        return ("public", "append_taxes_nadlan_full_f41fb496_fd06f5ae")

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    monkeypatch.setattr(nadlan_index, "deals_table", _deals_table)

    async def _gush(g, h, s=None):
        return list(parcels)

    async def _point(lat, lon, r=0.0, limit=50):
        return list(parcels)

    async def _zip(z):
        return list(addresses), list(parcels)

    async def _addr(city, street, number=None):
        return list(addresses), list(parcels)

    monkeypatch.setattr(nadlan_query, "by_gush_helka", _gush)
    monkeypatch.setattr(nadlan_query, "by_point", _point)
    monkeypatch.setattr(nadlan_query, "by_zip", _zip)
    monkeypatch.setattr(nadlan_query, "by_address", _addr)


# ── invariants ────────────────────────────────────────────────────────────────
def _routes():
    return [r for r in nadlan_router.routes if isinstance(r, APIRoute)]


def test_every_route_has_a_rate_limit():
    unlimited = [
        f"{sorted(r.methods)} {r.path}"
        for r in _routes()
        if not limiter._route_limits.get(f"{r.endpoint.__module__}.{r.endpoint.__name__}")
    ]
    assert unlimited == [], f"endpoints missing @limiter.limit: {unlimited}"


def test_the_geometry_route_is_the_most_restricted():
    """It is the only response that reads the 4.58 GB source table."""
    def amount(name):
        return limiter._route_limits[f"app.api.nadlan.{name}"][0].limit.amount

    assert amount("nadlan_parcel_geometry") == 30
    assert amount("nadlan_parcel_geometry") < amount("nadlan_parcel")


# ── the four modes ────────────────────────────────────────────────────────────
def test_gush_helka_returns_the_envelope(client, monkeypatch):
    _stub_lookup(monkeypatch)
    r = client.get("/api/nadlan/parcel/6319/225")
    assert r.status_code == 200
    body = r.json()
    assert body["query"]["mode"] == "gush_helka"
    assert body["count"] == 1
    prop = body["data"][0]
    assert prop["parcel_key"] == "6319-0-225"
    assert prop["identity"]["settlement"]["name"] == "פתח תקווה"
    # Every source is represented, each with a link to its untouched full row.
    assert set(prop["sources"]) == {"parcels", "gazetteer", "postal", "address_list",
                                    "deals", "stat_area"}
    assert prop["sources"]["parcels"]["row_url"].startswith("https://www.over.org.il/data?q=")


def test_point_mode_shares_the_same_shape(client, monkeypatch):
    _stub_lookup(monkeypatch)
    r = client.get("/api/nadlan/point?lat=32.0789&lon=34.9171&radius_m=250")
    assert r.status_code == 200
    body = r.json()
    assert body["query"] == {"mode": "point", "lat": 32.0789, "lon": 34.9171,
                             "radius_m": 250.0, "radius_used": 250.0, "widened": False}
    assert body["data"][0]["parcel_key"] == "6319-0-225"


def test_zip_and_address_modes(client, monkeypatch):
    _stub_lookup(monkeypatch)
    assert client.get("/api/nadlan/zip/4935048").json()["query"]["mode"] == "zip"
    r = client.get("/api/nadlan/address?city=פתח תקווה&street=אבימלך&number=8")
    assert r.status_code == 200 and r.json()["query"]["mode"] == "address"


def test_caveats_travel_with_every_answer(client, monkeypatch):
    """The coverage limits are part of the response, not UI folklore."""
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/parcel/6319/225").json()
    assert body["processed"] is True
    assert len(body["caveats"]) == 5
    assert any("91" in c for c in body["caveats"])         # postal locality limit
    assert any("מספר בית" in c for c in body["caveats"])   # gazetteer street-only
    assert any("תת-גוש" in c for c in body["caveats"])     # deals key on gush+helka
    assert any("2011" in c for c in body["caveats"])       # the socio index division


def test_ambiguous_parcel_is_downgraded_not_hidden(client, monkeypatch):
    """The gazetteer publishes no תת-גוש; where that is ambiguous the answer
    must say so rather than silently present another parcel's data."""
    _stub_lookup(monkeypatch, parcels=({**_PARCEL, "gp_ambiguous": True},))
    prop = client.get("/api/nadlan/parcel/6319/225").json()["data"][0]
    assert prop["match"]["confidence"] == "approximate"
    assert any("תת-גוש" in n for n in prop["match"]["notes"])


# ── polygons on the map ───────────────────────────────────────────────────────
_POLY = '{"type":"Polygon","coordinates":[[[34.9,32.07],[34.91,32.07],[34.91,32.08],[34.9,32.07]]]}'


def test_geometry_is_off_by_default(client, monkeypatch):
    """The polygon lives in the 4.58 GB source table — never fetched unasked."""
    called = []

    async def _geoms(keys, simplify=None):
        called.append(list(keys))
        return {}

    monkeypatch.setattr(nadlan_query, "parcel_geometries", _geoms)
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/parcel/6319/225").json()
    assert called == []
    assert body["data"][0]["geometry"] is None


@pytest.mark.parametrize("url", [
    "/api/nadlan/parcel/6319/225?geometry=true",
    "/api/nadlan/point?lat=32.0789&lon=34.9171&radius_m=250&geometry=true",
    "/api/nadlan/zip/4935048?geometry=true",
    "/api/nadlan/address?city=פתח תקווה&street=אבימלך&geometry=true",
])
def test_every_mode_can_return_the_parcel_polygon(client, monkeypatch, url):
    """Whichever identity you searched by, the result is locatable on the map."""
    async def _geoms(keys, simplify=None):
        return {k: _POLY for k in keys}

    monkeypatch.setattr(nadlan_query, "parcel_geometries", _geoms)
    _stub_lookup(monkeypatch)
    body = client.get(url).json()
    assert body["data"][0]["geometry"] == _POLY


# ── validation ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("url", [
    "/api/nadlan/point?lat=32.08&lon=34.88&radius_m=999999",  # unbounded radius
    "/api/nadlan/point?lat=99&lon=34.88",                     # outside Israel
    "/api/nadlan/point?lat=32.08",                            # missing lon
    "/api/nadlan/parcel/abc/225",                             # non-numeric gush
    "/api/nadlan/address?city=פתח תקווה",                      # street required
])
def test_bad_input_is_refused_before_the_database(client, monkeypatch, url):
    _stub_lookup(monkeypatch)
    assert client.get(url).status_code == 422


def test_zip_must_be_five_or_seven_digits(client, monkeypatch):
    _stub_lookup(monkeypatch)
    assert client.get("/api/nadlan/zip/123456").status_code == 422   # 6 digits
    assert client.get("/api/nadlan/zip/abcde").status_code == 422


def test_503_until_the_index_has_been_built(monkeypatch):
    async def _not_ready():
        return False

    monkeypatch.setattr(nadlan_query, "is_ready", _not_ready)
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(nadlan_router)
    limiter.reset()
    c = TestClient(app, raise_server_exceptions=False)
    r = c.get("/api/nadlan/parcel/6319/225")
    assert r.status_code == 503


# ── the omnibox ───────────────────────────────────────────────────────────────
def test_resolve_sniffs_the_mode(client, monkeypatch):
    _stub_lookup(monkeypatch)
    assert client.get("/api/nadlan/resolve?q=גוש 6319 חלקה 225").json()["query"]["mode"] == "gush_helka"
    assert client.get("/api/nadlan/resolve?q=4935048").json()["query"]["mode"] == "zip"
    assert client.get("/api/nadlan/resolve?q=32.08,34.88").json()["query"]["mode"] == "point"


def test_resolve_surfaces_the_five_digit_ambiguity(client, monkeypatch):
    """49350 is a valid ZIP5 and a valid gush — the caller gets both readings."""
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/resolve?q=49350").json()
    assert body["query"]["mode"] == "zip"
    assert body["alternatives"] == [{"mode": "gush", "parsed": {"gush": 49350}}]


# ── the stats cache ───────────────────────────────────────────────────────────
def test_stats_is_cached_and_invalidated_by_a_build(monkeypatch):
    """stats() is 12 COUNT(*)s over 1.1M parcels and 622k addresses — 2.1-2.7s
    against a 5s timeout, called on EVERY page load. Uncached it 500s the moment
    the Neon compute is cold (observed once in production) and bills compute for
    numbers that only move when a build runs."""
    import asyncio
    calls = []

    async def _fetch(sql, *args):
        calls.append(sql)
        return [{"parcels": 1, "addresses": 2}]

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    nadlan_query.invalidate_stats_cache()

    async def run():
        first = await nadlan_query.stats()
        await nadlan_query.stats()
        await nadlan_query.stats()
        assert len(calls) == 1, "repeat reads must not hit the database"
        assert "coverage" in first
        nadlan_query.invalidate_stats_cache()
        await nadlan_query.stats()
        assert len(calls) == 2, "a build must make the new numbers visible at once"

    asyncio.run(run())
    nadlan_query.invalidate_stats_cache()


# ── the two descriptive layers ────────────────────────────────────────────────
_AREA = {"code": 517, "yishuv_stat": 79000517, "settlement_name": "פתח תקווה",
         "rova": 5, "tat_rova": 51, "division": "2022", "population": 1574,
         "population_year": 2024, "main_function": "מגורים",
         "socio": {"eshkol": 7, "index_year": 2021, "division": "2011",
                   "yishuv_stat": 79000517}}
_DEALS = {"deals": 12, "first_deal": "2004-03-01", "last_deal": "2025-04-23",
          "sub_parcels": 4,
          "latest": {"date": "2025-04-23", "amount": 2_450_000,
                     "nature": "דירה בבית קומות", "rooms": 4}}


def _stub_layers(monkeypatch, area=_AREA, deals=_DEALS):
    calls = {"stat_area": 0, "deals": 0}

    async def _areas(keys):
        calls["stat_area"] += 1
        return {k: area for k in keys} if area else {}

    async def _summaries(parcels):
        calls["deals"] += 1
        return {p["parcel_key"]: deals for p in parcels} if deals else {}

    monkeypatch.setattr(nadlan_query, "stat_areas", _areas)
    monkeypatch.setattr(nadlan_query, "deal_summaries", _summaries)
    return calls


def test_every_mode_carries_the_statistical_area_and_the_deals(client, monkeypatch):
    """Both layers describe the property rather than identify it, so they ride
    the SAME envelope as the identity — whichever identity you searched by."""
    _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    for url in ("/api/nadlan/parcel/6319/225",
                "/api/nadlan/point?lat=32.0789&lon=34.9171",
                "/api/nadlan/zip/4935048",
                "/api/nadlan/address?city=פתח תקווה&street=אבימלך"):
        prop = client.get(url).json()["data"][0]
        assert prop["stat_area"]["code"] == 517, url
        assert prop["deals"]["deals"] == 12, url


def test_the_socio_index_keeps_its_own_division(client, monkeypatch):
    """It is published on the 2011 areas, which are NOT the 2022 areas — so it
    stays a block of its own, labelled, rather than a field of the 2022 area."""
    _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    area = client.get("/api/nadlan/parcel/6319/225").json()["data"][0]["stat_area"]
    assert area["division"] == "2022"
    assert area["socio"]["division"] == "2011" and area["socio"]["eshkol"] == 7


@pytest.mark.parametrize("url", [
    "/api/nadlan/parcel/6319/225?stat_area=false&deals=false",
    "/api/nadlan/point?lat=32.0789&lon=34.9171&stat_area=false&deals=false",
    "/api/nadlan/zip/4935048?stat_area=false&deals=false",
    "/api/nadlan/address?city=פתח תקווה&street=אבימלך&stat_area=false&deals=false",
])
def test_both_layers_can_be_switched_off(client, monkeypatch, url):
    """A caller that only wants the identity must not pay for either read.

    Every mode, because the two switches are ONE shared Query declaration: a
    mode where it silently stopped binding would be invisible otherwise."""
    calls = _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    body = client.get(url).json()
    assert calls == {"stat_area": 0, "deals": 0}, url
    assert body["data"][0]["stat_area"] is None
    assert body["data"][0]["deals"] is None


def test_the_deal_list_is_its_own_paged_endpoint(client, monkeypatch):
    """The envelope carries a summary; a condo tower's 1,850 deals do not."""
    async def _deals(gush, helka, limit=50, offset=0, sub_parcel=None):
        assert (gush, helka, sub_parcel) == (6319, 225, "007")
        return [{"date": "2025-04-23", "amount": 2_450_000}], 143

    monkeypatch.setattr(nadlan_query, "parcel_deals", _deals)
    body = client.get("/api/nadlan/parcel/6319/225/deals?sub_parcel=7").json()
    assert body["total"] == 143 and body["count"] == 1
    assert body["data"][0]["amount"] == 2_450_000


# ── the unified lookup ────────────────────────────────────────────────────────
def test_lookup_answers_every_identity_with_one_envelope(client, monkeypatch):
    _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    for url, mode in (
        ("/api/nadlan/lookup?gush=6319&helka=225", "gush_helka"),
        ("/api/nadlan/lookup?lat=32.0789&lon=34.9171", "point"),
        ("/api/nadlan/lookup?zip=4935048", "zip"),
        ("/api/nadlan/lookup?city=פתח תקווה&street=אבימלך", "address"),
        ("/api/nadlan/lookup?q=גוש 6319 חלקה 225", "gush_helka"),
    ):
        body = client.get(url).json()
        assert body["query"]["mode"] == mode, url
        assert body["data"][0]["parcel_key"] == "6319-0-225", url


def test_lookup_prefers_an_explicit_identifier_over_the_free_text_box(client, monkeypatch):
    """A caller that named גוש and חלקה has said what it means; sniffing over
    that could only get it wrong."""
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/lookup?gush=6319&helka=225&q=4935048").json()
    assert body["query"]["mode"] == "gush_helka"


def test_lookup_returns_only_the_requested_fields(client, monkeypatch):
    _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/lookup?gush=6319&helka=225&fields=zip,stat_area").json()
    prop = body["data"][0]
    assert body["query"]["fields"] == ["stat_area", "zip"]
    assert set(prop) == {"parcel_key", "identity", "stat_area"}
    # The parcel identity always survives: an answer you cannot tie back to a
    # parcel is not an answer.
    assert prop["identity"]["gush"] == 6319 and "zip7" in prop["identity"]
    assert "addresses" not in prop["identity"] and "deals" not in prop


def test_lookup_field_selection_skips_the_reads_it_did_not_ask_for(client, monkeypatch):
    calls = _stub_layers(monkeypatch)
    _stub_lookup(monkeypatch)
    client.get("/api/nadlan/lookup?gush=6319&helka=225&fields=identity")
    assert calls == {"stat_area": 0, "deals": 0}


def test_lookup_refuses_an_unknown_field(client, monkeypatch):
    """A typo that quietly drops a block is the worst failure mode here."""
    _stub_lookup(monkeypatch)
    r = client.get("/api/nadlan/lookup?gush=6319&helka=225&fields=identity,prices")
    assert r.status_code == 422 and "prices" in r.json()["detail"]


def test_lookup_needs_at_least_one_identifier(client, monkeypatch):
    _stub_lookup(monkeypatch)
    assert client.get("/api/nadlan/lookup").status_code == 422


def test_lookup_all_includes_the_polygon(client, monkeypatch):
    _stub_layers(monkeypatch)

    async def _geoms(keys, simplify=None):
        return {k: _POLY for k in keys}

    monkeypatch.setattr(nadlan_query, "parcel_geometries", _geoms)
    _stub_lookup(monkeypatch)
    prop = client.get("/api/nadlan/lookup?gush=6319&helka=225&fields=all").json()["data"][0]
    assert prop["geometry"] == _POLY
    assert set(prop) >= {"stat_area", "deals", "sources", "match"}


# ── a tap that lands between parcels ─────────────────────────────────────────
def test_an_exact_tap_that_finds_nothing_widens_once_and_says_so(client, monkeypatch):
    """Parcels do not tile the country. Measured on random points inside a
    settlement's own envelope: 149/150 inside a parcel in Tel Aviv, 94/150 in
    Dimona — so a bare "nothing here" is the normal answer in a lot of the
    country, and it reads as a broken map rather than as a gap in the cadastre."""
    asked: list[float] = []

    async def _point(lat, lon, r=0.0, limit=50):
        asked.append(r)
        return [] if r == 0 else [_PARCEL]

    _stub_lookup(monkeypatch)                       # …then override the reader
    monkeypatch.setattr(nadlan_query, "by_point", _point)
    body = client.get("/api/nadlan/point?lat=32.0789&lon=34.9171").json()
    assert asked == [0.0, nadlan_query.POINT_FALLBACK_RADIUS_M]
    assert body["query"]["widened"] is True
    assert body["query"]["radius_used"] == nadlan_query.POINT_FALLBACK_RADIUS_M
    assert body["count"] == 1


def test_an_explicit_radius_is_never_quietly_widened(client, monkeypatch):
    """A caller that asked for 500 m and got nothing has been answered; re-asking
    a different question is how a result set stops meaning what the query said."""
    asked: list[float] = []

    async def _point(lat, lon, r=0.0, limit=50):
        asked.append(r)
        return []

    _stub_lookup(monkeypatch)
    monkeypatch.setattr(nadlan_query, "by_point", _point)
    body = client.get("/api/nadlan/point?lat=32.0789&lon=34.9171&radius_m=500").json()
    assert asked == [500.0]
    assert body["query"]["widened"] is False and body["count"] == 0


# ── an empty address answer explains itself ──────────────────────────────────
def test_an_empty_address_answer_says_why(client, monkeypatch):
    """Empty is the one answer a person cannot act on: unknown town, different
    spelling and a street nothing can place are three different next moves."""
    async def _miss(city, street, addresses=None):
        return {"reason": "street_not_located", "message": "…", "official_code": 391}

    monkeypatch.setattr(nadlan_query, "explain_address_miss", _miss)
    _stub_lookup(monkeypatch, parcels=())
    body = client.get("/api/nadlan/address?city=דימונה&street=הר הצופים&number=1").json()
    assert body["count"] == 0
    assert body["miss"]["reason"] == "street_not_located"


def test_a_found_address_carries_no_miss_block(client, monkeypatch):
    _stub_lookup(monkeypatch)
    body = client.get("/api/nadlan/address?city=פתח תקווה&street=אבימלך").json()
    assert body["count"] == 1 and "miss" not in body


def test_a_tap_on_open_ground_says_what_it_tried(client, monkeypatch):
    """Nothing under the point and nothing within the fallback either — real for
    points inside Dimona's own envelope. An empty list on a map reads as a
    broken map, so the answer names the radius it looked in."""
    async def _point(lat, lon, r=0.0, limit=50):
        return []

    _stub_lookup(monkeypatch)
    monkeypatch.setattr(nadlan_query, "by_point", _point)
    body = client.get("/api/nadlan/point?lat=31.1784&lon=34.9657").json()
    assert body["count"] == 0
    assert body["miss"]["reason"] == "no_parcel_near"
    assert body["miss"]["radius_tried_m"] == nadlan_query.POINT_FALLBACK_RADIUS_M
    assert "150" in body["miss"]["message"]


def test_street_suggestions_fall_back_to_the_first_word(monkeypatch):
    """A prefix match on the whole name answers nothing for a name we do not
    carry: "הר הצופים" starts with הרהצופים and nothing does. The first word is
    what turns a blank list into הר ארבל / הר גולן / הר חרמון."""
    import asyncio
    asked: list[str] = []

    async def _prefix(q, sc, limit):
        asked.append(q)
        return [] if " " in q else [{"name": "הר ארבל"}]

    monkeypatch.setattr(nadlan_query, "_suggest_streets_prefix", _prefix)
    out = asyncio.run(nadlan_query.suggest_streets("הר הצופים", 2200, 8))
    assert asked == ["הר הצופים", "הר"]
    assert out == [{"name": "הר ארבל"}]


def test_a_single_word_that_matches_nothing_is_not_retried(monkeypatch):
    """There is no shorter form to fall back to, and asking twice for the same
    thing is just a second query."""
    import asyncio
    asked: list[str] = []

    async def _prefix(q, sc, limit):
        asked.append(q)
        return []

    monkeypatch.setattr(nadlan_query, "_suggest_streets_prefix", _prefix)
    assert asyncio.run(nadlan_query.suggest_streets("קווזימודו", 2200, 8)) == []
    assert asked == ["קווזימודו"]


def test_the_gazetteer_rate_is_measured_over_what_it_could_match(monkeypatch):
    """Seeding the street index from the official register took it from 37,681
    to 65,795 rows. Reported against ALL of them, a gazetteer match rate that
    had RISEN from 76.6% to 94.7% published itself as a fall to 50.3%, because
    the new rows are streets the gazetteer has no reason to carry. A metric that
    reads as a regression while the thing it measures improved is worse than no
    metric."""
    import asyncio

    async def _fetch(sql, *args):
        return [{"streets": 65_795, "streets_located": 37_832,
                 "streets_register_only": 27_963, "streets_in_gazetteer": 32_875,
                 "addresses": 617_876}]

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    nadlan_query.invalidate_stats_cache()
    try:
        cov = asyncio.run(nadlan_query.stats())["coverage"]
        assert cov["streets_in_gazetteer_pct"] == 86.9     # 32,875 / 37,832
        assert cov["streets_register_only_pct"] == 42.5    # 27,963 / 65,795
    finally:
        nadlan_query.invalidate_stats_cache()


# ── an address we found is an answer, parcel or no parcel ────────────────────
_ADDR = {"parcel_key": None, "street_name": "שד יגאל אלון", "house_num": 221,
         "house_suffix": None, "entrance": None, "zip7": "8603162", "zip5": "86031",
         "neighbourhood": None, "lat": None, "lon": None, "parcel_match": None,
         "settlement_name": "דימונה", "point": None}


def test_an_address_with_no_parcel_is_reported_as_found_not_as_missing(client, monkeypatch):
    """Measured on the live index: 187,819 of 617,876 addresses (30.4%) carry no
    parcel_key and 186,313 of those carry a zip. The envelope is built from
    PARCELS, so all of them answered a blank screen — and the miss blamed the
    house number, which had matched perfectly. שד יגאל אלון 221 in Dimona is in
    the index with nine zip codes."""
    _stub_lookup(monkeypatch, parcels=(), addresses=(_ADDR,))
    body = client.get("/api/nadlan/address?city=דימונה&street=שד יגאל אלון&number=221").json()
    assert body["count"] == 0
    miss = body["miss"]
    assert miss["reason"] == "addresses_without_parcel"
    assert miss["n_addresses"] == 1 and miss["zip7"] == ["8603162"]
    assert "לא נמצאה" not in miss["message"], "it WAS found"
    # The rows themselves ride the envelope, so the page can show what we hold.
    assert body["addresses"][0]["zip7"] == "8603162"


def test_a_house_number_that_really_is_absent_still_says_so(client, monkeypatch):
    """The two must stay distinguishable: one is our gap, the other is the
    reader's typo, and they have different next moves."""
    _stub_lookup(monkeypatch, parcels=(), addresses=())
    body = client.get("/api/nadlan/address?city=דימונה&street=הרצל&number=99999").json()
    assert body["miss"]["reason"] in ("no_house_match", "street_unknown",
                                      "settlement_unknown")
