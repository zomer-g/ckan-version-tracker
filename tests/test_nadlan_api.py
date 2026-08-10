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
from app.services import nadlan_query


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

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)

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
    assert set(prop["sources"]) == {"parcels", "gazetteer", "postal", "address_list"}
    assert prop["sources"]["parcels"]["row_url"].startswith("https://www.over.org.il/data?q=")


def test_point_mode_shares_the_same_shape(client, monkeypatch):
    _stub_lookup(monkeypatch)
    r = client.get("/api/nadlan/point?lat=32.0789&lon=34.9171&radius_m=250")
    assert r.status_code == 200
    body = r.json()
    assert body["query"] == {"mode": "point", "lat": 32.0789, "lon": 34.9171, "radius_m": 250.0}
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
    assert len(body["caveats"]) == 3
    assert any("91" in c for c in body["caveats"])        # postal locality limit
    assert any("מספר בית" in c for c in body["caveats"])  # gazetteer street-only


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
