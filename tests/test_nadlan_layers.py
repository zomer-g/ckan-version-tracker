"""The information cubes of נדל"ן לעם — one parcel opened into every layer
OVER holds about its ground.

No Postgres: the table discovery and ``nadlan_query._fetch`` are stubbed. What
is pinned here:

  * the registry is well-formed (unique ids, a known grain, real dataset ids),
  * a GovMap table is found by the dataset-id suffix ``index_mirror`` gives it,
    so nothing depends on a hardcoded physical name,
  * a spatial cube drops GIS bookkeeping and labels columns with their aliases,
  * the crime cube compares area / settlement / country from ONE scan, per
    1,000 residents, and says when the area is not residential,
  * the routes refuse an unknown cube and a parcel that does not exist.
"""
import os
import re
import uuid

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.nadlan import router as nadlan_router
from app.rate_limit import limiter
from app.services import column_aliases, nadlan_layers, nadlan_query

_PARCEL = {
    "parcel_key": "6319-0-225", "gush": 6319, "gush_suffix": 0, "parcel": 225,
    "gp_key": "6319-225", "settlement_code": 7900, "locality_name": "פתח תקווה",
    "lat": 32.0789, "lon": 34.9171,
}
_TABA = "govmap_357_d9a9ad35_2156f30e"


@pytest.fixture(autouse=True)
def _fresh():
    nadlan_layers.invalidate_caches()
    nadlan_layers._years_cache.clear()
    yield
    nadlan_layers.invalidate_caches()


def _stub_tables(monkeypatch, idx=None, public=None):
    async def _cols(schema):
        return {"idx": idx or {}, "public": public or {}}.get(schema, {})

    async def _aliases(use_cache=True):
        return {("idx", _TABA): {"pl_number": "מספר תוכנית"}}

    monkeypatch.setattr(nadlan_layers.append_store, "schema_table_columns", _cols)
    monkeypatch.setattr(column_aliases, "load_map", _aliases)


# ── registry ──────────────────────────────────────────────────────────────────
def test_registry_ids_are_unique_and_well_formed():
    reg = nadlan_layers.registry()
    ids = [c["id"] for c in reg]
    assert len(ids) == len(set(ids))
    for c in reg:
        assert re.fullmatch(r"[a-z0-9_]+", c["id"]), c["id"]
        assert c["grain"] in ("spatial", "stat_area")
        for ds in c["datasets"]:
            uuid.UUID(ds)  # a typo in an id would silently never resolve
        if c["grain"] == "spatial":
            assert c["datasets"] and c["radius_m"] is not None


def test_no_dataset_sits_in_two_cubes():
    seen = [ds for c in nadlan_layers.registry() for ds in c["datasets"]]
    assert len(seen) == len(set(seen))


@pytest.mark.asyncio
async def test_a_layer_is_found_by_its_dataset_suffix(monkeypatch):
    _stub_tables(monkeypatch, idx={
        _TABA: [{"name": "pl_number"}, {"name": "geom"}],
        "no_geometry_2156f30f": [{"name": "x"}],
    })
    cat = {c["id"]: c for c in await nadlan_layers.catalog()}
    assert cat["taba_bounds"]["available"] is True
    assert cat["taba_plots"]["available"] is False
    assert cat["crime"]["available"] is False
    assert "datasets" not in cat["taba_bounds"]


# ── spatial ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_spatial_cube_drops_plumbing_and_uses_aliases(monkeypatch):
    _stub_tables(monkeypatch, idx={_TABA: [
        {"name": "objectid"}, {"name": "pl_number"}, {"name": "pl_name"},
        {"name": "Shape_Area"}, {"name": "geometry_wkt"}, {"name": "geom"}]})
    seen = {}

    async def _fetch(sql, *args):
        seen["sql"], seen["args"] = sql, args
        return [{"pl_number": "פת/1234", "pl_name": "  ", "__distance_m": 0}]

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    out = await nadlan_layers.spatial_cube(nadlan_layers.cube("taba_bounds"), _PARCEL)

    assert "objectid" not in seen["sql"] and "Shape_Area" not in seen["sql"]
    assert "ST_Intersects" in seen["sql"]            # a polygon cube: inside
    assert seen["args"] == ("6319-0-225",)
    assert out["count"] == 1
    # a blank value is not a field
    assert out["features"][0]["fields"] == [
        {"key": "pl_number", "label": "מספר תוכנית", "value": "פת/1234"}]
    assert out["sources"][0]["row_url"].startswith("https://www.over.org.il/data?q=")


@pytest.mark.asyncio
async def test_a_radius_cube_asks_for_distance_not_containment(monkeypatch):
    table = "govmap_240413_aaaaaaaa_d0dfe5f7"
    _stub_tables(monkeypatch, idx={table: [{"name": "name"}, {"name": "geom"}]})
    seen = {}

    async def _fetch(sql, *args):
        seen["sql"] = sql
        return []

    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    await nadlan_layers.spatial_cube(nadlan_layers.cube("edu_shelters"), _PARCEL)
    assert "ST_DWithin" in seen["sql"] and ", 500.0)" in seen["sql"]


# ── crime ─────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_crime_compares_three_scopes_per_1000(monkeypatch):
    cols = [{"name": c} for c in nadlan_layers.CRIME_COLS | {"StatisticArea"}]
    _stub_tables(monkeypatch, public={"append_crime_records_data_69a37f7d": cols,
                                      "append_crime_records_data_a7958e37": cols})

    async def _areas(keys):
        return {"6319-0-225": {"yishuv_stat": 79000213, "population": 2000,
                               "main_function": "תעסוקה", "settlement_name": "פתח תקווה"}}

    calls = []

    async def _fetch(sql, *args):
        calls.append((sql, args))
        if 'max("Year")' in sql:
            return [{"y": "2025" if "69a37f7d" in sql else "2024"}]
        if "over_settlements" in sql:
            return [{"s": 250_000, "n": 10_000_000}]
        return [
            {"grp": "עבירות כלפי הרכוש", "area": 30, "settlement": 2500,
             "national": 90_000, "area_name": "מרכז העיר"},
            {"grp": "עבירות נגד גוף", "area": 10, "settlement": 1000,
             "national": 60_000, "area_name": None},
        ]

    monkeypatch.setattr(nadlan_query, "stat_areas", _areas)
    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    out = await nadlan_layers.crime_cube(_PARCEL)

    assert out["year"] == "2025" and out["years"] == ["2025", "2024"]
    assert "69a37f7d" in out["table"]
    scan = next(a for s, a in calls if "FILTER" in s)
    assert scan == ("79000213", "7900")
    s = out["scopes"]
    assert s["area"]["total"] == 40 and s["area"]["per_1000"] == 20.0
    assert s["settlement"]["per_1000"] == 14.0
    assert s["national"]["per_1000"] == 15.0
    assert s["area"]["name"] == "מרכז העיר"
    assert out["groups"][0]["group"] == "עבירות כלפי הרכוש"
    assert any("תעסוקה" in n for n in out["notes"])


@pytest.mark.asyncio
async def test_crime_absent_when_the_register_is_not_tracked(monkeypatch):
    _stub_tables(monkeypatch)
    assert await nadlan_layers.crime_cube(_PARCEL) == {"available": False}


# ── routes ────────────────────────────────────────────────────────────────────
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


def test_unknown_cube_is_404(client):
    assert client.get("/api/nadlan/parcel/6319/225/layer/nope").status_code == 404


def test_cube_id_is_validated(client):
    assert client.get("/api/nadlan/parcel/6319/225/layer/DROP%20TABLE").status_code == 422


def test_missing_parcel_is_404(client, monkeypatch):
    async def _none(g, h, s=None):
        return []

    monkeypatch.setattr(nadlan_query, "by_gush_helka", _none)
    assert client.get("/api/nadlan/parcel/1/1/layer/taba_bounds").status_code == 404


def test_parcel_layers_carry_probe_counts(client, monkeypatch):
    async def _gush(g, h, s=None):
        return [_PARCEL]

    _stub_tables(monkeypatch, idx={_TABA: [{"name": "pl_number"}, {"name": "geom"}]})

    async def _fetch(sql, *args):
        return [{"cube": "taba_bounds", "n": 3}]

    monkeypatch.setattr(nadlan_query, "by_gush_helka", _gush)
    monkeypatch.setattr(nadlan_query, "_fetch", _fetch)
    body = client.get("/api/nadlan/parcel/6319/225/layers").json()
    by_id = {c["id"]: c for c in body["data"]}
    assert by_id["taba_bounds"]["hits"] == 3
    assert by_id["taba_plots"]["hits"] is None   # not on this deployment
