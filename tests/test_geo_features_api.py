"""The spatial surface: /api/tables/{table}/features and its bbox.

Written after an outside developer read the public API page, concluded that
OVER's mapping layers were "whole files behind CORS, no geographic functions",
and started planning a client-side ITM reprojection. Every part of that was
wrong — 815 mirrored layers carry a PostGIS `geom` in EPSG:4326 behind a GiST
index — but nothing in the documented API surface said so, and the one
documented route for a dataset's rows (/api/append) answered a bare 409 for
every GovMap layer.

So the rules pinned here are the ones that make the capability findable and
hard to misuse:
  * a bbox is min-corner-first, in WGS84 degrees, and a malformed one is a 400
    rather than an empty FeatureCollection (which reads as "nothing here");
  * a table with no geometry 404s with the reason, not with an empty result;
  * ?columns= is validated against the live column list, never interpolated;
  * the SQL filters with && (the index operator) against ST_MakeEnvelope.
"""
import asyncio
import os
import sys

import pytest
from fastapi import HTTPException

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import tables as api  # noqa: E402
from app.services import append_store  # noqa: E402

LAYER = "govmap_200541_b19acb42_c2bd90e1"
_REC = {
    "table": LAYER, "schema": "idx", "kind": "index",
    "title": "גבולות ישובים", "source_url": "https://www.govmap.gov.il/",
    "columns": [{"name": "objectId"}, {"name": "name_name"},
                {"name": "geometry_wkt"}, {"name": "_row_hash"},
                {"name": "_first_seen"}, {"name": "geom"}],
}


# -- bbox parsing ------------------------------------------------------------

def test_bbox_is_min_corner_first_in_wgs84():
    assert api._parse_bbox("35.0,31.8,35.3,32.0") == (35.0, 31.8, 35.3, 32.0)


def test_no_bbox_means_no_filter():
    assert api._parse_bbox(None) is None
    assert api._parse_bbox("") is None


@pytest.mark.parametrize("raw", [
    "35.0,31.8,35.3",          # three numbers
    "35.0,31.8,35.3,32.0,1",   # five numbers
    "a,b,c,d",                 # not numbers
    "35.3,31.8,35.0,32.0",     # lon reversed
    "35.0,32.0,35.3,31.8",     # lat reversed
    "35.0,31.8,35.0,32.0",     # zero width
    "200,31.8,201,32.0",       # lon out of range
    "35.0,-91,35.3,32.0",      # lat out of range
])
def test_a_malformed_bbox_is_an_error_not_an_empty_result(raw):
    """An empty FeatureCollection for a typo'd box is indistinguishable from
    "the layer has nothing here" — the caller would draw an empty map and
    believe it."""
    with pytest.raises(ValueError):
        api._parse_bbox(raw)


def test_the_bbox_error_says_the_expected_order():
    """The single most likely mistake is lat,lon (Leaflet's own LatLng order),
    so the message has to name the order rather than say "invalid"."""
    with pytest.raises(ValueError) as e:
        api._parse_bbox("35.3,31.8,35.0,32.0")
    assert "min_lon" in str(e.value)


# -- the endpoint's gates ----------------------------------------------------

def _call(table, monkeypatch, *, catalog=(_REC,), **kw):
    async def fake_catalog(_db):
        return list(catalog)

    monkeypatch.setattr(api.data_catalog, "build_catalog", fake_catalog)
    monkeypatch.setattr(api.append_store, "is_configured", lambda: True)
    # The route is rate-limited, and slowapi's wrapper reaches into the real
    # Request; turn it off so these tests exercise the handler, not the limiter.
    monkeypatch.setattr(api.limiter, "enabled", False)
    return asyncio.run(api.table_features(table, None, db=None, **kw))


def test_an_unknown_table_404s(monkeypatch):
    """The catalog is the security gate: only a table it lists is ever named in
    SQL, so an arbitrary identifier can never reach the database."""
    with pytest.raises(HTTPException) as e:
        _call("pg_authid", monkeypatch)
    assert e.value.status_code == 404


def test_a_table_without_geometry_404s_and_says_why(monkeypatch):
    flat = {**_REC, "table": "append_x", "schema": "public",
            "columns": [{"name": "a"}, {"name": "b"}]}
    with pytest.raises(HTTPException) as e:
        _call("append_x", monkeypatch, catalog=(flat,))
    assert e.value.status_code == 404
    assert "geometry" in str(e.value.detail)


def test_an_unknown_column_is_rejected_rather_than_ignored(monkeypatch):
    """Silently dropping it would hand back a FeatureCollection whose properties
    are missing the field the caller asked to join on."""
    with pytest.raises(HTTPException) as e:
        _call(LAYER, monkeypatch, columns="name_name,no_such_col")
    assert e.value.status_code == 400
    assert "no_such_col" in str(e.value.detail)


def test_a_bad_bbox_reaches_the_caller_as_400(monkeypatch):
    with pytest.raises(HTTPException) as e:
        _call(LAYER, monkeypatch, bbox="35.3,31.8,35.0,32.0")
    assert e.value.status_code == 400


def test_geometry_columns_are_not_repeated_in_the_properties(monkeypatch):
    """`geom` is already the feature's geometry and `geometry_wkt` is the same
    shape again as TOASTed text — including either would double (or triple) the
    payload for nothing. `_row_hash` is the mirror's bookkeeping."""
    seen = {}

    async def fake(table, **kw):
        seen.update(kw, table=table)
        return {"features": [], "number_returned": 0,
                "exceeded_transfer_limit": False}

    monkeypatch.setattr(api.append_store, "geo_features", fake)
    _call(LAYER, monkeypatch)
    assert seen["columns"] == ["objectId", "name_name", "_first_seen"]
    assert seen["schema"] == "idx"


def test_the_response_is_a_geojson_feature_collection(monkeypatch):
    async def fake(table, **kw):
        return {"features": [{"type": "Feature", "geometry": None,
                              "properties": {}}],
                "number_returned": 1, "exceeded_transfer_limit": True}

    monkeypatch.setattr(api.append_store, "geo_features", fake)
    out = _call(LAYER, monkeypatch, bbox="35.0,31.8,35.3,32.0")
    assert out["type"] == "FeatureCollection"
    assert out["numberReturned"] == 1
    # The caller has no other way to tell a full page from a truncated one.
    assert out["exceededTransferLimit"] is True
    assert out["crs"]["properties"]["name"].endswith("CRS84")


# -- the SQL geo_features builds ---------------------------------------------

class _FakeConn:
    def __init__(self, sink):
        self.sink = sink

    def transaction(self, **_kw):
        conn = self

        class _Tx:
            async def __aenter__(_self):
                return conn

            async def __aexit__(_self, *a):
                return False

        return _Tx()

    async def execute(self, sql):
        self.sink.setdefault("execute", []).append(sql)

    async def fetch(self, sql, *params):
        self.sink["sql"], self.sink["params"] = sql, params
        return [{"name_name": "טלמון",
                 "_geojson": '{"type":"Point","coordinates":[35.1,31.9]}'}]


class _FakePool:
    def __init__(self, sink):
        self.sink = sink

    def acquire(self):
        pool = self

        class _Acq:
            async def __aenter__(_self):
                return _FakeConn(pool.sink)

            async def __aexit__(_self, *a):
                return False

        return _Acq()


def _features(monkeypatch, **kw):
    sink = {}

    async def fake_pool():
        return _FakePool(sink)

    monkeypatch.setattr(append_store, "get_readonly_pool", fake_pool)
    res = asyncio.run(append_store.geo_features(
        LAYER, schema="idx", columns=["name_name"], **kw))
    return res, sink


def test_bbox_filters_with_the_index_operator(monkeypatch):
    """`&&` is what the GiST index answers, which is what makes a viewport query
    an index probe instead of a scan over a million polygons. ST_Intersects
    would be exact and would cost a per-candidate recheck for a difference no
    one can see at viewport scale."""
    _, sink = _features(monkeypatch, bbox=(35.0, 31.8, 35.3, 32.0))
    assert "&&" in sink["sql"] and "ST_MakeEnvelope" in sink["sql"]
    assert "4326)" in sink["sql"]
    # The numbers are BOUND, never formatted into the statement.
    assert sink["params"][:4] == (35.0, 31.8, 35.3, 32.0)
    assert "35.0" not in sink["sql"]


def test_no_bbox_means_no_where_clause(monkeypatch):
    _, sink = _features(monkeypatch)
    assert "WHERE" not in sink["sql"]


def test_postgis_is_schema_qualified(monkeypatch):
    """PostGIS lives in its own `extensions` schema so its ~1,000 functions stay
    out of the catalog — which means an unqualified ST_ call does not resolve."""
    _, sink = _features(monkeypatch, bbox=(35.0, 31.8, 35.3, 32.0))
    assert '"extensions".ST_MakeEnvelope' in sink["sql"]
    assert '"extensions".ST_AsGeoJSON' in sink["sql"]


def test_geometry_comes_back_parsed_not_as_a_string(monkeypatch):
    res, _ = _features(monkeypatch)
    geom = res["features"][0]["geometry"]
    assert geom["type"] == "Point" and geom["coordinates"] == [35.1, 31.9]
    assert res["features"][0]["properties"] == {"name_name": "טלמון"}


def test_one_extra_row_is_fetched_to_detect_truncation(monkeypatch):
    """LIMIT n would make "exactly n rows" and "n rows and more behind them"
    identical, and a map would stop paging one screen early."""
    _, sink = _features(monkeypatch, limit=1)
    assert sink["params"][-2] == 2      # limit + 1
    assert sink["params"][-1] == 0      # offset


def test_the_page_is_capped(monkeypatch):
    """A GeoJSON feature carries its whole polygon; the national layers hold
    million-vertex geometries, so an unbounded page is tens of MB on the wire."""
    _, sink = _features(monkeypatch, limit=10 ** 6)
    assert sink["params"][-2] == append_store.MAX_FEATURES + 1


def test_a_statement_timeout_is_always_set(monkeypatch):
    _, sink = _features(monkeypatch)
    assert any("statement_timeout" in s for s in sink["execute"])


def test_an_injected_schema_name_is_refused(monkeypatch):
    async def fake_pool():
        return _FakePool({})

    monkeypatch.setattr(append_store, "get_readonly_pool", fake_pool)
    with pytest.raises(ValueError):
        asyncio.run(append_store.geo_features(
            LAYER, schema="public; DROP TABLE x", columns=[]))
