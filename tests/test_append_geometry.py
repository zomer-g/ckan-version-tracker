"""Points built from append tables must land in Israel, or not be built.

The plan for this assumed append tables carry `geometry_wkt` like `idx` does.
A census of the live append DB (2026-08-07) found none do — not one of 244
tables. What the spatial ones carry is coordinate PAIRS, and in the three
`X_Coordinate`/`Y_Coordinate` tables the source has the axes BACKWARDS:
`X_Coordinate` holds the latitude, `Y_Coordinate` the longitude, in 5,802 of
5,833 rows.

So the obvious reading — X is longitude, because it is called X — puts almost
the entire corpus in Saudi Arabia and the Indian Ocean, as valid geometry that
draws fine on a map. That is the failure mode the plan singled out as the worst
possible one, and it is why the classification is per ROW, driven by the values,
with anything ambiguous refused rather than guessed.

Verified against production before shipping:

    append_automated_devices_32f7dea8   2,995/2,999 placed, 4 refused
                                        lon 34.5582–35.8550  lat 29.5544–33.2829
    append_light_traffics_…_289eb9f6      446/447 placed, 1 refused ('Infinity')
                                        lon 34.6347–35.0245  lat 31.0575–31.3735

Every placed point inside the country; the one refusal on the traffic lights is
the literal `Infinity` the GeoJSON carries at feature #110 — NULL geom, not a
failed load, which is what the plan's own acceptance check asked for.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.services import append_geometry as ag  # noqa: E402


# ---------------------------------------------------------------------------
# finding the pair
# ---------------------------------------------------------------------------

def test_the_two_spellings_the_corpus_actually_uses():
    assert ag.find_pair(["id", "lat", "lon"]) == ("lon", "lat")
    assert ag.find_pair(["id", "X_Coordinate", "Y_Coordinate"]) == (
        "X_Coordinate", "Y_Coordinate")


def test_a_table_with_no_coordinates_is_left_alone():
    assert ag.find_pair(["id", "שם", "כתובת"]) is None
    assert ag.find_pair([]) is None


def test_only_one_half_of_a_pair_is_not_a_pair():
    assert ag.find_pair(["id", "lat"]) is None


# ---------------------------------------------------------------------------
# the bands — the property that makes a row's reading DECIDED, not chosen
# ---------------------------------------------------------------------------

def test_the_longitude_and_latitude_bands_cannot_both_match():
    """If they overlapped, a value inside the overlap would satisfy two
    branches of the CASE and the row's meaning would depend on branch order —
    i.e. on nothing. Israel makes this free: lat tops out below where lon
    starts."""
    assert ag.LAT_MAX < ag.LON_MIN, "a coordinate could be read either way"


def test_the_itm_bands_cannot_be_confused_with_degrees():
    assert ag.ITM_E_MIN > ag.LON_MAX and ag.ITM_N_MIN > ag.LAT_MAX


def test_the_bands_actually_contain_the_measured_corpus():
    """Ranges measured live. If a band is ever tightened past these, real rows
    start silently dropping to NULL."""
    assert ag.LON_MIN <= 34.5582 and ag.LON_MAX >= 35.8550
    assert ag.LAT_MIN <= 29.5544 and ag.LAT_MAX >= 33.2829


# ---------------------------------------------------------------------------
# the generated SQL
# ---------------------------------------------------------------------------

def test_the_pair_is_read_both_ways():
    """The whole point. One branch builds MakePoint(a, b), the other (b, a)."""
    sql = ag.point_expr("X_Coordinate", "Y_Coordinate")
    a = '(CASE WHEN "X_Coordinate" ~'
    b = '(CASE WHEN "Y_Coordinate" ~'
    assert sql.count("ST_MakePoint") == 3
    assert sql.index(a) < sql.index(b), "expected the as-written branch first"
    assert "ST_Transform" in sql, "the ITM branch must transform, not relabel"


def test_an_unreadable_pair_yields_null_rather_than_a_point():
    assert ag.point_expr("lon", "lat").rstrip().endswith("ELSE NULL END")


def test_the_numeric_guard_survives_a_typescript_template_literal():
    """These expressions get pasted into the /data page, where \\d becomes d."""
    assert "[0-9]" in ag._NUMERIC and "\\d" not in ag._NUMERIC


def test_identifiers_are_quoted_so_a_hebrew_or_mixed_case_column_works():
    sql = ag.point_expr("X_Coordinate", "קו אורך")
    assert '"X_Coordinate"' in sql and '"קו אורך"' in sql


def test_postgis_calls_are_schema_qualified():
    """search_path is not guaranteed on the pool this runs on."""
    sql = ag.point_expr("lon", "lat", ext="extensions")
    assert "extensions.ST_MakePoint" in sql
    assert "extensions.ST_SetSRID" in sql


# ---------------------------------------------------------------------------
# the guard rails around it
# ---------------------------------------------------------------------------

def test_the_switch_still_stops_it(monkeypatch):
    """ON by default now that the rescued corpora arrive carrying geometry.
    The switch stays because the work is not free — on a table whose WKT is
    ITM the geometry step rewrites the whole column and rehashes every row
    before it can build anything — so there has to be a way to stop it without
    a deploy."""
    from app.config import settings
    monkeypatch.setattr(settings, "append_postgis_enabled", False)
    out = asyncio.run(ag.fill(None, "append_x", ["lon", "lat"]))
    assert out == {"skipped": "append postgis disabled"}


def test_a_wkt_table_is_handed_to_the_tested_idx_path(monkeypatch):
    """When the worker starts returning features extracted from the blocked
    files it will write geometry_wkt in EPSG:4326 — `idx`'s shape, and `idx`'s
    code already handles it, reprojection and row-hash included."""
    from app.config import settings
    from app.services import index_mirror

    monkeypatch.setattr(settings, "append_postgis_enabled", True)

    async def _yes(conn):
        return True

    seen = {}

    async def _fake_fill(conn, table, columns, schema):
        seen.update(table=table, schema=schema)
        return {"rows": 7}

    monkeypatch.setattr(index_mirror, "_postgis_available", _yes)
    monkeypatch.setattr(index_mirror, "_fill_geometry", _fake_fill)

    out = asyncio.run(ag.fill(None, "append_x", ["id", "geometry_wkt"]))
    assert out == {"rows": 7}
    assert seen == {"table": "append_x", "schema": "public"}
