"""Unit tests for the content field-flags classifier (app/services/field_flags).

Pure column-name classification — no DB. Guards the regexes against the
recurring Hebrew/English false-friends we audited on the live catalog.
"""
import asyncio

from app.services import field_flags as ff


def _has(flag_key, col):
    return ff._col_has([{"name": col}], ff.FLAG_PATTERNS[flag_key])


def test_locality_matches_real_columns():
    for col in ["שם_ישוב", "שם יישוב", "ישוב", "עיר", "שם עיר", "City", "cityName", "locality"]:
        assert _has("has_locality", col), col


def test_locality_rejects_false_friends():
    # capacity contains "city"; עירוני contains "עיר" but means "urban", not a place.
    for col in ["capacity", "שם אתר טבע עירוני", "מחיר", "שער"]:
        assert not _has("has_locality", col), col


def test_authority_matches_and_excludes():
    for col in ["רשות", "רשות מקומית", "שם מועצה", "authority", "municipal_"]:
        assert _has("has_authority", col), col
    # "עומד לרשותם" (at their disposal) / "ברשות" are not authority names.
    for col in ["אחוז משקי הבית שלא עומד לרשותם כלי", "תוכניות הקיימות ברשות"]:
        assert not _has("has_authority", col), col


def test_ministry_matches_and_excludes_officelineid():
    for col in ["משרד", "משרד אב", "govministryid", "Data.Office", "ministrycategorydesc"]:
        assert _has("has_ministry", col), col
    # OfficeLineId is a GTFS licensing id, not a government office.
    assert not _has("has_ministry", "OfficeLineId")


def test_date_matches_but_not_first_seen():
    for col in ["תאריך פרסום", "שנה", "votedatetime", "submitdate", "trip_year", "data_dt"]:
        assert _has("has_date", col), col
    # first_seen is the ingest timestamp, never counted as a source date.
    assert not _has("has_date", "first_seen")


def test_compute_for_columns_flags_by_any_table():
    cols = {
        "d1": [{"name": "שם_ישוב"}, {"name": "סהכ"}],
        "d2": [{"name": "מספר חברה"}, {"name": "תאריך התאגדות"}],
    }
    out = asyncio.run(ff.compute_for_columns(cols, ("has_locality", "has_date")))
    assert out["d1"] == {"has_locality": True, "has_date": False}
    assert out["d2"] == {"has_locality": False, "has_date": True}


def test_parcel_matches_the_cadastral_keys_that_make_a_join_possible():
    """גוש/חלקה is what lets a dataset be joined to the parcel map and, through
    it, to anything else carrying the same key — Jerusalem's licensing register,
    the block shapefiles, the real-estate transactions. Every spelling below is
    a real column name from the live catalog."""
    for col in ["גוש", "מספר גוש", "תת גוש", "מחלקה", "עד חלקה", "מספר חלקה",
                "gush", "gush_num", "gushnum", "GUSH_NUM", "gush_helka",
                "parcel", "parcel_id", "parcel_1"]:
        assert _has("has_parcel", col), col


def test_parcel_rejects_place_names_and_forest_blocks():
    """The false friends are measured, not imagined: one CBS file carries three
    Hebrew place names containing גוש, and the KKL layer's blocks are forestry,
    not cadastre. Flagging either would send someone joining on a town name."""
    for col in ["אבו גוש", "ג'ש (גוש חלב)", "גוש עציון",
                "מספר גוש יער", "שם הגוש היערני",
                "מחלקת רכש", "המחלקה המשפטית"]:
        assert not _has("has_parcel", col), col


def test_geometry_matches_shapes_and_coordinates():
    for col in ["geom", "geometry_wkt", "the_geom", "lat", "lon", "lng",
                "latitude", "longitude", "נ.צ", "קואורדינטת X"]:
        assert _has("has_geometry", col), col


def test_geometry_rejects_words_that_merely_contain_the_letters():
    # "lat"/"lon" inside a longer word is not a coordinate.
    for col in ["plate_number", "מספר טלפון", "along_route", "translation",
                "latest", "מחיר"]:
        assert not _has("has_geometry", col), col


def test_the_two_spatial_flags_answer_different_questions():
    """has_geometry is "can I draw it"; has_parcel is "can I join it to
    something I can draw". Jerusalem's register is the case that matters: it
    holds no shape at all, and is reachable on the map only through its גוש."""
    jlm = [{"name": c} for c in ["מספר תיק", "גוש", "מחלקה", "רחוב"]]
    flags = asyncio.run(ff.compute_for_columns(
        {"jlm": jlm}, ("has_parcel", "has_geometry")))
    assert flags["jlm"] == {"has_parcel": True, "has_geometry": False}
