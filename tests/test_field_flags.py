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
