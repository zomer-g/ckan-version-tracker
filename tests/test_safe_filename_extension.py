"""An object key must not swallow the file's extension.

Reported from the site: some GeoPackage/GeoParquet downloads carry a `.gpkg`
suffix and some carry none, with no visible pattern. There is one, and it is
entirely in the layer's TITLE.

`_safe_filename` collapsed every non-ASCII run to `_` and then stripped `._-`
off both ends. A title written entirely in Hebrew collapses to a single
underscore, so "מעג\"ל מבנים.gpkg" became "_.gpkg" and the strip took the
underscore AND the dot with it, leaving "gpkg". A title carrying any ASCII at
all — "חופות עצים 2024" — kept a character before the dot and came out
"2024.gpkg". Hence identical layers landing with and without an extension.

It bites hardest on the heavy GovMap layers, which publish the GeoPackage and
the GeoParquet INSTEAD of a CSV: those extensionless files ARE the data, and no
GIS tool will open them until the reader guesses the format and renames.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.services.storage_client import (  # noqa: E402
    _filename_from_value,
    _safe_filename,
)


# ── the sanitiser keeps the extension ──────────────────────────────────

def test_an_all_hebrew_name_keeps_its_extension():
    assert _safe_filename('מעג"ל מבנים.gpkg') == "file.gpkg"
    assert _safe_filename('מעג"ל מבנים.parquet') == "file.parquet"
    assert _safe_filename("מגרשים נס ציונה.csv") == "file.csv"


def test_a_double_extension_survives_whole():
    # ".geojson.gz" used to come out as "geojson.gz" — which LOOKS like a
    # filename with an extension and is actually the extension alone.
    assert _safe_filename('מעג"ל מבנים.geojson.gz') == "file.geojson.gz"


def test_a_name_with_any_ascii_is_unchanged_from_before():
    # These already worked; the fix must not move them.
    assert _safe_filename("חופות עצים  2024.gpkg") == "2024.gpkg"
    assert _safe_filename("Tel Aviv buildings.gpkg") == "Tel_Aviv_buildings.gpkg"


def test_the_symbology_bundle_still_ends_in_symbology_zip():
    # worker._is_doc_bundle recognises the bundle by this tail; renaming it
    # would route the layer's SLD back into the generic attachment channel.
    assert _safe_filename("שם עברי_symbology.zip").endswith("symbology.zip")
    assert _safe_filename("מגרשים נס ציונה_fields.zip").endswith("fields.zip")


def test_a_name_without_a_known_extension_is_left_alone():
    assert _safe_filename("no-extension-at-all") == "no-extension-at-all"
    # A dotted layer name is not an extension — truncating it would lose "1.2".
    assert _safe_filename("מגרשים 1.2") == "1.2"


def test_empty_input_still_has_a_name():
    assert _safe_filename("") == "file"
    assert _safe_filename(None) == "file"


# ── historical keys are repaired where we hand out a filename ──────────

def test_an_extension_only_key_is_given_a_stem():
    # These keys are permanent — resource_mappings and every existing link
    # point at them — so the repair belongs on the name, not the key.
    for tail, want in (("f78405e6_gpkg", "data.gpkg"),
                       ("db568966_parquet", "data.parquet"),
                       ("db488c69_csv", "data.csv")):
        assert _filename_from_value(f"r2:datasets/x/v1/{tail}", "fb") == want


def test_a_healthy_key_is_not_rewritten():
    assert _filename_from_value("r2:datasets/x/v1/b4db9a2b_2024.gpkg", "fb") == "2024.gpkg"
    assert _filename_from_value("r2:datasets/x/v1/6475e20d_symbology.zip", "fb") == "symbology.zip"


def test_an_odata_id_still_falls_back():
    assert _filename_from_value("3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10", "fb") == "fb"
