"""The GovMap documentation bundle (SLD symbology + field dictionary) is a
resource in its own right, not an attachment.

Two halves, both covered here:
  * ingest — push-version routes the bundle to `_symbology` whether the worker
    declares it or (as the pinned worker does) ships it inside the generic
    `zip_resource_ids` channel;
  * exposure — the public v1 API emits every list-valued mapping key, so the
    bundle, the GeoPackage and the GeoParquet stop being archived-but-invisible.

No DB, no network: both are pure functions over a mappings dict.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.api.v1 as v1  # noqa: E402
import app.api.worker as worker  # noqa: E402


DS = "c1a9e9cc-ef3a-49e1-b53a-852b3d2f47aa"
SYMB = f"r2:datasets/{DS}/v1/047bcd0d_symbology.zip"
FIELDS = f"r2:datasets/{DS}/v1/11aa22bb_fields.zip"
CSV = f"r2:datasets/{DS}/v1/db488c69_csv"
ATTACH = f"r2:datasets/{DS}/v1/99887766_attachments.zip"
ODATA_ID = "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10"


# ── ingest: which channel a pre-uploaded ZIP belongs to ────────────────

def test_doc_bundle_recognised_by_key_tail():
    assert worker._is_doc_bundle(SYMB)
    assert worker._is_doc_bundle(FIELDS)


def test_attachment_zip_is_not_a_doc_bundle():
    assert not worker._is_doc_bundle(ATTACH)
    assert not worker._is_doc_bundle(CSV)


def test_odata_resource_id_stays_in_the_zip_channel():
    # An ODATA id carries no filename, so it can't be judged — and must not be
    # guessed at. Govmap versions are R2-backed, so nothing real is missed.
    assert not worker._is_doc_bundle(ODATA_ID)


def test_split_recovers_bundle_from_the_legacy_zip_channel():
    zips, docs = worker._split_doc_bundles([ATTACH, SYMB], None)
    assert zips == [ATTACH]
    assert docs == [SYMB]


def test_split_honours_an_explicitly_declared_bundle():
    zips, docs = worker._split_doc_bundles([ATTACH], [SYMB])
    assert zips == [ATTACH]
    assert docs == [SYMB]


def test_split_leaves_a_pure_attachment_push_alone():
    zips, docs = worker._split_doc_bundles([ATTACH], None)
    assert zips == [ATTACH]
    assert docs == []


# ── exposure: the public v1 API ────────────────────────────────────────

class _DS:
    """Just enough TrackedDataset for the R2 path (no ODATA ids involved)."""
    odata_dataset_id = None


def _names(mappings):
    return {r.name: r for r in v1._extract_version_resources(_DS(), mappings)}


def test_symbology_is_exposed_as_its_own_resource():
    got = _names({
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
        "נתוני הסורק": CSV,
        "_geojson": [f"r2:datasets/{DS}/v1/5951d22f_geojson.gz"],
        "_symbology": [SYMB],
    })
    assert set(got) == {"נתוני הסורק", "_geojson", "_symbology"}
    assert got["_symbology"].format == "ZIP"
    assert got["_symbology"].storage == "r2"
    assert got["_symbology"].download_url.endswith("047bcd0d_symbology.zip")


def test_heavy_layer_artifacts_are_no_longer_invisible():
    # _gpkg/_parquet/_zip_parts are list-valued and used to fall through the
    # string-only check, so the API reported a version as having no files.
    got = _names({
        "_gpkg": [f"r2:datasets/{DS}/v1/aabbccdd_layer.gpkg"],
        "_parquet": [f"r2:datasets/{DS}/v1/eeff0011_layer.parquet"],
        "_zip_parts": [ATTACH],
    })
    assert set(got) == {"_gpkg", "_parquet", "_zip_parts"}
    assert got["_gpkg"].format == "GPKG"
    assert got["_parquet"].format == "GeoParquet"
    assert got["_zip_parts"].format == "ZIP"


def test_multi_element_lists_keep_unique_indexed_names():
    parts = [ATTACH, f"r2:datasets/{DS}/v1/55667788_attachments.zip"]
    got = _names({"_zip_parts": parts})
    assert set(got) == {"_zip_parts_0", "_zip_parts_1"}


def test_geojson_naming_is_unchanged():
    # The single-layer case keeps the bare "_geojson" name the map component
    # and existing API consumers look for.
    one = f"r2:datasets/{DS}/v1/5951d22f_geojson.gz"
    two = f"r2:datasets/{DS}/v1/aaaabbbb_geojson.gz"
    assert set(_names({"_geojson": [one]})) == {"_geojson"}
    assert _names({"_geojson": [one]})["_geojson"].format == "GeoJSON"
    assert set(_names({"_geojson": [one, two]})) == {"_geojson_0", "_geojson_1"}


def test_bookkeeping_keys_are_never_emitted():
    got = _names({
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
        "_appendonly_seen": ["k1", "k2"],
        "_filedates": {"x": "2026-07-30"},
        "_names": {"x": "שם"},
        "_probes": {},
        "נתוני הסורק": CSV,
    })
    assert set(got) == {"נתוני הסורק"}


# ── the empty-version guard must not discard a GPKG-only layer ─────────

def test_gpkg_only_layer_counts_as_landed():
    # Layer 335's real shape: the features ARE the GeoPackage and the Parquet,
    # and there is no source-named key at all. Counting only human-named keys
    # read this as "nothing landed" and threw the scrape away.
    assert worker._landed_resource_count({
        "_gpkg": [f"r2:datasets/{DS}/v1/aabbccdd_layer.gpkg"],
        "_parquet": [f"r2:datasets/{DS}/v1/eeff0011_layer.parquet"],
        "_symbology": [SYMB],
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
    }) == 2


def test_a_documentation_only_version_still_counts_as_empty():
    # The guard's whole purpose: a push that landed the layer's SLD and no data
    # is an empty version, and must stay one.
    assert worker._landed_resource_count({
        "_symbology": [SYMB],
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
    }) == 0


def test_named_resources_still_count():
    assert worker._landed_resource_count({
        "נתוני הסורק": CSV,
        "_geojson": [f"r2:datasets/{DS}/v1/5951d22f_geojson.gz"],
        "_hashes": {"scraper": "abc"},
    }) == 2


def test_empty_aggregates_are_not_counted():
    assert worker._landed_resource_count({
        "_gpkg": [], "_parquet": None, "_hashes": {"scraper": "abc"},
    }) == 0
