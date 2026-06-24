"""Unit tests for the R2/object-storage layer (app/services/storage_client.py)
and the R2-aware resource_mappings helpers in app/api/versions.py.

No network: these cover the pure marker/key/url/gating logic and the
extract helpers that route a mapping value to ODATA vs R2.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.services.storage_client as sc  # noqa: E402
from app.config import settings  # noqa: E402


# ── marker round-trip ───────────────────────────────────────────────────

def test_mark_and_unwrap_roundtrip():
    key = "datasets/abc/v3/deadbeef_file.pdf"
    val = sc.mark(key)
    assert val == "r2:" + key
    assert sc.is_storage_value(val)
    assert sc.key_of(val) == key


def test_is_storage_value_rejects_odata_uuid():
    # A bare ODATA resource_id (UUID) must NOT look like an R2 value.
    assert not sc.is_storage_value("3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10")
    assert not sc.is_storage_value(None)
    assert not sc.is_storage_value(123)


def test_key_of_passthrough_for_bare_key():
    # key_of on an unmarked string returns it unchanged.
    assert sc.key_of("plain/key.txt") == "plain/key.txt"


# ── key building ────────────────────────────────────────────────────────

def test_build_key_shape_and_sanitization():
    key = sc.build_key("ds-123", 4, "דוח שנתי/2026 final.pdf")
    assert key.startswith("datasets/ds-123/v4/")
    # Hebrew + spaces + slash collapse to ASCII-safe; .pdf extension kept.
    assert key.endswith(".pdf")
    assert " " not in key  # spaces sanitized out of the filename segment
    # Path has exactly 4 segments: datasets / ds / vN / filename (the slash
    # in the original filename was collapsed, so no extra segment).
    assert len(key.split("/")) == 4


def test_build_key_is_unique_across_calls():
    # Same args twice → different keys (the random component), so a
    # placeholder version_number can't cause cross-version overwrites.
    a = sc.build_key("ds", 1, "attachments.zip")
    b = sc.build_key("ds", 1, "attachments.zip")
    assert a != b


# ── public_url ──────────────────────────────────────────────────────────

def test_public_url_from_marked_value(monkeypatch):
    monkeypatch.setattr(settings, "s3_public_base_url", "https://files.over.org.il/")
    url = sc.storage_client.public_url(sc.mark("datasets/x/v1/k_file.pdf"))
    # Single slash join, marker stripped.
    assert url == "https://files.over.org.il/datasets/x/v1/k_file.pdf"


def test_public_url_accepts_bare_key(monkeypatch):
    monkeypatch.setattr(settings, "s3_public_base_url", "https://cdn.example.com")
    assert sc.storage_client.public_url("a/b.csv") == "https://cdn.example.com/a/b.csv"


# ── is_enabled gating ───────────────────────────────────────────────────

def _set(monkeypatch, **kw):
    for k, v in kw.items():
        monkeypatch.setattr(settings, k, v)


def test_is_enabled_false_when_backend_is_odata(monkeypatch):
    _set(monkeypatch, storage_backend="odata", s3_endpoint="e", s3_bucket="b",
         s3_access_key="a", s3_secret_key="s", s3_public_base_url="u")
    assert sc.storage_client.is_enabled() is False


def test_is_enabled_false_when_creds_missing(monkeypatch):
    _set(monkeypatch, storage_backend="r2", s3_endpoint="", s3_bucket="b",
         s3_access_key="a", s3_secret_key="s", s3_public_base_url="u")
    assert sc.storage_client.is_enabled() is False


def test_is_enabled_true_when_r2_fully_configured(monkeypatch):
    _set(monkeypatch, storage_backend="r2", s3_endpoint="https://x.r2",
         s3_bucket="bucket", s3_access_key="ak", s3_secret_key="sk",
         s3_public_base_url="https://files.over.org.il")
    assert sc.storage_client.is_enabled() is True


# ── extract helpers (ODATA vs R2 routing) ───────────────────────────────

def _import_versions_helpers():
    from app.api.versions import _extract_resource_ids, _extract_storage_keys
    return _extract_resource_ids, _extract_storage_keys


def test_extract_resource_ids_skips_r2_and_keeps_odata():
    _extract_resource_ids, _ = _import_versions_helpers()
    odata_uuid = "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10"
    mappings = {
        "data.csv": odata_uuid,
        "_zip": sc.mark("datasets/d/v2/zz_a.zip"),
        "_zip_parts": [sc.mark("datasets/d/v2/p1.zip"), odata_uuid],
        "_hashes": {"scraper": "abc"},
    }
    ids = set(_extract_resource_ids(mappings))
    assert odata_uuid in ids
    # No r2-marked value leaks into the ODATA delete set.
    assert all("r2:" not in i for i in ids)


def test_extract_storage_keys_collects_only_r2():
    _, _extract_storage_keys = _import_versions_helpers()
    mappings = {
        "data.csv": "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10",  # ODATA, ignored
        "_zip": sc.mark("datasets/d/v2/zz_a.zip"),
        "_geojson": [sc.mark("datasets/d/v2/g1.geojson")],
        "_hashes": {"scraper": "abc"},
    }
    keys = set(_extract_storage_keys(mappings))
    assert keys == {"datasets/d/v2/zz_a.zip", "datasets/d/v2/g1.geojson"}


def test_v1_extract_handles_r2_and_odata(monkeypatch):
    """The public v1 versions payload must route r2: markers to the object
    store's public URL and bare UUIDs to ODATA — not build a broken ODATA
    URL out of an r2: key."""
    monkeypatch.setattr(settings, "s3_public_base_url", "https://files.over.org.il")
    from app.api.v1 import _extract_version_resources

    class _DS:
        odata_dataset_id = "odata-ds-1"

    odata_uuid = "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10"
    mappings = {
        "data.csv": odata_uuid,
        "_zip": sc.mark("datasets/d/v2/zz_a.zip"),
        "_geojson": [sc.mark("datasets/d/v2/g.geojson")],
        "_hashes": {"x": "y"},
        "_appendonly_seen": ["k1", "k2"],
    }
    by_name = {r.name: r for r in _extract_version_resources(_DS(), mappings)}

    # ODATA resource
    assert by_name["data.csv"].storage == "odata"
    assert by_name["data.csv"].odata_resource_id == odata_uuid
    assert "odata.org.il" in by_name["data.csv"].download_url

    # R2 resources → public URL, no odata id, never a broken odata URL
    assert by_name["_zip"].storage == "r2"
    assert by_name["_zip"].odata_resource_id is None
    assert by_name["_zip"].download_url == "https://files.over.org.il/datasets/d/v2/zz_a.zip"
    assert "odata.org.il" not in by_name["_zip"].download_url
    assert by_name["_geojson"].storage == "r2"
    assert by_name["_geojson"].format == "GeoJSON"

    # bookkeeping keys are never emitted as resources
    assert "_hashes" not in by_name
    assert "_appendonly_seen" not in by_name


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
