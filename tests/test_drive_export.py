"""Unit tests for the Drive-export pure logic:

  * storage_client.enumerate_files — flatten a version's resource_mappings
    into the (filename, value) list the runner uploads.
  * drive_client.extract_folder_id — parse a pasted Drive folder URL / id.

No network: these are the deterministic bits the rest of the feature relies on.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.services.storage_client as sc  # noqa: E402
from app.services import drive_client as dc  # noqa: E402


# ── enumerate_files ─────────────────────────────────────────────────────

def test_enumerate_files_zip_parts_and_csv():
    mappings = {
        "_hashes": {"x": "y"},
        "_resource_ids": [],
        "_zip_parts": [
            "r2:datasets/d/v1/aabbccdd_part-1.zip",
            "r2:datasets/d/v1/11223344_part-2.zip",
        ],
        "_csv": "r2:datasets/d/v1/26613db3_csv",
        # The CSV is referenced twice (also under its human name) — dedupe.
        "נתוני הסורק": "r2:datasets/d/v1/26613db3_csv",
    }
    files = sc.enumerate_files(mappings)
    names = [n for n, _ in files]
    values = [v for _, v in files]
    # CSV counted once, with a sensible extension; both zip parts present.
    assert "part-1.zip" in names
    assert "part-2.zip" in names
    assert names.count("data.csv") == 1
    assert len(values) == len(set(values)) == 3


def test_enumerate_files_skips_internal_and_nonfiles():
    mappings = {
        "_hashes": {"a": "b"},
        "_filedates": {"k": "2026-01-01"},
        "_names": {"k": "Some Name"},
        "metadata": "short",  # too short to be a resource id → skipped
        "doc": "r2:datasets/d/v2/feedface_report.pdf",
    }
    files = sc.enumerate_files(mappings)
    assert files == [("report.pdf", "r2:datasets/d/v2/feedface_report.pdf")]


def test_enumerate_files_odata_uuid_uses_key_name():
    mappings = {"budget.csv": "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10"}
    files = sc.enumerate_files(mappings)
    assert files == [("budget.csv", "3f1c0e22-7a0b-4d9e-9c1a-2b6f5e8d4a10")]


def test_enumerate_files_empty():
    assert sc.enumerate_files(None) == []
    assert sc.enumerate_files({}) == []


# ── extract_folder_id ───────────────────────────────────────────────────

def test_extract_folder_id_variants():
    fid = "1AbC_dEfGhIjKlMnOpQrS"
    assert dc.extract_folder_id(f"https://drive.google.com/drive/folders/{fid}?usp=sharing") == fid
    assert dc.extract_folder_id(f"https://drive.google.com/open?id={fid}") == fid
    assert dc.extract_folder_id(fid) == fid
    assert dc.extract_folder_id("   ") is None
    assert dc.extract_folder_id("not a link") is None


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
