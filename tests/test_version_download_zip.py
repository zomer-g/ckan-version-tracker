"""One version, one ZIP.

The per-file links redirect to storage and cost this server nothing, which is
why they exist — but a version of a 51-resource dataset offered 51 of them,
fired 500ms apart: 25 seconds of clicking, 51 entries in the download shelf and
a browser permission prompt. And the page's list is the changelog, so it could
not hand over the resources a version carried forward at all.

These pin the selection and the archive's shape. The streaming itself (one
member in memory, the sink that never seeks) is exercised by building a real
ZIP from fake storage and reading it back with zipfile.
"""
import asyncio
import io
import os
import sys
import types
import zipfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import versions as vapi  # noqa: E402
from app.services import storage_client as storage  # noqa: E402

run = asyncio.run


class _DS:
    title = 'עסקאות נדל"ן'
    odata_dataset_id = None


class _V:
    version_number = 4

    def __init__(self, mappings):
        self.resource_mappings = mappings


def _entries(mappings):
    return vapi.version_zip_entries(_V(mappings), _DS())


def test_carried_resources_are_in_the_archive():
    """The whole point: a version that uploaded 2 files and re-referenced 3
    still CONTAINS 5, and the ZIP is the only way to get them."""
    m = {
        "5000 — תל אביב": "r2:datasets/x/v4/aaaaaaaa_5000.csv",
        "3000 — ירושלים": "r2:datasets/x/v4/bbbbbbbb_3000.csv",
        "4000 — חיפה": "r2:datasets/x/v3/cccccccc_4000.csv",
        "7900 — פתח תקווה": "r2:datasets/x/v3/dddddddd_7900.csv",
        "8300 — ראשון לציון": "r2:datasets/x/v2/eeeeeeee_8300.csv",
    }
    assert len(_entries(m)) == 5


def test_bookkeeping_keys_are_not_files():
    """resource_mappings mixes real resources with internal state. Only the
    internal keys that genuinely carry files are kept."""
    m = {
        "רשומות": "r2:datasets/x/v1/aaaaaaaa_rows.csv",
        "_hashes": {"scraper": "deadbeef"},
        "_resource_ids": [],
        "_appendonly_seen": ["a", "b"],
        "_names": {"k": "v"},
        "_filedates": {"k": "2026-09-19"},
        "append_table": "append_x_deadbeef",
        "_zip": "r2:datasets/x/v1/ffffffff_files.zip",
    }
    keys = {e["key"] for e in _entries(m)}
    assert keys == {"רשומות", "_zip"}


def test_a_multi_part_mapping_becomes_one_member_per_part():
    m = {"_zip_parts": ["r2:datasets/x/v1/aaaaaaaa_p1.zip",
                        "r2:datasets/x/v1/bbbbbbbb_p2.zip"]}
    es = _entries(m)
    assert len(es) == 2
    assert es[0]["filename"] != es[1]["filename"], "parts must not collide"


def test_short_and_non_string_values_are_ignored():
    """A mapping value can be a flag, a count or an empty string — none of
    those name a file, and treating one as a key would 404 mid-archive."""
    m = {"good": "r2:datasets/x/v1/aaaaaaaa_rows.csv",
         "empty": "", "short": "abc", "flag": True, "count": 7}
    assert {e["key"] for e in _entries(m)} == {"good"}


def test_member_names_cannot_escape_the_archive():
    """A name is a name, not a path. Hebrew survives — ZIP carries UTF-8 — but
    separators and Windows-reserved characters do not."""
    for raw in ("a/b.csv", "a\\b.csv", "../../etc/passwd", "C:\\windows\\x"):
        safe = vapi._zip_safe(raw, "x")
        assert "/" not in safe and "\\" not in safe, raw
        assert not safe.startswith("."), raw   # no traversal, no dotfile
    assert vapi._zip_safe("עסקאות נדלן.csv", "x") == "עסקאות נדלן.csv"
    assert vapi._zip_safe("   ", "fallback") == "fallback"


def test_the_stream_builds_a_readable_zip(monkeypatch):
    """End to end over the real generator: a valid archive, every member
    present, plus the index."""
    async def _get(value):
        return ("rows for " + value).encode("utf-8")

    monkeypatch.setattr(storage.storage_client, "get_object_bytes", _get)
    entries = [
        {"key": "a", "value": "r2:x/a.csv", "stored": True, "filename": "a.csv"},
        {"key": "b", "value": "r2:x/b.csv", "stored": True, "filename": "b.csv"},
    ]

    async def collect():
        out = b""
        async for chunk in vapi._version_zip_stream(entries, "pkg", False):
            out += chunk
        return out

    blob = run(collect())
    zf = zipfile.ZipFile(io.BytesIO(blob))
    assert zf.testzip() is None
    names = set(zf.namelist())
    assert {"a.csv", "b.csv", "_index.csv"} <= names
    assert "_errors.txt" not in names
    assert zf.read("a.csv") == b"rows for r2:x/a.csv"


def test_a_file_that_fails_is_reported_not_swallowed(monkeypatch):
    """Once the first byte is out, failing the request hands over a truncated
    archive with no explanation. The rest of the files still go, and the
    failure is named inside."""
    async def _get(value):
        return None if "b" in value else b"ok"

    monkeypatch.setattr(storage.storage_client, "get_object_bytes", _get)
    entries = [
        {"key": "a", "value": "r2:x/a.csv", "stored": True, "filename": "a.csv"},
        {"key": "b", "value": "r2:x/b.csv", "stored": True, "filename": "b.csv"},
    ]

    async def collect():
        out = b""
        async for chunk in vapi._version_zip_stream(entries, "pkg", False):
            out += chunk
        return out

    zf = zipfile.ZipFile(io.BytesIO(run(collect())))
    assert zf.testzip() is None
    assert "a.csv" in zf.namelist()
    assert "b.csv" not in zf.namelist()
    assert b"b\t" in zf.read("_errors.txt")


def test_the_concurrency_slot_is_released_by_the_generator(monkeypatch):
    """The slot is reserved before the response is handed off, so the only
    thing that can free it is the generator's finally — including when the
    client disconnects mid-archive."""
    async def _get(value):
        return b"x"

    monkeypatch.setattr(storage.storage_client, "get_object_bytes", _get)
    before = vapi._zip_in_flight
    assert vapi._reserve_version_zip_slot() is True
    assert vapi._zip_in_flight == before + 1

    entries = [{"key": "a", "value": "r2:x/a.csv", "stored": True,
                "filename": "a.csv"}]

    async def collect():
        async for _ in vapi._version_zip_stream(entries, "pkg", True):
            pass

    run(collect())
    assert vapi._zip_in_flight == before, "the slot leaked"
