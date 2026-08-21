"""Switching a tracked dataset from full_snapshot to append_only.

jeden.co.il cleared 37 of the 39 tenders off its מכרזים tab, so the archive
source — which publishes the page as the whole index — pushed 7 rows against
a stored 196 and OVER's shrink guard rejected it. The answer is to make that
dataset cumulative (append_only), and two things have to hold for the switch
to be worth anything:

  1. The first append push must START from the snapshot history, not from
     whatever the source still lists, or the conversion drops the very rows
     it was made to keep.
  2. The shrink guard must not reject that first push. It is already inert
     for datasets with an append version behind them (those record
     ``rows_total``, not ``total_rows``); a converted dataset's baseline is
     its last SNAPSHOT version, so the exemption has to be explicit.
"""
import asyncio
import os
import pathlib

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import worker as worker_api  # noqa: E402
from app.services.csv_parser import records_to_csv_bytes  # noqa: E402
from app.services.version_detector import compute_new_rows  # noqa: E402


class _Ds:
    id = "fc410dc2-0278-4bdd-903c-98f21fa31a67"


class _Version:
    def __init__(self, mappings):
        self.resource_mappings = mappings


class _FakeStorage:
    """Only get_object_bytes is exercised by the seed."""

    def __init__(self, objects):
        self.objects = objects
        self.asked = []

    async def get_object_bytes(self, value):
        self.asked.append(value)
        return self.objects.get(value)


_FIELDS = [{"id": "item_id", "type": "text"},
           {"id": "title", "type": "text"},
           {"id": "documents.file_url", "type": "text"}]
_ARCHIVED = [
    {"item_id": f"tender-{n}", "title": f"מכרז {n}",
     "documents.file_url": f"https://jeden.co.il/f/{n}.pdf"}
    for n in range(39)
]


def _csv(rows):
    return records_to_csv_bytes(_FIELDS, rows)


def test_seed_reads_the_previous_snapshot_csv(monkeypatch):
    value = "r2:datasets/fc410dc2/v1/68376c7d_file.csv"
    fake = _FakeStorage({value: _csv(_ARCHIVED)})
    monkeypatch.setattr(worker_api, "storage_client", fake)

    seed = asyncio.run(worker_api._append_seed_from_snapshot(
        _Ds(), _Version({"נתוני הסורק": value}),
    ))
    assert list(seed) == ["נתוני הסורק"]
    assert len(seed["נתוני הסורק"]) == 39


def test_seed_skips_zips_aggregates_and_non_storage_values(monkeypatch):
    fake = _FakeStorage({})
    monkeypatch.setattr(worker_api, "storage_client", fake)

    seed = asyncio.run(worker_api._append_seed_from_snapshot(
        _Ds(),
        _Version({
            "_zip_parts": "r2:datasets/fc410dc2/v1/98da9db7_part-1.zip",
            "_appendonly_seen": ["k1", "k2"],
            "bundle": "r2:datasets/fc410dc2/v1/98da9db7_part-1.zip",
            "odata-backed": "e4233f6c-d888-4923-a175-cd4cad3e6461",
        }),
    ))
    assert seed == {}
    assert fake.asked == [], "nothing but a CSV object should be read back"


def test_conversion_keeps_every_archived_row(monkeypatch):
    """The real shape: the source now lists 2 of the 39 tenders plus 1 new."""
    value = "r2:datasets/fc410dc2/v1/68376c7d_file.csv"
    monkeypatch.setattr(worker_api, "storage_client",
                        _FakeStorage({value: _csv(_ARCHIVED)}))

    seed = asyncio.run(worker_api._append_seed_from_snapshot(
        _Ds(), _Version({"נתוני הסורק": value})))

    seen: list[str] = []
    for rows in seed.values():
        _, seen = compute_new_rows(seen, rows, "documents.file_url")

    still_listed = [_ARCHIVED[3], _ARCHIVED[7]]
    brand_new = {"item_id": "tender-4-26", "title": "מכרז 4/26",
                 "documents.file_url": "https://jeden.co.il/f/426.pdf"}
    new_rows, seen = compute_new_rows(seen, still_listed + [brand_new],
                                      "documents.file_url")

    assert new_rows == [brand_new], "already-archived rows must not duplicate"
    cumulative = list(seed["נתוני הסורק"]) + new_rows
    assert len(cumulative) == 40
    assert len(seen) == 40


def test_shrink_guard_exempts_append_only():
    """Asserted at the source: the guard runs before is_append is computed, so
    the exemption lives inside the condition itself and nothing else would
    notice if it were dropped."""
    src = (pathlib.Path(__file__).resolve().parents[1]
           / "app" / "api" / "worker.py").read_text(encoding="utf-8")
    guard = src.split("# ---- Shrink guard")[1].split("min_fraction")[0]
    assert 'ds.storage_mode != "append_only"' in guard


# ── the pre-uploaded CSV path ──────────────────────────────────────────────
# The archive worker uploads its index CSV out-of-band and pushes a reference
# instead of inline records. That branch mapped the file in whole, on purpose,
# because "append mode can't dedupe a file we never parsed" — so the eden
# conversion published a 7-row snapshot wearing an append label: seen-set 196,
# rows_added 0, and a version pointing at 7 rows.


def test_a_pre_uploaded_csv_is_read_back_in_append_mode():
    src = (pathlib.Path(__file__).resolve().parents[1]
           / "app" / "api" / "worker.py").read_text(encoding="utf-8")
    branch = (src.split("pre_uploaded = csv_resource_ids.get(res.name)")[1]
                 .split("\n            if pre_uploaded:")[0])
    assert "is_append" in branch, "append mode must not map the file in whole"
    assert "APPEND_MERGE_MAX_BYTES" in branch, "the read-back must stay bounded"


def test_the_seed_is_keyed_on_the_cumulative_not_the_seen_set():
    """A seen-set can outlive a push that never wrote a cumulative file (that
    is exactly what the botched conversion left behind). Reading it as
    'already converted' would publish an empty archive."""
    src = (pathlib.Path(__file__).resolve().parents[1]
           / "app" / "api" / "worker.py").read_text(encoding="utf-8")
    seed_call = src.split("append_seed = await _append_seed_from_snapshot")[0]
    guard = seed_call.rsplit("if is_append and latest is not None:", 1)[1]
    assert "storage.is_storage_value(ds.appendonly_resource_id)" in guard
    assert "not seen_keys" not in guard


def test_csv_shaped_rows_dedupe_against_the_seed():
    """Both sides now arrive as CSV rows — every column present, blank where
    empty — so the hash identity matches and nothing re-appends."""
    from app.services.csv_parser import parse_csv

    _f, seeded = parse_csv(_csv(_ARCHIVED))
    _f2, current = parse_csv(_csv([_ARCHIVED[3], _ARCHIVED[7]]))

    seen: list[str] = []
    _, seen = compute_new_rows(seen, seeded, None)
    new_rows, seen = compute_new_rows(seen, current, None)

    assert new_rows == [], "a row already in the archive must not re-append"
    assert len(seen) == 39
