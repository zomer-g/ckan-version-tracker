"""Tests for the append-only ``first_seen`` per-row timestamp.

Both append paths (delta_archiver streaming for the vehicle registry, and
poll_job._poll_append_only for the flights board) must stamp every appended
row with a ``first_seen`` timestamp column WITHOUT letting that synthetic value
leak into the dedup identity. These tests lock both invariants.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.snapshot_service import (  # noqa: E402
    APPEND_FIRST_SEEN_FIELD,
    stamp_first_seen,
)
from app.services.version_detector import (  # noqa: E402
    compute_new_rows,
    compute_new_rows_windowed,
)


def test_stamp_adds_field_once_and_stamps_rows():
    fields = [{"id": "mispar_rechev", "type": "numeric"}]
    rows = [{"mispar_rechev": "1"}, {"mispar_rechev": "2"}]

    out_fields, out_rows = stamp_first_seen(fields, rows, when="2026-06-26T14:30:00")

    # Column added exactly once, typed as timestamp.
    stamp_cols = [f for f in out_fields if f["id"] == APPEND_FIRST_SEEN_FIELD]
    assert len(stamp_cols) == 1
    assert stamp_cols[0]["type"] == "timestamp"
    # Every row carries the pinned value.
    assert all(r[APPEND_FIRST_SEEN_FIELD] == "2026-06-26T14:30:00" for r in out_rows)


def test_stamp_is_idempotent_on_fields_and_preserves_existing_values():
    fields = [
        {"id": "mispar_rechev", "type": "numeric"},
        {"id": APPEND_FIRST_SEEN_FIELD, "type": "timestamp"},
    ]
    rows = [{"mispar_rechev": "1", APPEND_FIRST_SEEN_FIELD: "2020-01-01T00:00:00"}]

    out_fields, out_rows = stamp_first_seen(fields, rows, when="2026-06-26T14:30:00")

    # Field not duplicated when already present.
    assert sum(f["id"] == APPEND_FIRST_SEEN_FIELD for f in out_fields) == 1
    # A row that already has a first_seen keeps its original value — a later
    # re-stamp must never rewrite when the row first entered the archive.
    assert out_rows[0][APPEND_FIRST_SEEN_FIELD] == "2020-01-01T00:00:00"


def test_first_seen_does_not_perturb_full_row_hash_identity():
    """The flights path dedups by full-row hash. Stamping must happen AFTER
    dedup, so two polls of the same source row collapse to one — proven here
    by hashing the source rows (no stamp) and confirming the second poll adds
    nothing."""
    source_row = {"CHOPER": "LY", "CHFLTN": "356", "CHRMINE": "LANDED"}

    new1, seen = compute_new_rows(None, [dict(source_row)], key_field=None)
    assert len(new1) == 1

    # Stamp the first batch as the push path would — must not feed back into seen.
    stamp_first_seen([], new1, when="2026-06-26T14:30:00")

    # Second poll: same source row again, dedup against carried seen-keys.
    new2, seen = compute_new_rows(seen, [dict(source_row)], key_field=None)
    assert new2 == []  # identity unaffected by the first_seen stamp


def test_first_seen_does_not_perturb_keyed_identity():
    """Vehicle path dedups by append_key (mispar_rechev). Same invariant."""
    row = {"mispar_rechev": "1000039", "baalut": "פרטי"}
    new1, seen = compute_new_rows([], [dict(row)], key_field="mispar_rechev")
    assert len(new1) == 1
    stamp_first_seen([], new1, when="2026-06-26T14:30:00")
    new2, seen = compute_new_rows(seen, [dict(row)], key_field="mispar_rechev")
    assert new2 == []


# --- windowed seen-set (flights high-churn board) ---------------------------

def _flight(status):
    return {"CHOPER": "LY", "CHFLTN": "356", "CHSTOL": "2026-06-25T00:25:00",
            "CHAORD": "A", "CHRMINE": status}


def test_windowed_flags_new_and_refreshes_generation():
    # Gen 1: brand-new row.
    new1, seen = compute_new_rows_windowed({}, [_flight("SCHEDULED")], None, 1)
    assert len(new1) == 1
    assert set(seen.values()) == {1}

    # Gen 2: identical row still on board → NOT new, but generation refreshed.
    new2, seen = compute_new_rows_windowed(seen, [_flight("SCHEDULED")], None, 2)
    assert new2 == []
    assert set(seen.values()) == {2}  # refresh-on-seen keeps it alive

    # Gen 3: same flight, status changed → distinct full-row hash → new row.
    new3, seen = compute_new_rows_windowed(seen, [_flight("LANDED")], None, 3)
    assert len(new3) == 1
    assert len(seen) == 2  # two distinct states now tracked


def test_windowed_dedups_within_a_single_poll():
    rows = [_flight("SCHEDULED"), _flight("SCHEDULED")]
    new, seen = compute_new_rows_windowed({}, rows, None, 1)
    assert len(new) == 1  # same row twice in one page → inserted once
    assert len(seen) == 1


def test_windowed_eviction_drops_only_long_absent_keys():
    # Simulate the caller's eviction: keep entries with gen > next_version - window.
    window = 3
    # A row present at gen 1 then absent. By gen 5 its gen (1) is outside the
    # window (cutoff = 5 - 3 = 2), so it evicts.
    _, seen = compute_new_rows_windowed({}, [_flight("SCHEDULED")], None, 1)
    next_version, cutoff = 5, 5 - window
    evicted = {k: g for k, g in seen.items() if g > cutoff}
    assert evicted == {}  # aged out — safe, a dated flight row never recurs

    # A row refreshed at gen 4 survives the same cutoff.
    _, seen2 = compute_new_rows_windowed({}, [_flight("SCHEDULED")], None, 4)
    kept = {k: g for k, g in seen2.items() if g > cutoff}
    assert len(kept) == 1
