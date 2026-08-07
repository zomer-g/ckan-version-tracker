"""Files data.gov.il withholds must stay visible between polls.

data.gov.il serves an HTML challenge instead of some resource FILES, which
``version_detector`` reports as blocked ids. The warning built from them lived
only in ``poll_dataset``, AFTER two shortcuts that return earlier:

  * the metadata revision is one we already hold, and
  * a version already exists for this revision.

Being blocked is a property of the resource, not of one poll — the wall does not
come down because ``metadata_modified`` stood still. So a package frozen since
before the detection shipped took a shortcut past the only code that could
notice, and lost its files in total silence. Measured 2026-08-07 across the 13
CKAN datasets holding blocked files: only 6 carried any warning. רמזורים was
missing 11 resources, תושבים בישראל לפי ישובים another 11, with nothing said.

The finding is now remembered on the dataset, which fixes the warning and also
produces the structured work-list a browser-capable worker needs — instead of
it parsing a Hebrew sentence out of an error field.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.services import blocked_resources as br  # noqa: E402

RESOURCES = [
    {"id": "a", "name": "גושים", "format": "zip", "url": "https://e.data.gov.il/a.zip"},
    {"id": "b", "name": "טבלה", "format": "CSV", "url": "https://e.data.gov.il/b.csv",
     "datastore_active": True},
    {"id": "c", "name": "נחלים", "format": "shp", "url": "https://e.data.gov.il/c.zip"},
]


class _DS:
    """Just the two attributes the module touches."""

    def __init__(self, config=None):
        self.scraper_config = config
        self.last_error = None


# ---------------------------------------------------------------------------
# describing what was found
# ---------------------------------------------------------------------------

def test_entries_carry_what_a_worker_needs_to_fetch_the_file():
    entries = br.describe(RESOURCES, {"a", "c"})
    assert [e["id"] for e in entries] == ["a", "c"]
    assert entries[0] == {
        "id": "a", "name": "גושים", "format": "ZIP",
        "url": "https://e.data.gov.il/a.zip",
    }


def test_entry_order_follows_the_dataset_not_the_id_set():
    """A set iterates in whatever order it likes. If that leaked into the
    stored list, every poll would look like a change and rewrite the config."""
    assert [e["id"] for e in br.describe(RESOURCES, {"c", "a"})] == ["a", "c"]


def test_nothing_blocked_is_an_empty_list_and_no_note():
    assert br.describe(RESOURCES, set()) == []
    assert br.note_for([]) is None
    assert br.note_for(None) is None


def test_the_note_names_the_files():
    note = br.note_for(br.describe(RESOURCES, {"a", "c"}))
    assert "2 קבצים" in note
    assert "גושים (ZIP)" in note and "נחלים (SHP)" in note


# ---------------------------------------------------------------------------
# remembering it
# ---------------------------------------------------------------------------

def test_remember_replaces_the_config_rather_than_mutating_it():
    """scraper_config is plain JSONB, not a MutableDict — an in-place edit is
    never flagged dirty and never reaches the database."""
    original = {"storage_backend": "r2"}
    ds = _DS(original)
    assert br.remember(ds, br.describe(RESOURCES, {"a"})) is True
    assert ds.scraper_config is not original, "mutated in place — the write would be lost"
    assert ds.scraper_config["storage_backend"] == "r2", "clobbered the rest of the config"
    assert [e["id"] for e in ds.scraper_config[br.CONFIG_KEY]] == ["a"]


def test_an_unchanged_finding_is_not_rewritten():
    """Otherwise every poll dirties the row for nothing."""
    ds = _DS()
    entries = br.describe(RESOURCES, {"a"})
    assert br.remember(ds, entries) is True
    assert br.remember(ds, br.describe(RESOURCES, {"a"})) is False


def test_a_file_becoming_reachable_clears_the_finding():
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a"}))
    assert br.remember(ds, []) is True
    assert br.stored(ds) == []
    assert br.note_for(br.stored(ds)) is None


def test_assessed_tells_checked_and_clear_apart_from_never_checked():
    """The whole one-time-assessment gate rests on this distinction."""
    assert br.assessed(_DS()) is False
    assert br.assessed(_DS({"storage_backend": "r2"})) is False
    checked = _DS()
    br.remember(checked, [])
    assert br.assessed(checked) is True
    assert br.stored(checked) == []


def test_a_corrupt_stored_value_reads_as_never_checked():
    """Never crash a poll over a hand-edited config."""
    ds = _DS({br.CONFIG_KEY: "not a list"})
    assert br.assessed(ds) is False
    assert br.stored(ds) == []


# ---------------------------------------------------------------------------
# blocked is not the same as missing
#
# A worker CAN reach these files; this server cannot. Once one has been
# delivered the resource stays blocked — data.gov.il goes on refusing us, and
# the next poll detects it again, correctly — but it is no longer missing.
# Saying "waiting to be fetched" over a fully archived dataset would be false
# in the direction that actually misleads.
# ---------------------------------------------------------------------------

MOD = "2026-08-07T09:00:00"


def test_a_fetched_resource_stops_being_reported():
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a", "c"}))
    assert "2 קבצים" in br.note_for(br.stored(ds))

    br.mark_fetched(ds, ["a", "c"], modified=MOD, version=1)
    assert br.note_for(br.stored(ds)) is None
    assert br.pending(br.stored(ds)) == []
    # …but still recorded as blocked, because it still is.
    assert len(br.stored(ds)) == 2


def test_a_partial_rescue_reads_as_partial():
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a", "c"}))
    br.mark_fetched(ds, ["a"], modified=MOD, version=1)
    still = br.pending(br.stored(ds))
    assert [e["id"] for e in still] == ["c"]
    assert "1 קבצים" in br.note_for(br.stored(ds))


def test_the_next_poll_does_not_forget_what_was_fetched():
    """Detection rebuilds the entries from the source and has no memory. Without
    carry_fetch_state the notice returns on a dataset that is complete."""
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a"}))
    br.mark_fetched(ds, ["a"], modified=MOD, version=1)

    fresh = br.describe(RESOURCES, {"a"})            # a later poll, no memory
    assert br.note_for(fresh) is not None            # …would report it again
    carried = br.carry_fetch_state(fresh, br.stored(ds), modified=MOD)
    assert br.note_for(carried) is None


def test_a_revised_package_makes_the_file_pending_again():
    """The stamp records the revision it was fetched at. A package that moved
    may be offering a different file under the same resource id, so the rescue
    is asked for again — which is why this keys on the revision and not on a
    bare done-flag."""
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a"}))
    br.mark_fetched(ds, ["a"], modified=MOD, version=1)

    carried = br.carry_fetch_state(
        br.describe(RESOURCES, {"a"}), br.stored(ds),
        modified="2026-09-01T00:00:00")           # the source moved
    assert br.pending(carried), "a revised package must be re-fetched"


def test_marking_an_unknown_resource_changes_nothing():
    ds = _DS()
    br.remember(ds, br.describe(RESOURCES, {"a"}))
    assert br.mark_fetched(ds, ["zzz"], modified=MOD) is False
    assert br.mark_fetched(ds, [], modified=MOD) is False
