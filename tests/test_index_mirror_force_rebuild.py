"""The idx mirror needs a way to be told "these values were corrected".

``_can_append`` rebuilds when the column SET changes, because a new set is a new
identity and every row would read as new. But the row hash is over VALUES, so
back-filling a column that ALREADY existed causes exactly the same doubling with
no schema change to detect.

Measured in production on 2026-08-12: repairing the attachment join key on
jda-tenders re-inserted 159 of 198 rows beside their old copies, leaving
idx.jda_tenders at 357 rows for a 198-row corpus. The published version was
correct; only the queryable mirror was wrong, which is the worse half — an empty
column is visible, an inflated count looks healthy.

Nothing in the data distinguishes "corrected" from "new", so it is an operator's
call. These tests pin the lever that call needs.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import inspect  # noqa: E402

from app.services import index_mirror  # noqa: E402


def test_can_append_still_catches_the_schema_case():
    """The automatic guard is unchanged — a different column set rebuilds."""
    live = ["a", "b", index_mirror.HASH_COLUMN]
    assert index_mirror._can_append(live, ["a", "b"]) is True
    assert index_mirror._can_append(live, ["a", "b", "c"]) is False, (
        "a new column set is a new identity; appending would double the table")
    assert index_mirror._can_append(None, ["a"]) is False, "no table ⇒ rebuild"
    assert index_mirror._can_append(["a", "b"], ["a", "b"]) is False, (
        "no _row_hash ⇒ nothing to diff against ⇒ rebuild")


def test_can_append_cannot_see_a_value_backfill():
    """The case that bit us, stated as a fact rather than a wish.

    Identical column set, corrected values — the guard says "append", which is
    what produced 357 rows for a 198-row corpus. This is not a bug in
    _can_append: values are not visible to it. It is why force_rebuild exists.
    """
    live = ["item_id", "attachment_filename", index_mirror.HASH_COLUMN]
    assert index_mirror._can_append(live, ["item_id", "attachment_filename"]) is True


def test_the_force_rebuild_lever_is_threaded_all_the_way_down():
    """A flag that stops halfway is worse than none: the caller believes the
    table was replaced while it was appended to again."""
    assert "force_rebuild" in inspect.signature(index_mirror.load_index_csv).parameters
    assert "force_rebuild" in inspect.signature(index_mirror.sync_one).parameters
    assert "force_rebuild" in inspect.signature(index_mirror.sync_due).parameters
    # pending() needs its own flag: a corrected table's CSV did NOT move, so the
    # checkpoint considers it settled and would never offer it for reload.
    assert "force" in inspect.signature(index_mirror.pending).parameters


def test_force_rebuild_skips_the_append_branch():
    """Read the branch itself rather than trusting the signature."""
    src = inspect.getsource(index_mirror.load_index_csv)
    assert "if not force_rebuild and _can_append(live, columns):" in src


def test_force_clears_BOTH_checkpoint_gates_in_pending():
    """pending() filters on the checkpoint twice — once in the PASS-1 version
    diff, once again after the index CSV is resolved. The first attempt at this
    cleared only the first, so a forced run answered "pending: 0" and read as
    though the dataset were ineligible rather than as a flag that stopped
    halfway. Both gates, or the lever does nothing.
    """
    src = inspect.getsource(index_mirror.pending)
    assert "if force or done.get(str(r[0]), -1) < int(r[1])" in src, "PASS-1 gate"
    assert "if not force and done.get(str(ds.id), -1) >= vnum:" in src, "second gate"
    assert src.count("done.get(") == 2, (
        "a third checkpoint test appeared — make sure it honours `force` too")


def test_the_admin_endpoint_exposes_it():
    from app.api import admin
    sig = inspect.signature(admin.index_mirror_sync).parameters
    assert "force_rebuild" in sig
    assert sig["force_rebuild"].default is False, (
        "must be opt-in — a full reload of every mirrored table is not a default")


def test_forcing_without_a_dataset_id_is_possible_but_deliberate():
    """force_rebuild has no implicit scope guard, so `limit` is the only thing
    standing between a mistyped call and reloading every mirrored table. Pinned
    so that if a scope guard is ever added, this test is where it gets stated."""
    from app.api import admin
    sig = inspect.signature(admin.index_mirror_sync).parameters
    assert sig["dataset_id"].default is None
    assert sig["limit"].default == 20
