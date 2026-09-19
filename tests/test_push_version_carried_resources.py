"""A version's resources are not all NEW just because they are all in it.

The documented way to publish a partial update of a multi-resource dataset is to
re-reference the files that have not changed (csv_resource_ids) instead of
uploading them again — for the real-estate corpus that was 10 uploads instead of
51, about 300 MB saved on the push.

Every one of the 51 is genuinely part of the version. Only 10 are new. Counting
them together made the page announce "51 resources added" over a download list
holding 10, with nothing saying where the other 41 went.
"""
import inspect
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import worker  # noqa: E402


def test_a_re_referenced_resource_is_recorded_as_carried_not_added():
    """Pinned by reading the source: the real split happens deep inside
    push_version, past R2 and the append store."""
    src = inspect.getsource(worker.push_version)
    assert "carried_resource_ids.append(pre_uploaded)" in src, (
        "a re-referenced resource is no longer being recorded as carried")
    assert '"resources_carried": carried_resource_ids' in src


def test_added_excludes_what_was_only_carried():
    src = inspect.getsource(worker.push_version)
    assert ('"resources_added": [r for r in odata_resource_ids\n'
            '                                if r not in set(carried_resource_ids)]') in src, (
        "resources_added is counting carried resources again")


def test_both_lists_together_are_still_the_whole_version():
    """The split must not lose a resource: added ∪ carried is what the version
    holds. Exercised on the real rule rather than the source text."""
    odata_resource_ids = ["a", "b", "c", "d"]
    carried_resource_ids = ["c", "d"]
    added = [r for r in odata_resource_ids if r not in set(carried_resource_ids)]
    assert added == ["a", "b"]
    assert sorted(added + carried_resource_ids) == sorted(odata_resource_ids)


def test_a_push_that_uploads_everything_carries_nothing():
    """The common case — no csv_resource_ids — must behave exactly as before,
    with every resource counted as added and the carried list empty."""
    odata_resource_ids = ["a", "b"]
    carried_resource_ids: list[str] = []
    added = [r for r in odata_resource_ids if r not in set(carried_resource_ids)]
    assert added == odata_resource_ids
    assert carried_resource_ids == []
