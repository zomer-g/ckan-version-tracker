"""A worker that was closed is not a scrape that failed.

Long GovMap layers take hours, and the operator routinely stops the worker
part-way. OVER's heartbeat watchdog then marks the task failed — putting a
closed laptop in the same list as GeometryFetchError and a short extraction,
where a real defect and an ordinary interruption look identical. Measured on
prod 2.8.2026: "סוג בעלות בחלקות רשומות" and "חלקות" both sat in the failures
panel purely because the worker went away after 8 hours of work.

These tests pin the classifier that keeps the two apart.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.models.scrape_task import (  # noqa: E402
    INTERRUPTED_MESSAGE,
    PHASE_INTERRUPTED,
    is_interrupted,
)


def test_the_phase_marks_an_interrupted_run():
    assert is_interrupted(PHASE_INTERRUPTED, "anything at all")


def test_the_new_message_is_recognised():
    msg = INTERRUPTED_MESSAGE.format(hb=13, age=486)
    assert is_interrupted(PHASE_INTERRUPTED, msg)


def test_historical_rows_classify_without_a_migration():
    # Written before the phase existed — both wordings, from the two watchdogs.
    for err in (
        "Task auto-reset: no heartbeat for 10 min (task age 31 min) — worker likely crashed",
        "Task auto-reset by scheduler: no heartbeat for 13 min (task age 486 min) — worker likely crashed",
    ):
        assert is_interrupted("timeout", err), err


def test_a_real_scrape_failure_is_not_an_interruption():
    for phase, err in (
        ("scraping", "GeometryFetchError: object-geojson-data failed for 233969#23918 after 5 tries"),
        ("scraping", "SpatialQueryError: layer 233910: extracted 146 features but the layer declares 147"),
        ("exporting", 'push-version failed: 502 {"error":"all_pushes_failed"}'),
        ("scraping", "GovMap layer 456 is not in the catalog and returned 0 features"),
        ("scraping", "Partial scrape: 94766 features vs the previous version's 1000000 (<50%)"),
        ("scraping", "ArcGIS page@0 failed after 3 tries"),
    ):
        assert not is_interrupted(phase, err), err


def test_missing_fields_are_not_interruptions():
    assert not is_interrupted(None, None)
    assert not is_interrupted("", "")


def test_the_message_names_the_cause_and_the_remedy():
    msg = INTERRUPTED_MESSAGE.format(hb=13, age=486)
    # The reader has to be able to tell this apart from a defect at a glance,
    # and know that re-running is the action.
    assert "אינה שגיאת גרידה" in msg
    assert "13" in msg and "486" in msg
