"""A complete row count is not proof of a faithful import.

Measured 1.8.2026: קווי גובה 50 ס"מ published 93,866 features against 93,866
declared by the source — a perfect completeness score — having replaced 93,436
contour LINES with points. Every counter agreed; the layer was gone. These tests
pin the one degradation OVER can detect without reading the files: the scrape
fell back to the point-identify engine after previously using a full one.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.api.worker as worker  # noqa: E402

W = worker._import_warning_for


def test_falling_back_to_quadtree_after_a_full_engine_warns():
    # The measured case: layer 228013, spatial-analysis before, quadtree now.
    assert W("quadtree", ["spatial-analysis"], None)


def test_any_full_engine_in_the_history_counts():
    for prev in ("wfs-paging", "wfs-bbox", "arcgis"):
        assert W("quadtree", [prev, "quadtree"], None), prev


def test_a_layer_that_only_ever_used_quadtree_is_not_flagged():
    # quadtree produced correct polygons for most layers. Flagging those would
    # make the warning noise, which is worse than no warning.
    assert W("quadtree", ["quadtree", "quadtree"], None) is None
    assert W("quadtree", [], None) is None


def test_a_first_version_is_not_flagged():
    assert W("quadtree", [], None) is None


def test_upgrading_to_the_full_engine_is_not_a_warning():
    # The opposite direction is the fix, not the defect: the new engine cleaned
    # 100 stray points out of ייעודי קרקע אשקלון and 149 out of תאונות דרכים.
    assert W("spatial-analysis", ["quadtree"], None) is None


def test_the_worker_can_declare_its_own_verdict():
    # The scraper is the only side that sees geometry, so its own finding wins
    # over anything inferred here.
    got = W("spatial-analysis", ["spatial-analysis"], "קווים נשמרו כנקודות")
    assert got == "קווים נשמרו כנקודות"


def test_a_declared_verdict_beats_the_inferred_one():
    got = W("quadtree", ["spatial-analysis"], "סיבה ספציפית מהסורק")
    assert got == "סיבה ספציפית מהסורק"


def test_a_blank_declaration_falls_through_to_the_rule():
    assert W("quadtree", ["spatial-analysis"], "   ") == W("quadtree", ["spatial-analysis"], None)


def test_a_clean_scrape_produces_no_warning():
    assert W("spatial-analysis", ["spatial-analysis", "quadtree"], None) is None


def test_the_message_matches_the_migration():
    # Migration 052 backfills with the same text; if they drift, a backfilled
    # dataset and a freshly-flagged one would say different things.
    import re
    from pathlib import Path

    mig = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "052_import_warning.py"
    text = mig.read_text(encoding="utf-8")
    m = re.search(r'_MSG = \(\s*(.+?)\s*\)\n', text, re.S)
    assert m, "migration 052 no longer declares _MSG"
    msg = "".join(re.findall(r'"([^"]*)"', m.group(1)))
    assert msg == worker._ENGINE_DOWNGRADE_WARNING
