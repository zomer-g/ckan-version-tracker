"""A complete row count is not proof of a faithful import.

Measured 1.8.2026: קווי גובה 50 ס"מ published 93,866 features against 93,866
declared by the source — a perfect completeness score — having replaced 93,436
contour LINES with points. Every counter agreed; the layer was gone.

These tests pin a boundary rather than a detector: OVER never parses the files
it stores, so it cannot see that, and an attempt to infer it from the engine
name was measured wrong in both directions (see migration 053). Only a verdict
the WORKER declares is trusted.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.api.worker as worker  # noqa: E402

W = worker._import_warning_for


def test_only_a_declared_verdict_produces_a_warning():
    # The scraper is the only side that parses geometry, so its finding is the
    # only thing trusted here.
    assert W("spatial-analysis", ["spatial-analysis"], "קווים נשמרו כנקודות") == "קווים נשמרו כנקודות"
    assert W("quadtree", ["spatial-analysis"], "סיבה מהסורק") == "סיבה מהסורק"


def test_an_engine_downgrade_alone_is_not_a_warning():
    # Measured on prod 1.8.2026: גני ילדים went spatial-analysis → quadtree with
    # both versions holding the identical 20,465 POINTs. Flagging that is a false
    # alarm, and a false alarm is worse than silence.
    assert W("quadtree", ["spatial-analysis"], None) is None
    assert W("quadtree", ["wfs-paging", "arcgis"], None) is None


def test_the_motivating_case_shows_why_inference_was_dropped():
    # קווי גובה 50 ס"מ has only ever used quadtree, so an engine-downgrade rule
    # could never have caught it — and its lines HAD just become points.
    assert W("quadtree", ["quadtree"], None) is None


def test_a_layer_that_only_ever_used_quadtree_is_not_flagged():
    assert W("quadtree", ["quadtree", "quadtree"], None) is None
    assert W("quadtree", [], None) is None


def test_upgrading_to_the_full_engine_is_not_a_warning():
    assert W("spatial-analysis", ["quadtree"], None) is None


def test_a_blank_declaration_is_not_a_warning():
    assert W("quadtree", ["spatial-analysis"], "   ") is None
    assert W("quadtree", ["spatial-analysis"], "") is None


def test_a_clean_scrape_produces_no_warning():
    assert W("spatial-analysis", ["spatial-analysis", "quadtree"], None) is None


def test_a_long_declaration_is_truncated():
    assert len(W("quadtree", [], "x" * 5000)) == 2000
