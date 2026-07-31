"""A source the publisher removed is a finding, not a failed poll.

The GovMap engine only says "is not in the catalog and returned 0 features"
after it fetched the catalog successfully and found the layer id absent; a
catalog it could not reach, or one that DOES list the layer, raises an
explicitly transient error instead. These tests pin that distinction, because
the whole feature rests on it: mark a dataset on the certain verdict, and never
on the transient ones.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import app.api.worker as worker  # noqa: E402

# Verbatim from govscraper/scrapers/govmap/legacy_engine.py.
GONE = (
    "GovMap layer 237469 is not in the catalog and returned 0 features — the "
    "layer id was likely removed or renumbered in GovMap's 2026 rebuild. "
    "Update the dataset's lay= id or remove it; not publishing an empty version."
)
TRANSIENT_UNREACHABLE = (
    "GovMap flaked during this scrape (catalog unreachable) — layer 237469 "
    "returned 0 features. Treating as a TRANSIENT GovMap-side failure (deploy "
    "window / outage), not publishing an empty version."
)
TRANSIENT_LISTED = (
    "GovMap flaked during this scrape (catalog has the layer but the sweep saw "
    "none) — layer 237469 returned 0 features. Treating as a TRANSIENT "
    "GovMap-side failure (deploy window / outage), not publishing an empty version."
)


def test_the_certain_verdict_is_recognised():
    assert worker._is_source_gone_error(GONE)


def test_a_catalog_outage_is_not_a_removed_source():
    # The layer may well be alive; GovMap just could not be asked. Marking it
    # would put "removed at the publisher" on the public page during an outage.
    assert not worker._is_source_gone_error(TRANSIENT_UNREACHABLE)


def test_a_listed_layer_that_returned_nothing_is_not_removed():
    assert not worker._is_source_gone_error(TRANSIENT_LISTED)


def test_unrelated_failures_are_not_removed_sources():
    for err in (
        "GeometryFetchError: object-geojson-data failed for 233969#23918 after 5 tries",
        "SpatialQueryError: layer 233910: extracted 146 features but the layer declares 147",
        "Task auto-reset: no heartbeat for 10 min",
        "push-version failed: 502 all_pushes_failed",
        "Partial scrape: 94766 features vs the previous version's 1000000 (<50%)",
        "",
        None,
    ):
        assert not worker._is_source_gone_error(err), err


def test_marker_matches_the_migration():
    # Migration 047 backfills with a LIKE on the same substring. If one drifts
    # the backfill and the live detection stop agreeing.
    import re
    from pathlib import Path

    mig = Path(__file__).resolve().parents[1] / "alembic" / "versions" / "047_source_gone_at.py"
    text = mig.read_text(encoding="utf-8")
    m = re.search(r'_MARKER\s*=\s*"([^"]+)"', text)
    assert m, "migration 047 no longer declares _MARKER"
    assert m.group(1) == worker._SOURCE_GONE_MARKER
