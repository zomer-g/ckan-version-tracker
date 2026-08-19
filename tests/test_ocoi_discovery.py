"""Regression guards for OCOI candidate discovery.

The first version of `discover_candidates` returned ZERO new candidates against
a catalog holding 420 undiscovered declarations, for two compounding reasons —
both cheap to re-introduce, hence these tests:

  1. a fixed `start < 400` page ceiling, while the query matches 994 packages;
  2. no `sort`, so CKAN ordered by relevance — and the highest-relevance
     packages are precisely the ones already imported, because the corpus was
     built from this same query. The two together made discovery structurally
     incapable of finding anything new.
"""
import inspect

from app.services import ocoi_ingest


def test_page_bound_is_far_above_the_live_catalog():
    """994 packages match today. A bound near that is a truncation waiting to
    happen; it must have real headroom."""
    assert ocoi_ingest._MAX_PACKAGES >= 5000
    assert ocoi_ingest._PAGE <= 100  # CKAN caps rows per request


def test_discovery_pages_by_an_explicit_stable_sort():
    """Relevance order is not stable across requests, so paging without an
    explicit sort can skip packages entirely."""
    src = inspect.getsource(ocoi_ingest.discover_candidates)
    assert '"sort"' in src, "package_search must pass an explicit sort"
    assert "metadata_modified desc" in src, (
        "sort must be newest-first: it is what makes the early exit find NEW "
        "declarations rather than re-walking the oldest ones")


def test_discovery_stops_on_ckan_reported_count_not_a_magic_number():
    """The walk must terminate on the count CKAN reports, so the catalog can
    grow without silently outgrowing a hardcoded ceiling."""
    src = inspect.getsource(ocoi_ingest.discover_candidates)
    assert "start >= total" in src
    assert "start < 400" not in src, "the fixed 400-package ceiling is back"
