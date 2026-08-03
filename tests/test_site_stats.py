"""Unit tests for the home-page site totals (no DB needed).

Covers the two things that can silently go wrong in app/services/site_stats.py:
the row total (which mixes live planner estimates with the counts the catalog
already carries) and the fail-soft/caching contract the home page depends on.
"""
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.services import site_stats as S  # noqa: E402


@pytest.fixture(autouse=True)
def _clear_cache():
    S._cache, S._cache_at = None, 0.0
    yield
    S._cache, S._cache_at = None, 0.0


CATALOG = [
    {"schema": "public", "table": "append_a", "est_rows": 5},
    {"schema": "public", "table": "append_b", "est_rows": None},
    # Never analyzed (pg_class says 0) but the catalog knows the real count —
    # e.g. a knesset mirror table's exact total or an odata import's row count.
    {"schema": "knesset", "table": "kns_bill", "est_rows": 4_000},
    {"schema": "idx", "table": "idx_layer", "est_rows": None},
]
ESTIMATES = {
    ("public", "append_a"): 1_000_000,
    ("public", "append_b"): 250,
    ("knesset", "kns_bill"): 0,
    # idx_layer absent entirely (dropped between catalog and estimate)
}


def _patch(monkeypatch, *, catalog=CATALOG, est=ESTIMATES, catalog_exc=None,
           est_exc=None):
    async def _catalog(db):
        if catalog_exc:
            raise catalog_exc
        return catalog

    async def _est(schemas):
        if est_exc:
            raise est_exc
        _est.schemas = schemas
        return est

    monkeypatch.setattr(S.data_catalog, "build_catalog", _catalog)
    monkeypatch.setattr(S.append_store, "schema_row_estimates", _est)
    return _est


def test_row_total_prefers_live_estimate_and_falls_back_to_catalog(monkeypatch):
    seen = _patch(monkeypatch)
    tables, rows = asyncio.run(S._catalog_totals(None))
    assert tables == 4
    # 1,000,000 + 250 (live) + 4,000 (catalog fallback for the un-analyzed
    # table) + 0 (missing from pg_class, no catalog count either)
    assert rows == 1_004_250
    # Schemas are derived from the catalog, not hardcoded.
    assert seen.schemas == ["idx", "knesset", "public"]


def test_row_total_survives_missing_estimates(monkeypatch):
    _patch(monkeypatch, est_exc=RuntimeError("append DB down"))
    tables, rows = asyncio.run(S._catalog_totals(None))
    assert tables == 4
    assert rows == 4_005          # catalog's own counts only


def test_catalog_failure_yields_nulls_not_an_error(monkeypatch):
    _patch(monkeypatch, catalog_exc=RuntimeError("boom"))
    assert asyncio.run(S._catalog_totals(None)) == (None, None)


def test_get_site_stats_caches_and_fails_soft(monkeypatch):
    _patch(monkeypatch)
    calls = {"n": 0}

    async def _files(db):
        calls["n"] += 1
        return 4_321

    monkeypatch.setattr(S, "_file_total", _files)

    first = asyncio.run(S.get_site_stats(None))
    assert first == {"tables": 4, "rows": 1_004_250, "files": 4_321}
    asyncio.run(S.get_site_stats(None))
    assert calls["n"] == 1, "second call inside the TTL must not rebuild"


def test_file_total_reports_none_when_the_query_fails(monkeypatch):
    class _Db:
        async def execute(self, *a, **k):
            raise RuntimeError("statement timeout")

    assert asyncio.run(S._file_total(_Db())) is None
