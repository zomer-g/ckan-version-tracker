"""Unit tests for the municipal-data.org URL parser / catalog / page_type
round-trip in ``app/api/munidata.py`` (no DB, no network)."""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api.munidata import (
    MUNIDATA_METRICS,
    _parse_munidata_url,
    canonical_url,
    target_of_page_type,
    title_for,
)


IN_SCOPE = [
    ("https://municipal-data.org/demographics?metric=population", "demographics", "population"),
    ("https://municipal-data.org/budget?metric=audit_deficiencies_count", "budget_economy", "audit_deficiencies_count"),
    ("https://municipal-data.org/governance?metric=kolot_korim", "gov_mechanisms", "kolot_korim"),
    ("https://municipal-data.org/human-capital?metric=888", "human_capital", "888"),
    ("https://municipal-data.org/?screen=gov_mechanisms&metric=maanak_izun", "gov_mechanisms", "maanak_izun"),
    ("https://www.municipal-data.org/budget?metric=total_income", "budget_economy", "total_income"),
]

OUT_OF_SCOPE = [
    "https://municipal-data.org/demographics",   # bare screen, no metric
    "https://municipal-data.org/",
    "https://municipal-data.org/nonsense?metric=population",  # unknown screen
    "https://municipal-data.org/demographics?metric=",        # empty metric
    "https://example.com/demographics?metric=population",     # wrong host
]


def test_catalog_has_38_metrics():
    assert len(MUNIDATA_METRICS) == 38
    ids = {(m["screen_id"], m["metric_id"]) for m in MUNIDATA_METRICS}
    assert len(ids) == 38  # unique


def test_parse_in_scope():
    for url, screen_id, metric_id in IN_SCOPE:
        page_type, collector = _parse_munidata_url(url)
        assert page_type == f"munidata_metric:{screen_id}:{metric_id}", url
        assert collector and collector.startswith("munidata-"), url
        assert target_of_page_type(page_type) == (screen_id, metric_id)


def test_parse_out_of_scope():
    for url in OUT_OF_SCOPE:
        assert _parse_munidata_url(url) == (None, None), url


def test_canonical_round_trips_through_parser():
    for m in MUNIDATA_METRICS:
        url = canonical_url(m["screen_id"], m["metric_id"])
        page_type, collector = _parse_munidata_url(url)
        assert target_of_page_type(page_type) == (m["screen_id"], m["metric_id"]), url


def test_title_for_uses_catalog():
    t = title_for("demographics", "population")
    assert t.startswith("מצב שלטון מקומי — דמוגרפיה — ")
    assert "מספר תושבים" in t
