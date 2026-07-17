"""Unit tests for the deterministic question parser behind /resolve's chips."""
from app.api.cbs_parse import geo_matrix, parse_question


def test_full_benchmark_style_question():
    # Four dimensions in one real benchmark phrasing.
    u = parse_question("מספר ילדים 0-4 בשנים 2000-2009 לפי אזור סטטיסטי בירושלים")
    assert u["geo_level"] == "אזור סטטיסטי"
    assert u["years"] == [2000, 2009]
    assert "count" in u["metrics"]
    assert "age" in u["cuts"]
    assert u["latest"] is False


def test_latest_and_product_form():
    u = parse_question("איפה יש שכבת אזורים סטטיסטיים עדכנית להורדה?")
    assert u["product_form"] == "gis_layer"
    assert u["latest"] is True
    assert u["geo_level"] == "אזור סטטיסטי"


def test_series_and_source():
    u = parse_question("אוכלוסייה לפי יישוב לאורך השנים מסקר כוח אדם")
    assert u["series"] is True
    assert u["geo_level"] == "יישוב"
    assert u["source_op"] == "סקר כוח אדם"


def test_no_false_chips_on_plain_question():
    u = parse_question("מדד המחירים לצרכן")
    assert u["geo_level"] is None
    assert u["years"] == []
    assert u["product_form"] is None


def test_geo_matrix_marks_requested_missing_level():
    results = [
        {"geo_levels": ["ארצי", "נפה"]},
        {"geo_levels": ["ארצי", "יישוב"]},
        {"geo_levels": None},
    ]
    m = geo_matrix(results, requested="אזור סטטיסטי")
    assert m["נפה"] is True and m["יישוב"] is True
    assert m["אזור סטטיסטי"] is False  # requested but not available
    assert "שכונה" not in m  # neither available nor requested


def test_geo_matrix_empty_results():
    assert geo_matrix([], requested=None) == {}
