"""Unit tests for the CBS enrichment derivations.

Titles below are real (or benchmark-shaped) CBS page titles — the derivers are
heuristic, so the tests pin the exact phrasing families each rule must catch
and, just as importantly, must NOT catch (precision beats recall here).
"""
from app.services.cbs_enrich import (
    derive_cuts,
    derive_data_vintage,
    derive_freq,
    derive_geo_coverage,
    derive_geo_levels,
    derive_geo_vintage,
    derive_metrics,
    derive_product_form,
    derive_series_key,
    derive_source_op,
    enrich,
)


# ── product_form ───────────────────────────────────────────────────────────

def test_product_form_tools_and_files():
    assert derive_product_form({"item_type": "generator"}) == "generator"
    assert derive_product_form({"item_type": "dashboard"}) == "dashboard"
    assert derive_product_form({"section": "tools", "item_type": "tool"}) == "generator"
    assert derive_product_form(
        {"item_type": "הודעה לתקשורת", "file_types": ["pdf", "xlsx"]}
    ) == "data_file"
    assert derive_product_form(
        {"item_type": "פרסום", "file_types": ["pdf"]}
    ) == "publication"


def test_product_form_gis_puf_methodology():
    assert derive_product_form({"title": "קטלוג השכבות הגאוגרפיות"}) == "gis_layer"
    assert derive_product_form({"title": "קובץ PUF — סקר הוצאות משק הבית"}) == "puf"
    assert derive_product_form({"title": "מילון מונחים סטטיסטיים"}) == "methodology"
    # intents stay untagged — they are pointers, not products
    assert derive_product_form({"item_type": "intent", "title": "קובץ הרשויות"}) is None
    assert derive_product_form({"item_type": "intent_negative", "title": "אין"}) is None


# ── freq / source ──────────────────────────────────────────────────────────

def test_freq_from_extra_interval():
    assert derive_freq({"extra": {"interval": ["שנתי"]}}) == "שנתי"
    assert derive_freq({"extra": {"interval": ["רבעונית"]}}) == "רבעוני"
    assert derive_freq({"extra": {}}) is None


def test_source_op_census_beats_generic_survey():
    assert derive_source_op({"title": "נתונים ממפקד האוכלוסין 2022"}) == "מפקד אוכלוסין"
    assert derive_source_op({"title": "ממצאים מסקר כוח אדם 2023"}) == "סקר כוח אדם"
    assert derive_source_op({"title": "הסקר החברתי 2023 — בריאות"}) == "הסקר החברתי"
    # crawler's managed-metadata term wins when present
    assert derive_source_op(
        {"title": "דוח כלשהו", "extra": {"surveys": ["סקר אמון הצרכנים"]}}
    ) == "סקר אמון הצרכנים"
    assert derive_source_op({"title": "מדד המחירים לצרכן"}) is None


# ── time fields ────────────────────────────────────────────────────────────

def test_data_vintage_takes_max_year_in_title():
    assert derive_data_vintage(
        {"title": "התחלות בנייה וגמר בנייה - אוקטובר 2022-ספטמבר 2023"}
    ) == 2023
    assert derive_data_vintage({"title": "קובץ הרשויות המקומיות בישראל 2021"}) == 2021
    assert derive_data_vintage({"title": "שנתון סטטיסטי לישראל"}) is None


def test_geo_vintage():
    assert derive_geo_vintage({"title": "אפיון יחידות גאוגרפיות בגבולות 2011"}) == 'א"ס 2011'
    assert derive_geo_vintage({"title": "יחידות דיור לפי אזורי סקר"}) == "אזורי סקר"
    assert derive_geo_vintage({"title": "קובץ הרשויות 2021"}) is None


def test_geo_coverage_threshold():
    assert derive_geo_coverage(
        {"title": "אוכלוסיית עולי 1990 ואילך ביישובים שבהם 5,000 תושבים ויותר"}
    ) == "יישובים 5,000+ תושבים בלבד"
    assert derive_geo_coverage({"title": "אוכלוסייה לפי יישוב"}) is None


# ── series identity ────────────────────────────────────────────────────────

def test_series_key_unifies_editions():
    k2021 = derive_series_key({"title": "קובץ הרשויות המקומיות בישראל 2021"})
    k2019 = derive_series_key({"title": "קובץ הרשויות המקומיות בישראל 2019"})
    assert k2021 and k2021 == k2019
    # month + year stripped for sub-annual series
    a = derive_series_key({"title": "מדד המחירים לצרכן - ינואר 2024"})
    b = derive_series_key({"title": "מדד המחירים לצרכן - דצמבר 2023"})
    assert a and a == b


def test_series_key_guards():
    # all-volatile title → no key, and intents never join series
    assert derive_series_key({"title": "2021"}) is None
    assert derive_series_key({"title": "קובץ הרשויות", "item_type": "intent"}) is None
    # different products stay distinct
    assert derive_series_key({"title": "קובץ הרשויות המקומיות בישראל 2021"}) != \
        derive_series_key({"title": "קובץ היישובים 2021"})


# ── metrics / cuts / geo levels ────────────────────────────────────────────

def test_metrics_median_vs_avg():
    got = derive_metrics({"title": "שכר חציוני ושכר ממוצע לשכיר"})
    assert "median" in got and "avg" in got
    assert derive_metrics({"title": "התפלגות ההכנסות"}) == ["distribution"]


def test_cuts():
    got = derive_cuts({"title": "אוכלוסייה לפי גיל, מין ומגזר", "summary": None})
    assert set(got) >= {"age", "gender", "sector_religion"}
    assert derive_cuts({"title": "אשכול סוציו-אקונומי של רשויות"}) == ["ses"]


def test_geo_levels_completed_from_title_keeping_existing():
    got = derive_geo_levels({
        "geo_levels": ["ארצי"],
        "title": "אומדני משרות שכיר לפי נפה ואזור סטטיסטי",
    })
    assert "ארצי" in got and "נפה" in got and "אזור סטטיסטי" in got
    # no evidence → unchanged (None stays None)
    assert derive_geo_levels({"title": "מדד אמון הצרכנים"}) is None


# ── entry point ────────────────────────────────────────────────────────────

def test_enrich_is_total_on_sparse_rows():
    # A row with nothing but a URL must not raise and must return every key.
    out = enrich({"url": "https://www.cbs.gov.il/x"})
    assert set(out) == {
        "product_form", "freq", "source_op", "data_vintage", "geo_vintage",
        "geo_coverage", "series_key", "edition_year", "metrics", "cuts",
        "geo_levels",
    }


def test_enrich_realistic_mediarelease():
    row = {
        "title": "התחלות וגמר בנייה - סיכום שנת 2022",
        "item_type": "הודעה לתקשורת",
        "section": "mediarelease",
        "file_types": ["pdf", "xls"],
        "extra": {"interval": ["שנתי"], "article_type": ["הודעה לתקשורת"]},
    }
    out = enrich(row)
    assert out["product_form"] == "data_file"
    assert out["freq"] == "שנתי"
    assert out["data_vintage"] == 2022
    assert out["edition_year"] == 2022
    assert out["series_key"]  # joins the other yearly סיכום editions
