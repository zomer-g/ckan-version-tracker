"""Unit tests for the shared CBS search SQL builder.

``build_search`` is the single source of truth the three read paths — REST
/search, /ask, and the MCP search tool — now share. These tests pin the
behaviour that used to differ between them (the MCP used to AND words via
plainto_tsquery and ignore ``lang``) plus the relevance-ordering guarantees
(intent boost, catch-all demotion, recency tie-breakers). Pure string building,
no DB needed.
"""
from app.api.cbs_search_util import build_search, or_tsquery


def test_or_tsquery_is_or_of_prefixes():
    # OR-of-words with prefix match, stopwords dropped.
    out = or_tsquery("האם יש נתונים על שכר לפי נפה")
    assert " | " in out
    assert "שכר:*" in out and "נפה:*" in out
    assert "האם:*" not in out  # stopword dropped


def test_query_matches_fts_or_ilike():
    where, order, params = build_search({"q": "שכר לפי נפה"})
    assert "to_tsquery('simple', :tsq)" in where
    assert "title ILIKE :qlike" in where
    assert params["tsq"]  # non-empty OR query
    assert params["qlike"] == "%שכר לפי נפה%"


def test_facets_and_year_window():
    where, order, params = build_search(
        {"q": "בינוי", "geo": "נפה", "file_type": "xlsx", "section": "mediarelease",
         "item_type": "table", "lang": "he", "year_from": 2020, "year_to": 2024}
    )
    assert "geo_levels @> :geo" in where and params["geo"] == '["נפה"]'
    assert "file_types @> :file_type" in where and params["file_type"] == '["xlsx"]'
    assert "section = :section" in where and params["section"] == "mediarelease"
    assert "item_type = :item_type" in where and params["item_type"] == "table"
    assert "lang = :lang" in where and params["lang"] == "he"       # was ignored by MCP
    assert params["yfrom"] == 2020 and params["yto"] == 2024


def test_relevance_order_boosts_intents_and_demotes_catchall():
    _, order, params = build_search({"q": "שכר"}, sort="relevance")
    # intent rows first, catch-all navigational pages last, then rank, then recency.
    assert order.index("item_type = 'intent'") < order.index("catch0")
    assert order.index("catch0") < order.index("ts_rank")
    assert "coalesce(year_end, year_start) DESC" in order  # recency tie-breaker
    assert params["catch0"].startswith("פעולות ופרסומים סטטיסטיים")


def test_chrono_order_is_year_then_crawl():
    _, order, params = build_search({"q": "בינוי"}, sort="chrono")
    assert order.startswith("coalesce(year_end, year_start) DESC")
    assert "to_tsquery" not in order  # chrono never ranks by text
    assert "catch0" not in params      # no catch-all params in chrono mode


def test_empty_query_has_no_tsquery_param():
    where, order, params = build_search({"section": "publications"})
    assert "tsq" not in params
    assert "to_tsquery" not in where
    # relevance order without a query falls back to a literal rank of 0.
    assert " 0 DESC" in order
