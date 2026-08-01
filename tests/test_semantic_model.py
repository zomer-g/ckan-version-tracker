"""Unit tests for the semantic layer — validation, compilation, retrieval.

These are the tests that matter most in the whole free-text feature, because
this module is the ONLY thing standing between a language model's output and a
generated SQL statement. The model can say anything; validate_query is what
makes "anything" safe. So the emphasis here is on refusal: every case where a
field, operator, value or comparison is not declared must raise, not degrade.

No DB and no network — the model is a plain list of dicts, which is exactly how
build_model returns it.
"""
import os

import pytest

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import semantic_model as sm
from app.services.semantic_model import SemanticError

ENTITY = {
    "key": "append_business",
    "schema": "public",
    "title": "רישיונות עסק",
    "summary": "רישיונות עסק שניתנו על ידי רשויות מקומיות",
    "rows": 120_000,
    "synonyms": ["עסקים", "רישוי"],
    "dimensions": [
        {"key": "יישוב", "kind": "text", "title": "יישוב", "entity_type": "locality",
         "samples": ["תל אביב-יפו", "חיפה", "באר שבע"], "groupable": True},
        {"key": "סוג_עסק", "kind": "text", "title": "סוג עסק",
         "samples": ["מסעדה", "מספרה"], "groupable": True},
        {"key": "מזהה", "kind": "text", "title": "מזהה", "samples": [], "groupable": False},
        {"key": "שנה", "kind": "number", "title": "שנה", "min": 2010, "max": 2025,
         "samples": [], "groupable": True},
        {"key": "תאריך_הנפקה", "kind": "date", "title": "תאריך הנפקה",
         "min": "2010-01-01", "max": "2025-12-31", "samples": [], "groupable": True},
    ],
    "measures": [
        {"key": "count", "title": "מספר שורות"},
        {"key": "sum:שנה", "title": "סכום שנה"},
        {"key": "avg:שנה", "title": "ממוצע שנה"},
        {"key": "min:שנה", "title": "מינימום שנה"},
        {"key": "max:שנה", "title": "מקסימום שנה"},
    ],
    "geo_dims": ["יישוב"],
    "source_url": "", "page_url": "",
}
MODEL = [ENTITY]


def _compile(q):
    ent, clean = sm.validate_query(MODEL, q)
    return sm.compile_sql(ent, clean)


# ── validation: everything undeclared must be refused ────────────────────────

@pytest.mark.parametrize("bad, why", [
    ({"entity": "append_nope"}, "unknown table"),
    ({"entity": "append_business", "dimensions": ["לא_קיים"]}, "unknown dimension"),
    ({"entity": "append_business", "measures": ["sum:לא_קיים"]}, "unknown measure"),
    ({"entity": "append_business", "measures": ["count", "median:שנה"]}, "unsupported aggregate"),
    ({"entity": "append_business",
      "filters": [{"field": "לא_קיים", "op": "=", "value": "x"}]}, "unknown filter field"),
    ({"entity": "append_business",
      "filters": [{"field": "יישוב", "op": "~~", "value": "x"}]}, "unknown operator"),
    ({"entity": "append_business", "enrich": ["salary"]}, "undeclared enrichment field"),
])
def test_undeclared_things_are_refused(bad, why):
    with pytest.raises(SemanticError):
        sm.validate_query(MODEL, bad)


def test_grouping_by_a_unique_identifier_is_refused():
    """A near-unique column is an id, not a category. Grouping by it returns one
    row per record — a plausible-looking result that answers nothing."""
    with pytest.raises(SemanticError):
        sm.validate_query(MODEL, {"entity": "append_business", "dimensions": ["מזהה"]})


def test_range_comparison_on_text_is_refused():
    """'9' > '10' is true for strings. A range test on a text column would give
    a confidently wrong answer, which is the exact failure this layer exists to
    prevent — so it must raise rather than silently compare lexically."""
    with pytest.raises(SemanticError):
        sm.validate_query(MODEL, {"entity": "append_business",
                                  "filters": [{"field": "יישוב", "op": ">", "value": "מ"}]})


def test_enrichment_without_a_locality_column_is_refused():
    ent = {**ENTITY, "geo_dims": []}
    with pytest.raises(SemanticError):
        sm.validate_query([ent], {"entity": "append_business", "enrich": ["district"]})


def test_between_needs_exactly_two_values():
    with pytest.raises(SemanticError):
        sm.validate_query(MODEL, {"entity": "append_business",
                                  "filters": [{"field": "שנה", "op": "between", "value": [2020]}]})


def test_limit_is_clamped_not_rejected():
    _, clean = sm.validate_query(MODEL, {"entity": "append_business", "limit": 99999})
    assert clean["limit"] == sm.MAX_LIMIT
    _, clean = sm.validate_query(MODEL, {"entity": "append_business", "limit": -3})
    assert clean["limit"] == 1


# ── injection: values are the only user-influenced text in the statement ─────

def test_quotes_in_a_value_are_escaped():
    sql = _compile({"entity": "append_business",
                    "filters": [{"field": "יישוב", "op": "=", "value": "ג'ת"}]})
    assert "''" in sql  # the apostrophe was doubled, not left to close the literal
    assert sql.count("'") % 2 == 0


def test_a_value_that_tries_to_close_the_statement_stays_a_literal():
    """The classic payload. Escaping is what makes it safe: the doubled quote
    keeps the whole thing inside the literal, so nothing after it is code. It is
    searched for, not refused — and the downstream console guard agrees, since
    it reads values as data rather than counting their semicolons."""
    from app.services.append_store import validate_readonly_sql
    sql = _compile({"entity": "append_business",
                    "filters": [{"field": "יישוב", "op": "=",
                                 "value": "x'; DROP TABLE t; --"}]})
    assert "'x''; DROP TABLE t; --'" in sql   # one literal, quote doubled
    assert sql.count("'") % 2 == 0
    validate_readonly_sql(sql)


def test_a_quote_alone_is_escaped_and_still_runs():
    """Apostrophes are ordinary in Hebrew place names (ג'ת, מג'דל) — they must
    survive as data, not be refused along with the injection payload."""
    from app.services.append_store import validate_readonly_sql
    sql = _compile({"entity": "append_business",
                    "filters": [{"field": "יישוב", "op": "=", "value": "ג'ת"}]})
    assert "''" in sql and sql.count("'") % 2 == 0
    validate_readonly_sql(sql)


def test_backslash_and_nul_in_a_value_are_refused():
    """Neither can appear in a legitimate filter value, and both would make the
    escaping depend on server settings — so they are refused, not escaped."""
    for bad in ("a\\b", "a\x00b"):
        with pytest.raises(SemanticError):
            _compile({"entity": "append_business",
                      "filters": [{"field": "יישוב", "op": "=", "value": bad}]})


def test_a_semicolon_in_a_value_is_ordinary_data():
    """Free-text archive fields really do hold semicolons (multi-value cells).
    Inside a literal one is data, and the console guard reads it that way."""
    from app.services.append_store import validate_readonly_sql
    sql = _compile({"entity": "append_business",
                    "filters": [{"field": "יישוב", "op": "=", "value": "a;b"}]})
    assert "'a;b'" in sql
    validate_readonly_sql(sql)


# ── compilation ──────────────────────────────────────────────────────────────

def test_plain_count_has_no_group_by():
    sql = _compile({"entity": "append_business", "measures": ["count"]})
    assert "count(*)" in sql
    assert "GROUP BY" not in sql
    assert sql.rstrip().endswith("LIMIT 50")


def test_group_by_uses_ordinals_matching_the_key_count():
    sql = _compile({"entity": "append_business", "measures": ["count"],
                    "dimensions": ["יישוב", "סוג_עסק"]})
    assert "GROUP BY 1, 2" in sql
    assert "ORDER BY 3 DESC" in sql  # the measure sits right after the two keys


def test_date_dimension_is_bucketed_by_month():
    sql = _compile({"entity": "append_business", "dimensions": ["תאריך_הנפקה"]})
    assert "date_trunc('month'" in sql


def test_numeric_measure_casts_text_stored_numbers():
    """Half this corpus arrives as CSV, so a numeric column is often text. The
    sum must cast, or it errors on every real dataset."""
    sql = _compile({"entity": "append_business", "measures": ["sum:שנה"]})
    assert "::numeric" in sql
    assert "regexp_replace" in sql


def test_text_equality_trims_both_sides():
    sql = _compile({"entity": "append_business",
                    "filters": [{"field": "יישוב", "op": "=", "value": "חיפה"}]})
    assert "btrim" in sql


def test_enrichment_joins_the_settlement_index_on_the_healed_code():
    sql = _compile({"entity": "append_business", "dimensions": ["יישוב"],
                    "enrich": ["district", "population"]})
    assert "LEFT JOIN over_settlements" in sql
    assert "over_settlement_code" in sql and "over_authority_code" in sql
    assert '"מחוז"' in sql and '"אוכלוסייה"' in sql
    # Enrichment columns are grouped too, or the GROUP BY is invalid.
    assert "GROUP BY 1, 2, 3" in sql


def test_every_column_is_qualified_with_the_table_alias():
    """The base table is always aliased `t`. If some references were qualified
    and others were not, the enrichment path would produce ambiguous-column
    errors only for tables that happen to share a name with over_settlements."""
    sql = _compile({"entity": "append_business", "dimensions": ["יישוב"],
                    "filters": [{"field": "סוג_עסק", "op": "=", "value": "מסעדה"}],
                    "enrich": ["district"]})
    assert 't."יישוב"' in sql and 't."סוג_עסק"' in sql
    assert "FROM append_business" not in sql  # must be aliased


def test_non_public_schema_is_qualified():
    ent = {**ENTITY, "schema": "knesset", "key": "kns_bill"}
    _, clean = sm.validate_query([ent], {"entity": "kns_bill"})
    assert "knesset." in sm.compile_sql(ent, clean)


def test_compiled_sql_passes_the_console_guard():
    """Everything this module emits still goes through append_store's read-only
    validator. If the two ever disagree the feature 400s on every query, so pin
    the contract here rather than discovering it in production."""
    from app.services.append_store import validate_readonly_sql
    for q in (
        {"entity": "append_business"},
        {"entity": "append_business", "dimensions": ["יישוב"], "measures": ["count", "avg:שנה"]},
        {"entity": "append_business", "dimensions": ["יישוב"], "enrich": ["district"],
         "filters": [{"field": "שנה", "op": "between", "value": [2020, 2024]}]},
        {"entity": "append_business",
         "filters": [{"field": "סוג_עסק", "op": "in", "value": ["מסעדה", "מספרה"]},
                     {"field": "יישוב", "op": "not_null"}]},
    ):
        validate_readonly_sql(_compile(q))


# ── retrieval ────────────────────────────────────────────────────────────────

def test_a_stored_value_is_a_strong_retrieval_signal():
    """Matching a real cell value is the value-linking step — the mitigation for
    the measured accuracy drop on non-English questions."""
    q = sm.tokens("כמה עסקים בתל אביב-יפו")
    assert sm.score_entity(ENTITY, q) > sm.score_entity({**ENTITY, "dimensions": [
        {**d, "samples": []} for d in ENTITY["dimensions"]]}, q)


def test_hebrew_clitic_yields_both_readings():
    """'בתל' must match the stored value 'תל אביב-יפו'; not handling the ב is the
    most common lexical miss in Hebrew questions. But 'בית' must still match
    'בית'. Same length, same shape — so both readings are emitted and the model
    decides which one finds something."""
    assert set(sm.tokens("בתל")) == {"בתל", "תל"}
    assert "בית" in sm.tokens("בית")
    # And the stripped form of a real word is inert: it matches nothing.
    q = set(sm.tokens("כמה עסקים בתל אביב-יפו"))
    assert "תל" in q and "אביב" in q


def test_retrieve_returns_nothing_for_an_unrelated_question():
    assert sm.retrieve(MODEL, "מה מזג האוויר מחר") == []


def test_explain_describes_the_filters_that_were_actually_applied():
    ent, clean = sm.validate_query(MODEL, {
        "entity": "append_business", "dimensions": ["יישוב"],
        "filters": [{"field": "שנה", "op": "=", "value": 2024}]})
    text = sm.explain_query(ent, clean)
    assert "רישיונות עסק" in text and "יישוב" in text and "2024" in text


# ── model derivation (catalog + profile → entity) ────────────────────────────
# The model is DERIVED, so a bug here degrades everything downstream silently:
# a lost top_values list costs value linking, a mis-detected kind costs a cast,
# and a missing groupable flag makes a legitimate "לפי X" refuse.

CATALOG_REC = {
    "table": "append_licences", "schema": "public", "title": "רישיונות עסק",
    "description": "", "est_rows": 5000, "tags": ["רישוי"],
    "field_flags": {"has_locality": True},
    "columns": [
        {"name": "יישוב", "type": "text"},
        {"name": "שנה", "type": "text"},          # numeric stored as text (CSV)
        {"name": "מזהה", "type": "text"},          # near-unique ⇒ not groupable
        {"name": "row_hash", "type": "text"},      # internal ⇒ hidden
        {"name": "first_seen", "type": "timestamp"},
    ],
}
PROFILE_REC = {
    "summary_he": "רישיונות עסק לפי רשות",
    "sql_profile": {
        "keywords": [{"token": "עסק", "count": 9}],
        "columns": {
            "יישוב": {"detected_kind": "text", "distinct_ratio": 0.02, "distinct_est": 90,
                      "top_values": [{"value": "חיפה", "count": 40}],
                      "entity_guess": {"guess": "locality"}},
            "שנה": {"detected_kind": "numeric", "min": 2010, "max": 2025},
            "מזהה": {"detected_kind": "text", "distinct_ratio": 0.999, "distinct_est": 4995,
                     "top_values": []},
        },
    },
    "llm_enrichment": {"columns": {"יישוב": {"description_he": "יישוב העסק"}}},
}


def _derived():
    return sm._entity_from(CATALOG_REC, PROFILE_REC)


def test_internal_columns_are_never_exposed_to_the_model():
    """row_hash / first_seen are archive bookkeeping. Offering them would let a
    grouping be built on something no question ever meant."""
    keys = {d["key"] for d in _derived()["dimensions"]}
    assert "row_hash" not in keys and "first_seen" not in keys
    assert "יישוב" in keys


def test_profiler_kind_overrides_the_declared_column_type():
    """'שנה' is stored as text but holds numbers. Trusting the DDL type would
    lose every range filter and every sum on CSV-sourced tables — which is most
    of this corpus."""
    d = next(x for x in _derived()["dimensions"] if x["key"] == "שנה")
    assert d["kind"] == "number"
    assert "sum:שנה" in {m["key"] for m in _derived()["measures"]}


def test_a_near_unique_column_is_filterable_but_not_groupable():
    dims = {d["key"]: d for d in _derived()["dimensions"]}
    assert dims["מזהה"]["groupable"] is False
    assert dims["יישוב"]["groupable"] is True


def test_real_values_and_enriched_titles_reach_the_model():
    dims = {d["key"]: d for d in _derived()["dimensions"]}
    assert dims["יישוב"]["samples"] == ["חיפה"]      # value linking payload
    assert dims["יישוב"]["title"] == "יישוב העסק"     # LLM description wins over the name


def test_a_locality_column_declares_the_enrichment_join_path():
    assert _derived()["geo_dims"] == ["יישוב"]


def test_an_unprofiled_table_still_yields_a_usable_entity():
    """Profiling is a background job, so a freshly-tracked dataset has no
    profile. It must still be queryable — being conservative here would make new
    datasets invisible to the feature until the profiler catches up."""
    ent = sm._entity_from(CATALOG_REC, None)
    assert ent is not None
    assert all(d["groupable"] for d in ent["dimensions"])


def test_a_table_with_only_internal_columns_is_dropped():
    rec = {**CATALOG_REC, "columns": [{"name": "row_hash", "type": "text"}]}
    assert sm._entity_from(rec, None) is None
