"""Unit tests for the free, deterministic fast-path.

The matcher's job is not to answer as many questions as possible — it is to
answer only the ones it fully understood, and hand everything else to the paid
tier. So most of these tests assert that it DECLINES. A template that fires on a
partially-understood question silently drops a constraint, which turns "כמה
עסקים בחיפה" into the national total with no visible sign that it happened.

No DB and no network.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import nl_templates, semantic_model as sm

ENTITY = {
    "key": "append_business",
    "schema": "public",
    "title": "רישיונות עסק",
    "summary": "רישיונות עסק שניתנו על ידי רשויות מקומיות",
    "rows": 120_000,
    "synonyms": ["עסקים"],
    "dimensions": [
        {"key": "יישוב", "kind": "text", "title": "יישוב", "entity_type": "locality",
         "samples": ["תל אביב-יפו", "חיפה"], "groupable": True},
        {"key": "סוג_עסק", "kind": "text", "title": "סוג עסק",
         "samples": ["מסעדה", "מספרה"], "groupable": True},
        {"key": "שנה", "kind": "number", "title": "שנה", "min": 2010, "max": 2025,
         "samples": [], "groupable": True},
        {"key": "שטח", "kind": "number", "title": "שטח", "min": 10, "max": 900,
         "samples": [], "groupable": True},
    ],
    "measures": [{"key": "count", "title": "מספר שורות"},
                 {"key": "sum:שנה", "title": ""}, {"key": "avg:שנה", "title": ""},
                 {"key": "min:שנה", "title": ""}, {"key": "max:שנה", "title": ""},
                 {"key": "sum:שטח", "title": ""}, {"key": "avg:שטח", "title": ""},
                 {"key": "min:שטח", "title": ""}, {"key": "max:שטח", "title": ""}],
    "geo_dims": ["יישוב"],
    "source_url": "", "page_url": "",
}
MODEL = [ENTITY]


def test_count_grouped_by_a_column():
    q = nl_templates.match(MODEL, "כמה רישיונות עסק לפי יישוב")
    assert q is not None
    assert q["entity"] == "append_business"
    assert q["measures"] == ["count"]
    assert q["dimensions"] == ["יישוב"]
    # And it must survive the same validator the LLM path goes through.
    sm.validate_query(MODEL, q)


def test_a_value_filter_comes_from_a_real_stored_value():
    q = nl_templates.match(MODEL, "כמה רישיונות עסק בחיפה")
    assert q is not None
    assert {"field": "יישוב", "op": "=", "value": "חיפה"} in q["filters"]


def test_year_becomes_a_filter_on_the_year_column():
    q = nl_templates.match(MODEL, "כמה רישיונות עסק בשנת 2024 לפי יישוב")
    assert q is not None
    assert {"field": "שנה", "op": "=", "value": 2024} in q["filters"]


def test_average_picks_the_named_numeric_column():
    q = nl_templates.match(MODEL, "ממוצע שטח רישיונות עסק לפי יישוב")
    assert q is not None
    assert q["measures"] == ["avg:שטח"]


def test_top_n_sets_the_limit():
    q = nl_templates.match(MODEL, "10 המובילים רישיונות עסק לפי יישוב")
    assert q is not None and q["limit"] == 10


# ── declining is the important behaviour ─────────────────────────────────────

def test_an_unrecognized_constraint_declines_rather_than_dropping_it():
    """'שנסגרו' is a real constraint this matcher cannot express. Answering
    without it would return every licence, not the closed ones — the exact
    silent-widening failure the coverage gate exists to prevent."""
    assert nl_templates.match(MODEL, "כמה רישיונות עסק שנסגרו בחיפה") is None


def test_an_unresolvable_grouping_declines():
    assert nl_templates.match(MODEL, "כמה רישיונות עסק לפי צבע") is None


def test_ambiguous_aggregate_declines():
    """'ממוצע' with two numeric columns and no hint about which — a human would
    ask; so should we, by falling through to a model that can read the whole
    sentence."""
    assert nl_templates.match(MODEL, "ממוצע רישיונות עסק") is None


def test_a_year_with_nowhere_to_put_it_declines():
    ent = {**ENTITY, "dimensions": [d for d in ENTITY["dimensions"] if d["key"] != "שנה"]}
    assert nl_templates.match([ent], "כמה רישיונות עסק בשנת 2024") is None


def test_an_unrelated_question_declines():
    assert nl_templates.match(MODEL, "מה מזג האוויר מחר בתל אביב") is None


def test_an_ambiguous_entity_declines():
    """Two datasets matching the question nearly equally is precisely when a
    human would ask 'which one?'. Guessing here picks the wrong source without
    telling anyone."""
    twin = {**ENTITY, "key": "append_business_2", "title": "רישיונות עסק היסטוריים"}
    assert nl_templates.match([ENTITY, twin], "כמה רישיונות עסק לפי יישוב") is None


def test_every_match_compiles_to_valid_console_sql():
    """End-to-end contract for the free path: whatever the matcher returns must
    validate AND compile AND pass the console's own read-only guard."""
    from app.services.append_store import validate_readonly_sql
    for question in ("כמה רישיונות עסק לפי יישוב",
                     "כמה רישיונות עסק בחיפה",
                     "כמה רישיונות עסק בשנת 2024 לפי סוג עסק",
                     "ממוצע שטח רישיונות עסק לפי יישוב"):
        q = nl_templates.match(MODEL, question)
        assert q is not None, question
        ent, clean = sm.validate_query(MODEL, q)
        validate_readonly_sql(sm.compile_sql(ent, clean))
