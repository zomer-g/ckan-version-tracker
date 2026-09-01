"""Tests for the election-finance MCP (app/mcp/elections_server.py).

The register is six NEON tables whose recipient columns deliberately differ, and
the MCP's whole job is to make them answer one question. What can go wrong
quietly:

* a per-table projection that names a column the table lacks — the UNION fails,
  or worse, silently drops a type;
* a recipient mapping that points at the wrong column, so "who funded X" comes
  back empty for a whole election type while looking fine for the others;
* a caller-supplied name interpolated into SQL instead of parameterized;
* "no tables yet" rendering as "no donations", which is the difference between
  "not collected" and "nobody gave anything".
"""
from __future__ import annotations

import asyncio
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.mcp import elections_server as es  # noqa: E402


# --------------------------------------------------------------------------
# The election-type table
# --------------------------------------------------------------------------

def test_every_election_type_the_scraper_emits_is_mapped():
    """The MCP's six keys must be exactly the scraper's six election types.

    They are two independent lists in two repos; if the scraper adds a type and
    this one doesn't, that type's dataset is collected and then invisible here.
    """
    assert set(es.ELECTION_TYPES) == {
        "local", "regional", "parties", "primaries", "special",
        "politicalarrangement",
    }


def test_every_type_names_at_least_one_recipient_column():
    """A type with no recipient mapping answers 'who funded X' with silence."""
    for key, spec in es.ELECTION_TYPES.items():
        assert spec["recipient"], f"{key} has no recipient column"
        assert spec["label_he"]


def test_recipient_columns_match_the_scrapers_schema():
    """Pinned against the columns govil-scraper's engine actually writes.

    Cross-repo, so nothing catches a rename automatically — this is the
    tripwire. See govscraper/scrapers/mevaker_statements/_engine.py.
    """
    expected = {
        "local": ["election_faction"],
        "regional": ["candidate_name"],
        "parties": ["list_name", "party_name"],
        "primaries": ["candidate_name"],
        "special": ["candidate_name"],
        "politicalarrangement": ["political_arrangement_name",
                                 "authorized_factor_name"],
    }
    for key, cols in expected.items():
        assert es.ELECTION_TYPES[key]["recipient"] == cols, key


# --------------------------------------------------------------------------
# Publication types — the user-facing filter
# --------------------------------------------------------------------------

@pytest.mark.parametrize("given,expected", [
    ("תרומה", 2), ("תרומות", 2), ("donation", 2), ("Donations", 2), (2, 2), ("2", 2),
    ("ערבות", 3), ("guarantee", 3), (3, 3),
    ("הלוואה", 4), ("loans", 4), (4, 4),
])
def test_publication_type_accepts_hebrew_english_and_id(given, expected):
    assert es._norm_pub_type(given) == expected


def test_publication_type_unset_is_no_filter():
    assert es._norm_pub_type(None) is None
    assert es._norm_pub_type("") is None


@pytest.mark.parametrize("bad", ["מענק", "bribe", 99, "0"])
def test_unknown_publication_type_raises_rather_than_silently_matching_nothing(bad):
    """A typo must be an error, not an empty result that reads as 'none found'."""
    with pytest.raises(ValueError, match="publication_type"):
        es._norm_pub_type(bad)


def test_unknown_election_type_raises():
    with pytest.raises(ValueError, match="election_type"):
        es._norm_types("knesset")


def test_election_types_parse_from_a_comma_list():
    assert es._norm_types("local, parties") == ["local", "parties"]
    assert es._norm_types(["Local", "SPECIAL"]) == ["local", "special"]
    assert es._norm_types("") is None


# --------------------------------------------------------------------------
# SQL construction
# --------------------------------------------------------------------------

def test_coalesce_skips_columns_the_table_does_not_have():
    """A table lacking a mapped column must not be named in the SELECT."""
    present = {"candidate_name"}
    assert es._coalesce(["candidate_name"], present) == "COALESCE(NULLIF(candidate_name, ''), '')"
    # None of the candidates exist -> a literal, so the UNION still lines up.
    assert es._coalesce(["party_name", "list_name"], present) == "''"
    # Several exist -> priority order preserved.
    both = es._coalesce(["list_name", "party_name"], {"list_name", "party_name"})
    assert both.index("list_name") < both.index("party_name")


def test_filters_parameterize_every_caller_supplied_value():
    """Names go in as $n, never interpolated.

    A donor called O'Brien, or a search for 100%, must be a query — not a
    syntax error, and certainly not an injection.
    """
    where, params = es._filters({
        "donor": "O'Brien", "recipient": "100%", "donor_city": "תל אביב",
        "publication_type": "תרומה", "min_sum": 1000, "max_sum": 50000,
        "from_date": "2020-01-01", "to_date": "2021-12-31",
    })
    assert "O'Brien" not in where and "100%" not in where
    assert "O'Brien" in params[0] and "100%" in params[1]
    # Every placeholder present exactly once, numbered 1..n.
    for i in range(1, len(params) + 1):
        assert f"${i}" in where


def test_no_filters_is_a_valid_where_clause():
    where, params = es._filters({})
    assert where == "TRUE" and params == []


def test_bare_dates_are_widened_to_the_registers_iso_shape():
    """publication_date is text like '2026-02-18T00:00:00'.

    Comparing a bare '2026-02-18' as text would exclude that whole day's rows
    from a to_date bound, because '2026-02-18' < '2026-02-18T00:00:00'.
    """
    assert es._date_bound("2020-01-01", "T00:00:00") == "2020-01-01T00:00:00"
    assert es._date_bound("2021-12-31", "T23:59:59") == "2021-12-31T23:59:59"
    # An already-full timestamp is left alone.
    assert es._date_bound("2020-01-01T12:00:00", "T00:00:00") == "2020-01-01T12:00:00"


def test_to_date_bound_includes_the_whole_final_day():
    where, params = es._filters({"to_date": "2021-12-31"})
    assert "2021-12-31T23:59:59" in params
    assert "publication_date <=" in where


def test_amount_treats_the_empty_string_as_null():
    """The register's null is '' — a direct ::numeric would raise on row one."""
    assert es._amount() == "NULLIF(publication_sum, '')::numeric"


def test_identifier_quoting_escapes_embedded_quotes():
    assert es._qi('tbl"x') == '"tbl""x"'


def test_literal_quoting_escapes_embedded_quotes():
    assert es._lit("a'b") == "'a''b'"


# --------------------------------------------------------------------------
# Result shaping
# --------------------------------------------------------------------------

def test_group_rows_get_readable_names():
    out = es._grp({"donor_name": "פלוני", "n": 3, "s": 1500})
    assert out == {"donor_name": "פלוני", "publications": 3, "total_sum": 1500.0}


def test_missing_sum_is_zero_not_none():
    assert es._num(None) == 0.0
    assert es._num("abc") == 0.0
    assert es._num("12.5") == 12.5


# --------------------------------------------------------------------------
# Tool surface
# --------------------------------------------------------------------------

def test_every_declared_tool_has_an_implementation():
    assert {t["name"] for t in es.TOOLS} == set(es._IMPL)


def test_tools_declare_the_two_filters_the_register_is_asked_for():
    """Query by person, filter by donation type — the reason this MCP exists."""
    search = next(t for t in es.TOOLS if t["name"] == "search_donations")
    props = search["inputSchema"]["properties"]
    for field in ("donor", "recipient", "publication_type", "election_type"):
        assert field in props, field
    assert "donor" in next(
        t for t in es.TOOLS if t["name"] == "donor_profile")["inputSchema"]["required"]


def test_unified_columns_cover_donor_recipient_amount_and_date():
    for col in ("donor_name", "recipient_name", "publication_sum",
                "publication_date", "publication_type", "election_type"):
        assert col in es._UNIFIED_COLUMNS


def test_server_instructions_warn_that_a_name_is_not_an_identifier():
    """Two people share a name; one person appears in several spellings.

    A model that sums by name without saying so misattributes giving to a
    private individual, so the caveat has to travel with the data.
    """
    assert "מזהה ייחודי" in es.SERVER_INSTRUCTIONS


def test_provenance_says_the_data_is_not_processed():
    """Unlike OCAL/OCOI this is a faithful mirror — claiming otherwise would
    wrongly discount a government record."""
    assert es.PROVENANCE_BASE["is_processed"] is False
    assert "statements-p.mevaker.gov.il" in es.PROVENANCE_BASE["upstream_source"]


# --------------------------------------------------------------------------
# "Not collected yet" must not read as "no donations"
# --------------------------------------------------------------------------

def test_no_published_tables_raises_a_distinguishable_error(monkeypatch):
    async def _none(db):
        return []
    monkeypatch.setattr(es, "_tables", _none)
    with pytest.raises(es._NotCollectedYet, match="טרם נאסף"):
        asyncio.run(es._corpus_sql(object(), None))


def test_filtering_to_an_uncollected_type_also_raises(monkeypatch):
    """Asking only for a type that has no table is the same situation."""
    async def _one(db):
        return [{"election_type": "local", "label_he": "x", "table": "t",
                 "dataset_id": "d", "title": "t", "version_id": "v",
                 "last_polled_at": None}]
    monkeypatch.setattr(es, "_tables", _one)
    with pytest.raises(es._NotCollectedYet):
        asyncio.run(es._corpus_sql(object(), ["parties"]))


def test_a_bad_argument_is_reported_as_such_even_when_nothing_is_collected(monkeypatch):
    """Argument validation must precede table resolution.

    Otherwise a typo in publication_type comes back as "nothing collected yet"
    whenever the register happens to be uncollected — telling the caller the
    wrong thing about their own query, and one they cannot fix by waiting.
    """
    async def _none(db):
        return []
    monkeypatch.setattr(es, "_tables", _none)

    for tool, args in [
        ("search_donations", {"publication_type": "מענק"}),
        ("top_donors", {"publication_type": "מענק"}),
        ("donor_profile", {"donor": "כהן", "publication_type": "מענק"}),
        ("recipient_profile", {"recipient": "הליכוד", "publication_type": "מענק"}),
    ]:
        with pytest.raises(ValueError, match="publication_type"):
            asyncio.run(es._IMPL[tool](None, object(), None, args))


def test_an_unknown_election_type_is_also_reported_before_the_db(monkeypatch):
    async def _none(db):
        return []
    monkeypatch.setattr(es, "_tables", _none)
    with pytest.raises(ValueError, match="election_type"):
        asyncio.run(es._IMPL["search_donations"](None, object(), None,
                                                 {"election_type": "knesset"}))
