"""A multi-word Hebrew search must not come back empty because Solr ANDs it.

CKAN hands ``q`` to Solr, which ANDs the tokens, and its Hebrew analyser does not
strip clitics. So "הנחיות משטרה" matches nothing the moment a dataset writes
"המשטרה" instead of "משטרה" — the words are all there, the page is blank, and the
user concludes OVER does not have the data.

The fix re-asks with OR plus a prefix wildcard, but only when the strict search
came up short, and only for a query that is words and nothing else. These tests
pin both halves: that the widening happens where it should, and — the part that
can quietly break a working search — that it stays out of the way everywhere else.
"""
import asyncio
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services.ckan_client import ckan_client


def _fake_ckan(monkeypatch, pages):
    """Serve a canned result per query string; record the calls in order."""
    calls: list[dict] = []

    async def _fake_get(action, params=None):
        params = params or {}
        calls.append(params)
        return pages.get(params.get("q"), {"count": 0, "results": []})

    monkeypatch.setattr(ckan_client, "_get", _fake_get)
    return calls


def _hit(i):
    return {"id": f"id{i}", "title": f"ds{i}"}


def test_an_underfilled_hebrew_query_is_retried_with_or(monkeypatch):
    calls = _fake_ckan(monkeypatch, {
        "הנחיות משטרה": {"count": 0, "results": []},
        "הנחיות OR הנחיות* OR משטרה OR משטרה*": {
            "count": 2, "results": [_hit(1), _hit(2)]
        },
    })

    out = asyncio.run(ckan_client.package_search("הנחיות משטרה", rows=20))

    assert [c["q"] for c in calls] == [
        "הנחיות משטרה",
        "הנחיות OR הנחיות* OR משטרה OR משטרה*",
    ]
    assert [r["id"] for r in out["results"]] == ["id1", "id2"]


def test_exact_matches_keep_their_place_at_the_top(monkeypatch):
    """The widened results are appended, never interleaved — otherwise a loose
    match could outrank the dataset the user actually typed the name of."""
    _fake_ckan(monkeypatch, {
        "מים ביוב": {"count": 1, "results": [_hit(9)]},
        "מים OR מים* OR ביוב OR ביוב*": {
            "count": 3, "results": [_hit(1), _hit(9), _hit(2)]
        },
    })

    out = asyncio.run(ckan_client.package_search("מים ביוב", rows=20))

    # id9 came from the strict search and is not repeated by the merge.
    assert [r["id"] for r in out["results"]] == ["id9", "id1", "id2"]


def test_the_merge_never_overflows_the_page(monkeypatch):
    _fake_ckan(monkeypatch, {
        "a b": {"count": 1, "results": [_hit(0)]},
        "a OR a* OR b OR b*": {"count": 99, "results": [_hit(i) for i in range(1, 9)]},
    })

    out = asyncio.run(ckan_client.package_search("a b", rows=3))

    assert len(out["results"]) == 3


def test_the_count_describes_the_page_not_the_or_query(monkeypatch):
    """Reporting the OR query's site-wide total would promise pages 2..n that the
    page-local merge cannot serve — the user would page into nothing."""
    _fake_ckan(monkeypatch, {
        "a b": {"count": 0, "results": []},
        "a OR a* OR b OR b*": {"count": 500, "results": [_hit(1), _hit(2)]},
    })

    out = asyncio.run(ckan_client.package_search("a b", rows=20))

    assert out["count"] == 2


def test_a_full_page_is_left_alone(monkeypatch):
    calls = _fake_ckan(monkeypatch, {
        "a b": {"count": 50, "results": [_hit(i) for i in range(3)]},
    })

    asyncio.run(ckan_client.package_search("a b", rows=3))

    assert len(calls) == 1


def test_later_pages_are_left_alone(monkeypatch):
    """The merge is page-local, so widening page 2 would repeat page 1's extras."""
    calls = _fake_ckan(monkeypatch, {"a b": {"count": 1, "results": []}})

    asyncio.run(ckan_client.package_search("a b", rows=20, start=20))

    assert len(calls) == 1


def test_a_single_word_is_left_alone(monkeypatch):
    """There is no AND to defeat, and `word OR word*` is just a wildcard search
    the user did not ask for."""
    calls = _fake_ckan(monkeypatch, {"בריאות": {"count": 0, "results": []}})

    asyncio.run(ckan_client.package_search("בריאות", rows=20))

    assert len(calls) == 1


@pytest.mark.parametrize("query", [
    "title:water res_format:XLSX",   # field queries — colons would be OR'd apart
    "water AND sewage",              # an explicit boolean
    'organization:foo bar',
    '"exact phrase here"',           # a phrase the user quoted on purpose
    "water (sewage)",
    "https://example.com/a b",       # a pasted link, already handled by quoting
])
def test_solr_syntax_is_never_rewritten(monkeypatch, query):
    """These all have >= 2 tokens and return nothing, so only the syntax check
    stands between them and a rewrite that answers a different question."""
    calls = _fake_ckan(monkeypatch, {})

    asyncio.run(ckan_client.package_search(query, rows=20))

    assert len(calls) == 1


def test_a_failing_fallback_does_not_fail_the_search(monkeypatch):
    """The widening is a bonus. If Solr rejects the OR query, the user still gets
    the strict results rather than a 500."""
    async def _fake_get(action, params=None):
        if " OR " in (params or {}).get("q", ""):
            raise RuntimeError("SOLR returned an error running query")
        return {"count": 1, "results": [_hit(1)]}

    monkeypatch.setattr(ckan_client, "_get", _fake_get)

    out = asyncio.run(ckan_client.package_search("a b", rows=20))

    assert [r["id"] for r in out["results"]] == ["id1"]
