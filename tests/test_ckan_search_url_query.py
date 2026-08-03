"""A URL pasted into the home search box must not 500 the page.

The home search box tries every source validator in turn and, when none of them
recognises the pasted link, falls back to a CKAN keyword search. CKAN hands ``q``
to Solr verbatim, where ``https://…`` is not a keyword but syntax: the scheme's
colon makes ``https`` a field name, Solr has no such field and rejects the whole
query, CKAN answers 409, and raise_for_status turns that into a 500.

So the red "שגיאת שרת (500)" banner appeared for EVERY link OVER doesn't track
yet — which is precisely the moment a user pastes one. Verified against
data.gov.il on 2026-08-03: the raw URL returns 409 "SOLR returned an error
running query", the same URL quoted returns 200.
"""
import asyncio
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services.ckan_client import _solr_safe_query, ckan_client


@pytest.mark.parametrize("url", [
    "https://t.me/s/Israel_Cyber",
    "https://t.me/MOHreport",
    "http://foo",
    "https://example.com/x?a=1",
    "HTTPS://EXAMPLE.COM/X",
    "  https://example.com/x  ",
])
def test_a_pasted_url_is_quoted_into_a_phrase(url):
    quoted = _solr_safe_query(url)
    assert quoted.startswith('"') and quoted.endswith('"')
    assert url.strip() in quoted


@pytest.mark.parametrize("query", [
    "בריאות",
    "res_format:CSV",
    "organization:ministry-health",
    "title:water AND res_format:XLSX",
    "a:b",
    "",
])
def test_a_real_query_reaches_solr_untouched(query):
    """Field queries are a documented CKAN feature — quoting them would turn a
    working search into a phrase that matches nothing."""
    assert _solr_safe_query(query) == query


def test_quotes_and_backslashes_in_a_url_are_escaped():
    """Otherwise the closing quote lands early and the query is malformed
    again — the same 500, from a different direction."""
    assert _solr_safe_query('https://x/a"b') == '"https://x/a\\"b"'
    assert _solr_safe_query("https://x/a\\b") == '"https://x/a\\\\b"'


def test_none_is_tolerated():
    assert _solr_safe_query(None) is None


def test_package_search_sends_the_quoted_query(monkeypatch):
    sent = {}

    async def _fake_get(action, params=None):
        sent.update(action=action, **(params or {}))
        return {"count": 0, "results": []}

    monkeypatch.setattr(ckan_client, "_get", _fake_get)
    asyncio.run(
        ckan_client.package_search("https://t.me/s/Israel_Cyber", rows=5, start=0)
    )

    assert sent["q"] == '"https://t.me/s/Israel_Cyber"'
    assert sent["rows"] == 5 and sent["start"] == 0
