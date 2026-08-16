"""Title resolution for gov.il /he/pages/ (content_page) URLs.

Two things pinned here:

1. A page can be a stack of unrelated tabs. grants-grants has eleven, and
   ``contentHead.title`` reports "תמיכות ומענקים" for every one of them —
   though chapterIndex=5 is the balance grants and chapterIndex=7 is the
   minister's grant. Each tab is tracked as its own dataset (the slug hashes
   the full URL), so a pinned chapter must be titled after its own tab.

2. The resolver reaches the Apigee gateway with a client id + gov.il Origin.
   The old www.gov.il/ContentPageWebApi path is behind Cloudflare and 403s a
   plain client, which silently turned every title into a slug.

No network — httpx is stubbed.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from app.api import govil


_TABS = [
    {"title": "תמיכה בקריות חינוך", "url": "/he/pages/grants-grants"},
    {"title": "מענק איזון של משרד הפנים", "url": "/he/pages/grants-grants?chapterIndex=5"},
    {"title": "מענק שר", "url": "/he/pages/grants-grants?chapterIndex=7"},
]

_CLIENT_CONFIG = (
    "window['govilRunConfig'] = "
    + json.dumps({
        "contentPageWebApi": "https://gateway.example/pub/contentpage/v1",
        "clientId": "test-client-id",
    })
    + ";"
)


class _Resp:
    def __init__(self, status=200, text="", payload=None, content_type="application/json"):
        self.status_code = status
        self.headers = {"content-type": content_type}
        self.text = text or (json.dumps(payload, ensure_ascii=False) if payload else "")
        self._payload = payload

    def json(self):
        return self._payload


class _StubClient:
    """Serves client-config.js and the gateway; records every call."""

    def __init__(self, *args, **kwargs):
        self.calls: list[tuple[str, dict]] = []

    async def __aenter__(self):
        _StubClient.last = self
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, headers=None):
        self.calls.append((url, headers or {}))
        if url == govil._GOVIL_CLIENT_CONFIG_JS:
            return _Resp(text=_CLIENT_CONFIG, content_type="application/javascript")
        if not url.startswith("https://gateway.example/"):
            # Cloudflare's answer on the old www path.
            return _Resp(status=403, text="<html>Attention Required!</html>",
                         content_type="text/html")
        return _Resp(payload={
            "contentHead": {"title": "תמיכות ומענקים"},
            "contentMain": {"sideNav": {"tagItems": _TABS}},
        })


@pytest.fixture(autouse=True)
def _stub_httpx(monkeypatch):
    govil._content_page_api_cache = None
    monkeypatch.setattr(govil.httpx, "AsyncClient", _StubClient)
    yield
    govil._content_page_api_cache = None


def _title(url):
    """The repo has no pytest-asyncio; drive the coroutine directly."""
    _, collector_name = govil._parse_govil_url(url)
    return asyncio.run(govil._fetch_content_page_title(collector_name, url))


def test_pinned_chapter_is_titled_after_its_own_tab():
    title = _title("https://www.gov.il/he/pages/grants-grants?chapterIndex=5")
    assert title == "תמיכות ומענקים — מענק איזון של משרד הפנים"


def test_sibling_chapters_get_different_titles():
    five = _title("https://www.gov.il/he/pages/grants-grants?chapterIndex=5")
    seven = _title("https://www.gov.il/he/pages/grants-grants?chapterIndex=7")
    assert five != seven
    assert seven.endswith("מענק שר")


def test_page_without_chapter_keeps_the_page_title():
    assert _title("https://www.gov.il/he/pages/grants-grants") == "תמיכות ומענקים"


def test_non_numeric_chapter_index_is_ignored():
    assert _title("https://www.gov.il/he/pages/grants-grants?chapterIndex=x%27") == "תמיכות ומענקים"


def test_request_carries_client_id_and_gov_il_origin():
    """Without the Origin the gateway answers RF-OriginError; without the
    client id, FailedToResolveAPIKey. Both are 500s that read like an outage."""
    _title("https://www.gov.il/he/pages/grants-grants?chapterIndex=5")
    api_url, headers = _StubClient.last.calls[-1]
    assert api_url.startswith("https://gateway.example/pub/contentpage/v1/api/content-pages/")
    assert "chapterIndex=5" in api_url
    assert headers["x-client-id"] == "test-client-id"
    assert headers["Origin"] == "https://www.gov.il"


def test_unusable_client_config_falls_back_to_the_www_path():
    """A gateway we can't resolve must degrade to the old URL (and a slug
    title from the caller), never raise."""
    class _NoConfig(_StubClient):
        async def get(self, url, headers=None):
            self.calls.append((url, headers or {}))
            if url == govil._GOVIL_CLIENT_CONFIG_JS:
                return _Resp(text="/* nothing here */", content_type="application/javascript")
            return _Resp(status=403, text="<html>Attention Required!</html>",
                         content_type="text/html")

    govil._content_page_api_cache = None
    govil.httpx.AsyncClient = _NoConfig
    try:
        assert _title("https://www.gov.il/he/pages/grants-grants") is None
        assert _NoConfig.last.calls[-1][0].startswith(govil._GOVIL_API_FALLBACK)
    finally:
        govil.httpx.AsyncClient = _StubClient


def test_chapter_makes_the_dataset_slug_distinct():
    """The two tabs must not collide onto one dataset."""
    from app.api.utils import scraper_url_slug

    base = "https://www.gov.il/he/pages/grants-grants"
    assert (scraper_url_slug("grants-grants", f"{base}?chapterIndex=5")
            != scraper_url_slug("grants-grants", f"{base}?chapterIndex=7"))
