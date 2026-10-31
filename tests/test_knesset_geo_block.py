"""knesset.gov.il geo-blocks non-Israeli addresses (303 → /maintenance-page-geo).

Production runs abroad, so on 2026-10-31 the ODATA mirror logged a full
traceback every 3 minutes and /api/knesset-db/tables failed. These tests pin:
the block is recognised by name, the sync backs off quietly, the read side
survives it, and KNESSET_PROXY_URL routes ONLY knesset hosts through a proxy.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx
import pytest

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.config import settings  # noqa: E402
from app.services import knesset_db, knesset_http  # noqa: E402
from app.services.knesset_http import KnessetGeoBlocked  # noqa: E402

GEO = "https://www.knesset.gov.il/maintenance-page-geo"


def _geo_handler(calls: list):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        if "maintenance" in request.url.path:
            return httpx.Response(200, text="<html>maintenance</html>")
        return httpx.Response(303, headers={"location": GEO})
    return handler


@pytest.fixture(autouse=True)
def _fresh_module_state(monkeypatch):
    monkeypatch.setattr(knesset_db, "_metadata_cache", None)
    monkeypatch.setattr(knesset_db, "_infra_ready", False)
    monkeypatch.setattr(knesset_db, "_next_due_at", None)
    monkeypatch.setattr(knesset_db, "_geo_blocked_until", None)
    monkeypatch.setattr(knesset_db, "_geo_block_error", None)
    monkeypatch.setattr(settings, "knesset_proxy_url", "")


# ── detection ────────────────────────────────────────────────────────────────

async def _get(url: str, *, follow: bool) -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.MockTransport(_geo_handler([])),
                                 follow_redirects=follow) as c:
        return await c.get(url)


@pytest.mark.asyncio
async def test_unfollowed_303_to_the_geo_page_is_named():
    resp = await _get("https://knesset.gov.il/OdataV4/ParliamentInfo/$metadata", follow=False)
    with pytest.raises(KnessetGeoBlocked) as ei:
        knesset_http.check_geo_block(resp)
    assert "maintenance-page-geo" in str(ei.value)
    assert "KNESSET_PROXY_URL" in str(ei.value)


@pytest.mark.asyncio
async def test_followed_redirect_landing_on_the_geo_page_is_named():
    resp = await _get("https://fs.knesset.gov.il/a.pdf", follow=True)
    assert resp.status_code == 200          # the trap: a "happy" 200
    with pytest.raises(KnessetGeoBlocked) as ei:
        knesset_http.check_geo_block(resp)
    assert ei.value.url == "https://fs.knesset.gov.il/a.pdf"


def test_ordinary_responses_pass():
    req = httpx.Request("GET", "https://knesset.gov.il/x")
    knesset_http.check_geo_block(httpx.Response(200, request=req))
    knesset_http.check_geo_block(httpx.Response(404, request=req))
    knesset_http.check_geo_block(
        httpx.Response(302, headers={"location": "https://knesset.gov.il/y"}, request=req))


# ── proxy routing ────────────────────────────────────────────────────────────

def _is_proxied(c: httpx.AsyncClient, url: str) -> bool:
    import httpcore
    t = c._transport_for_url(httpx.URL(url))
    pool = getattr(t, "_pool", None)
    # OUR proxy, not whatever egress proxy the test host has in its env.
    return (isinstance(pool, httpcore.AsyncHTTPProxy)
            and pool._proxy_url.host == b"il-proxy.test")


@pytest.mark.asyncio
async def test_without_proxy_setting_nothing_is_mounted(monkeypatch):
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
                "http_proxy", "https_proxy", "all_proxy"):
        monkeypatch.delenv(var, raising=False)   # the test host's own egress proxy
    async with knesset_http.client() as c:
        assert not _is_proxied(c, "https://knesset.gov.il/OdataV4/ParliamentInfo")


@pytest.mark.asyncio
async def test_proxy_applies_to_knesset_hosts_only(monkeypatch):
    monkeypatch.setattr(settings, "knesset_proxy_url", "http://u:p@il-proxy.test:8888")
    calls: list = []
    mock = httpx.MockTransport(lambda r: (calls.append(str(r.url)), httpx.Response(200))[1])
    async with knesset_http.client(transport=mock) as c:
        for url in ("https://knesset.gov.il/OdataV4/ParliamentInfo/$metadata",
                    "https://www.knesset.gov.il/", "https://main.knesset.gov.il/x",
                    "https://fs.knesset.gov.il/protocol.pdf"):
            assert _is_proxied(c, url), url
        for url in ("https://r2.example.dev/file.csv", "https://data.gov.il/api",
                    "https://evilknesset.gov.il/"):
            assert not _is_proxied(c, url), url
        # A non-knesset request on the same client still reaches the caller's
        # transport (here the mock) — i.e. it went direct, not via the proxy.
        await c.get("https://r2.example.dev/file.csv")
    assert calls == ["https://r2.example.dev/file.csv"]


@pytest.mark.asyncio
async def test_mirror_client_uses_the_proxy(monkeypatch):
    monkeypatch.setattr(settings, "knesset_proxy_url", "http://il-proxy.test:8888")
    async with knesset_db._http_client() as c:
        assert _is_proxied(c, f"{knesset_db.PARLIAMENT_BASE}/$metadata")


# ── mirror: metadata, pages, backoff ─────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_metadata_raises_geo_block_not_http_status_error():
    async with httpx.AsyncClient(transport=httpx.MockTransport(_geo_handler([]))) as c:
        with pytest.raises(KnessetGeoBlocked):
            await knesset_db.fetch_metadata(c)


@pytest.mark.asyncio
async def test_page_fetch_does_not_retry_a_geo_block(monkeypatch):
    calls: list = []
    sleeps: list = []

    async def no_sleep(s):
        sleeps.append(s)
    monkeypatch.setattr(knesset_db.asyncio, "sleep", no_sleep)
    es = knesset_db.EntitySet("KNS_Bill", [("Id", "Edm.Int32")])
    async with httpx.AsyncClient(transport=httpx.MockTransport(_geo_handler(calls))) as c:
        with pytest.raises(KnessetGeoBlocked):
            await knesset_db._fetch_page(c, es, "Id gt 0", want_count=False)
    assert len(calls) == 1 and sleeps == []


@pytest.mark.asyncio
async def test_sync_tick_warns_once_then_backs_off(monkeypatch, caplog):
    calls: list = []
    monkeypatch.setattr(knesset_db, "is_configured", lambda: True)
    monkeypatch.setattr(
        knesset_db, "_http_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(_geo_handler(calls))))

    caplog.set_level(logging.WARNING, logger="app.services.knesset_db")
    res = await knesset_db.sync_tick()
    assert res["skipped"] == "geo-blocked"
    recs = [r for r in caplog.records if r.name == "app.services.knesset_db"]
    assert len(recs) == 1
    assert recs[0].levelno == logging.WARNING and recs[0].exc_info is None
    assert "maintenance-page-geo" in recs[0].getMessage()
    assert len(calls) == 1

    # The next 3-minute ticks: no request, no log line.
    caplog.clear()
    for _ in range(5):
        assert (await knesset_db.sync_tick())["skipped"] == "geo-blocked"
    assert len(calls) == 1
    assert not [r for r in caplog.records if r.name == "app.services.knesset_db"]

    # Once the backoff lapses, the tick asks again (and is refused again).
    knesset_db._geo_blocked_until = datetime.now(timezone.utc) - timedelta(seconds=1)
    assert (await knesset_db.sync_tick())["skipped"] == "geo-blocked"
    assert len(calls) == 2
    assert knesset_db.geo_blocked_until() is not None


@pytest.mark.asyncio
async def test_admin_run_bypasses_the_backoff(monkeypatch):
    calls: list = []
    monkeypatch.setattr(knesset_db, "is_configured", lambda: True)
    monkeypatch.setattr(
        knesset_db, "_http_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(_geo_handler(calls))))
    await knesset_db.sync_tick()
    await knesset_db.sync_tick(only_table="kns_bill")
    assert len(calls) == 2


# ── read side ────────────────────────────────────────────────────────────────

class _FakeConn:
    def __init__(self, rows=None, missing=False):
        self.rows, self.missing = rows or [], missing

    async def fetch(self, sql, *a):
        if self.missing:
            import asyncpg
            raise asyncpg.UndefinedTableError("relation does not exist")
        return self.rows


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *a):
                return False
        return _Ctx()


def _block_infra(monkeypatch, conn):
    async def blocked():
        raise KnessetGeoBlocked("https://knesset.gov.il/x", GEO)

    async def get_pool():
        return _FakePool(conn)
    monkeypatch.setattr(knesset_db, "ensure_infra", blocked)
    monkeypatch.setattr(knesset_db.append_store, "get_pool", get_pool)


@pytest.mark.asyncio
async def test_list_tables_survives_the_block_from_sync_state(monkeypatch):
    row = {"table_name": "kns_bill", "entity_set": "KNS_Bill",
           "columns": '[["Id", "Edm.Int32"], ["Name", "Edm.String"]]',
           "total_rows": 10, "source_count": 10, "full_loaded": True,
           "status": "ok", "error": None, "last_synced_at": None}
    _block_infra(monkeypatch, _FakeConn([row]))
    out = await knesset_db.list_tables()
    assert out[0]["table"] == "kns_bill"
    assert [c["name"] for c in out[0]["columns"]] == ["id", "name"]


@pytest.mark.asyncio
async def test_list_tables_without_a_mirror_reports_the_block(monkeypatch):
    _block_infra(monkeypatch, _FakeConn(missing=True))
    with pytest.raises(KnessetGeoBlocked):
        await knesset_db.list_tables()


@pytest.mark.asyncio
async def test_tables_endpoint_answers_503_in_hebrew(monkeypatch):
    from fastapi import FastAPI

    from app.api import knesset_db as api
    from app.rate_limit import limiter

    async def blocked():
        raise KnessetGeoBlocked("https://knesset.gov.il/x", GEO)
    monkeypatch.setattr(api.knesset_db, "is_configured", lambda: True)
    monkeypatch.setattr(api.knesset_db, "list_tables", blocked)
    app = FastAPI()
    app.state.limiter = limiter
    app.include_router(api.router)
    limiter.reset()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://t") as c:
        r = await c.get("/api/knesset-db/tables")
    assert r.status_code == 503
    detail = r.json()["detail"]
    assert detail == knesset_http.GEO_BLOCK_MESSAGE_HE
    assert "הכנסת" in detail and "KNESSET_PROXY_URL" in detail
