"""Resource downloads refuse internal addresses: literal, behind a hostname,
and after a redirect."""
import asyncio
import ipaddress
import socket

import httpx
import pytest

from app.services import ckan_client as C


@pytest.mark.parametrize("ip,bad", [
    ("8.8.8.8", False), ("2001:4860:4860::8888", False),
    ("127.0.0.1", True), ("10.1.2.3", True), ("169.254.169.254", True),
    ("100.64.0.1", True), ("0.0.0.0", True), ("::ffff:127.0.0.1", True), ("fd00::1", True),
])
def test_forbidden_ip(ip, bad):
    assert C._forbidden_ip(ipaddress.ip_address(ip)) is bad


def _resolve_to(monkeypatch, mapping):
    async def fake(host):
        if host not in mapping:
            raise socket.gaierror("nope")
        return list(mapping[host])

    monkeypatch.setattr(C, "_resolve", fake)


def test_hostname_resolving_inside_is_refused(monkeypatch):
    _resolve_to(monkeypatch, {"metadata.internal": ["169.254.169.254"],
                              "data.gov.il": ["104.18.1.1"],
                              "mixed.example": ["104.18.1.1", "10.0.0.5"]})
    asyncio.run(C._check_resolved("https://data.gov.il/x.csv"))
    for url in ("http://metadata.internal/latest/meta-data", "https://mixed.example/x",
                "https://unresolvable.example/x", "http://127.0.0.1/x", "ftp://data.gov.il/x"):
        with pytest.raises(ValueError):
            asyncio.run(C._check_resolved(url))


def test_redirect_to_an_internal_host_is_refused(monkeypatch):
    _resolve_to(monkeypatch, {"public.example": ["104.18.1.1"],
                              "metadata.internal": ["169.254.169.254"]})
    seen = []

    def handler(request):
        seen.append(request.url.host)
        if request.url.host == "public.example":
            return httpx.Response(302, headers={"location": "http://metadata.internal/secret"})
        return httpx.Response(200, text="SECRET")

    async def go():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True,
                                     event_hooks={"request": [C._guard_request]}) as client:
            await client.get("https://public.example/x.csv")

    with pytest.raises(ValueError):
        asyncio.run(go())
    assert seen == ["public.example"]          # the internal hop was never sent
