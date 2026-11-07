"""Google-IP check for the Looker connector: own ranges only, fail closed,
never fetch on the request path."""
import ipaddress
import threading

import pytest

from app.config import settings
from app.services import google_ips as g


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    monkeypatch.setattr(g, "_google", None)
    monkeypatch.setattr(g, "_cloud", [])
    monkeypatch.setattr(g, "_fetched_at", 0.0)
    monkeypatch.setattr(g, "_attempted_at", 0.0)
    monkeypatch.setattr(g, "_refreshing", False)
    monkeypatch.setattr(settings, "connector_exclude_cloud_ranges", True, raising=False)


def _loaded(monkeypatch, google, cloud):
    import time
    monkeypatch.setattr(g, "_google", [ipaddress.ip_network(n) for n in google])
    monkeypatch.setattr(g, "_cloud", [ipaddress.ip_network(n) for n in cloud])
    monkeypatch.setattr(g, "_fetched_at", time.monotonic())


def test_fails_closed_before_ranges_load(monkeypatch):
    started = []
    monkeypatch.setattr(threading, "Thread",
                        lambda **kw: type("T", (), {"start": lambda self: started.append(kw)})())
    assert g.is_google_ip("8.8.8.8") is False
    assert started and started[0]["target"] is g._refresh   # refresh runs off-thread


def test_cloud_customer_ranges_are_not_google(monkeypatch):
    _loaded(monkeypatch, ["8.8.8.0/24", "34.0.0.0/8"], ["34.0.0.0/8"])
    assert g.is_google_ip("8.8.8.8") is True
    assert g.is_google_ip("34.1.2.3") is False       # a rented GCE address
    assert g.is_google_ip("1.1.1.1") is False
    assert g.is_google_ip("not-an-ip") is False


def test_exclusion_can_be_turned_off(monkeypatch):
    _loaded(monkeypatch, ["34.0.0.0/8"], ["34.0.0.0/8"])
    monkeypatch.setattr(settings, "connector_exclude_cloud_ranges", False, raising=False)
    assert g.is_google_ip("34.1.2.3") is True


def test_failed_refresh_keeps_failing_closed(monkeypatch):
    def boom(url):
        raise RuntimeError("gstatic down")
    monkeypatch.setattr(g, "_prefixes", boom)
    g._refresh()
    assert g._google is None and g._refreshing is False
    monkeypatch.setattr(threading, "Thread",
                        lambda **kw: type("T", (), {"start": lambda self: None})())
    assert g.is_google_ip("8.8.8.8") is False


def test_refresh_loads_both_feeds(monkeypatch):
    feeds = {g._GOOG_URL: ["8.8.8.0/24"], g._CLOUD_URL: ["34.0.0.0/8"]}
    monkeypatch.setattr(g, "_prefixes",
                        lambda url: [ipaddress.ip_network(n) for n in feeds[url]])
    g._refresh()
    assert g.is_google_ip("8.8.8.8") is True
