"""Is this request coming from Google's infrastructure?

The Looker Studio connector (Apps Script UrlFetchApp) egresses from Google's
published IP ranges. Classifying connector traffic by source range lets the
backend route it to the shared byte-budget bucket WITHOUT a client-side
secret — an Apps Script project shared "anyone with link: Viewer" exposes its
Script Properties to viewers, so no value stored there can be treated as
secret (learned the hard way; see looker-connector/GALLERY-PLAN.md).

Ranges come from https://www.gstatic.com/ipranges/goog.json (all of Google,
refreshed daily, cached in-process). If the feed has never loaded we FAIL
OPEN with a warning: the classification only picks a budget bucket for a
read-only public API, so availability beats bucket purity.
"""
from __future__ import annotations

import ipaddress
import logging
import threading
import time

import httpx

logger = logging.getLogger(__name__)

_GOOG_URL = "https://www.gstatic.com/ipranges/goog.json"
_TTL_SECONDS = 24 * 3600

_lock = threading.Lock()
_networks: list[ipaddress._BaseNetwork] | None = None
_fetched_at: float = 0.0


def _refresh_locked() -> None:
    global _networks, _fetched_at
    resp = httpx.get(_GOOG_URL, timeout=10)
    resp.raise_for_status()
    nets: list[ipaddress._BaseNetwork] = []
    for entry in resp.json().get("prefixes", []):
        prefix = entry.get("ipv4Prefix") or entry.get("ipv6Prefix")
        if prefix:
            nets.append(ipaddress.ip_network(prefix))
    if not nets:
        raise ValueError("goog.json returned no prefixes")
    _networks = nets
    _fetched_at = time.monotonic()
    logger.info("Loaded %d Google IP ranges", len(nets))


def is_google_ip(ip: str) -> bool:
    """True if ip falls in Google's published ranges (or the feed is down —
    fail open, see module docstring)."""
    global _networks
    now = time.monotonic()
    if _networks is None or now - _fetched_at > _TTL_SECONDS:
        with _lock:
            if _networks is None or now - _fetched_at > _TTL_SECONDS:
                try:
                    _refresh_locked()
                except Exception:  # noqa: BLE001 — keep serving on stale/absent feed
                    if _networks is None:
                        logger.warning("Google IP ranges unavailable — failing open")
                        return True
                    logger.warning("Google IP ranges refresh failed — using stale set")
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return any(addr in net for net in _networks)
