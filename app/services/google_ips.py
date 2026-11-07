"""Is this request coming from Google's own infrastructure?

The Looker Studio connector (Apps Script UrlFetchApp) egresses from Google's
published IP ranges. Classifying connector traffic by source range lets the
backend route it to the shared byte-budget bucket WITHOUT a client-side
secret — an Apps Script project shared "anyone with link: Viewer" exposes its
Script Properties to viewers, so no value stored there can be treated as
secret (learned the hard way; see looker-connector/GALLERY-PLAN.md).

Ranges come from https://www.gstatic.com/ipranges/goog.json (every address
Google holds) MINUS https://www.gstatic.com/ipranges/cloud.json (the part of
it Google Cloud customers rent). That difference is how Google itself says to
recognise its own services. Admitting all of goog.json, as this did, let any
Google Cloud VM, Cloud Run service or Colab notebook in as "the connector".
``settings.connector_exclude_cloud_ranges`` turns the subtraction off, as an
escape hatch in case Apps Script is ever seen egressing from a customer range
(every refusal of a goog.json address is logged, so that would show).

Two more rules:

* **Fail closed.** Until the feeds have loaded nothing is Google. The previous
  rule — fail open, so every address on the internet counted as Google while
  the feed was down — made an outage of gstatic.com an outage of the check.
* **Never fetch on the request path.** The check is called from middleware on
  the event loop; a synchronous 10-second fetch there stalled every request
  in flight once a day. Refreshes run on a background thread, and the check
  answers from whatever set is loaded.
"""
from __future__ import annotations

import ipaddress
import logging
import threading
import time

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_GOOG_URL = "https://www.gstatic.com/ipranges/goog.json"
_CLOUD_URL = "https://www.gstatic.com/ipranges/cloud.json"
_TTL_SECONDS = 24 * 3600
# After a failed refresh, try again sooner than the TTL.
_RETRY_SECONDS = 15 * 60

_lock = threading.Lock()
_refreshing = False
_google: list[ipaddress._BaseNetwork] | None = None
_cloud: list[ipaddress._BaseNetwork] = []
_fetched_at: float = 0.0
_attempted_at: float = 0.0


def _prefixes(url: str) -> list[ipaddress._BaseNetwork]:
    resp = httpx.get(url, timeout=10)
    resp.raise_for_status()
    nets: list[ipaddress._BaseNetwork] = []
    for entry in resp.json().get("prefixes", []):
        prefix = entry.get("ipv4Prefix") or entry.get("ipv6Prefix")
        if prefix:
            nets.append(ipaddress.ip_network(prefix))
    if not nets:
        raise ValueError(f"{url} returned no prefixes")
    return nets


def _refresh() -> None:
    """Load both feeds. Runs on a background thread."""
    global _google, _cloud, _fetched_at, _refreshing
    try:
        google = _prefixes(_GOOG_URL)
        cloud = _prefixes(_CLOUD_URL)
        with _lock:
            _google, _cloud = google, cloud
            _fetched_at = time.monotonic()
        logger.info("Loaded %d Google IP ranges (%d of them Google Cloud customer ranges)",
                    len(google), len(cloud))
    except Exception:  # noqa: BLE001 — keep the set we have; retry later
        logger.warning("Google IP ranges refresh failed — %s",
                       "keeping the stale set" if _google is not None
                       else "nothing counts as Google until it loads", exc_info=True)
    finally:
        with _lock:
            _refreshing = False


def ensure_fresh() -> None:
    """Start a background refresh if the set is missing or stale. Never blocks."""
    global _refreshing, _attempted_at
    now = time.monotonic()
    with _lock:
        if _refreshing:
            return
        if _google is not None and now - _fetched_at < _TTL_SECONDS:
            return
        if now - _attempted_at < (_RETRY_SECONDS if _attempted_at else 0):
            return
        _refreshing = True
        _attempted_at = now
    threading.Thread(target=_refresh, name="google-ip-ranges", daemon=True).start()


def is_google_ip(ip: str) -> bool:
    """True if ``ip`` is in Google's own ranges: goog.json, less the Google
    Cloud customer ranges. False until the ranges have loaded."""
    ensure_fresh()
    google, cloud = _google, _cloud
    if google is None:
        return False
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    if not any(addr in net for net in google):
        return False
    if getattr(settings, "connector_exclude_cloud_ranges", True) and any(
            addr in net for net in cloud):
        logger.info("Google Cloud customer address %s not treated as Google", ip)
        return False
    return True
