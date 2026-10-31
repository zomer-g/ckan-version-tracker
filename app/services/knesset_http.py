"""The one way the server talks to knesset.gov.il: optional egress proxy +
geo-block recognition.

Why this exists: knesset.gov.il geo-blocks addresses outside Israel. Every
request from the production host (xhostd, abroad) is answered with
``303 See Other → https://www.knesset.gov.il/maintenance-page-geo``, which then
serves a cheerful 200. Confirmed in the production logs on 2026-10-31: the
ODATA mirror had not advanced since at least 2026-10-28, every 3-minute tick
logged a full traceback, and /api/knesset-db/tables answered 500.

Two things follow, and both live here so no call site can forget one:

* ``client(**kw)`` returns an ``httpx.AsyncClient`` that, when
  ``settings.knesset_proxy_url`` is set, routes knesset.gov.il and every
  subdomain (www., main., fs., the OdataV4 base) through that proxy — and ONLY
  those hosts. The routing is done with httpx ``mounts`` rather than a
  client-wide ``proxy=`` on purpose: some callers fetch mixed hosts on one
  client (the MMM catalog CSV may live on R2), and sending R2/data.gov.il
  traffic through a residential box in Israel would be slower and pointless.
  How to run the proxy: docs/knesset-geo-block.md.

* ``KnessetGeoBlocked`` + ``check_geo_block(resp)`` turn the redirect into a
  named, clearly-worded failure instead of a generic ``HTTPStatusError`` (or,
  with follow_redirects, a 200 whose body is an HTML maintenance page that
  then fails to parse as XML/JSON somewhere far away). Callers catch it to
  back off and to answer 503 with a Hebrew explanation.
"""
from __future__ import annotations

import httpx

from app.config import settings

# httpx URL patterns: "all://knesset.gov.il" matches only the apex, and
# "all://*.knesset.gov.il" only subdomains — so both are needed. (The tempting
# "all://*knesset.gov.il" would also match e.g. "evilknesset.gov.il".)
KNESSET_MOUNT_PATTERNS = ("all://knesset.gov.il", "all://*.knesset.gov.il")

GEO_BLOCK_MESSAGE_HE = (
    "אתר הכנסת חוסם גישה מכתובות מחוץ לישראל, והשרת של גרסאות לעם נמצא בחו\"ל. "
    "הנתונים יתעדכנו מחדש כשתוגדר יציאה דרך שרת בישראל (KNESSET_PROXY_URL)."
)


class KnessetGeoBlocked(RuntimeError):
    """knesset.gov.il redirected us to its geo-block / maintenance page."""

    def __init__(self, url: str, location: str):
        self.url = url
        self.location = location
        super().__init__(
            f"knesset.gov.il geo-blocked this server: {url} → {location} "
            "(set KNESSET_PROXY_URL to egress through Israel; "
            "see docs/knesset-geo-block.md)"
        )


def is_knesset_host(host: str | None) -> bool:
    h = (host or "").lower().rstrip(".")
    return h == "knesset.gov.il" or h.endswith(".knesset.gov.il")


def _looks_like_block(location: str) -> bool:
    loc = (location or "").lower()
    return "maintenance-page-geo" in loc or "maintenance" in loc


def check_geo_block(resp: httpx.Response) -> None:
    """Raise KnessetGeoBlocked if ``resp`` is (or ended at) the geo-block page.

    Handles both shapes: a client that does NOT follow redirects sees the 3xx
    itself (its Location names the maintenance page); one that does follows it
    and lands on the maintenance URL with a non-empty ``history``. Call BEFORE
    ``raise_for_status()`` — the 303 would otherwise surface as an anonymous
    "Redirect response '303 See Other'" HTTPStatusError."""
    # getattr: duck-typed, so a test double standing in for a response (see
    # tests/test_knesset_batch_guard.py) passes as "not blocked".
    if getattr(resp, "is_redirect", False):
        loc = resp.headers.get("location", "")
        if _looks_like_block(loc):
            raise KnessetGeoBlocked(str(resp.request.url), loc)
    if getattr(resp, "history", None) and _looks_like_block(str(resp.url)):
        first = resp.history[0]
        raise KnessetGeoBlocked(str(first.request.url), str(resp.url))


def _proxy_mounts() -> dict[str, httpx.AsyncBaseTransport] | None:
    proxy = (settings.knesset_proxy_url or "").strip()
    if not proxy:
        return None
    return {p: httpx.AsyncHTTPTransport(proxy=proxy) for p in KNESSET_MOUNT_PATTERNS}


def client(**kw) -> httpx.AsyncClient:
    """An ``httpx.AsyncClient`` for code that talks to knesset.gov.il.

    Accepts every AsyncClient kwarg. With KNESSET_PROXY_URL set, requests to
    knesset hosts go through the proxy; everything else on the same client goes
    direct (or via ``transport=`` when a test passes one). Explicit ``mounts``
    from the caller win over ours for the same pattern."""
    mounts = _proxy_mounts()
    if mounts:
        mounts.update(kw.pop("mounts", None) or {})
        kw["mounts"] = mounts
    return httpx.AsyncClient(**kw)
