"""Single source of truth for the *real* client IP behind the proxy chain.

Both the per-IP request rate limiter (``app/rate_limit.py``) and the per-IP
bulk-data budget (``app/api_budget_middleware.py``) key off this value, so it is
security-relevant: a client that could forge its own IP would (a) escape its
rate-limit bucket and (b) reset its data-budget tally at will simply by rotating
the ``X-Forwarded-For`` header. Keep this the ONE place IP is derived — the two
consumers must agree, and the trust reasoning must live in exactly one spot.

Topology
--------
over.org.il is served ``client → Cloudflare edge → Render edge/LB → Render
internal → app`` (``Server: cloudflare`` + ``CF-RAY`` confirm Cloudflare
fronting; the app runs on Render behind ``uvicorn --proxy-headers``).

Trust model — nothing the client sets is trusted directly
---------------------------------------------------------
* ``X-Forwarded-For`` is *appended* to by each hop, and neither Cloudflare nor
  Render strips a client-supplied value, so its LEFT entries are
  attacker-controlled. Only the hops OUR infra appended (from the right) are
  trustworthy. The naive "take XFF[0]" that Render's own docs suggest is exactly
  the spoofable position — never use it.
* ``CF-Connecting-IP`` is set by Cloudflare to the real visitor IP on every
  request it proxies, overwriting any client-supplied value — unforgeable *for
  traffic that truly goes through Cloudflare*. But the Render origin
  (``*.onrender.com``) is reachable DIRECTLY too, where an attacker could set
  ``CF-Connecting-IP`` themselves. So we trust it only after PROVING the request
  transited Cloudflare.

Derivation (:func:`get_client_ip`):
  1. Walk ``X-Forwarded-For`` from the RIGHT, skipping private/reserved/loopback
     hops (Render's internal proxies), to find ``edge`` — the rightmost PUBLIC
     IP, i.e. the address that actually connected to our infrastructure
     (a Cloudflare edge for real traffic; the attacker's own IP for a direct
     origin hit).
  2. If ``edge`` is within Cloudflare's published ranges, the request came
     through Cloudflare ⇒ return ``CF-Connecting-IP`` (the validated visitor).
     A direct attacker cannot make ``edge`` be a Cloudflare IP: Render's edge
     appends the attacker's true peer IP to the RIGHT of anything they forged,
     so their real IP — not a forged Cloudflare one — is the rightmost public
     hop. Hence they cannot trip this branch to smuggle a spoofed
     ``CF-Connecting-IP``.
  3. Otherwise the request did NOT come through Cloudflare (direct origin hit)
     ⇒ return ``edge`` itself — the attacker's real connecting IP, which is
     exactly what we want to bucket.
  4. No usable XFF (local/dev, all-internal chain) ⇒ ``request.client.host``,
     else ``"unknown"``.

Both step 1 (rightmost non-internal) and step 2 (Cloudflare validation) make the
returned value robust to X-Forwarded-For / CF-Connecting-IP spoofing.
"""
from __future__ import annotations

import ipaddress
import logging
import os

from starlette.requests import Request

logger = logging.getLogger(__name__)

# Cloudflare's published edge ranges (https://www.cloudflare.com/ips/). Embedded
# so IP derivation has NO network dependency on the request path; Cloudflare
# egresses to origins from these same ranges, so a request that reached us via
# Cloudflare shows one of these as its rightmost public XFF hop. Override with
# the CLOUDFLARE_IPS env (comma-separated CIDRs) if Cloudflare ever rotates them.
_CLOUDFLARE_CIDRS_DEFAULT = (
    # IPv4
    "173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
    "141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20",
    "197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13",
    "104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22",
    # IPv6
    "2400:cb00::/32", "2606:4700::/32", "2803:f800::/32", "2405:b500::/32",
    "2405:8100::/32", "2a06:98c0::/29", "2c0f:f248::/32",
)


def _load_cidrs(env_name: str, default: tuple[str, ...]) -> tuple:
    raw = os.environ.get(env_name, "")
    items = [s.strip() for s in raw.split(",") if s.strip()] or list(default)
    nets = []
    for item in items:
        try:
            nets.append(ipaddress.ip_network(item, strict=False))
        except ValueError:
            logger.warning("client_ip: ignoring invalid CIDR %r in %s", item, env_name)
    return tuple(nets)


_CF_NETS = _load_cidrs("CLOUDFLARE_IPS", _CLOUDFLARE_CIDRS_DEFAULT)


def _parse_ip(token: str | None):
    """Best-effort parse of one XFF/header token into an ip address, or None.

    Tolerates surrounding brackets on ``[IPv6]`` and an optional ``:port`` on an
    IPv4 literal (some proxies emit ``ip:port``); anything unparseable → None.
    """
    if not token:
        return None
    s = token.strip()
    if not s:
        return None
    if s.startswith("["):                        # [2001:db8::1] or [::1]:443
        s = s[1:].split("]", 1)[0]
    elif s.count(":") == 1 and "." in s:         # 203.0.113.7:54321 (ipv4:port)
        s = s.split(":", 1)[0]
    try:
        return ipaddress.ip_address(s)
    except ValueError:
        return None


def _is_internal(ip) -> bool:
    """A hop added by our own infra (Render internal / loopback) — never the
    real client. Public internet clients are none of these."""
    return (
        ip.is_private or ip.is_loopback or ip.is_link_local
        or ip.is_reserved or ip.is_unspecified or ip.is_multicast
    )


def _in_cloudflare(ip) -> bool:
    return any(ip in net for net in _CF_NETS)


def get_client_ip(request: Request) -> str:
    """The trustworthy real client IP for rate-limiting / budgeting.

    See the module docstring for the full trust model. Returns a printable IP
    string, or ``"unknown"`` when nothing usable is present.
    """
    xff = request.headers.get("x-forwarded-for", "")
    hops = [ip for ip in (_parse_ip(tok) for tok in xff.split(",")) if ip is not None] if xff else []

    # Rightmost PUBLIC hop = the address that actually connected to our infra
    # (a Cloudflare edge for real traffic; the attacker's own IP for a direct
    # origin hit). Everything Render adds inside that is private and skipped.
    edge = next((ip for ip in reversed(hops) if not _is_internal(ip)), None)

    if edge is not None and _in_cloudflare(edge):
        # Proven to have transited Cloudflare → CF-Connecting-IP is authoritative.
        cf_ip = _parse_ip(request.headers.get("cf-connecting-ip"))
        if cf_ip is not None:
            return str(cf_ip)
        # Through Cloudflare but no CF-Connecting-IP (shouldn't happen): bucket
        # by the CF edge rather than trusting a spoofable left-hand XFF value.
        return str(edge)

    if edge is not None:
        return str(edge)

    # No usable XFF (local/dev, or an all-internal chain): the direct peer.
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def client_ip_key(request: Request) -> str:
    """``slowapi`` ``key_func`` — same derivation as everything else, so the
    request rate limiter and the data budget bucket by an identical key."""
    return get_client_ip(request)
