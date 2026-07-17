"""Tests for the central, spoof-resistant client-IP derivation (app/client_ip.py).

Topology under test: client → Cloudflare edge → Render edge/LB → Render internal
→ app. The security properties we assert:
  * two genuinely different clients resolve to two different keys (real per-client
    buckets, not one global bucket),
  * a client that forges X-Forwarded-For cannot change its resolved key,
  * a direct-to-origin attacker forging CF-Connecting-IP is bucketed by their
    real connecting IP, not the forged value.

NOTE on fixture IPs: RFC 5737 documentation ranges (203.0.113.0/24 etc.) are
classified as *private* by Python's ipaddress module, so they'd be treated as
internal hops and skipped. Real internet clients have globally-routable IPs, so
these tests use real global addresses (8.8.8.8, 9.9.9.9, …) as opaque stand-ins.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.client_ip import get_client_ip

# A real Cloudflare egress IP (within 104.16.0.0/13) — stands in for the edge
# address Render sees when a request truly transits Cloudflare.
CF_EDGE = "104.16.5.5"
# Render's private internal proxy hop, appended after the connecting IP.
RENDER_INTERNAL = "10.0.0.7"


class _Req:
    def __init__(self, headers=None, host="127.0.0.1"):
        # lower-cased keys — matches Starlette's case-insensitive header access
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}
        self.client = type("C", (), {"host": host})() if host else None


def _through_cloudflare(real_client, *, forged_xff_prefix="", cf_connecting=None):
    """Build the XFF a request that genuinely transited Cloudflare would carry.

    Cloudflare sets CF-Connecting-IP and appends the visitor to XFF; Render's
    edge then appends the Cloudflare egress IP, and a Render internal hop appends
    its private address. Any `forged_xff_prefix` is what the client injected —
    it lands to the LEFT of everything our infra appended.
    """
    parts = []
    if forged_xff_prefix:
        parts.append(forged_xff_prefix)
    parts += [real_client, CF_EDGE, RENDER_INTERNAL]
    headers = {"X-Forwarded-For": ", ".join(parts)}
    headers["CF-Connecting-IP"] = cf_connecting or real_client
    return _Req(headers=headers, host=RENDER_INTERNAL)


def test_two_real_clients_get_distinct_keys():
    a = get_client_ip(_through_cloudflare("8.8.8.8"))
    b = get_client_ip(_through_cloudflare("9.9.9.9"))
    assert a == "8.8.8.8"
    assert b == "9.9.9.9"
    assert a != b  # real per-client buckets, not one shared Cloudflare-edge bucket


def test_forged_xff_cannot_change_key():
    """Same real client, once with a forged left-hand XFF — key is unchanged."""
    honest = get_client_ip(_through_cloudflare("8.8.8.8"))
    spoofed = get_client_ip(
        _through_cloudflare("8.8.8.8", forged_xff_prefix="1.2.3.4, 5.6.7.8")
    )
    assert honest == spoofed == "8.8.8.8"


def test_cf_connecting_ip_is_authoritative_when_via_cloudflare():
    """When the edge is a real Cloudflare IP, the resolved key is whatever
    Cloudflare attests in CF-Connecting-IP — never a left-hand XFF value."""
    req = _through_cloudflare(
        "8.8.8.8", forged_xff_prefix="1.2.3.4", cf_connecting="8.8.8.8"
    )
    assert get_client_ip(req) == "8.8.8.8"


def test_direct_origin_hit_uses_real_peer_not_forged_cf_header():
    """Attacker hits *.onrender.com directly, forging both CF-Connecting-IP and a
    Cloudflare-looking XFF entry. Render's edge appends their REAL peer IP to the
    right; that (non-Cloudflare) IP is what we bucket — the forgeries are ignored."""
    attacker_ip = "45.79.1.1"
    req = _Req(
        headers={
            # forged: a spoofed client + a Cloudflare-range IP to try to look "via CF"
            "X-Forwarded-For": f"1.1.1.1, {CF_EDGE}, {attacker_ip}, {RENDER_INTERNAL}",
            "CF-Connecting-IP": "1.1.1.1",
        },
        host=RENDER_INTERNAL,
    )
    assert get_client_ip(req) == attacker_ip


def test_no_xff_falls_back_to_peer():
    assert get_client_ip(_Req(headers={}, host="8.8.4.4")) == "8.8.4.4"


def test_all_internal_chain_falls_back_to_peer():
    req = _Req(headers={"X-Forwarded-For": "10.0.0.1, 172.16.0.9"}, host="10.0.0.1")
    assert get_client_ip(req) == "10.0.0.1"


def test_ipv4_with_port_is_normalized():
    req = _Req(headers={"X-Forwarded-For": f"8.8.8.8:54321, {RENDER_INTERNAL}"},
               host=RENDER_INTERNAL)
    assert get_client_ip(req) == "8.8.8.8"


def test_unknown_when_nothing_available():
    assert get_client_ip(_Req(headers={}, host=None)) == "unknown"


def test_budget_and_ratelimit_share_one_derivation():
    """The data-budget middleware helper and the rate-limiter key_func must both
    delegate to the same function — one source of truth."""
    from app.api_budget_middleware import _client_ip
    from app.client_ip import client_ip_key

    req = _through_cloudflare("8.8.8.8")
    assert _client_ip(req) == client_ip_key(req) == get_client_ip(req) == "8.8.8.8"
