"""Unit tests for the public-API per-IP data budget (anti-abuse cost guard)."""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import json

from app.config import settings
from app.services.api_budget import _RollingByteBudget


def _fresh(limit=1000, window=86400):
    settings.api_budget_enabled = True
    settings.api_daily_byte_budget = limit
    settings.api_budget_window_seconds = window
    return _RollingByteBudget()


def test_under_budget_not_blocked():
    b = _fresh(limit=1000)
    b.record("1.1.1.1", 400)
    assert b.used("1.1.1.1") == 400
    assert b.is_over("1.1.1.1") is False


def test_over_budget_blocks():
    b = _fresh(limit=1000)
    b.record("2.2.2.2", 600)
    b.record("2.2.2.2", 600)  # 1200 >= 1000
    assert b.used("2.2.2.2") == 1200
    assert b.is_over("2.2.2.2") is True


def test_budget_is_per_ip():
    b = _fresh(limit=1000)
    b.record("3.3.3.3", 5000)
    assert b.is_over("3.3.3.3") is True
    # a different client is unaffected
    assert b.is_over("4.4.4.4") is False
    assert b.used("4.4.4.4") == 0


def test_disabled_never_blocks():
    b = _fresh(limit=1)
    settings.api_budget_enabled = False
    b.record("5.5.5.5", 10_000)
    assert b.is_over("5.5.5.5") is False
    settings.api_budget_enabled = True


def test_zero_limit_disables_enforcement():
    b = _fresh(limit=0)
    b.record("6.6.6.6", 10_000)
    assert b.is_over("6.6.6.6") is False


def test_rolling_window_prunes_old_bytes():
    import app.services.api_budget as mod
    b = _fresh(limit=1000, window=100)
    # Force-insert an entry "in the past" beyond the window, then a fresh one.
    now = mod.time.monotonic()
    from collections import deque
    dq = deque()
    dq.append((now - 200, 5000))  # older than the 100s window → should prune
    b._ips["7.7.7.7"] = (now, dq)
    b.record("7.7.7.7", 300)      # fresh
    # Only the fresh 300 remains within the window.
    assert b.used("7.7.7.7") == 300
    assert b.is_over("7.7.7.7") is False


def test_blocked_response_has_contact_message():
    from app.api_budget_middleware import _blocked_response
    settings.api_contact_email = "guy@z-g.co.il"
    resp = _blocked_response()
    assert resp.status_code == 429
    assert resp.headers.get("Retry-After")
    body = json.loads(bytes(resp.body))
    assert body["error"] == "data_budget_exceeded"
    assert body["contact"] == "guy@z-g.co.il"
    assert "guy@z-g.co.il" in body["message"]      # Hebrew message names the contact
    assert "guy@z-g.co.il" in body["message_en"]   # English too


def test_client_ip_uses_rightmost_public_hop_not_spoofable_left():
    # The budget helper delegates to the central spoof-resistant derivation
    # (app/client_ip.py). It takes the RIGHTMOST public hop our infra appended,
    # skipping Render's private internal IPs — NOT the client-controllable left
    # entry. Full trust-model coverage lives in tests/test_client_ip.py.
    from app.api_budget_middleware import _client_ip

    class _Req:
        def __init__(self, xff=None, host="9.9.9.9"):
            self.headers = {"x-forwarded-for": xff} if xff else {}
            self.client = type("C", (), {"host": host})()

    # forged left entry (1.1.1.1) is ignored; the real appended public hop wins.
    # (8.8.8.8 is a global IP; RFC5737 doc ranges read as private, so aren't used.)
    assert _client_ip(_Req(xff="1.1.1.1, 8.8.8.8, 10.0.0.1")) == "8.8.8.8"
    assert _client_ip(_Req(xff=None, host="9.9.9.9")) == "9.9.9.9"


def test_metered_prefixes_only():
    from app.api_budget_middleware import _METERED_PREFIXES
    assert "/api/v1/datasets".startswith(_METERED_PREFIXES)
    assert "/api/append/x/schema".startswith(_METERED_PREFIXES)
    # Admin / worker / metadata listings are NOT metered.
    assert not "/api/admin/pending".startswith(_METERED_PREFIXES)
    assert not "/api/datasets".startswith(_METERED_PREFIXES)
    assert not "/api/worker/poll".startswith(_METERED_PREFIXES)
