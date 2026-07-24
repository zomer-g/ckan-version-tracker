"""Middleware enforcing the per-IP public-API data budget (see api_budget.py).

Meters the bytes streamed back for the bulk public data API (``/api/v1`` and
``/api/append``). When an IP has already blown its rolling budget the request is
rejected with HTTP 429 and a clear "contact us to arrange access" message,
before any more data goes out. Otherwise the response body is wrapped to count
the bytes actually sent and add them to the IP's tally.
"""
from __future__ import annotations

import logging
import secrets

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from app.client_ip import get_client_ip
from app.config import settings
from app.services.api_budget import budget

logger = logging.getLogger(__name__)

# Only the bulk DATA endpoints are metered — metadata listings, auth, admin,
# worker and the gov.il proxy are not (they're either trivial or trusted).
_METERED_PREFIXES = ("/api/v1", "/api/append", "/api/knesset-db", "/api/tables",
                     "/api/connector")


def _client_ip(request: Request) -> str:
    """Real client IP behind the Cloudflare→Render proxy chain.

    Delegates to the single source of truth in app/client_ip.py — the SAME
    derivation the rate limiter uses — so a forged X-Forwarded-For can neither
    escape the rate-limit bucket nor reset this IP's data-budget tally."""
    return get_client_ip(request)


def _budget_bucket(request: Request, path: str) -> tuple[str, int | None]:
    """(bucket_key, limit_override) for this request.

    A valid ``X-Connector-Key`` on the Looker-connector API routes the request
    to one shared "connector" bucket with its own cap — all Looker Studio
    traffic comes from the same few Google IPs, so per-IP metering would both
    starve legitimate dashboards and let the connector eat bystanders' quota.
    Anything else (including a wrong/absent key, which the router 401s) stays
    on the spoof-resistant per-IP bucket."""
    if path.startswith("/api/connector"):
        key = (getattr(settings, "connector_api_key", "") or "").strip()
        supplied = request.headers.get("x-connector-key", "").strip()
        if key and secrets.compare_digest(supplied, key):
            return "connector", int(getattr(settings, "connector_daily_byte_budget", 0) or 0)
    return _client_ip(request), None


def _blocked_response(limit_bytes: int | None = None) -> JSONResponse:
    email = getattr(settings, "api_contact_email", "guy@z-g.co.il")
    if limit_bytes is None:
        limit_bytes = int(getattr(settings, "api_daily_byte_budget", 0) or 0)
    limit_gb = round(limit_bytes / (1024 ** 3), 1)
    return JSONResponse(
        status_code=429,
        headers={"Retry-After": "3600"},
        content={
            "error": "data_budget_exceeded",
            "message": (
                "חרגת ממכסת הנתונים היומית של ה-API הציבורי. הגישה החופשית "
                "מיועדת לחוקרים, עיתונאים ופרויקטים אזרחיים בהיקף סביר. "
                "לשאיבת נתונים בהיקף גדול יותר — נא ליצור קשר להסדרת הרשאות "
                f"בכתובת {email}, ונשמח לפתוח גישה רחבה יותר."
            ),
            "message_en": (
                "You have exceeded the daily data quota of the public API. Free "
                "access is intended for research, journalism and civic projects "
                "at a reasonable scale. For larger data pulls please contact "
                f"{email} to arrange access — we're happy to grant a higher quota."
            ),
            "contact": email,
            "daily_quota_gb": limit_gb,
        },
    )


class ApiBudgetMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if not path.startswith(_METERED_PREFIXES):
            return await call_next(request)

        bucket, limit = _budget_bucket(request, path)
        if budget.is_over(bucket, limit=limit):
            logger.info("API budget block for %s on %s", bucket, path)
            return _blocked_response(limit)

        response: Response = await call_next(request)

        # Wrap the body stream to count exactly what we send this IP, then add
        # it to the rolling tally once the response has fully streamed.
        orig_iter = response.body_iterator

        async def _counting():
            total = 0
            try:
                async for chunk in orig_iter:
                    total += len(chunk)
                    yield chunk
            finally:
                budget.record(bucket, total)

        response.body_iterator = _counting()
        return response
