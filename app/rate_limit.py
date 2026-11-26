import re

from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded

from app.client_ip import client_ip_key
from app.config import settings

USER_KEY_PREFIX = "u:"


def rate_limit_key(request: Request) -> str:
    """Who a per-minute limit counts against.

    A caller with a valid site token is counted per user (``u:<id>``), so a
    signed-in collector is not lumped in with everyone behind its address and
    gets the signed-in tier (see TieredLimiter). Everyone else is counted by the
    REAL client IP (app/client_ip.py), never slowapi's get_remote_address, which
    behind a proxy collapses every limit into one global bucket.
    """
    auth = request.headers.get("authorization") or ""
    if auth[:7].lower() == "bearer ":
        token = auth[7:].strip()
        if token:
            from app.auth.security import decode_access_token
            uid = decode_access_token(token)
            if uid:
                return f"{USER_KEY_PREFIX}{uid}"
    return client_ip_key(request)


_AMOUNT = re.compile(r"^\s*(\d+)")


def scale_limit(limit: str, factor: int) -> str:
    """``"60/minute"`` × 10 → ``"600/minute"``; every ``;``-separated part."""
    if factor <= 1:
        return limit
    return ";".join(_AMOUNT.sub(lambda m: str(int(m.group(1)) * factor), part)
                    for part in limit.split(";"))


class TieredLimiter(Limiter):
    """slowapi's Limiter with a signed-in tier on every string limit.

    ``@limiter.limit("60/minute")`` (with the default key) becomes a limit whose
    amount depends on who is asking: 60/minute for an address, the configured
    multiple for a signed-in user. slowapi only resolves amounts per key for a
    *callable* limit, so the string is wrapped in one; the declared string is
    kept in ``declared`` for introspection (tests, docs).

    A limit given its own ``key_func`` (SQL's per-user key, the worker progress
    key) is left exactly as declared.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.declared: dict[str, list[str]] = {}         # route's own limits
        self.declared_shared: dict[str, list[str]] = {}  # family-wide (shared_limit)

    def _tiered(self, limit_value):
        def provider(key: str) -> str:
            if key.startswith(USER_KEY_PREFIX):
                return scale_limit(limit_value, int(settings.signed_in_rate_multiplier or 1))
            return limit_value
        return provider

    @staticmethod
    def _record(into: dict, limit_value, decorator):
        def wrap(func):
            if isinstance(limit_value, str):
                into.setdefault(f"{func.__module__}.{func.__name__}", []).append(limit_value)
            return decorator(func)
        return wrap

    def limit(self, limit_value, key_func=None, *args, **kwargs):
        value = self._tiered(limit_value) if isinstance(limit_value, str) and key_func is None \
            else limit_value
        return self._record(self.declared, limit_value, super().limit(value, key_func, *args, **kwargs))

    def shared_limit(self, limit_value, scope, key_func=None, *args, **kwargs):
        value = self._tiered(limit_value) if isinstance(limit_value, str) and key_func is None \
            else limit_value
        return self._record(self.declared_shared, limit_value, super().shared_limit(value, scope, key_func, *args, **kwargs))

    def declared_amounts(self, name: str) -> list[int]:
        """The anonymous per-period amounts of the route's own limits (not the
        family-wide shared ones) on ``name`` (``module.function``); empty when
        the route has no limit."""
        return [int(_AMOUNT.match(v).group(1)) for v in self.declared.get(name, [])]


# key_style="endpoint": one bucket per ROUTE (the view function), not per URL.
# slowapi's default ("url") keys every bucket by the literal path, so
# /api/nadlan/parcel/1/2 and /api/nadlan/parcel/1/3 each got a fresh 60/minute
# and a sweep over ids was never limited at all — on 25.11 one address made
# 126,746 calls to one such route in nine hours without a single 429.
#
# headers_enabled stays OFF: slowapi injects its X-RateLimit-* headers from
# inside the endpoint wrapper and hard-fails unless every limited endpoint
# declares a `response: Response` parameter — which none of ours do. The 429
# handler below sets Retry-After itself, which is the header that matters.
limiter = TieredLimiter(key_func=rate_limit_key, key_style="endpoint")


# Sent with every 429, so a collector that hits a limit learns the sanctioned
# ways to get data at scale instead of rotating addresses.
BULK_HINT = {
    "he": "לאיסוף בהיקף גדול: התחברו לאתר ושלחו Authorization: Bearer <token> — "
          "המגבלות לחשבון מחובר גבוהות פי כמה; או משכו טבלה שלמה בבת אחת "
          "(‎/api/append/{id}/download.csv, קובצי Parquet, או SQL ב-/data) "
          "במקום פריט-פריט. פירוט: https://over.org.il/api#limits",
    "en": "For bulk collection: sign in and send Authorization: Bearer <token> "
          "(signed-in limits are several times higher), or pull a whole table at "
          "once (/api/append/{id}/download.csv, Parquet files, or SQL at /data) "
          "instead of item by item. See https://over.org.il/api#limits",
}


def _retry_after_seconds(request: Request) -> int | None:
    """Seconds until the hit window reopens, or None if the storage can't say."""
    try:
        limit = request.state.view_rate_limit
        reset_at, _remaining = limiter.limiter.get_window_stats(limit[0], *limit[1])
        import time

        return max(1, int(reset_at - time.time()))
    except Exception:
        return None


def _humanize(seconds: int | None) -> str:
    if not seconds:
        return "מעט"
    if seconds < 60:
        return f"{seconds} שניות"
    minutes = (seconds + 59) // 60
    if minutes < 60:
        return f"{minutes} דקות"
    hours = (minutes + 59) // 60
    return f"{hours} שעות"


def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    """Answer a 429 in the shape the frontend actually reads.

    slowapi's default handler returns ``{"error": "Rate limit exceeded: …"}``,
    but api/client.ts only looks at ``detail`` — so every rate limit surfaced in
    the UI as a bare "שגיאת שרת (429)" with no hint that waiting fixes it. Same
    status, same headers, but a Hebrew ``detail`` that says how long to wait.
    """
    retry_after = _retry_after_seconds(request)
    who = "מהחשבון שלכם" if rate_limit_key(request).startswith(USER_KEY_PREFIX) \
        else "מכתובת ה-IP שלכם"
    detail = (
        f"יותר מדי בקשות {who} — הגעתם למגבלת הקצב של הפעולה הזו "
        f"({exc.detail}). נסו שוב בעוד {_humanize(retry_after)}."
    )
    response = JSONResponse(
        {"detail": detail, "error": f"Rate limit exceeded: {exc.detail}",
         "retry_after": retry_after, "bulk": BULK_HINT},
        status_code=429,
    )
    if retry_after:
        response.headers["Retry-After"] = str(retry_after)
    return response
