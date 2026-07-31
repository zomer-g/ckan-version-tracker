from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded

from app.client_ip import client_ip_key

# Key the per-endpoint rate limits by the REAL client IP (see app/client_ip.py),
# NOT slowapi's default get_remote_address. Behind Cloudflare→Render the direct
# peer is a shared edge address, so get_remote_address would collapse every
# @limiter.limit into a single global bucket; client_ip_key resolves the actual
# per-client IP (Cloudflare-validated) instead.
#
# headers_enabled stays OFF: slowapi injects its X-RateLimit-* headers from
# inside the endpoint wrapper and hard-fails unless every limited endpoint
# declares a `response: Response` parameter — which none of ours do. The 429
# handler below sets Retry-After itself, which is the header that matters.
limiter = Limiter(key_func=client_ip_key)


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
    detail = (
        "יותר מדי בקשות מכתובת ה-IP שלכם — הגעתם למגבלת הקצב של הפעולה הזו "
        f"({exc.detail}). נסו שוב בעוד {_humanize(retry_after)}."
    )
    response = JSONResponse(
        {"detail": detail, "error": f"Rate limit exceeded: {exc.detail}"},
        status_code=429,
    )
    if retry_after:
        response.headers["Retry-After"] = str(retry_after)
    return response
