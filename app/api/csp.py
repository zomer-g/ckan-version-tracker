"""Where browsers send Content-Security-Policy reports (app/security_headers.py).

While the policy is Report-Only, these reports are how we learn what an
enforced policy would break. Each is logged as one short line: the directive,
the blocked source's origin and the page path. No query strings, no IPs.
"""
import json
import logging
from urllib.parse import urlsplit

from fastapi import APIRouter, Request, Response

from app.rate_limit import limiter

logger = logging.getLogger("app.csp")

router = APIRouter(prefix="/api", tags=["csp"], include_in_schema=False)

MAX_BODY = 16 * 1024


def _origin(url: str) -> str:
    if not url or "://" not in url:
        return url[:40]  # "inline", "eval", "data", ...
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}"[:120]


@router.post("/csp-report")
@limiter.limit("30/minute")
async def csp_report(request: Request) -> Response:
    body = (await request.body())[:MAX_BODY]
    try:
        data = json.loads(body or b"{}")
    except ValueError:
        return Response(status_code=204)
    reports = data if isinstance(data, list) else [data]
    for item in reports[:10]:
        if not isinstance(item, dict):
            continue
        r = item.get("csp-report") or item.get("body") or item
        if not isinstance(r, dict):
            continue
        directive = r.get("effective-directive") or r.get("effectiveDirective") \
            or r.get("violated-directive") or ""
        blocked = r.get("blocked-uri") or r.get("blockedURL") or ""
        page = r.get("document-uri") or r.get("documentURL") or ""
        logger.info("csp violation: %s blocked=%s page=%s", str(directive)[:60],
                    _origin(str(blocked)), urlsplit(str(page)).path[:200])
    return Response(status_code=204)
