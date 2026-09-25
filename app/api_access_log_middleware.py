"""Record every request to /api/* and the MCP servers in the API access log.

Pure ASGI, not BaseHTTPMiddleware, so a streamed response (a CSV export, an MCP
stream) is timed and measured to its last byte without being buffered. The row
is handed to app/services/api_access_log.py, which writes in batches off the
request path. See that module for what is and is not stored.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from starlette.datastructures import Headers
from starlette.requests import Request

from app.client_ip import came_through_cloudflare, get_client_ip
from app.services import api_access_log as log


class ApiAccessLogMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not log.enabled():
            return await self.app(scope, receive, send)
        method = scope.get("method", "GET")
        path = scope.get("path", "")
        if not log.should_log(method, path):
            return await self.app(scope, receive, send)

        started = time.perf_counter()
        ts = datetime.now(timezone.utc)
        box = {"status": 500, "bytes": 0, "done": False}

        async def _send(message):
            t = message["type"]
            if t == "http.response.start":
                box["status"] = message.get("status", 500)
            elif t == "http.response.body":
                box["bytes"] += len(message.get("body", b""))
                if not message.get("more_body", False):
                    box["done"] = True
            await send(message)

        try:
            await self.app(scope, receive, _send)
        finally:
            try:
                self._record(scope, ts, started, box)
            except Exception:  # noqa: BLE001 — logging must never fail a request
                pass

    def _record(self, scope, ts, started, box) -> None:
        path = scope.get("path", "")
        # A client that hung up mid-stream is recorded as 499, like nginx.
        status = box["status"] if box["done"] or box["status"] >= 400 else 499
        if log.skip_success(path, status):
            return
        headers = Headers(scope=scope)
        request = Request(scope)
        ip = get_client_ip(request)
        area = log.area_of(path)
        ua = headers.get("user-agent") or ""
        try:
            client = log.client_family(ua)
            channel = log.channel_of(area, headers, client)
        except Exception:  # noqa: BLE001
            client, channel = None, "other"
        try:
            kind, actor_id, label = log.resolve_actor(scope.get("state") or {}, headers, path, ip)
        except Exception:  # noqa: BLE001 — a malformed header must not drop the row
            kind, actor_id, label = "unknown", None, None
        route_path = getattr(scope.get("route"), "path", None)
        referer_host = log._host(headers.get("referer")) or log._host(headers.get("origin"))
        # CF-IPCountry is only true when Cloudflare set it.
        country = headers.get("cf-ipcountry") if came_through_cloudflare(request) else None
        query = (scope.get("query_string") or b"").decode("latin-1")
        log.enqueue({
            "ts": ts,
            "method": scope.get("method", "GET")[:8],
            # An unmatched path (a scan) is kept short and without its query.
            "path": path[:500] if route_path else path[:120],
            "route": route_path[:300] if route_path else None,
            "area": area,
            "query": log.scrub_query(query) if route_path else None,
            "target": log.target_of(scope.get("path_params")),
            "status": status,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "bytes_out": box["bytes"],
            "ip": ip[:64] if ip and ip != "unknown" else None,
            "country": country[:8] if country else None,
            "user_agent": ua[:400] or None,
            "client": client,
            "channel": channel,
            "actor_kind": kind,
            "actor_id": actor_id,
            "actor_label": label,
            "referer_host": referer_host,
        })
