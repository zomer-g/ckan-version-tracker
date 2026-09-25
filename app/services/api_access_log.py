"""API access log: classify a request, buffer the row, write rows in batches.

The middleware (app/api_access_log_middleware.py) calls :func:`enqueue` once a
response has finished. Nothing here touches the database on the request path:
rows go into an in-memory buffer that a background task writes every few
seconds in one INSERT. If the database is down the batch is dropped with a
warning, because this is observability and must never slow or fail a request.

Who, how and what are read from the request itself:
  * who  — ``request.state.api_actor`` when a handler stamped it (the MCP
           authenticate() does), else a cheap look at the bearer / connector key.
  * how  — the channel (our own site, MCP, a script, a browser elsewhere, a bot)
           and the client family, from Sec-Fetch-Site and the User-Agent.
  * what — the route template and the id the route was called with.

Secrets are never stored: not the Authorization header, not a key, and the
query string loses every parameter whose name looks like a credential.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit

from app.config import settings

logger = logging.getLogger(__name__)

FLUSH_SECONDS = 5.0
MAX_BUFFER = 10_000       # rows held if the database is unreachable; oldest dropped first
MAX_BATCH = 2_000

# ── which requests are logged ────────────────────────────────────────────────

# Internal traffic that would drown everything else: the worker fleet polls
# constantly, and the admin panel is the site owner. Only their SUCCESSFUL
# calls are skipped: a 401/403/404 on them is a probe worth seeing (the
# middleware decides once the status is known, via skip_success()).
_QUIET_PREFIXES = ("/api/worker/", "/api/admin/", "/api/health")
_QUIET_EXACT = ("/api/worker", "/api/admin")

# The MCP servers, by their route prefixes (app/mcp/*_routes.py). An exact list,
# so an arbitrary "/whatever/mcp" path is not logged as MCP traffic.
MCP_PREFIXES = ("/mcp", "/cbs/mcp", "/knesset/mcp", "/ocal/mcp", "/ocoi/mcp", "/odata/mcp",
                "/elections/mcp", "/prices/mcp", "/data/mcp", "/nadlan/mcp", "/deals/mcp")


def should_log(method: str, path: str) -> bool:
    if method == "OPTIONS":
        return False
    return path.startswith("/api/") or is_mcp_path(path)


def skip_success(path: str, status: int) -> bool:
    """Worker/admin calls are recorded only when they failed."""
    return status < 400 and (path.startswith(_QUIET_PREFIXES) or path in _QUIET_EXACT)


def is_mcp_path(path: str) -> bool:
    return any(path == p or path.startswith(p + "/") for p in MCP_PREFIXES)


def area_of(path: str) -> str:
    """'/api/v1/datasets/x' -> 'v1'; '/deals/mcp' -> 'mcp:deals'; '/mcp' -> 'mcp:over'."""
    if is_mcp_path(path) and not path.startswith("/api/"):
        head = path.split("/mcp", 1)[0].strip("/")
        return f"mcp:{(head.rsplit('/', 1)[-1] or 'over')}"[:60]
    parts = path.split("/")
    return (parts[2] if len(parts) > 2 and parts[2] else "api")[:60]


# ── how: channel + client family ─────────────────────────────────────────────

_BOT_RE = re.compile(r"bot\b|crawler|spider|slurp|facebookexternalhit|embedly|preview", re.I)

# Ordered: the first match names the client. AI agents first, since their UAs
# often also carry "Mozilla" or a library name.
_CLIENTS: tuple[tuple[str, str], ...] = (
    (r"Claude-User|Claude-SearchBot|ClaudeBot|anthropic", "Claude"),
    (r"ChatGPT|OAI-SearchBot|GPTBot|openai", "ChatGPT/OpenAI"),
    (r"PerplexityBot|Perplexity", "Perplexity"),
    (r"Google-Apps-Script", "Google Apps Script"),
    (r"Googlebot|Google-InspectionTool|GoogleOther", "Googlebot"),
    (r"bingbot", "Bingbot"),
    (r"^curl/", "curl"),
    (r"^Wget", "wget"),
    (r"python-requests", "python-requests"),
    (r"python-httpx", "python-httpx"),
    (r"aiohttp", "aiohttp"),
    (r"Python-urllib", "python-urllib"),
    (r"^python|pandas", "python"),
    (r"axios", "axios"),
    (r"node-fetch|undici|^node|Node\.js", "node"),
    (r"Go-http-client", "go"),
    (r"okhttp|^Java", "java"),
    (r"PostmanRuntime", "postman"),
    (r"insomnia", "insomnia"),
    (r"libcurl|httr|^R ", "R"),
    (r"WindowsPowerShell|PowerShell", "powershell"),
    (r"Excel|Microsoft Office|PowerBI|Power Query", "Excel/PowerBI"),
    (r"QGIS", "QGIS"),
    (r"Edg/", "Edge"),
    (r"OPR/|Opera", "Opera"),
    (r"Firefox/", "Firefox"),
    (r"Chrome/|CriOS/", "Chrome"),
    (r"Safari/", "Safari"),
)
_CLIENTS_RE = tuple((re.compile(p, re.I), name) for p, name in _CLIENTS)
_SCRIPT_CLIENTS = frozenset({
    "curl", "wget", "python-requests", "python-httpx", "aiohttp", "python-urllib", "python",
    "axios", "node", "go", "java", "postman", "insomnia", "R", "powershell",
    "Excel/PowerBI", "QGIS",
})
_BOT_CLIENTS = frozenset({"Googlebot", "Bingbot", "Perplexity"})


def client_family(ua: str) -> str | None:
    ua = (ua or "").strip()
    if not ua:
        return None
    for rx, name in _CLIENTS_RE:
        if rx.search(ua):
            return name
    return ua.split("/", 1)[0].split(" ", 1)[0][:60] or None


def _host(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return (urlsplit(url).hostname or None) and urlsplit(url).hostname[:200]
    except ValueError:
        return None


def _is_own_host(host: str | None) -> bool:
    if not host:
        return False
    return host == "over.org.il" or host.endswith(".over.org.il") or host in ("localhost", "127.0.0.1")


def channel_of(area: str, headers, client: str | None) -> str:
    """site | mcp | apps_script | bot | script | browser | other."""
    if area.startswith("mcp:"):
        return "mcp"
    fetch_site = (headers.get("sec-fetch-site") or "").lower()
    origin_host = _host(headers.get("origin")) or _host(headers.get("referer"))
    if fetch_site == "same-origin" or (fetch_site in ("", "same-site") and _is_own_host(origin_host)):
        return "site"
    ua = headers.get("user-agent") or ""
    if client == "Google Apps Script":
        return "apps_script"
    if client in ("Claude", "ChatGPT/OpenAI"):
        return "bot" if _BOT_RE.search(ua) else "script"
    if client in _BOT_CLIENTS or _BOT_RE.search(ua):
        return "bot"
    if client in _SCRIPT_CLIENTS or not ua:
        return "script"
    if ua.startswith("Mozilla"):
        return "browser"
    return "other"


# ── who ──────────────────────────────────────────────────────────────────────

def stamp_actor(request, kind: str, actor_id: str | None = None, label: str | None = None) -> None:
    """Called by a handler that knows who is asking (e.g. MCP authenticate())."""
    try:
        request.state.api_actor = (kind, actor_id, label)
    except Exception:  # noqa: BLE001 — never let attribution break a request
        pass


def resolve_actor(state: dict, headers, path: str, ip: str) -> tuple[str, str | None, str | None]:
    """(kind, id, label). Never returns or stores a token."""
    stamped = (state or {}).get("api_actor")
    if stamped:
        kind, aid, label = stamped
        return kind, (str(aid)[:100] if aid else None), (label[:200] if label else None)

    auth = headers.get("authorization") or ""
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        svc = settings.sql_service_token or ""
        if svc and token and hmac.compare_digest(token.encode(), svc.encode()):
            return "sql_service", None, "sql-service"
        from app.auth.security import decode_access_token
        uid = decode_access_token(token) if token else None
        if uid:
            return "user", str(uid)[:100], None
        return "bearer_invalid", None, None

    if path.startswith("/api/connector"):
        key = (getattr(settings, "connector_api_key", "") or "").strip()
        supplied = (headers.get("x-connector-key") or "").strip()
        if key and supplied and hmac.compare_digest(supplied.encode(), key.encode()):
            return "connector", None, "connector-key"
        try:
            from app.services import google_ips
            if key and google_ips.is_google_ip(ip):
                return "connector", None, "google-ip"
        except Exception:  # noqa: BLE001
            pass
    return "anonymous", None, None


# ── what ─────────────────────────────────────────────────────────────────────

# Whole names only: "keyword" and "settlement_code" are search terms worth
# keeping; "api_key", "sso_token" and an OAuth "code"/"state" are not.
_SECRET_PARAM = re.compile(
    r"^(?:.*[_-])?(?:token|secret|password|passwd|pwd|pass|api_?key|apikey|key|sig|signature|jwt"
    r"|auth|bearer|session|sid|otp|nonce|credential|credentials|auth_code|login_code)$"
    r"|^(?:code|state|code_verifier|authorization)$|^x-amz-", re.I)
_TARGET_KEYS = ("dataset_id", "dataset", "table", "table_name", "name", "ckan_id", "id", "slug")


def scrub_query(qs: str) -> str | None:
    if not qs:
        return None
    try:
        pairs = parse_qsl(qs, keep_blank_values=True)
    except ValueError:
        return None
    clean = [(k, "***" if _SECRET_PARAM.search(k) else v) for k, v in pairs]
    return urlencode(clean)[:500] or None


def target_of(path_params: dict | None) -> str | None:
    if not path_params:
        return None
    for k in _TARGET_KEYS:
        if path_params.get(k):
            return str(path_params[k])[:200]
    k, v = next(iter(path_params.items()))
    return f"{k}={v}"[:200] if v else None


# ── buffer + writer ──────────────────────────────────────────────────────────

_buffer: deque = deque(maxlen=MAX_BUFFER)
_flusher: asyncio.Task | None = None
_last_warn = 0.0


def enabled() -> bool:
    return bool(settings.api_access_log_enabled) and not settings.maintenance_mode


# Per-IP cap on rows per minute, so an unauthenticated flood (a 404 scan, a
# retry loop) cannot fill the table. Past the cap the IP's rows for that minute
# are counted and written as ONE summary row when the minute rolls over.
PER_IP_PER_MINUTE = 300
_ip_minute: dict[str, list] = {}   # ip -> [minute, count, template_row]


def _admit(row: dict) -> bool:
    ip = row.get("ip") or "?"
    minute = int(time.time() // 60)
    slot = _ip_minute.get(ip)
    if slot is None or slot[0] != minute:
        if slot is not None and slot[1] > PER_IP_PER_MINUTE:
            _buffer.append(_suppressed_row(slot))
        if len(_ip_minute) > 20_000:
            _ip_minute.clear()
        _ip_minute[ip] = [minute, 1, row]
        return True
    slot[1] += 1
    return slot[1] <= PER_IP_PER_MINUTE


def _suppressed_row(slot: list) -> dict:
    tpl = slot[2]
    return {**tpl, "ts": datetime.fromtimestamp(slot[0] * 60 + 59, timezone.utc),
            "method": "-", "path": "(suppressed)", "route": None, "query": None, "target": None,
            "status": 0, "duration_ms": 0, "bytes_out": 0,
            "actor_label": f"{slot[1] - PER_IP_PER_MINUTE} requests over the per-minute cap"}


def enqueue(row: dict) -> None:
    global _flusher
    if not enabled():
        return
    if not _admit(row):
        return
    _buffer.append(row)
    if _flusher is None or _flusher.done():
        try:
            _flusher = asyncio.get_running_loop().create_task(_flush_loop())
        except RuntimeError:
            pass  # no loop (sync context); the next request starts it


async def _flush_loop() -> None:
    while True:
        await asyncio.sleep(FLUSH_SECONDS)
        await flush()


async def flush() -> int:
    """Write everything buffered. Returns rows written."""
    global _last_warn
    written = 0
    while _buffer:
        batch = []
        while _buffer and len(batch) < MAX_BATCH:
            batch.append(_buffer.popleft())
        try:
            from sqlalchemy import insert

            from app.database import async_session
            from app.models.api_access_log import ApiAccessLog
            async with async_session() as s:
                await s.execute(insert(ApiAccessLog), batch)
                await s.commit()
            written += len(batch)
        except Exception as e:  # noqa: BLE001
            now = time.monotonic()
            if now - _last_warn > 60:
                _last_warn = now
                # The exception's text is NOT logged: SQLAlchemy puts the bound
                # parameters (IPs, user agents, emails) into it, and platform
                # logs are not where those may go.
                logger.warning("API access log: dropped %d rows (%s)", len(batch), type(e).__name__)
            break
    return written


async def purge_old() -> int:
    """Delete rows past the retention. Run daily by the scheduler."""
    days = int(settings.api_access_log_retention_days or 0)
    if days <= 0:
        return 0
    from sqlalchemy import delete

    from app.database import async_session
    from app.models.api_access_log import ApiAccessLog
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    from sqlalchemy import update
    query_cutoff = datetime.now(timezone.utc) - timedelta(
        days=min(days, int(settings.api_access_log_query_retention_days or days)))
    async with async_session() as s:
        res = await s.execute(delete(ApiAccessLog).where(ApiAccessLog.ts < cutoff))
        # What people searched for, next to their IP, is kept for less time
        # than the rest of the row.
        await s.execute(update(ApiAccessLog)
                        .where(ApiAccessLog.ts < query_cutoff, ApiAccessLog.query.is_not(None))
                        .values(query=None))
        await s.commit()
    n = res.rowcount or 0
    if n:
        logger.info("API access log: purged %d rows older than %d days", n, days)
    return n
