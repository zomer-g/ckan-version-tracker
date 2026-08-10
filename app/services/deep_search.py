"""Fan-out engine for "שאלות לעם" — the cross-source deep search.

Takes one free-text query, runs it against each requested source from
``deep_search_sources.SOURCES``, and returns one normalized column per source.

Two transports:

* **local** — the source is an MCP server of THIS process (``/mcp``,
  ``/data/mcp``, ``/cbs/mcp``, ``/knesset/mcp``, ``/ocal/mcp``). All five modules
  expose the same ``_IMPL[tool](request, db, user, args) -> (payload, count)``
  contract, so we call the implementation directly as the fixed service
  principal. No HTTP, no loopback through Cloudflare, no service token, no cold
  start — and we still write an ``mcp_usage_events`` row per call (tagged with
  the ``deep-search`` session id) so this traffic shows up in the admin usage
  view alongside real MCP clients.
* **remote** — a third-party MCP endpoint over stateless JSON-RPC, generalized
  from ``app/services/tagit_mcp.py``. Nothing uses it yet; it exists so adding
  TAG-IT / OCOI / BudgetKey is a registry entry plus one env var.

The contract the page depends on: **run_source never raises.** A dead source
becomes a column carrying an ``error`` string, so one broken corpus can never
blank the whole page.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time

import httpx

from app.config import settings
from app.services.deep_search_sources import Card, Source

logger = logging.getLogger(__name__)

# The five in-process MCP servers, by the registry's ``local`` key.
LOCAL_MODULES: dict[str, str] = {
    "over": "app.mcp.server",
    "sql": "app.mcp.sql_server",
    "cbs": "app.mcp.cbs_server",
    "knesset": "app.mcp.knesset_server",
    "ocal": "app.mcp.ocal_server",
}

# Marks this traffic in mcp_usage_events.mcp_session_id so the admin usage view
# can separate gateway calls from real MCP clients.
_USAGE_SESSION = "deep-search"

# SECURITY: the only tools this gateway may ever invoke in-process.
#
# In-process dispatch bypasses app/mcp/auth.py, so nothing but this list stands
# between an anonymous visitor and a tool. `/data/mcp` exposes `run_sql` and
# `/knesset/mcp` exposes its own `run_sql`; if a tool name could ever be
# influenced by the request, this page would become an unauthenticated SQL
# console. Tool names come only from the registry — this list is the belt to
# that braces, and tests/test_deep_search.py asserts run_sql is not in it.
ALLOWED_TOOLS = frozenset({
    "search_datasets", "query_dataset_rows", "list_tables",
    "search", "search_protocols", "search_mmm", "search_events",
})

# Remote transport: retry a cold/booting upstream, but on a far shorter leash
# than tagit_mcp's 100s — here seven columns are in flight and the user is
# watching, so a sleeping third party must fail fast rather than hold the page.
_RETRY_STATUS = {502, 503, 504}
_RETRY_RPC_CODES = {-32001, -32603}


class SourceError(RuntimeError):
    """The source was reachable but could not answer."""


# ── token resolution ────────────────────────────────────────────────────────

def resolve_token(source: Source) -> str | None:
    """The service token for a remote source, or None.

    Checked in the environment first and then on ``settings`` (so a source can
    reuse an already-declared setting such as ``TAGIT_MCP_TOKEN``).
    """
    if source.local or source.public or not source.token_env:
        return None
    val = os.environ.get(source.token_env) or getattr(settings, source.token_env.lower(), "")
    return (str(val).strip() or None)


def is_configured(source: Source) -> bool:
    """Can this source be queried at all? Local and public sources always can;
    a remote one needs its token present."""
    if source.local or source.public:
        return True
    return resolve_token(source) is not None


# ── local transport ─────────────────────────────────────────────────────────

def _local_caller(request, source: Source):
    """An async ``call(tool, args) -> payload`` bound to one in-process server.

    Each call opens its OWN AsyncSession rather than reusing the request's. The
    page sends one source per request, but the endpoint accepts ``sources=a,b``
    and gathers them — and a single AsyncSession is not safe for concurrent use,
    so sharing one here would corrupt state exactly when several columns are
    asked for at once.
    """
    import importlib

    from app.database import async_session
    from app.mcp.auth import _service_user
    from app.mcp.usage import log_usage

    mod_path = LOCAL_MODULES.get(source.local or "")
    if not mod_path:
        raise SourceError(f"מקור לא ידוע: {source.local}")
    mod = importlib.import_module(mod_path)
    user = _service_user()

    async def call(tool: str, args: dict) -> dict:
        # The tool name always comes from the registry, never from the request.
        # This path bypasses app/mcp/auth.py entirely, and /data/mcp exposes
        # run_sql — a user-controllable tool name would turn a public page into
        # an unauthenticated SQL console. See ALLOWED_TOOLS.
        if tool not in ALLOWED_TOOLS:
            raise SourceError(f"הכלי {tool} אינו מורשה בחיפוש הרוחבי")
        impl = getattr(mod, "_IMPL", {}).get(tool)
        if impl is None:
            raise SourceError(f"הכלי {tool} אינו קיים בשרת {source.local}")
        started = time.time()
        try:
            async with async_session() as db:
                payload, count = await impl(request, db, user, args or {})
        except Exception as e:  # noqa: BLE001
            await log_usage(
                api_user_id=user.id, client_id=None, session_id=_USAGE_SESSION,
                tool_name=tool, request_params=args, result_count=None,
                result_bytes=None, latency_ms=int((time.time() - started) * 1000),
                status="error", error_message=str(e)[:1000])
            raise
        await log_usage(
            api_user_id=user.id, client_id=None, session_id=_USAGE_SESSION,
            tool_name=tool, request_params=args, result_count=count,
            result_bytes=None, latency_ms=int((time.time() - started) * 1000),
            status="ok", error_message=None)
        return payload if isinstance(payload, dict) else {}

    return call


# ── remote transport ────────────────────────────────────────────────────────

def _extract_json(resp: httpx.Response) -> dict:
    """Streamable-HTTP answers as application/json OR as an SSE frame; both
    carry a single JSON-RPC object."""
    if "text/event-stream" in resp.headers.get("content-type", ""):
        data = "".join(line[5:].strip() for line in resp.text.splitlines()
                       if line.startswith("data:"))
        return json.loads(data) if data else {}
    return resp.json()


def _tool_payload(result: dict) -> dict:
    """A tools/call result wraps the tool's real return as JSON text inside
    ``content[0].text`` (per the MCP spec). Unwrap and parse it."""
    if isinstance(result.get("structuredContent"), dict):
        return result["structuredContent"]
    for part in result.get("content") or []:
        if part.get("type") == "text" and part.get("text"):
            try:
                return json.loads(part["text"])
            except (ValueError, TypeError):
                return {"text": part["text"]}
    return {}


def _remote_caller(source: Source, token: str | None, budget_s: float):
    """An async ``call(tool, args) -> payload`` bound to one remote MCP endpoint."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    async def call(tool: str, args: dict) -> dict:
        payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                   "params": {"name": tool, "arguments": args or {}}}
        start = time.monotonic()
        delay = 1.0
        last: Exception = SourceError("המקור אינו זמין")
        while True:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(budget_s, connect=10.0), follow_redirects=True
                ) as client:
                    resp = await client.post(source.mcp_url, headers=headers, json=payload)
            except (httpx.TransportError, httpx.TimeoutException) as e:
                last = SourceError(f"המקור אינו זמין: {type(e).__name__}")
            else:
                if resp.status_code == 401:
                    raise SourceError("המקור דחה את טוקן השירות (401)")
                if resp.status_code in _RETRY_STATUS:
                    last = SourceError(f"המקור החזיר {resp.status_code} (מתעורר…)")
                elif resp.status_code >= 400:
                    raise SourceError(f"המקור החזיר {resp.status_code}")
                else:
                    body = _extract_json(resp)
                    if "error" in body:
                        err = body.get("error") or {}
                        msg = str(err.get("message") or "").lower()
                        if err.get("code") in _RETRY_RPC_CODES or "timeout" in msg or "timed out" in msg:
                            last = SourceError("המקור לא הספיק לענות בזמן")
                        else:
                            raise SourceError(str(err.get("message") or err))
                    else:
                        return _tool_payload(body.get("result") or {})
            if time.monotonic() - start + delay >= budget_s:
                break
            await asyncio.sleep(delay)
            delay = min(delay * 1.6, 5.0)
        raise last

    return call


# ── the fan-out ─────────────────────────────────────────────────────────────

def _column(source: Source, *, configured: bool = True, error: str | None = None,
            results: list | None = None, total: int | None = None) -> dict:
    cards = results or []
    return {
        "id": source.id, "name": source.name, "color": source.color,
        "attribution": dict(source.attribution), "server": source.server,
        "configured": configured, "error": error,
        "total": total if total is not None else len(cards),
        "results": [c.as_dict() if isinstance(c, Card) else c for c in cards],
    }


async def _query(request, source: Source, q: str, limit: int, filters: dict) -> dict:
    """Run one source and return its column. Raises — wrapped by run_source."""
    if source.local:
        call = _local_caller(request, source)
    else:
        call = _remote_caller(source, resolve_token(source),
                              float(settings.deep_search_source_timeout))

    if source.run is not None:
        out = await source.run(call, q, limit, filters)
        cards = [c for c in (out.get("results") or []) if c]
        return _column(source, results=cards, total=out.get("total"))

    payload = await call(source.tool, source.build_args(q, limit, filters))
    raw = payload.get(source.results_path)
    rows = raw if isinstance(raw, list) else []
    cards: list[Card] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        try:
            card = source.normalize(item)
        except Exception:  # noqa: BLE001
            # One malformed row must not empty a column that is otherwise fine.
            logger.debug("deep_search: normalize failed for %s", source.id, exc_info=True)
            continue
        if card and card.title:
            cards.append(card)
    total = payload.get("total")
    return _column(source, results=cards,
                   total=int(total) if isinstance(total, int) else len(cards))


async def run_source(request, source: Source, q: str, limit: int,
                     filters: dict | None = None) -> dict:
    """One source's column. NEVER raises — every failure becomes ``error``."""
    if not is_configured(source):
        return _column(source, configured=False,
                       error=None if source.local else "המקור אינו מוגדר בשרת")
    try:
        return await asyncio.wait_for(
            _query(request, source, q, limit, filters or {}),
            timeout=float(settings.deep_search_source_timeout),
        )
    except asyncio.TimeoutError:
        return _column(source, error="המקור לא הספיק לענות בזמן")
    except SourceError as e:
        return _column(source, error=str(e))
    except ValueError as e:
        # The MCP tools raise ValueError with a deliberate, user-facing Hebrew
        # message ("מראה נתוני הכנסת אינו מוגדר בשרת זה"). Showing that beats
        # showing the exception class name.
        logger.info("deep_search: source %s rejected the query: %s", source.id, e)
        return _column(source, error=str(e)[:300] or "המקור דחה את השאילתה")
    except Exception as e:  # noqa: BLE001
        logger.warning("deep_search: source %s failed", source.id, exc_info=True)
        return _column(source, error=f"שגיאה בשליפה מהמקור ({type(e).__name__})")


async def fan_out(request, sources: list[Source], q: str, limit: int,
                  filters: dict | None = None) -> list[dict]:
    """Every requested source, concurrently, in registry order.

    The page normally asks for ONE source per request (so each column paints as
    soon as it lands), but the endpoint accepts any subset — hence the
    semaphore, which keeps a "search everything in one call" from opening seven
    append-DB queries at once.
    """
    sem = asyncio.Semaphore(4)

    async def one(s: Source) -> dict:
        async with sem:
            return await run_source(request, s, q, limit, filters)

    settled = await asyncio.gather(*(one(s) for s in sources), return_exceptions=True)
    out: list[dict] = []
    for s, r in zip(sources, settled):
        if isinstance(r, BaseException):
            # run_source is not supposed to raise; if it ever does, the column
            # still has to render.
            logger.error("deep_search: run_source raised for %s", s.id, exc_info=r)
            out.append(_column(s, error=f"שגיאה בלתי צפויה ({type(r).__name__})"))
        else:
            out.append(r)
    return out
