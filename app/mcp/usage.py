"""Fire-and-forget MCP tool-call usage logging.

Each tool call writes one ``mcp_usage_events`` row on its own short-lived
session so logging never blocks or poisons the request. Errors are swallowed —
usage logging is observability, never load-bearing.
"""
from __future__ import annotations

import contextvars
import logging
import uuid

from app.database import async_session
from app.models.mcp import McpUsageEvent

logger = logging.getLogger(__name__)

# Which MCP server the current request is being served by. Set once per request
# by app.mcp.auth.authenticate (every server's route calls it before any tool
# runs), so the ten servers' own log_usage calls need not name themselves.
current_server: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "mcp_server", default=None)
# The in-process deep search dispatches tools without an MCP request and logs
# under this fixed session id (app/services/deep_search.py:_USAGE_SESSION).
_DEEP_SEARCH_SESSION = "deep-search"


def server_from_path(path: str) -> str:
    """'/deals/mcp/...' -> 'deals'; the main '/mcp' resource is 'over'."""
    head = path.split("/mcp", 1)[0].strip("/")
    return (head.rsplit("/", 1)[-1] or "over")[:40]


async def log_usage(
    *,
    api_user_id: uuid.UUID,
    client_id: uuid.UUID | None,
    session_id: str | None,
    tool_name: str,
    request_params: dict | None,
    result_count: int | None,
    result_bytes: int | None,
    latency_ms: int,
    status: str,
    error_message: str | None,
) -> None:
    try:
        async with async_session() as s:
            s.add(McpUsageEvent(
                api_user_id=api_user_id,
                client_id=client_id,
                mcp_session_id=(session_id or None),
                tool_name=tool_name[:200],
                mcp_server=current_server.get() or (
                    "deep_search" if session_id == _DEEP_SEARCH_SESSION else None),
                request_params=request_params,
                result_count=result_count,
                result_bytes=result_bytes,
                latency_ms=latency_ms,
                status=status[:20],
                error_message=(error_message or None) and error_message[:1000],
            ))
            await s.commit()
    except Exception as e:  # noqa: BLE001
        logger.warning("MCP usage log failed for %s: %s", tool_name, e)
