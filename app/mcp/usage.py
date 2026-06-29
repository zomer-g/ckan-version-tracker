"""Fire-and-forget MCP tool-call usage logging.

Each tool call writes one ``mcp_usage_events`` row on its own short-lived
session so logging never blocks or poisons the request. Errors are swallowed —
usage logging is observability, never load-bearing.
"""
from __future__ import annotations

import logging
import uuid

from app.database import async_session
from app.models.mcp import McpUsageEvent

logger = logging.getLogger(__name__)


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
