"""Global daily hard cap on paid-LLM calls made by the public CBS endpoints.

``/api/cbs/ask`` and ``/api/cbs/resolve`` invoke a paid LLM on every request.
slowapi's per-IP ``20/minute`` limit throttles a single client, but an attacker
rotating IPs (trivial from a botnet / proxy pool) could still drive unbounded
spend — and rotating IPs also defeats the per-IP byte budget. This is a
different guard: ONE global counter, persisted per calendar day in Postgres
(``llm_daily_usage``) and keyed ONLY by the day, never by IP. So it caps total
LLM spend across ALL callers at once and cannot be reset by X-Forwarded-For
rotation or by a process restart/deploy. Because it doesn't look at the client
IP at all, it is unaffected by whether ``--proxy-headers`` is enabled.

The reservation is a single atomic conditional UPSERT (increment + ceiling
check in one statement), committed immediately, so it is race-free under
concurrency and never holds a row lock across the slow LLM call. It runs BEFORE
the LLM call, so a request that is over budget is rejected without spending a
cent. Authenticated MCP callers do not go through here — they are trusted and
separately accountable via api_users.
"""
from __future__ import annotations

import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

logger = logging.getLogger(__name__)


async def reserve_llm_call(db: AsyncSession) -> bool:
    """Atomically reserve one LLM call against today's global budget.

    Returns ``True`` if the call is within budget (today's tally has been
    incremented and the caller may proceed to the LLM), ``False`` if the daily
    cap is already exhausted (caller must reject with 429 and NOT call the LLM).

    Disabled — always allows — when ``llm_budget_enabled`` is False or the
    configured budget is <= 0.
    """
    if not getattr(settings, "llm_budget_enabled", True):
        return True
    budget = int(getattr(settings, "cbs_ask_daily_budget", 0) or 0)
    if budget <= 0:
        return True

    # One statement does everything atomically:
    #   * first call of the day     → INSERT (day, 1), RETURNING 1
    #   * still under budget         → DO UPDATE calls = calls + 1, RETURNING new
    #   * already at/over the budget → the WHERE makes DO UPDATE affect 0 rows,
    #                                  RETURNING yields nothing → row is None
    # The row lock the UPSERT takes serialises concurrent reservations, so the
    # ceiling can never be overshot; committing right away releases it before
    # the (slow) LLM call, so requests don't queue behind each other.
    try:
        row = (
            await db.execute(
                text(
                    "INSERT INTO llm_daily_usage (day, calls) "
                    "VALUES (CURRENT_DATE, 1) "
                    "ON CONFLICT (day) DO UPDATE "
                    "SET calls = llm_daily_usage.calls + 1, updated_at = now() "
                    "WHERE llm_daily_usage.calls < :budget "
                    "RETURNING calls"
                ),
                {"budget": budget},
            )
        ).first()
        await db.commit()
    except Exception:  # noqa: BLE001
        # Fail OPEN: a budget-bookkeeping error must not take the feature down.
        # The per-IP request limiter still bounds abuse in this rare case.
        logger.exception("reserve_llm_call: budget check failed; allowing the call")
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
        return True

    return row is not None
