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


async def reserve_llm_call(db: AsyncSession, *, call_budget: int | None = None,
                           token_budget_override: int | None = None) -> bool:
    """Atomically reserve one LLM call against today's global budget.

    Returns ``True`` if the call is within budget (today's tally has been
    incremented and the caller may proceed to the LLM), ``False`` if the daily
    cap is already exhausted (caller must reject with 429 and NOT call the LLM).

    Two independent ceilings, because a call count alone does not bound spend
    once prompts vary in size (see migration 050): today's ``calls`` must be
    under ``cbs_ask_daily_budget``, AND today's ``output_tokens`` must be under
    ``llm_daily_output_token_budget``. Output tokens are the gate rather than
    total tokens because output is billed ~5x input on every current model and
    is the part a crafted question can inflate — long reasoning costs many times
    a short answer while counting as one call either way.

    Disabled — always allows — when ``llm_budget_enabled`` is False or the
    configured budget is <= 0. A token budget of <= 0 disables only that half.
    """
    if not getattr(settings, "llm_budget_enabled", True):
        return True
    # An admin override from nl_query_config wins over the deployed default, so
    # a budget can be tightened during an incident without a redeploy. None
    # means "no override"; 0 is a real value and disables that ceiling.
    budget = int((call_budget if call_budget is not None
                  else getattr(settings, "cbs_ask_daily_budget", 0)) or 0)
    if budget <= 0:
        return True
    token_budget = int((token_budget_override if token_budget_override is not None
                        else getattr(settings, "llm_daily_output_token_budget", 0)) or 0)
    # A non-positive token budget means "no token ceiling"; express that as a
    # condition that is always true rather than branching the SQL.
    token_clause = ("AND llm_daily_usage.output_tokens < :token_budget"
                    if token_budget > 0 else "")

    # One statement does everything atomically:
    #   * first call of the day     → INSERT (day, 1), RETURNING 1
    #   * still under budget         → DO UPDATE calls = calls + 1, RETURNING new
    #   * already at/over the budget → the WHERE makes DO UPDATE affect 0 rows,
    #                                  RETURNING yields nothing → row is None
    # The row lock the UPSERT takes serialises concurrent reservations, so the
    # ceiling can never be overshot; committing right away releases it before
    # the (slow) LLM call, so requests don't queue behind each other.
    params: dict = {"budget": budget}
    if token_budget > 0:
        params["token_budget"] = token_budget
    try:
        row = (
            await db.execute(
                text(
                    "INSERT INTO llm_daily_usage (day, calls) "
                    "VALUES (CURRENT_DATE, 1) "
                    "ON CONFLICT (day) DO UPDATE "
                    "SET calls = llm_daily_usage.calls + 1, updated_at = now() "
                    "WHERE llm_daily_usage.calls < :budget "
                    f"{token_clause} "
                    "RETURNING calls"
                ),
                params,
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


async def record_llm_tokens(db: AsyncSession, input_tokens: int, output_tokens: int) -> None:
    """Add one call's token usage to today's row, after the call returned.

    Necessarily after the fact — the cost is not known until the response
    arrives. That means the output-token ceiling is enforced with a one-call
    lag: the request that crosses the line is paid for, and the next one is
    refused. With a per-call output cap of ~1.5k tokens the overshoot is
    bounded and small, which is the right trade against pre-charging an
    estimate and refusing requests that would have been affordable.

    Best-effort by design: usage bookkeeping must never fail a successful
    answer, so an error here is logged and swallowed. The consequence of a lost
    write is undercounting, so pair this with the call ceiling — which is
    reserved up-front and cannot be lost — rather than relying on it alone.
    """
    if not getattr(settings, "llm_budget_enabled", True):
        return
    if input_tokens <= 0 and output_tokens <= 0:
        return
    try:
        await db.execute(
            text(
                "INSERT INTO llm_daily_usage (day, calls, input_tokens, output_tokens) "
                "VALUES (CURRENT_DATE, 0, :i, :o) "
                "ON CONFLICT (day) DO UPDATE SET "
                "input_tokens = llm_daily_usage.input_tokens + :i, "
                "output_tokens = llm_daily_usage.output_tokens + :o, "
                "updated_at = now()"
            ),
            {"i": max(0, int(input_tokens)), "o": max(0, int(output_tokens))},
        )
        await db.commit()
    except Exception:  # noqa: BLE001
        logger.exception("record_llm_tokens: usage write failed")
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass


async def usage_today(db: AsyncSession) -> dict:
    """Today's tallies + the configured ceilings — for the admin card and for
    telling a rate-limited user how long they have to wait."""
    try:
        row = (await db.execute(text(
            "SELECT calls, input_tokens, output_tokens FROM llm_daily_usage "
            "WHERE day = CURRENT_DATE"))).first()
    except Exception:  # noqa: BLE001
        row = None
    return {
        "calls": int(row[0]) if row else 0,
        "input_tokens": int(row[1]) if row else 0,
        "output_tokens": int(row[2]) if row else 0,
        "call_budget": int(getattr(settings, "cbs_ask_daily_budget", 0) or 0),
        "output_token_budget": int(getattr(settings, "llm_daily_output_token_budget", 0) or 0),
        "enabled": bool(getattr(settings, "llm_budget_enabled", True)),
    }
