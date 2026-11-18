"""How many free-SQL queries one client may have RUNNING at once.

The per-minute limit (``@limiter.limit``) counts requests, not load: a client
can send its whole minute's allowance in one second, and each statement may
run up to the statement timeout, so a burst of N requests is N heavy queries
on the database together. This gate caps that per client IP. A request over
the cap waits in a short queue for a slot rather than being refused outright,
so a browser that fires a few queries together is slowed, not failed; only a
client that keeps the queue full, or waits too long, gets a 429.

State is in process memory: OVER runs as a single uvicorn process (launch.sh,
no --workers), the same assumption slowapi's in-memory storage already makes.
Keyed by ``client_ip_key``, the same real-IP derivation the rate limiter uses.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from fastapi import HTTPException, Request

from app.client_ip import client_ip_key

# The per-minute allowance of each free-SQL endpoint (was 20/minute). Safe to
# raise because the running-query cap below, not the count, bounds the load.
SQL_RATE = "120/minute"
MAX_RUNNING_PER_IP = 4
MAX_WAITING_PER_IP = 16
WAIT_SECONDS = 20.0
RETRY_AFTER_SECONDS = 5


@dataclass
class _Slot:
    sem: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(MAX_RUNNING_PER_IP))
    users: int = 0  # running + waiting; the entry is dropped when it reaches 0


_slots: dict[str, _Slot] = {}


def _busy(detail: str) -> HTTPException:
    return HTTPException(status_code=429, detail=detail,
                         headers={"Retry-After": str(RETRY_AFTER_SECONDS)})


async def sql_slot(request: Request):
    """FastAPI dependency: hold one of the client's running-query slots for
    the duration of the endpoint."""
    ip = client_ip_key(request)
    slot = _slots.get(ip)
    if slot is None:
        slot = _slots[ip] = _Slot()
    if slot.users >= MAX_RUNNING_PER_IP + MAX_WAITING_PER_IP:
        raise _busy(f"יותר מדי שאילתות SQL ממתינות מכתובת ה-IP שלכם (עד {MAX_RUNNING_PER_IP} "
                    f"רצות במקביל). המתינו שהקודמות יסתיימו ונסו שוב.")
    slot.users += 1
    try:
        try:
            await asyncio.wait_for(slot.sem.acquire(), WAIT_SECONDS)
        except TimeoutError:
            raise _busy(f"כבר רצות {MAX_RUNNING_PER_IP} שאילתות SQL מכתובת ה-IP שלכם, "
                        f"ולא התפנה מקום תוך {int(WAIT_SECONDS)} שניות. נסו שוב בעוד כמה שניות.") from None
        try:
            yield
        finally:
            slot.sem.release()
    finally:
        slot.users -= 1
        if slot.users == 0 and _slots.get(ip) is slot:
            del _slots[ip]
