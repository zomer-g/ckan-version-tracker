"""Per-IP rolling data budget for the public API.

Guards against someone siphoning an unreasonable VOLUME of data (the kind of
bulk scrape that would run up real bandwidth/compute cost) — distinct from
slowapi's per-minute REQUEST rate limit. We meter the bytes actually streamed
back to each client IP over a rolling window; once an IP crosses the budget it
is blocked (HTTP 429) and told to make contact to arrange higher access.

In-memory + per-process (no Redis dependency): fine for OVER's single Render
instance. State resets on restart/deploy — acceptable for abuse mitigation, and
the budget is sized so a determined scraper is blocked long before the cost is
material, while normal browsing/research never comes close. A deploy/restart is
the ONLY thing that resets a tally: the per-IP key comes from the spoof-resistant
derivation in app/client_ip.py (Cloudflare-validated), so a client can no longer
zero its own tally by rotating X-Forwarded-For. If OVER ever scales past one
instance, move this counter to a shared store (Postgres/Redis) keyed by the same
IP — until then per-process is both correct (one process) and sufficient.

Sizing: Render bandwidth overage is ~$0.3/GB, so "tens of shekels" of cost is
~25-80 GB of egress. The per-IP budget defaults to 2 GB / 24h — generous for a
legitimate full-dataset pull or two, but it blocks a single actor LONG before
they could rack up that cost. Tune via env API_DAILY_BYTE_BUDGET (bytes).
"""
from __future__ import annotations

import threading
import time
from collections import deque

from app.config import settings

# Don't let the tracking table itself become a memory-abuse vector: cap the
# number of distinct IPs we keep, evicting the least-recently-active.
_MAX_IPS = 50_000


class _RollingByteBudget:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # ip -> (last_active_monotonic, deque[(ts, bytes)])
        self._ips: dict[str, tuple[float, deque]] = {}

    @staticmethod
    def _window() -> float:
        return float(getattr(settings, "api_budget_window_seconds", 86400) or 86400)

    @staticmethod
    def _limit() -> int:
        return int(getattr(settings, "api_daily_byte_budget", 2 * 1024 ** 3) or 0)

    def _prune_locked(self, dq: deque, now: float) -> int:
        cutoff = now - self._window()
        used = 0
        while dq and dq[0][0] < cutoff:
            dq.popleft()
        for _, n in dq:
            used += n
        return used

    def _evict_if_needed_locked(self) -> None:
        if len(self._ips) <= _MAX_IPS:
            return
        # Drop the ~10% least-recently-active IPs.
        victims = sorted(self._ips.items(), key=lambda kv: kv[1][0])[: _MAX_IPS // 10]
        for ip, _ in victims:
            self._ips.pop(ip, None)

    def used(self, ip: str) -> int:
        """Bytes served to this IP within the current rolling window."""
        now = time.monotonic()
        with self._lock:
            entry = self._ips.get(ip)
            if not entry:
                return 0
            return self._prune_locked(entry[1], now)

    def is_over(self, ip: str) -> bool:
        """True if this IP has already exhausted its budget (block further calls)."""
        if not getattr(settings, "api_budget_enabled", True):
            return False
        limit = self._limit()
        if limit <= 0:
            return False
        return self.used(ip) >= limit

    def record(self, ip: str, nbytes: int) -> None:
        """Add bytes served to this IP's rolling tally."""
        if nbytes <= 0 or not getattr(settings, "api_budget_enabled", True):
            return
        now = time.monotonic()
        with self._lock:
            entry = self._ips.get(ip)
            if entry is None:
                dq: deque = deque()
                self._ips[ip] = (now, dq)
            else:
                dq = entry[1]
                self._ips[ip] = (now, dq)
                self._prune_locked(dq, now)
            dq.append((now, int(nbytes)))
            self._evict_if_needed_locked()


budget = _RollingByteBudget()
