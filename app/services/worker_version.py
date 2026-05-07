"""Resolve the git SHA a worker must be running, with a TTL cache.

The /api/worker/poll endpoint refuses to dispatch tasks to a worker whose
reported git SHA doesn't match what's on the upstream repo's tracked
branch. This module owns the "what SHA does the worker need?" question.

Resolution order:
  1. settings.worker_required_version — explicit pin (no network call).
  2. GitHub API for the configured repo+branch — cached for TTL_SECONDS.
  3. Unknown — caller decides whether to fail open (allow) or closed (deny).

Cache is process-local. With one Render dyno this is fine; if we ever run
multiple workers polling the same dyno, ~60 polls/min/worker would still
hit GitHub at most every TTL_SECONDS.
"""
import logging
import time

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TTL_SECONDS = 300  # 5 minutes — bound on the lag between push and enforcement

_cache: dict[str, tuple[float, str | None]] = {}


def _key() -> str:
    return f"{settings.worker_repo}@{settings.worker_branch}"


async def get_required_worker_sha() -> str | None:
    """Return the SHA the worker must report, or None if undetermined.

    None means the caller should fail open — we don't want a transient
    GitHub outage to block all scraping. Pinning via
    WORKER_REQUIRED_VERSION sidesteps the network entirely.
    """
    if settings.worker_required_version:
        return settings.worker_required_version.strip()

    if not settings.worker_repo or not settings.worker_branch:
        return None

    key = _key()
    now = time.time()
    cached = _cache.get(key)
    if cached and (now - cached[0]) < TTL_SECONDS:
        return cached[1]

    url = f"https://api.github.com/repos/{settings.worker_repo}/commits/{settings.worker_branch}"
    sha: str | None = None
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                url,
                headers={"Accept": "application/vnd.github+json"},
            )
        if resp.status_code == 200:
            data = resp.json()
            sha = (data.get("sha") or "").strip() or None
        else:
            logger.warning(
                "GitHub API for %s returned %s; failing open on worker version check",
                url, resp.status_code,
            )
    except Exception as e:
        logger.warning("Failed to fetch required worker SHA from %s: %s", url, e)

    # Cache even None so a flaky GitHub doesn't blast us with retries every poll.
    # On the next TTL boundary we try again.
    _cache[key] = (now, sha)
    return sha
