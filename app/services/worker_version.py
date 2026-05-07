"""Resolve what a worker must be running, with TTL caches.

The /api/worker/poll endpoint refuses to dispatch tasks unless the worker
matches upstream on TWO axes:

  1. Git SHA — what the worker reports via X-Worker-Version. Cheap to
     check (string compare), but spoofable: the worker computes it via
     `git rev-parse HEAD` at startup, and a WORKER_VERSION env override
     bypasses the file system entirely.

  2. Engine file content — SHA-256 of the worker's loaded
     legacy_engine.py, sent via X-Worker-Engine-Hash. This is what
     actually decides scrape behaviour, so checking the bytes themselves
     defeats SHA spoofing AND catches the real-world failure mode of
     "operator pulled but didn't restart" (HEAD moved, in-memory module
     didn't).

Resolution order for both:
  1. explicit pin in settings (worker_required_version /
     worker_required_engine_hash) — no network call.
  2. GitHub API/raw URL for the configured repo+branch — cached TTL_SECONDS.
  3. Unknown — caller decides fail-open vs fail-closed.

Cache is process-local. With one Render dyno this is fine; if we ever
scale, ~60 polls/min/worker still hits GitHub at most every TTL_SECONDS.

Cache-staleness handling: the poll endpoint can pass refresh=True on a
mismatch to force one re-fetch (rate-limited to MIN_REFRESH_INTERVAL).
That turns a 60-second post-push false-positive window into a single
self-healing poll rather than waiting out the TTL.
"""
import hashlib
import logging
import time

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

TTL_SECONDS = 60                # serve from cache for this long
MIN_REFRESH_INTERVAL = 20       # rate-limit forced refreshes (DOS protection)

# The single file whose hash we use as the worker's "actual code" identity.
# Picked because every scrape path runs through this module — if its bytes
# differ from upstream, the worker's behaviour will differ too. over_worker.py
# matters too but is small and rarely the source of bugs we'd reject for.
ENGINE_FILE_PATH = "govscraper/scrapers/govil/legacy_engine.py"

_cache: dict[str, tuple[float, str | None]] = {}
_last_fetch: dict[str, float] = {}


def _key() -> str:
    return f"{settings.worker_repo}@{settings.worker_branch}"


def _engine_key() -> str:
    return f"{settings.worker_repo}@{settings.worker_branch}::engine"


async def _fetch_from_github(key: str) -> str | None:
    """Single GitHub call; updates the cache. Returns the new SHA or None."""
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

    now = time.time()
    # Cache even None so a flaky GitHub doesn't blast us with retries every poll.
    _cache[key] = (now, sha)
    _last_fetch[key] = now
    return sha


async def get_required_worker_sha(*, refresh: bool = False) -> str | None:
    """Return the SHA the worker must report, or None if undetermined.

    None means the caller should fail open — we don't want a transient
    GitHub outage to block all scraping. Pinning via
    WORKER_REQUIRED_VERSION sidesteps the network entirely.

    refresh=True forces a GitHub re-fetch even within the TTL, but is
    still rate-limited globally to once per MIN_REFRESH_INTERVAL seconds
    so a malicious or buggy worker can't flood our outbound calls.
    """
    if settings.worker_required_version:
        return settings.worker_required_version.strip()

    if not settings.worker_repo or not settings.worker_branch:
        return None

    key = _key()
    now = time.time()
    cached = _cache.get(key)

    if refresh:
        last = _last_fetch.get(key, 0.0)
        if (now - last) >= MIN_REFRESH_INTERVAL:
            return await _fetch_from_github(key)
        # Hit the rate limit — fall through to whatever we have cached.

    if cached and (now - cached[0]) < TTL_SECONDS:
        return cached[1]

    return await _fetch_from_github(key)


def _normalize_line_endings(data: bytes) -> bytes:
    """Collapse CRLF and lone CR to LF so hashes match across OSes.

    GitHub raw serves the repo bytes (LF since the file was committed
    on a unix-friendly autocrlf=input checkout); a Windows worker reads
    the file from disk with CRLF after autocrlf=true converted on
    checkout. The bytes differ but the logical content is identical —
    normalize to LF on both sides so the hash agrees."""
    return data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")


async def _fetch_engine_hash_from_github(key: str) -> str | None:
    """Download the upstream legacy_engine.py and SHA-256 it. Returns the
    hex digest or None on any error."""
    url = (
        f"https://raw.githubusercontent.com/{settings.worker_repo}/"
        f"{settings.worker_branch}/{ENGINE_FILE_PATH}"
    )
    digest: str | None = None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
        if resp.status_code == 200 and resp.content:
            digest = hashlib.sha256(_normalize_line_endings(resp.content)).hexdigest()
        else:
            logger.warning(
                "GitHub raw for %s returned %s (len=%d); failing open on engine-hash check",
                url, resp.status_code, len(resp.content or b""),
            )
    except Exception as e:
        logger.warning("Failed to fetch engine file from %s: %s", url, e)

    now = time.time()
    _cache[key] = (now, digest)
    _last_fetch[key] = now
    return digest


async def get_required_engine_hash(*, refresh: bool = False) -> str | None:
    """Return the SHA-256 the worker's loaded legacy_engine.py must match.

    Same null-on-failure semantics as get_required_worker_sha — None
    means "we couldn't determine, fail open". An explicit pin via
    settings.worker_required_engine_hash sidesteps the network entirely.
    """
    if settings.worker_required_engine_hash:
        return settings.worker_required_engine_hash.strip()

    if not settings.worker_repo or not settings.worker_branch:
        return None

    key = _engine_key()
    now = time.time()
    cached = _cache.get(key)

    if refresh:
        last = _last_fetch.get(key, 0.0)
        if (now - last) >= MIN_REFRESH_INTERVAL:
            return await _fetch_engine_hash_from_github(key)

    if cached and (now - cached[0]) < TTL_SECONDS:
        return cached[1]

    return await _fetch_engine_hash_from_github(key)
