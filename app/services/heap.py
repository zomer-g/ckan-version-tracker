"""Hand free heap pages back to the operating system.

Why this exists
---------------
This service was being OOM-killed on a 512Mi dyno roughly nineteen times a
day, with a profile that reads exactly like a leak: RSS climbs from ~185MB
at boot into the high 400s over a couple of hours and never comes back
down. Two separate explanations were argued from the source and both were
wrong, because the premise was wrong — nothing is being retained.

Measured on the running process (see /api/admin/memory in app/api/admin.py),
over a window in which RSS grew 24MB:

    RSS                       +24 MB
    tracemalloc traced bytes  +3.3 MB
    live GC-tracked objects   415,902 -> 410,703   (declining)

Python is freeing the memory. glibc is not returning it. A threaded process
— SQLAlchemy's pool, botocore, uvicorn's workers — gets its own malloc arena
per thread, up to 8x the core count, and each arena keeps the pages it once
grew into. Every burst of transient allocation (the dataset-sizes fan-out,
a NEON push, JSON decoding a large response) permanently raises the
high-water mark of whichever arena served it. RSS is therefore the sum of
every arena's worst moment, which only ever ratchets upward.

So there is no accumulator to find and delete. There are two real levers,
and this module is the second one:

1. Allocate less at peak. That is a per-caller job — halving the
   dataset-sizes read was the first step.
2. Ask glibc to give the free pages back. That is ``malloc_trim``, and
   nothing in Python's standard library exposes it.

``MALLOC_ARENA_MAX`` (set in render.yaml) is the matching half: it caps how
many arenas can exist at all, so freed memory lands back in a pool this can
actually trim rather than being stranded in a thread-local arena.
"""
import ctypes
import ctypes.util
import logging
import platform

logger = logging.getLogger(__name__)

_libc: ctypes.CDLL | None = None
_resolved = False
_unavailable_reason: str | None = None


def _libc_handle() -> ctypes.CDLL | None:
    """Resolve libc once, and remember failure so we don't retry per call.

    Returns None anywhere that isn't glibc — macOS and Windows dev boxes,
    and musl-based images, none of which expose ``malloc_trim``. Callers
    treat that as "nothing to do", not as an error.
    """
    global _libc, _resolved, _unavailable_reason
    if _resolved:
        return _libc
    _resolved = True

    if platform.system() != "Linux":
        _unavailable_reason = f"not Linux ({platform.system()})"
        return None
    try:
        candidate = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        if not hasattr(candidate, "malloc_trim"):
            _unavailable_reason = "libc has no malloc_trim (musl?)"
            return None
        candidate.malloc_trim.argtypes = [ctypes.c_size_t]
        candidate.malloc_trim.restype = ctypes.c_int
        _libc = candidate
    except Exception as e:  # pragma: no cover - platform dependent
        _unavailable_reason = f"{type(e).__name__}: {e}"
        _libc = None
    return _libc


def trim() -> bool | None:
    """Release free heap pages back to the OS.

    Returns True if glibc actually returned memory, False if it had nothing
    to give back, and None where malloc_trim doesn't exist. Never raises —
    a failure to trim is a missed optimisation, not a reason to take down
    the job that called it.
    """
    libc = _libc_handle()
    if libc is None:
        return None
    try:
        return bool(libc.malloc_trim(0))
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("malloc_trim failed: %s", e)
        return None


def unavailable_reason() -> str | None:
    """Why trim() is a no-op here, for the admin memory endpoint to report."""
    _libc_handle()
    return _unavailable_reason


def rss_kb() -> int | None:
    """Resident set size in KB from /proc, or None off Linux."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def trim_and_log(context: str) -> None:
    """Trim, and log what it bought — so the effect stays visible in the
    logs after the admin endpoint has served its purpose and gone quiet.

    Logged at INFO only when it actually recovers something worth naming;
    a trim that frees nothing is the boring, healthy case and would
    otherwise print every few minutes forever.
    """
    before = rss_kb()
    freed = trim()
    if freed is None:
        return
    after = rss_kb()
    if before is not None and after is not None and before - after >= 4096:
        logger.info(
            "heap trim after %s: RSS %.1fMB -> %.1fMB (returned %.1fMB to the OS)",
            context, before / 1024, after / 1024, (before - after) / 1024,
        )
