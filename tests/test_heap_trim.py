"""Tests for app/services/heap.py — the malloc_trim wrapper.

Context: the 512Mi dyno was OOM-killed ~19 times a day by a profile that
looked like a leak and measurably wasn't one. Over a window where RSS grew
24MB, tracemalloc saw only 3.3MB more live Python bytes and the GC-tracked
object count went down. The pages were free; glibc just wasn't returning
them. This module reaches malloc_trim through ctypes, which nothing in the
standard library exposes.

The important property under test is not that trimming works — that is
glibc's job and only observable on glibc — but that this never takes down a
caller. It runs inside a scheduled job and immediately after the
dataset-sizes refresh; a platform without malloc_trim (every dev machine
here is Windows, and CI may be macOS or musl) must quietly do nothing
rather than raise.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.services import heap  # noqa: E402


def _reset():
    """Clear the memoised libc handle so a test can re-resolve it."""
    heap._libc = None
    heap._resolved = False
    heap._unavailable_reason = None


def test_trim_never_raises_on_any_platform():
    """The whole contract. Returns None where malloc_trim doesn't exist,
    a bool where it does, and raises nowhere."""
    _reset()
    result = heap.trim()
    assert result is None or isinstance(result, bool)


def test_trim_is_a_no_op_off_glibc_and_says_why():
    """On a non-Linux dev box the reason must be populated, so the admin
    endpoint can report 'not available here' instead of implying a trim
    happened."""
    _reset()
    result = heap.trim()
    if result is None:
        assert heap.unavailable_reason(), "a no-op trim must explain itself"
    else:
        assert heap.unavailable_reason() is None


def test_libc_resolution_is_memoised():
    """Resolution happens once, not per call — this runs every 5 minutes
    forever and after every sizes refresh."""
    _reset()
    assert heap._resolved is False
    heap.trim()
    assert heap._resolved is True
    handle_after_first = heap._libc
    heap.trim()
    assert heap._libc is handle_after_first


def test_a_broken_libc_is_swallowed_not_raised(monkeypatch):
    """If malloc_trim itself blows up, the caller must not notice. A failed
    trim is a missed optimisation; taking down the dataset-sizes job over it
    would be a strictly worse outcome than the fragmentation."""
    class _Exploding:
        def malloc_trim(self, _):
            raise OSError("boom")

    _reset()
    monkeypatch.setattr(heap, "_libc", _Exploding())
    monkeypatch.setattr(heap, "_resolved", True)
    assert heap.trim() is None


def test_trim_and_log_survives_an_unavailable_platform():
    """The scheduled job calls this directly; it must return cleanly whether
    or not anything was trimmed, and whether or not /proc exists."""
    _reset()
    heap.trim_and_log("unit test")  # must not raise


def test_rss_kb_is_an_int_or_none():
    """None off Linux (no /proc), never a crash — the admin endpoint renders
    it either way."""
    value = heap.rss_kb()
    assert value is None or (isinstance(value, int) and value > 0)


def test_trim_and_log_does_not_log_when_nothing_is_recovered(monkeypatch, caplog):
    """A trim that frees nothing is the healthy steady state and would
    otherwise print every five minutes forever."""
    _reset()
    monkeypatch.setattr(heap, "trim", lambda: True)
    monkeypatch.setattr(heap, "rss_kb", lambda: 200_000)  # identical before/after
    with caplog.at_level("INFO", logger="app.services.heap"):
        heap.trim_and_log("quiet case")
    assert not [r for r in caplog.records if "heap trim" in r.message]


def test_trim_and_log_reports_a_real_recovery(monkeypatch, caplog):
    """And when it does return real memory, that has to be visible in the
    logs after the admin endpoint has gone quiet."""
    _reset()
    sizes = iter([400_000, 300_000])  # before, after — 100MB returned
    monkeypatch.setattr(heap, "trim", lambda: True)
    monkeypatch.setattr(heap, "rss_kb", lambda: next(sizes))
    with caplog.at_level("INFO", logger="app.services.heap"):
        heap.trim_and_log("big job")
    messages = [r.getMessage() for r in caplog.records]
    assert any("heap trim after big job" in m for m in messages), messages
    assert any("97.7MB to the OS" in m for m in messages), messages
