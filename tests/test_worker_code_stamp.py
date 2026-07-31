"""Which commit a worker is running, on every task card.

A worker on another machine ran an early commit of a new source and produced a
version in the old row shape; OVER took it, because freshness is self-reported
and only an explicit "behind" is refused. The SHA on the queue card is what
makes that a glance instead of a debugging cycle.

The requirement that makes or breaks it is STICKINESS. The stamp was first
written only on assignment, and `/progress` overwrites `task.message` — so a
running fleet showed the SHA on whichever cards happened not to have reported
yet. A stamp present on one card and absent on the others is worse than no
stamp at all: it reads as "no information here", not "same code as the others".
The worker sends X-Worker-Version as a session-level default header, so every
progress report carries it and the stamp can simply be re-applied.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api.worker import worker_code_stamp  # noqa: E402


def test_the_sha_is_short_enough_to_scan_a_column_of_them():
    assert worker_code_stamp("c3d0e8012ab34cd56ef78901234567890abcdef0") == "[c3d0e80]"


def test_the_normal_verdict_is_not_printed():
    """An assigned task's worker cannot be "behind" — dispatch refuses that —
    so "/current" on every row is noise that hides the SHA."""
    assert worker_code_stamp("c3d0e80", "current") == "[c3d0e80]"
    assert worker_code_stamp("c3d0e80", "") == "[c3d0e80]"


def test_an_abnormal_verdict_is_printed():
    assert worker_code_stamp("c3d0e80", "unknown") == "[c3d0e80/unknown]"
    assert worker_code_stamp("c3d0e80", "BEHIND") == "[c3d0e80/behind]"


def test_a_worker_too_old_to_report_says_so():
    """Silence and "same code as everyone else" must not look alike."""
    assert worker_code_stamp(None) == "[no version]"
    assert worker_code_stamp("   ") == "[no version]"


def test_two_machines_on_different_commits_are_distinguishable():
    assert worker_code_stamp("c3d0e80aaa") != worker_code_stamp("f713610bbb")
