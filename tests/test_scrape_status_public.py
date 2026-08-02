"""A running collection is visible without credentials.

A long collection publishes nothing until it finishes — the Jerusalem
building-licensing register runs ~50 hours and produces its single version at
the very end. While it runs, the only evidence it is alive lives on the task
row, which was admin-only, behind a token that expires every two hours. So the
question "is it still working?" was unanswerable exactly on the runs where it
matters, and the honest answer had to be "send me a fresh token".

What is locked here: the endpoint answers the question, and answers ONLY the
question — no worker identity, no IP, no build stamp.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import app.api.v1 as v1  # noqa: E402


def test_the_worker_build_stamp_is_stripped_from_the_public_message():
    """The worker appends its commit to every progress message. That is an
    operator's diagnostic, not part of "what is this collection doing"."""
    strip = lambda s: v1._WORKER_STAMP.sub("", s).strip()  # noqa: E731

    assert strip("מעשיר 100072 תיקים (2 קריאות לתיק)… [87d5b7a]") == \
        "מעשיר 100072 תיקים (2 קריאות לתיק)…"
    # The assignment message carries the upstream verdict too.
    assert strip("Assigned to worker host#ab12 [3f9c1d2/current]") == \
        "Assigned to worker host#ab12"
    # A message with no stamp is untouched — including one ending in a number,
    # which a looser pattern would have eaten.
    assert strip("חלקות: 800/77800 — 1548 תיקים") == "חלקות: 800/77800 — 1548 תיקים"
    assert strip("") == ""


def test_the_response_carries_no_operator_detail():
    """Whoever is asking wants to know if the collection is alive. The machine
    running it, its address, and its commit are not part of that answer."""
    fields = set(v1.ScrapeStatus.model_fields)
    assert not fields & {"worker_id", "worker_ip", "worker_version", "error"}
    # And it does carry the two things that make "alive" decidable: how long it
    # has been going, and how long since the worker last said anything.
    assert {"elapsed_seconds", "seconds_since_heartbeat", "phase"} <= fields


def test_a_dataset_with_nothing_running_reports_the_last_outcome():
    """Not-running is an answer, not an error — and "the last attempt failed"
    is the answer a caller actually needs when there is no progress to show."""
    fields = set(v1.ScrapeStatus.model_fields)
    assert {"running", "last_outcome", "last_finished_at"} <= fields
    assert v1.ScrapeStatus(dataset_id="x", running=False).phase is None
