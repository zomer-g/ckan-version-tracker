"""A daily poll can be pinned to an Israel-time hour (scraper_config.poll_at_hour).

The retail-prices datasets were approved near midnight and then ran every
night at 00:04 — when laibcatalog's today-only listing is empty. The schedule
anchors on "last poll + interval", so without a pin the hour never moves.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.worker import scheduler as sch  # noqa: E402

IL = ZoneInfo("Asia/Jerusalem")


def test_poll_at_hour_reads_config():
    assert sch.poll_at_hour(SimpleNamespace(scraper_config={"poll_at_hour": 7})) == 7
    assert sch.poll_at_hour(SimpleNamespace(scraper_config={"poll_at_hour": "7"})) == 7
    assert sch.poll_at_hour(SimpleNamespace(scraper_config={"poll_at_hour": 24})) is None
    assert sch.poll_at_hour(SimpleNamespace(scraper_config={})) is None
    assert sch.poll_at_hour(SimpleNamespace(scraper_config=None)) is None


def test_next_occurrence_is_today_or_tomorrow_in_israel_time():
    just_after_midnight = datetime(2026, 9, 25, 0, 4, tzinfo=IL).astimezone(timezone.utc)
    nxt = sch._next_at_hour(7, just_after_midnight).astimezone(IL)
    assert (nxt.day, nxt.hour, nxt.minute) == (25, 7, 0)
    after = datetime(2026, 9, 25, 7, 30, tzinfo=IL).astimezone(timezone.utc)
    nxt = sch._next_at_hour(7, after).astimezone(IL)
    assert (nxt.day, nxt.hour) == (26, 7)


def test_a_pinned_daily_job_starts_at_its_hour_even_when_overdue(monkeypatch):
    added = {}
    monkeypatch.setattr(sch.scheduler, "add_job",
                        lambda func, trigger, **kw: added.update(trigger=trigger, **kw))
    long_ago = datetime(2026, 9, 1, tzinfo=timezone.utc)
    sch.add_poll_job("d1", 86400, last_polled_at=long_ago, at_hour=7)
    start = added["trigger"].start_date.astimezone(IL)
    assert (start.hour, start.minute) == (7, 0)


def test_an_unpinned_or_sub_daily_job_is_unchanged(monkeypatch):
    added = {}
    monkeypatch.setattr(sch.scheduler, "add_job",
                        lambda func, trigger, **kw: added.update(trigger=trigger))
    sch.add_poll_job("d2", 3600, last_polled_at=None, at_hour=7)
    delta = (added["trigger"].start_date - datetime.now(timezone.utc)).total_seconds()
    assert delta < 5   # hourly: the pin does not apply, fires on the next tick
