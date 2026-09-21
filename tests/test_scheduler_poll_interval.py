"""A dataset that is not polled must not get a poll job.

APScheduler silently rewrites IntervalTrigger(seconds=0) to ONE SECOND. The
נדל"ן לעם address dataset was registered with poll_interval=0 precisely because
nothing polls it, and was therefore polled every second from the moment it
existed — each poll queueing a scrape task a worker could not run.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from apscheduler.triggers.interval import IntervalTrigger  # noqa: E402

from app.worker import scheduler as sch  # noqa: E402


def test_apscheduler_really_does_turn_zero_into_one_second():
    """The trap itself, pinned — if a future APScheduler stops doing this, the
    guard below becomes redundant, and this test says so."""
    assert IntervalTrigger(seconds=0).interval_length == 1


def test_a_zero_interval_schedules_nothing():
    sch.add_poll_job("zero-interval-ds", 0)
    assert sch.scheduler.get_job("poll_zero-interval-ds") is None


def test_a_none_interval_schedules_nothing():
    sch.add_poll_job("none-interval-ds", None)
    assert sch.scheduler.get_job("poll_none-interval-ds") is None


def test_setting_an_interval_to_zero_removes_the_existing_job():
    """The admin can change a dataset's interval; changing it to 0 must stop the
    polling, not leave the old job running."""
    sch.add_poll_job("was-polled-ds", 3600)
    assert sch.scheduler.get_job("poll_was-polled-ds") is not None
    sch.add_poll_job("was-polled-ds", 0)
    assert sch.scheduler.get_job("poll_was-polled-ds") is None


def test_a_real_interval_still_schedules():
    sch.add_poll_job("hourly-ds", 3600)
    try:
        job = sch.scheduler.get_job("poll_hourly-ds")
        assert job is not None
        assert job.trigger.interval_length == 3600
    finally:
        sch.remove_poll_job("hourly-ds")
