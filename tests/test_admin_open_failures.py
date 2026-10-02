"""The queue panel lists failures that are still failures.

On 2026-10-02 every one of its 20 rows was a data.gov.il WAF failure from the
morning before — each dataset archived by a later run that same day. The panel
read "full of errors" a day after the fix. A failure whose dataset completed a
task after it is counted as resolved, not listed.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api.admin import _open_failures  # noqa: E402
from app.models.scrape_task import ScrapeTask  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402

NOW = datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)


def _ds(name):
    return TrackedDataset(id=uuid.uuid4(), ckan_id=name, ckan_name=name, title=name,
                          source_type="ckan", poll_interval=86400, is_active=True,
                          status="active", storage_mode="full_snapshot", source_url="u")


def _task(ds, status, hours_ago):
    at = NOW - timedelta(hours=hours_ago)
    return ScrapeTask(id=uuid.uuid4(), tracked_dataset_id=ds.id, status=status,
                      phase="scraping", params={}, created_at=at, completed_at=at,
                      error="boom" if status == "failed" else None)


def test_a_failure_a_later_run_fixed_is_counted_not_listed():
    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__, ScrapeTask.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))
            # Partial in Postgres (active tasks only); SQLite builds it whole,
            # which would forbid the history this test is about.
            await conn.exec_driver_sql("DROP INDEX IF EXISTS uq_scrape_tasks_active_per_dataset")
        S = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        fixed, still, old = _ds("fixed"), _ds("still"), _ds("old")
        async with S() as db:
            db.add_all([fixed, still, old])
            db.add_all([
                _task(fixed, "failed", 20), _task(fixed, "completed", 15),  # resolved
                _task(still, "completed", 20), _task(still, "failed", 10),  # failed after its success
                _task(old, "failed", 30),                                   # outside 24h
            ])
            await db.commit()
            return await _open_failures(db, now=NOW)

    rows, resolved = asyncio.run(go())
    assert [ds.ckan_name for _t, ds in rows] == ["still"]
    assert resolved == 1
