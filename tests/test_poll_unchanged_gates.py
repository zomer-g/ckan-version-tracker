"""A poll that has work to do must not be skipped as "nothing changed".

Two ways that went wrong on a data.gov.il package whose metadata_modified has
been frozen since 2024:

  • The admin's דגום passes force=True, but the "a version already exists with
    this metadata_modified" gate never looked at `force` — only at a NULL
    last_modified. The button answered "Poll triggered" and did nothing.

  • Switching a dataset to r2+neon (or migration 048 doing it) leaves the plan
    ahead of the archive. With the source frozen, the unchanged-skip meant the
    NEON half would never run — not on this poll, not ever.

Both now reach the archive path, and once the archive carries a row-archive
marker the cheap skip comes back.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services import conditional_archiver  # noqa: E402
from app.worker import poll_job  # noqa: E402

_DS = uuid.uuid4()
_RID = "res-1"
_MODIFIED = "2024-11-27T08:00:40.849663"

PKG = {
    "id": "pkg-uuid",
    "name": "elections",
    "title": "תוצאות בחירות",
    "metadata_modified": _MODIFIED,
    "organization": {"name": "knesset"},
    "resources": [
        {"id": _RID, "name": "תוצאות אמת", "format": "CSV", "datastore_active": True,
         "url": "https://data.gov.il/x.csv"},
    ],
}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def env(monkeypatch):
    """poll_dataset over in-memory SQLite, with the archive path stubbed out.

    The assertion is only ever "did the poll get as far as archiving", so the
    heavy half (download, datastore stream, R2) is replaced by a recorder.
    """
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _create():
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      VersionIndex.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))

    _run(_create())

    monkeypatch.setattr(poll_job, "async_session", Session)
    monkeypatch.setattr(poll_job.ckan_client, "package_show",
                        lambda _id: _async(PKG))
    monkeypatch.setattr(poll_job.ckan_client, "datastore_info",
                        lambda _rid: _async({"total": 42, "fields": [{"id": "a"}]}))
    # Never let the cheap probe path answer for us — this is about the gates.
    monkeypatch.setattr(conditional_archiver, "try_conditional_archive",
                        lambda ds, db: _async(conditional_archiver.Result.FALLBACK))

    # Which path the poll reached AFTER the unchanged-gates: the datastore
    # stream (NEON) or the file snapshot. Empty means it never got there.
    archived: list[str] = []

    async def _fake_large(ds, pkg, resource, ds_info, next_version, old_mappings, db):
        archived.append("neon")

    # What data.gov.il is withholding this poll. Tests set it before polling.
    blocked: list[str] = []

    async def _fake_detect(old_mappings, resources, max_bytes=None):
        archived.append("file")
        return [], {}, [], list(blocked)

    monkeypatch.setattr(poll_job, "_poll_large_dataset", _fake_large)
    monkeypatch.setattr(poll_job, "detect_resource_changes", _fake_detect)
    poll_job._active_polls.clear()
    return Session, archived, blocked


def _async(value):
    async def _c():
        return value
    return _c()


def _seed(Session, *, plan: dict | None, version_mappings: dict):
    async def _go():
        async with Session() as db:
            db.add(TrackedDataset(
                id=_DS, ckan_id="pkg-1", ckan_name="elections", resource_id=_RID,
                resource_ids=[_RID], title="תוצאות בחירות", source_type="ckan",
                poll_interval=7776000, is_active=True, status="active",
                storage_mode="full_snapshot", scraper_config=plan,
                last_modified=_MODIFIED,
            ))
            db.add(VersionIndex(
                id=uuid.uuid4(), tracked_dataset_id=_DS, version_number=1,
                metadata_modified=_MODIFIED, resource_mappings=version_mappings,
                detected_at=datetime(2026, 7, 31, 22, 4, tzinfo=timezone.utc),
            ))
            await db.commit()

    _run(_go())


_FILE_ONLY = {"_resource_ids": [_RID], _RID: "r2:datasets/x/v1/a.csv"}
_WITH_NEON = {**_FILE_ONLY, "append_table": "append_elections_1234abcd"}
_R2_NEON = {"storage_backend": "r2", "archive_neon": True}


def test_an_unchanged_source_is_still_skipped(env):
    """The cheap skip is the whole point of a scheduled poll — keep it."""
    Session, archived, _blocked = env
    _seed(Session, plan={"storage_backend": "r2"}, version_mappings=_FILE_ONLY)
    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == []


def test_the_admin_force_button_actually_re_polls(env):
    """דגום used to return "Poll triggered" and no-op on an unchanged source."""
    Session, archived, _blocked = env
    _seed(Session, plan={"storage_backend": "r2"}, version_mappings=_FILE_ONLY)
    _run(poll_job.poll_dataset(str(_DS), force=True))
    assert archived == ["file"], "the forced poll never reached the archive"


def test_a_plan_that_gained_neon_re_polls_itself(env):
    """No source change, no admin click — the NEON half still has to happen."""
    Session, archived, _blocked = env
    _seed(Session, plan=_R2_NEON, version_mappings=_FILE_ONLY)
    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == ["neon"], "the NEON half never ran"


def test_once_the_rows_are_archived_the_skip_returns(env):
    """Self-clearing: a version carrying `append_table` is the plan fulfilled,
    so the dataset goes back to the cheap unchanged-skip."""
    Session, archived, _blocked = env
    _seed(Session, plan=_R2_NEON, version_mappings=_WITH_NEON)
    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == []


def test_a_source_with_no_datastore_is_assessed_once_then_skipped(env, monkeypatch):
    """Nothing can feed NEON here, so re-polling FOREVER would just burn
    downloads on an unchanged file — that was and remains the concern.

    But a file-bearing dataset gets exactly one full poll first, to find out
    whether data.gov.il is withholding those files. Without it, a package whose
    metadata_modified has been frozen since before the blocked-file detection
    shipped would take this shortcut every time and never learn it is losing
    them — which is how 7 of the 13 affected datasets lost files in silence.
    The assessment stores its answer, and the cheap skip comes straight back.
    """
    no_ds = {**PKG, "resources": [{**PKG["resources"][0], "datastore_active": False}]}
    monkeypatch.setattr(poll_job.ckan_client, "package_show", lambda _id: _async(no_ds))
    Session, archived, _blocked = env
    _seed(Session, plan=_R2_NEON, version_mappings=_FILE_ONLY)

    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == ["file"], "the one-time assessment never ran"

    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == ["file"], "assessed once already — the skip must return"


def test_a_datastore_backed_source_is_never_forced_to_assess(env):
    """A package with nothing downloadable has nothing that can be withheld,
    so it must not pay a full re-archive to be told so. Its table can be
    enormous; the old skip has to survive untouched."""
    Session, archived, _blocked = env
    _seed(Session, plan={"storage_backend": "r2"}, version_mappings=_FILE_ONLY)
    _run(poll_job.poll_dataset(str(_DS)))
    assert archived == []


# ---------------------------------------------------------------------------
# A shortcut must not swallow the "your files are being withheld" warning.
#
# The warning was built only after detect_resource_changes, which both
# shortcuts return before reaching — so a frozen package lost its files in
# silence. The finding is remembered on the dataset now, and re-stated here.
# ---------------------------------------------------------------------------

def _error_of(Session) -> str | None:
    async def _go():
        async with Session() as db:
            return (await db.get(TrackedDataset, _DS)).last_error
    return _run(_go())


def _set_error(Session, text):
    async def _go():
        async with Session() as db:
            ds = await db.get(TrackedDataset, _DS)
            ds.last_error = text
            await db.commit()
    _run(_go())


def test_a_blocked_file_is_still_reported_after_the_shortcut_takes_over(env, monkeypatch):
    """The reported bug, end to end: assess once, then keep saying it."""
    no_ds = {**PKG, "resources": [{**PKG["resources"][0], "datastore_active": False}]}
    monkeypatch.setattr(poll_job.ckan_client, "package_show", lambda _id: _async(no_ds))
    Session, archived, blocked = env
    _seed(Session, plan=_R2_NEON, version_mappings=_FILE_ONLY)

    blocked.append(_RID)
    _run(poll_job.poll_dataset(str(_DS)))          # the one assessing poll
    assert "חסומים להורדה שרתית" in (_error_of(Session) or "")

    _set_error(Session, None)                       # someone clears the banner
    _run(poll_job.poll_dataset(str(_DS)))          # now the shortcut answers
    assert archived == ["file"], "should have skipped the second time"
    assert "חסומים להורדה שרתית" in (_error_of(Session) or ""), \
        "the shortcut swallowed the warning — this is the bug"


def test_the_shortcut_does_not_overwrite_a_real_failure(env, monkeypatch):
    """It re-states a fact it did not verify this poll, so it must be additive:
    a genuine error from the poll that DID the work outranks it."""
    no_ds = {**PKG, "resources": [{**PKG["resources"][0], "datastore_active": False}]}
    monkeypatch.setattr(poll_job.ckan_client, "package_show", lambda _id: _async(no_ds))
    Session, archived, blocked = env
    _seed(Session, plan=_R2_NEON, version_mappings=_FILE_ONLY)

    _run(poll_job.poll_dataset(str(_DS)))          # assess: nothing blocked
    _set_error(Session, "download נתוני הסורק: timeout")
    _run(poll_job.poll_dataset(str(_DS)))          # shortcut
    assert _error_of(Session) == "download נתוני הסורק: timeout", \
        "a shortcut that verified nothing must not clear a real error"
