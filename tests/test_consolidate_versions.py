"""consolidate_dataset_versions merges a batched dataset's per-batch versions.

A size-capped archive run publishes one version PER batch, so a whole-corpus
snapshot ends up as N partial versions (each with the full index CSV + only its
batch's ZIP parts). This must collapse into ONE version that references a single
complete index CSV + EVERY batch's ZIP parts — and it must NOT delete the R2 ZIP
objects the merged version reuses.

No live R2/NEON: the CSV build and the R2 upload are stubbed; the DB is an
in-memory SQLite so the real VersionIndex delete/insert path runs.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


import asyncio  # noqa: E402

from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.services import r2_backfill  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


DS_ID = uuid.uuid4()


async def _make_db(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{(tmp_path/'c.sqlite').as_posix()}")
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
        await conn.run_sync(lambda c: VersionIndex.__table__.create(c))
        # TrackedDataset.tags is a selectin relationship — touched on load.
        await conn.run_sync(lambda c: Tag.__table__.create(c))
        await conn.run_sync(lambda c: dataset_tags.create(c))
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as db:
        db.add(TrackedDataset(
            id=DS_ID, ckan_id="workagreements-scraper-x", ckan_name="x",
            title="מרשם", source_type="scraper", last_modified="2026-07-24",
        ))
        # 3 batch versions: each has the full CSV (own copy) + its batch's ZIP parts.
        for n, zips in ((1, ["r2:z1a", "r2:z1b"]), (2, ["r2:z2"]), (3, ["r2:z3a", "r2:z3b"])):
            db.add(VersionIndex(
                tracked_dataset_id=DS_ID, version_number=n,
                metadata_modified="2026-07-24", source="legacy",
                resource_mappings={"נתוני הסורק": f"r2:csv{n}", "_zip_parts": zips,
                                   "_resource_ids": []},
                change_summary={"type": "scraper"},
            ))
        await db.commit()
    return Session


@pytest.fixture
def stub_csv_and_r2(monkeypatch):
    """Stub the NEON CSV build + R2 upload so the test needs no live services."""
    async def _fake_build(table, dedup_key):
        assert dedup_key == "מספר הסכם"
        import tempfile
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.write(fd, b"\xef\xbb\xbf")
        os.close(fd)
        return path, 39538

    monkeypatch.setattr(r2_backfill, "_build_deduped_index_csv", _fake_build)
    monkeypatch.setattr(r2_backfill.storage_client, "is_configured", lambda: True)

    uploaded = {}

    async def _fake_upload(key, *, file_path=None, content_type=None, file_content=None):
        uploaded["key"] = key
        return key

    monkeypatch.setattr(r2_backfill.storage_client, "upload_object", _fake_upload)

    # The dry-run path counts NEON rows for the preview — stub it so no live DB.
    from app.services import append_store
    async def _fake_count(table, **kw):
        return 78998
    monkeypatch.setattr(append_store, "table_count", _fake_count)
    monkeypatch.setattr(append_store, "table_name", lambda ds: "append_x")
    return uploaded


def test_dry_run_reports_the_plan_without_writing(tmp_path, stub_csv_and_r2):
    Session = _run(_make_db(tmp_path))

    async def go():
        async with Session() as db:
            s = await r2_backfill.consolidate_dataset_versions(
                db, DS_ID, dedup_key="מספר הסכם", apply=False)
            from sqlalchemy import select, func
            n = (await db.execute(
                select(func.count()).select_from(VersionIndex))).scalar()
            return s, n

    s, n = _run(go())
    assert s["apply"] is False and s["committed"] is False
    assert s["old_version_count"] == 3
    assert s["zip_parts"] == 5          # 2 + 1 + 2, in batch order
    assert n == 3                        # nothing deleted


def test_apply_merges_into_one_version_with_all_zip_parts(tmp_path, stub_csv_and_r2):
    Session = _run(_make_db(tmp_path))

    async def go():
        async with Session() as db:
            s = await r2_backfill.consolidate_dataset_versions(
                db, DS_ID, dedup_key="מספר הסכם", apply=True)
        from sqlalchemy import select
        async with Session() as db:
            rows = (await db.execute(select(VersionIndex))).scalars().all()
        return s, rows

    s, rows = _run(go())
    assert s["committed"] is True and s["new_version_count"] == 1
    assert len(rows) == 1
    v = rows[0]
    assert v.version_number == 1 and v.source == "consolidated"
    m = v.resource_mappings
    # The full CSV under the resource-name key, all 5 ZIP parts in batch order.
    assert m["נתוני הסורק"].startswith("r2:")
    assert m["_zip_parts"] == ["r2:z1a", "r2:z1b", "r2:z2", "r2:z3a", "r2:z3b"]
    assert m["_names"]["נתוני הסורק"]  # a friendly label
    assert v.change_summary["total_attachments"] == 5
    assert v.change_summary["consolidated_from_versions"] == 3


def test_apply_reuses_zip_keys_and_never_deletes_r2(tmp_path, stub_csv_and_r2, monkeypatch):
    """The old rows are removed with a raw db.delete — the R2 ZIP objects the
    merged version points at must NOT be deleted."""
    deleted = []
    monkeypatch.setattr(r2_backfill.storage_client, "delete_object",
                        lambda key: deleted.append(key), raising=False)
    Session = _run(_make_db(tmp_path))

    async def go():
        async with Session() as db:
            return await r2_backfill.consolidate_dataset_versions(
                db, DS_ID, dedup_key="מספר הסכם", apply=True)

    _run(go())
    assert deleted == []  # no R2 object deleted


def test_bad_dedup_key_is_rejected(tmp_path, stub_csv_and_r2, monkeypatch):
    async def _boom(table, dedup_key):
        raise ValueError(f"dedup_key {dedup_key!r} not a column of {table}")

    monkeypatch.setattr(r2_backfill, "_build_deduped_index_csv", _boom)
    Session = _run(_make_db(tmp_path))

    async def go():
        async with Session() as db:
            with pytest.raises(ValueError):
                await r2_backfill.consolidate_dataset_versions(
                    db, DS_ID, dedup_key="nope", apply=True)

    _run(go())
