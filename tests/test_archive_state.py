"""The archive-state derivation: what a dataset HOLDS vs what its plan SAYS.

The storage plan is a declaration; ``version_index.resource_mappings`` is the
record of what was actually written. app/services/archive_state.py reads the
second and classifies it, which is how a >50k-row CKAN dataset whose plan reads
"R2 — סנפשוט מלא" gets correctly reported as holding a metadata stub.

The cases pinned here are the ones that made the naive versions of this wrong:

  * ``_appendonly_seen`` holds 64-char row hashes — a "long string ⇒ file"
    heuristic counts every one of them as an archived file;
  * a metadata stub's ``sample`` mapping IS a real ODATA resource (the 200-row
    sample CSV), so file-detection alone reports 200-of-640,460 rows as fully
    archived;
  * a ``metadata_only`` version copies the previous version's mappings verbatim,
    so classifying on ``change_summary.type`` reports a healthy R2 dataset whose
    last poll found "bytes unchanged" as holding nothing.

Dependency-light: the derivation is pure, so most of this needs no DB at all.
The endpoint test runs on in-memory SQLite through a bare FastAPI app, in the
same style as tests/test_admin_datasets_page.py.
"""
import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.admin import router as admin_router  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import archive_state  # noqa: E402

R2 = "r2:datasets/abc/v3/deadbeef_csv"
ODATA_RID = "277db941-1819-46d6-a1b3-6faea163fabe"
ROW_HASH = "a" * 64


def _describe(mappings, *, has_versions=True, plan="r2",
              archives_neon=False, stores_files=True):
    return archive_state.describe(
        mappings, has_versions=has_versions, plan=plan,
        archives_neon=archives_neon, stores_files=stores_files,
    )


# ── fidelity ────────────────────────────────────────────────────────────────

def test_no_versions_is_none():
    st = _describe(None, has_versions=False)
    assert st["fidelity"] == archive_state.NONE
    assert archive_state.MISMATCH_NO_VERSION in st["mismatch"]


def test_files_only_when_mappings_hold_objects():
    st = _describe({"res-1": R2, "_hashes": {"res-1": "x"}})
    assert st["fidelity"] == archive_state.FILES
    assert st["file_store"] == "r2"
    assert st["row_store"] == "none"
    assert st["mismatch"] == []


def test_rows_only_for_a_neon_plan():
    st = _describe(
        {"append_table": "append_vehicles_1234abcd", "_resource_ids": ["r"]},
        plan="neon", archives_neon=True, stores_files=False,
    )
    assert st["fidelity"] == archive_state.ROWS
    assert st["row_store"] == "neon"
    # A neon plan storing no file is doing exactly what it says — not a finding.
    assert st["mismatch"] == []


def test_full_when_a_version_carries_both_a_table_and_a_snapshot():
    st = _describe(
        {"append_table": "append_x_1234abcd", "res-1": R2},
        plan="r2+neon", archives_neon=True,
    )
    assert st["fidelity"] == archive_state.FULL
    assert (st["file_store"], st["row_store"]) == ("r2", "neon")


def test_multi_resource_neon_tables_count_as_rows():
    st = _describe(
        {"_append_tables": {"rid-1": "append_a_1_2", "rid-2": "append_a_1_3"}},
        plan="neon", archives_neon=True, stores_files=False,
    )
    assert st["fidelity"] == archive_state.ROWS


# ── the metadata stub: the case the whole module exists for ──────────────────

def test_metadata_stub_is_sample_not_files():
    """The stub's `sample` value is a real ODATA resource. It must NOT read as
    "this dataset has files" — that is what let 16 CKAN datasets holding
    640k-row stubs report as fully archived."""
    st = _describe({
        "sample": "ba5856f0-ccd8-40ac-ab85-815720e25e39",
        "_hashes": {"lightweight": "abc"},
        "_resource_ids": ["c0938e1a-259a-41c3-a225-94e477264137"],
        "_large_dataset_info": {"record_count": 640460, "fields": ["a", "b"]},
    })
    assert st["fidelity"] == archive_state.SAMPLE
    assert st["sample_of"] == 640460
    assert archive_state.MISMATCH_SAMPLE_ONLY in st["mismatch"]


def test_a_stubs_own_sample_csv_is_not_counted_as_archived_files():
    """Caught in production, not by the mock: all 16 stub datasets reported
    ``file_store="odata"`` off the single `sample` key.

    That resource is real and fetchable — it's the 200-row excerpt
    create_lightweight_snapshot pushes to the mirror — so counting it put an
    "ODATA" chip next to "מטא־דאטה בלבד" on the public page (advertising
    downloadable data that does not exist) and fired a second, redundant
    file-store mismatch on top of sample_only.
    """
    st = _describe({
        "sample": "ba5856f0-ccd8-40ac-ab85-815720e25e39",
        "_large_dataset_info": {"record_count": 688256},
    }, plan="r2")
    assert st["fidelity"] == archive_state.SAMPLE
    assert st["file_store"] == "none"
    # sample_only says the real thing; file_store would only add noise.
    assert st["mismatch"] == [archive_state.MISMATCH_SAMPLE_ONLY]


def test_a_resource_actually_named_sample_still_counts_on_a_normal_dataset():
    """The skip above is conditional on the stub marker — it must not blind the
    derivation to a legitimately-named resource on a healthy dataset."""
    st = _describe({"sample": R2}, plan="r2")
    assert st["fidelity"] == archive_state.FILES
    assert st["file_store"] == "r2"


def test_an_append_only_dataset_that_only_ever_stubbed_reports_sample():
    """Prod case 4240a562: storage_mode=append_only, plan r2, and all 8 versions
    are metadata stubs — the delta archive declined every time, so nothing was
    ever appended. "append_only" is a declaration; it must not be read as
    evidence that rows exist."""
    st = _describe(
        {"sample": ODATA_RID, "_large_dataset_info": {"record_count": 4135444}},
        plan="r2", archives_neon=False,
    )
    assert st["fidelity"] == archive_state.SAMPLE
    assert st["row_store"] == "none"
    assert st["sample_of"] == 4135444


def test_a_stub_that_later_streamed_to_neon_is_no_longer_a_sample():
    """Fixing a stub means opting it into NEON. Once rows land, the dataset must
    stop being reported as a stub even if the old key is still carried."""
    st = _describe(
        {"_large_dataset_info": {"record_count": 640460},
         "append_table": "append_pdo_1234abcd"},
        plan="r2+neon", archives_neon=True,
    )
    assert st["fidelity"] == archive_state.ROWS
    assert archive_state.MISMATCH_SAMPLE_ONLY not in st["mismatch"]


# ── things that are NOT files ────────────────────────────────────────────────

def test_row_hashes_in_appendonly_seen_are_not_files():
    st = _describe({"_appendonly_seen": [ROW_HASH, "b" * 64], "_hashes": {}})
    assert st["file_store"] == "none"
    assert st["fidelity"] == archive_state.NONE


def test_a_neon_table_name_is_not_a_file():
    st = _describe(
        {"append_table": "append_policies_96dcbeac_03c59ca7"},
        plan="neon", archives_neon=True, stores_files=False,
    )
    assert st["file_store"] == "none"


def test_names_and_filedates_are_not_files():
    st = _describe({
        "res-1": R2,
        "_names": {"res-1": "תקציב — 30.07.2026"},
        "_filedates": {"res-1": "2026-07-30"},
    })
    assert st["file_store"] == "r2"
    assert st["fidelity"] == archive_state.FILES


# ── plan vs reality ─────────────────────────────────────────────────────────

def test_odata_bytes_under_an_r2_plan_are_flagged():
    """The plan belongs to the dataset, the bytes belong to each version. A
    dataset re-pointed at R2 keeps history on ODATA, and every UI showed the
    whole history as "R2"."""
    st = _describe({"res-1": ODATA_RID}, plan="r2")
    assert st["file_store"] == "odata"
    assert archive_state.MISMATCH_FILE_STORE in st["mismatch"]


def test_mixed_stores_within_one_version_are_flagged():
    st = _describe({"res-1": R2, "res-2": ODATA_RID}, plan="r2")
    assert st["file_store"] == "mixed"
    assert archive_state.MISMATCH_FILE_STORE in st["mismatch"]


def test_r2_bytes_under_an_r2_neon_plan_are_not_flagged():
    """The combo plan's file half is "r2" — comparing the whole plan string
    would flag every healthy dual-write dataset."""
    st = _describe(
        {"res-1": R2, "append_table": "append_x_1_2"},
        plan="r2+neon", archives_neon=True,
    )
    assert st["mismatch"] == []


def test_geojson_and_zip_aggregates_are_files():
    st = _describe({
        "נתוני הסורק": R2,
        "_geojson": ["r2:datasets/abc/v1/1234abcd_geojson.gz"],
        "_zip_parts": ["r2:datasets/abc/v1/5678efab_part-1.zip"],
    })
    assert st["fidelity"] == archive_state.FILES
    assert st["file_store"] == "r2"


# ── the admin endpoint actually serves it ───────────────────────────────────

def _ds(title, **kw):
    return TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=f"id-{title}",
        ckan_name=kw.pop("ckan_name", f"name-{title}"),
        title=title,
        poll_interval=3600,
        is_active=True,
        status="active",
        **kw,
    )


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    tables = [
        Organization.__table__, User.__table__, TrackedDataset.__table__,
        Tag.__table__, dataset_tags, VersionIndex.__table__,
    ]

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=tables)
        async with Session() as db:
            stub = _ds("פנקס הסניגורים", source_type="ckan",
                       scraper_config={"storage_backend": "r2"})
            healthy = _ds("קווי אוטובוס", source_type="ckan",
                          scraper_config={"storage_backend": "r2"})
            empty = _ds("מאגר בלי גרסאות", source_type="ckan",
                        scraper_config={"storage_backend": "r2"})
            db.add_all([stub, healthy, empty])
            await db.flush()
            # v1 held real files; v2 is the stub. The derivation must read the
            # LATEST version, not the first one it finds.
            db.add_all([
                VersionIndex(id=uuid.uuid4(), tracked_dataset_id=stub.id,
                             version_number=1, metadata_modified="2026-01-01",
                             resource_mappings={"res-1": R2}),
                VersionIndex(id=uuid.uuid4(), tracked_dataset_id=stub.id,
                             version_number=2, metadata_modified="2026-07-01",
                             resource_mappings={
                                 "_large_dataset_info": {"record_count": 640460},
                             }),
                VersionIndex(id=uuid.uuid4(), tracked_dataset_id=healthy.id,
                             version_number=1, metadata_modified="2026-07-01",
                             resource_mappings={"res-1": R2}),
            ])
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(admin_router)
    app.dependency_overrides[get_db] = _db
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    yield TestClient(app)


def test_catalog_serialization_omits_the_derivation():
    """The shared serializer must leave `archive` null unless a caller opted in.

    The unpaginated public catalog serves ~1,090 rows in one ~1MB response and
    doesn't render this field; carrying a placeholder object per row is pure
    weight on the endpoint whose payload already contributed to the 512MB-dyno
    OOMs. Opting in is what the paginated admin list does.
    """
    from app.api.datasets import build_dataset_response

    ds = _ds("x", source_type="ckan", scraper_config={"storage_backend": "r2"})
    assert build_dataset_response(ds, None, None, 3).archive is None
    opted_in = build_dataset_response(ds, None, None, 3, with_archive=True)
    assert opted_in.archive is not None
    assert opted_in.archive.fidelity == archive_state.NONE


def test_admin_page_reports_archive_state_per_dataset(client):
    r = client.get("/api/admin/datasets")
    assert r.status_code == 200, r.text
    by_title = {i["title"]: i for i in r.json()["items"]}

    stub = by_title["פנקס הסניגורים"]["archive"]
    assert stub["fidelity"] == "sample"
    assert stub["sample_of"] == 640460
    assert "sample_only" in stub["mismatch"]
    # …while its declared plan still reads r2 — that IS the mismatch.
    assert by_title["פנקס הסניגורים"]["storage_target"] == "r2"

    healthy = by_title["קווי אוטובוס"]["archive"]
    assert healthy["fidelity"] == "files"
    assert healthy["mismatch"] == []

    empty = by_title["מאגר בלי גרסאות"]["archive"]
    assert empty["fidelity"] == "none"
    assert "no_version" in empty["mismatch"]
