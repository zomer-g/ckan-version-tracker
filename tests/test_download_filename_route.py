"""The download link has to hand over a filename a GIS tool can open.

`/versions/<id>/download/<resource>` redirects at the object store, so the
object KEY becomes the filename the browser saves. Keys are random-prefixed
ASCII segments, and for an all-Hebrew layer title the stem collapses entirely:
the "GeoJSON" link on רשות העתיקות served `cee35a8b_geojson.gz`, a name with no
`.geojson` anywhere in it. GDAL strips the `.gz`, finds no extension it knows,
and QGIS / ArcGIS refuse the file — which from the outside looks exactly like
the archive holding a broken GeoJSON.

These pin the two halves: the route asks for a real filename, and it still
redirects when no signature can be produced.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api.versions import router as versions_router  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter, rate_limit_exceeded_handler  # noqa: E402
from app.services import storage_client as storage  # noqa: E402

_TABLES = [
    Organization.__table__, User.__table__, TrackedDataset.__table__,
    Tag.__table__, dataset_tags, VersionIndex.__table__,
]

DS = uuid.uuid4()
VER = uuid.uuid4()
# The real key shape behind the reported dataset: the extension is the whole
# name, because it was written before the key builder stopped eating it.
GEO_KEY = f"r2:datasets/{DS}/v1/cee35a8b_geojson.gz"


@pytest.fixture()
def ctx(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            db.add(TrackedDataset(
                id=DS, ckan_id="govmap-212189", ckan_name="govmap-212189",
                title="רשות העתיקות", source_type="govmap",
                source_url="https://www.govmap.gov.il/?lay=212189",
                poll_interval=86400, status="active",
            ))
            db.add(VersionIndex(
                id=VER, tracked_dataset_id=DS, version_number=1,
                metadata_modified="2026-07-31T10:27:01",
                detected_at=datetime(2026, 7, 31, tzinfo=timezone.utc),
                resource_mappings={
                    "_hashes": {"scraper": "abc"},
                    "_geojson": [GEO_KEY],
                    "נתוני הסורק": f"r2:datasets/{DS}/v1/916e4bd3_csv",
                    "_resource_ids": [],
                },
                source="scraper",
            ))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    asked: list[tuple[str, str | None]] = []

    async def _presign(_self, key_or_value, *, filename=None, expires_s=3600):
        asked.append((storage.key_of(key_or_value), filename))
        return f"https://s3.test/{storage.key_of(key_or_value)}?X-Amz-Signature=x"

    monkeypatch.setattr(storage.StorageClient, "presign_download", _presign)
    monkeypatch.setattr(storage.StorageClient, "public_url",
                        lambda _self, value: f"https://files.test/{storage.key_of(value)}")
    monkeypatch.setattr(storage.StorageClient, "is_configured",
                        lambda _self: True)

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(versions_router)
    app.dependency_overrides[get_db] = _db
    limiter.reset()
    client = TestClient(app, raise_server_exceptions=False, follow_redirects=False)
    yield client, asked, monkeypatch


def test_geojson_download_asks_for_a_name_ending_in_geojson(ctx):
    client, asked, _ = ctx
    r = client.get(f"/api/versions/{VER}/download/_geojson")
    assert r.status_code == 307
    assert asked == [(storage.key_of(GEO_KEY), "רשות העתיקות.geojson.gz")]
    # The signed URL is what the browser follows.
    assert "X-Amz-Signature" in r.headers["location"]


def test_a_named_csv_resource_keeps_its_own_name(ctx):
    client, asked, _ = ctx
    r = client.get(f"/api/versions/{VER}/download/נתוני הסורק")
    assert r.status_code == 307
    assert asked[0][1] == "רשות העתיקות.csv"


def test_download_still_works_when_signing_is_unavailable(ctx):
    """No credentials, a boto failure, an unconfigured store: the download must
    fall back to the plain public URL, never fail over its own filename."""
    client, _asked, monkeypatch = ctx

    async def _none(_self, key_or_value, *, filename=None, expires_s=3600):
        return None

    monkeypatch.setattr(storage.StorageClient, "presign_download", _none)
    r = client.get(f"/api/versions/{VER}/download/_geojson")
    assert r.status_code == 307
    assert r.headers["location"] == f"https://files.test/{storage.key_of(GEO_KEY)}"


def test_inline_keeps_the_cacheable_public_url(ctx):
    """The in-page map fetches the same route. A signed URL is unique per
    request, so it can never hit the browser cache — a 50 MB layer would be
    re-downloaded on every visit. `inline=1` opts out of the signature."""
    client, asked, _ = ctx
    r = client.get(f"/api/versions/{VER}/download/_geojson?inline=1")
    assert r.status_code == 307
    assert r.headers["location"] == f"https://files.test/{storage.key_of(GEO_KEY)}"
    assert asked == []
