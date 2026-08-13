"""Downloading a spatial layer hands over its symbology too.

A GovMap layer's style belongs to the LAYER, not to the snapshot: the scraper
attaches the bundle only when it changes, so most versions hold data alone.
Serving a version's download list as-is meant "הורד הכל" gave a bare GeoJSON on
every version except the one that happened to capture the style, and there was
no single action that produced the data and the style together.

Two halves are pinned here:
  * ``_symbology`` resolves to the newest bundle the DATASET has when the asked
    version carries none;
  * that bundle also converts to an ArcGIS ``.lyrx`` on the way out, for every
    version — including the hundreds archived before the converter existed.
"""
import asyncio
import io
import os
import sys
import uuid
import zipfile
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
V1, V2, V3 = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
SYMB_KEY = f"r2:datasets/{DS}/v3/309eb7f7_symbology.zip"

SLD = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<StyledLayerDescriptor version="1.0.0" xmlns="http://www.opengis.net/sld" '
    'xmlns:ogc="http://www.opengis.net/ogc">'
    '<NamedLayer><Name>שכבה</Name><UserStyle><Title>שכבה</Title>'
    '<FeatureTypeStyle><Rule><Title>הכל</Title>'
    '<PolygonSymbolizer><Fill>'
    '<CssParameter name="fill">#73dfff</CssParameter></Fill></PolygonSymbolizer>'
    '</Rule></FeatureTypeStyle></UserStyle></NamedLayer></StyledLayerDescriptor>'
)


def _bundle() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("שכבה_173.sld", SLD)
        z.writestr("שכבה_fields.csv", "machine_name\r\nname_h\r\n")
    return buf.getvalue()


def _mappings(version: int, with_symbology: bool) -> dict:
    m = {
        "_hashes": {"scraper": "abc"},
        "_geojson": [f"r2:datasets/{DS}/v{version}/geo_{version}.gz"],
        "_resource_ids": [],
    }
    if with_symbology:
        m["_symbology"] = [SYMB_KEY]
    return m


@pytest.fixture()
def client(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            db.add(TrackedDataset(
                id=DS, ckan_id="govmap-173", ckan_name="govmap-173",
                title='אבני ק"מ', source_type="govmap",
                source_url="https://www.govmap.gov.il/?lay=173",
                poll_interval=86400, status="active",
            ))
            # v3 is the only version that carries the bundle — the common shape.
            for vid, num, symb in ((V1, 1, False), (V2, 2, False), (V3, 3, True)):
                db.add(VersionIndex(
                    id=vid, tracked_dataset_id=DS, version_number=num,
                    metadata_modified=f"2026-07-0{num}T00:00:00",
                    detected_at=datetime(2026, 7, num, tzinfo=timezone.utc),
                    resource_mappings=_mappings(num, symb), source="scraper",
                ))
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    async def _bytes(_self, key_or_value):
        return _bundle() if storage.key_of(key_or_value) == storage.key_of(SYMB_KEY) else None

    monkeypatch.setattr(storage.StorageClient, "get_object_bytes", _bytes)
    monkeypatch.setattr(storage.StorageClient, "public_url",
                        lambda _self, value: f"https://files.test/{storage.key_of(value)}")

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(versions_router)
    app.dependency_overrides[get_db] = _db
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False, follow_redirects=False)


# ── the SLD bundle ─────────────────────────────────────────────────────


def test_a_version_that_carries_the_bundle_serves_its_own(client):
    r = client.get(f"/api/versions/{V3}/download/_symbology")
    assert r.status_code == 307
    assert r.headers["location"].endswith("309eb7f7_symbology.zip")


def test_a_version_without_a_bundle_inherits_the_newest_one(client):
    # Before this, v1 and v2 404'd — the archive held the style and would not
    # hand it over next to the data it belongs with.
    r = client.get(f"/api/versions/{V1}/download/_symbology")
    assert r.status_code == 307
    assert r.headers["location"].endswith("309eb7f7_symbology.zip")


def test_an_unrelated_missing_resource_still_404s(client):
    assert client.get(f"/api/versions/{V1}/download/_gpkg").status_code == 404


# ── the ArcGIS conversion ──────────────────────────────────────────────


def test_lyrx_download_converts_the_archived_bundle(client):
    r = client.get(f"/api/versions/{V3}/symbology.lyrx.zip")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    # Hebrew filenames need RFC 5987 — the ASCII fallback alone would lose the
    # layer's name for everyone.
    assert "filename*=UTF-8''" in r.headers["content-disposition"]
    out = zipfile.ZipFile(io.BytesIO(r.content))
    assert "שכבה_173.lyrx" in out.namelist()
    assert "שכבה_fields.csv" in out.namelist()
    assert b"CIMLayerDocument" in out.read("שכבה_173.lyrx")


def test_lyrx_is_offered_on_every_version_not_only_the_capturing_one(client):
    r = client.get(f"/api/versions/{V1}/symbology.lyrx.zip")
    assert r.status_code == 200
    assert "שכבה_173.lyrx" in zipfile.ZipFile(io.BytesIO(r.content)).namelist()


def test_a_dataset_with_no_symbology_at_all_says_so(client, monkeypatch):
    async def _none(_self, _key):
        return None

    monkeypatch.setattr(storage.StorageClient, "get_object_bytes", _none)
    assert client.get(f"/api/versions/{V3}/symbology.lyrx.zip").status_code == 502


def test_an_oversized_bundle_is_refused_rather_than_loaded(client, monkeypatch):
    # Real bundles are a few KB; the ceiling exists so no single request can be
    # what fills a 512 MB instance.
    from app.api import versions as versions_api

    monkeypatch.setattr(versions_api, "MAX_CONVERTIBLE_BUNDLE_BYTES", 10)
    assert client.get(f"/api/versions/{V3}/symbology.lyrx.zip").status_code == 413


def test_an_unknown_version_is_not_a_conversion_error(client):
    assert client.get(f"/api/versions/{uuid.uuid4()}/symbology.lyrx.zip").status_code == 404
