"""Search facets — content fields and file formats — on the home list, the
public API and the MCP's search_datasets (app/services/dataset_facets.py).

The three surfaces must answer the same question the same way: content fields
AND together, file formats OR together, unknown names are refused rather than
silently matching nothing. Runs on in-memory SQLite (conftest maps JSONB →
JSON), so the SQL filter is exercised through its portable branch; the Postgres
branch is the same predicate written as containment.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api.home import router as home_router  # noqa: E402
from app.api.v1 import router as v1_router  # noqa: E402
from app.database import Base, get_db  # noqa: E402
from app.mcp import server as mcp  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.page_content import PageContent  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.user import User  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import dataset_facets as df  # noqa: E402
from app.services import field_flags as ff  # noqa: E402
from app.services import home_catalog  # noqa: E402


# ── deriving formats from a version ─────────────────────────────────────────

def test_formats_come_from_stored_keys_declared_keys_and_the_change_summary():
    mappings = {
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
        # The row-archive's hash list is never a file, whatever it contains.
        "_appendonly_seen": ["x.pdf"],
        "נתוני הסורק": "r2:datasets/d/v1/1a2b3c4d_file.csv",
        "_geojson": ["r2:datasets/d/v1/aaaaaaaa_file.geojson.gz"],
        # GovMap's style bundle is not the data.
        "_symbology": ["r2:datasets/d/v1/bbbbbbbb_symbology.zip"],
        # A legacy ODATA id says nothing; the resource NAME does.
        "דוח שנתי.xlsx": "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
    }
    cs = {"resources": [{"name": "קבצים", "format": "PDF"}]}
    assert df.formats_of_version(mappings, cs) == {"csv", "geojson", "xlsx", "pdf"}


def test_zip_archives_and_a_geopackage_count():
    mappings = {"_zip_parts": ["r2:a/1_part1.zip", "r2:a/2_part2.zip"],
                "_gpkg": ["r2:a/3_layer.gpkg"]}
    assert df.formats_of_version(mappings, None) == {"zip", "gpkg"}


def test_declared_formats_normalize_and_unknowns_drop():
    assert df.format_code("ESRI Shapefile") == "shp"
    assert df.format_code("XLS") == "xlsx"
    assert df.format_code("application/x-unknown") is None
    assert df.formats_of_version({}, {"resources_modified": [{"format": "weird"}]}) == set()


def test_parsing_accepts_aliases_and_refuses_typos():
    assert df.parse_content_fields(["has_locality", "date,locality"]) == ["locality", "date"]
    assert df.parse_file_formats(["CSV,xls", "pdf"]) == ["csv", "xlsx", "pdf"]
    with pytest.raises(ValueError):
        df.parse_content_fields(["settlement"])
    with pytest.raises(ValueError):
        df.parse_file_formats(["exe"])


def test_spatial_flag_takes_a_postgis_column_whatever_its_name():
    cols = {"d": [{"name": "shape_col", "type": "geometry"}]}
    out = asyncio.run(ff.compute_for_columns(cols, ("has_geometry",)))
    assert out["d"] == {"has_geometry": True}


# ── the three surfaces ──────────────────────────────────────────────────────

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)

# title -> (field_flags, file_formats)
_FIXTURE = {
    "אוכלוסייה לפי יישוב": ({"has_locality": True, "has_date": True}, ["csv"]),
    "תקציבי רשויות": ({"has_authority": True, "has_date": True}, ["xlsx"]),
    "מקלטים": ({"has_geometry": True, "has_locality": True}, ["csv", "geojson"]),
    "חוזרי מנכ\"ל": ({}, ["pdf"]),
    "בלי סיווג": ({}, []),
}


def _ds(title, n, flags, formats):
    return TrackedDataset(
        id=uuid.uuid4(), ckan_id=f"id-{n}", ckan_name=f"name-{n}", title=title,
        poll_interval=3600, is_active=True, status="active", source_type="scraper",
        source_url="https://example.gov.il", created_at=_T0 - timedelta(minutes=n),
        field_flags=flags, file_formats=formats,
    )


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setattr(mcp, "base_url", lambda request: "https://over.org.il")
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=[
                Organization.__table__, User.__table__, TrackedDataset.__table__,
                Tag.__table__, dataset_tags, VersionIndex.__table__, PageContent.__table__])
        async with Session() as db:
            db.add_all([_ds(t, i, f, fm) for i, (t, (f, fm)) in enumerate(_FIXTURE.items())])
            await db.commit()

    asyncio.run(setup())

    async def _db():
        async with Session() as db:
            yield db

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(home_router)
    app.include_router(v1_router)
    app.dependency_overrides[get_db] = _db
    limiter.reset()
    home_catalog.cache.clear()

    def mcp_search(args):
        async def go():
            async with Session() as db:
                return (await mcp._tool_search_datasets(_Req(), db, None, args))[0]
        return asyncio.run(go())

    yield TestClient(app), mcp_search
    home_catalog.cache.clear()


class _Req:
    headers = {}
    url = type("U", (), {"scheme": "https", "netloc": "over.org.il"})()
    base_url = "https://over.org.il/"


def _home(client, **params):
    r = client.get("/api/home/datasets", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _v1(client, **params):
    r = client.get("/api/v1/datasets", params=params)
    assert r.status_code == 200, r.text
    return r.json()


def _titles(body):
    return sorted(i["title"] for i in body["items"])


# (home params, v1 params, mcp args, expected titles)
_CASES = [
    ({"field": "locality"}, {"has_field": "locality"}, {"content_fields": ["locality"]},
     ["אוכלוסייה לפי יישוב", "מקלטים"]),
    # Fields AND.
    ({"field": ["locality", "date"]}, {"has_field": ["locality", "date"]},
     {"content_fields": ["locality", "date"]}, ["אוכלוסייה לפי יישוב"]),
    ({"field": "geometry"}, {"has_field": "geometry"}, {"content_fields": ["geometry"]},
     ["מקלטים"]),
    # Formats OR.
    ({"format": ["xlsx", "pdf"]}, {"file_format": "xlsx,pdf"}, {"file_formats": ["xlsx", "pdf"]},
     ["חוזרי מנכ\"ל", "תקציבי רשויות"]),
    # Both: a field AND any of the formats.
    ({"field": "date", "format": "csv"}, {"has_field": "date", "file_format": "csv"},
     {"content_fields": ["date"], "file_formats": ["csv"]}, ["אוכלוסייה לפי יישוב"]),
]


@pytest.mark.parametrize("home_p,v1_p,mcp_a,expected", _CASES)
def test_every_surface_answers_the_same(env, home_p, v1_p, mcp_a, expected):
    client, mcp_search = env
    assert _titles(_home(client, **home_p)) == expected
    v1 = _v1(client, **v1_p)
    assert _titles(v1) == expected and v1["total"] == len(expected)
    found = mcp_search(mcp_a)
    assert _titles(found) == expected and found["total"] == len(expected)


def test_results_say_which_fields_and_formats_a_dataset_has(env):
    client, mcp_search = env
    want = {"content_fields": ["locality", "geometry"], "file_formats": ["csv", "geojson"]}
    for body in (_home(client, q="מקלטים"), _v1(client, has_field="geometry"),
                 mcp_search({"query": "מקלטים"})):
        item = body["items"][0]
        assert {k: item[k] for k in want} == want


def test_home_facet_counts_follow_the_selection(env):
    client, _ = env
    facets = _home(client)["facets"]
    assert facets["content_fields"]["locality"] == 2
    assert facets["content_fields"]["date"] == 2
    assert facets["file_formats"]["csv"] == 2 and facets["file_formats"]["pdf"] == 1
    # With "date" chosen, a field count says what adding it would leave, and a
    # format count says what that format adds within the date datasets.
    narrowed = _home(client, field="date")["facets"]
    assert narrowed["content_fields"]["locality"] == 1
    assert narrowed["file_formats"] == {**{c: 0 for c in df.FILE_FORMATS}, "csv": 1, "xlsx": 1}


def test_unknown_names_are_refused_not_ignored(env):
    client, mcp_search = env
    assert client.get("/api/home/datasets", params={"field": "settlement"}).status_code == 400
    assert client.get("/api/v1/datasets", params={"file_format": "exe"}).status_code == 400
    with pytest.raises(ValueError):
        mcp_search({"content_fields": ["settlement"]})


def test_mcp_takes_an_array_sent_as_its_json_text(env):
    _, mcp_search = env
    found = mcp_search({"content_fields": '["locality", "date"]', "file_formats": "csv"})
    assert _titles(found) == ["אוכלוסייה לפי יישוב"]


def test_mcp_schema_lists_exactly_the_shared_vocabulary():
    props = next(t for t in mcp.TOOLS if t["name"] == "search_datasets")["inputSchema"]["properties"]
    assert props["content_fields"]["items"]["enum"] == list(df.CONTENT_FIELDS)
    assert props["file_formats"]["items"]["enum"] == list(df.FILE_FORMATS)
