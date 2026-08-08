"""The admin datasets tab is paginated and searchable server-side.

GET /api/admin/datasets replaces the tab's old "fetch the entire catalog from
the public GET /api/datasets" load (~1,100 rows / 1MB in one shot). These tests
pin the two things that make that safe: the page slice + total are consistent,
and the free-text search really does span title / organization / tag / storage
plan — including the derived storage target, whose SQL twin (_storage_target_expr)
must stay in lockstep with storage_target_of in app/api/datasets.py.

Runs on in-memory SQLite (conftest maps JSONB → JSON), driven through a bare
FastAPI app carrying only the admin router, in the repo's dependency-light style.
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

_TABLES = [
    Organization.__table__,
    User.__table__,
    TrackedDataset.__table__,
    Tag.__table__,
    dataset_tags,
    VersionIndex.__table__,
]


def _ds(title, **kw):
    return TrackedDataset(
        id=uuid.uuid4(),
        ckan_id=kw.pop("ckan_id", f"id-{title}"),
        ckan_name=kw.pop("ckan_name", f"name-{title}"),
        title=title,
        poll_interval=3600,
        is_active=True,
        status=kw.pop("status", "active"),
        **kw,
    )


@pytest.fixture()
def client():
    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def setup():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all, tables=_TABLES)
        async with Session() as db:
            org = Organization(id=uuid.uuid4(), name="mot", title="משרד התחבורה")
            db.add(org)
            tag = Tag(id=uuid.uuid4(), name="תחבורה")
            db.add(tag)
            rows = [
                _ds("קווי אוטובוס", organization="mot", organization_id=org.id,
                    source_type="ckan",
                    scraper_config={"storage_backend": "r2", "archive_neon": True}),
                _ds("שכבות מיפוי", organization="govmap", source_type="govmap",
                    scraper_config={"upload_mode": "local_only"}),
                _ds("מרשם רכב", source_type="ckan", storage_mode="append_only",
                    scraper_config={"storage_backend": "neon", "append_key": "x"}),
                # Two DIFFERENT scraped sites. Under the old source_type filter
                # both were just "scraper" and neither could be isolated.
                _ds("חוזרי מנכ\"ל", ckan_id="mankal-scraper-1", source_type="scraper",
                    scraper_config={"storage_backend": "r2"}),
                _ds("מכרזי הרשות", ckan_id="jda-scraper-1", source_type="scraper",
                    storage_mode="append_only",
                    scraper_config={"storage_backend": "r2"}),
                _ds("ישיבת ועדה", ckan_name="knesset-committee-single-42"),
                _ds("מאגר שנדחה", status="pending"),
            ]
            db.add_all(rows)
            await db.flush()
            await db.execute(
                dataset_tags.insert().values(dataset_id=rows[1].id, tag_id=tag.id)
            )
            db.add(VersionIndex(
                id=uuid.uuid4(), tracked_dataset_id=rows[0].id, version_number=1,
                metadata_modified="2026-07-01T00:00:00",
            ))
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


def _titles(payload):
    return {i["title"] for i in payload["items"]}


_ALL = {"קווי אוטובוס", "שכבות מיפוי", "מרשם רכב", "חוזרי מנכ\"ל", "מכרזי הרשות"}


def test_lists_only_administrable_active_datasets(client):
    r = client.get("/api/admin/datasets")
    assert r.status_code == 200, r.text
    body = r.json()
    # The pending row and the bulk-managed per-meeting Knesset row are excluded,
    # exactly as they are from the public catalog.
    assert body["total"] == 5
    assert _titles(body) == _ALL


def test_page_slice_and_total_are_consistent(client):
    first = client.get("/api/admin/datasets?limit=3&offset=0").json()
    second = client.get("/api/admin/datasets?limit=3&offset=3").json()
    assert first["total"] == second["total"] == 5
    assert len(first["items"]) == 3 and len(second["items"]) == 2
    # No row served twice across the two pages.
    assert not (_titles(first) & _titles(second))


def test_search_matches_title_org_and_tag(client):
    assert _titles(client.get("/api/admin/datasets?q=אוטובוס").json()) == {"קווי אוטובוס"}
    # Organization title, not just the slug.
    assert _titles(client.get("/api/admin/datasets?q=התחבורה").json()) == {"קווי אוטובוס"}
    # Tag name — and the dataset appears exactly once.
    tagged = client.get("/api/admin/datasets?q=תחבורה").json()
    assert tagged["total"] == 2  # the org-title match + the tagged row
    assert _titles(tagged) == {"קווי אוטובוס", "שכבות מיפוי"}


def test_search_matches_storage_plan_keywords(client):
    # "neon" covers the NEON-only plan and the r2+neon combo.
    assert _titles(client.get("/api/admin/datasets?q=neon").json()) == {
        "קווי אוטובוס", "מרשם רכב",
    }
    assert _titles(client.get("/api/admin/datasets?q=מקומי").json()) == {"שכבות מיפוי"}
    assert _titles(client.get("/api/admin/datasets?q=תוספת").json()) == {
        "מרשם רכב", "מכרזי הרשות",
    }


def test_storage_filter_is_exact(client):
    assert _titles(client.get("/api/admin/datasets?storage=r2%2Bneon").json()) == {
        "קווי אוטובוס",
    }
    # An unescaped "+" is a SPACE by the time it reaches us; a plan name never
    # contains one, so it is read back as the plus it must have been rather
    # than quietly matching nothing.
    assert _titles(client.get("/api/admin/datasets?storage=r2+neon").json()) == {
        "קווי אוטובוס",
    }
    assert _titles(client.get("/api/admin/datasets?storage=local").json()) == {"שכבות מיפוי"}
    # A storage MODE on the target param still works — the old single dropdown
    # spoke that way, and bookmarked admin URLs still do.
    assert _titles(client.get("/api/admin/datasets?storage=append_only").json()) == {
        "מרשם רכב", "מכרזי הרשות",
    }
    assert client.get("/api/admin/datasets?storage=odata").json()["total"] == 0


def test_source_type_filter_and_version_count(client):
    body = client.get("/api/admin/datasets?source_type=govmap").json()
    assert _titles(body) == {"שכבות מיפוי"}
    counts = {
        i["title"]: i["version_count"]
        for i in client.get("/api/admin/datasets").json()["items"]
    }
    assert counts["קווי אוטובוס"] == 1
    assert counts["שכבות מיפוי"] == 0


def test_source_filter_isolates_one_scraped_site(client):
    """The distinction the old source_type filter threw away.

    "scraper" is an engine kind, not a source: mankal and jda are different
    sites that happen to share it, and the tab offered no way to see one
    without the other.
    """
    assert _titles(client.get("/api/admin/datasets?source=mankal").json()) == {
        "חוזרי מנכ\"ל",
    }
    assert _titles(client.get("/api/admin/datasets?source=jda").json()) == {"מכרזי הרשות"}
    # Non-scraper sources are keyed by their source_type, and a scraper row
    # must never leak into them.
    assert _titles(client.get("/api/admin/datasets?source=ckan").json()) == {
        "קווי אוטובוס", "מרשם רכב",
    }
    assert _titles(client.get("/api/admin/datasets?source=govmap").json()) == {"שכבות מיפוי"}


def test_storage_target_and_mode_are_independent_axes(client):
    """One dropdown for both made "append-only, on NEON" unaskable."""
    both = client.get("/api/admin/datasets?storage=r2&mode=append_only").json()
    assert _titles(both) == {"מכרזי הרשות"}
    # Each axis alone still works, and the mode axis spans sources.
    assert _titles(client.get("/api/admin/datasets?mode=append_only").json()) == {
        "מרשם רכב", "מכרזי הרשות",
    }
    assert _titles(client.get("/api/admin/datasets?storage=r2").json()) == {
        "חוזרי מנכ\"ל", "מכרזי הרשות",
    }


def test_facet_expressions_type_their_constants_for_postgres():
    """Every constant used as a VALUE must be cast, or production 500s.

    asyncpg sends each bound value as an untyped parameter and lets Postgres
    infer it. Compared against a column ("… = $1") that always works, which is
    why every filter kept working. But the facet counts SELECT and GROUP BY the
    derived storage plan, and there a bare "THEN $1" has nothing to infer from:
    Postgres answers "could not determine data type of parameter" and the whole
    endpoint dies.

    No SQLite test can catch this — SQLite types a parameter from the value it
    was handed — so this one reads the emitted Postgres SQL instead.
    """
    from sqlalchemy import func as sa_func, select as sa_select
    from sqlalchemy.dialects import postgresql

    from app.api.admin import _storage_mode_expr, _storage_target_expr

    for expr in (_storage_target_expr(), _storage_mode_expr()):
        sql = str(
            sa_select(expr, sa_func.count())
            .select_from(TrackedDataset)
            .group_by(expr)
            .compile(dialect=postgresql.dialect())
        )
        assert "THEN %(" not in sql, sql          # CASE branch
        assert "|| %(" not in sql, sql            # concat operand
        assert ", %(param" not in sql, sql        # coalesce default


def _facet(payload, dimension):
    return {f["value"]: f["count"] for f in payload[dimension]}


def test_facets_enumerate_every_real_source(client):
    body = client.get("/api/admin/dataset-facets").json()
    # Every upstream site present in the catalog, not four hardcoded options.
    assert _facet(body, "sources") == {"ckan": 2, "govmap": 1, "jda": 1, "mankal": 1}
    assert _facet(body, "storage_targets") == {"r2+neon": 1, "local": 1, "neon": 1, "r2": 2}
    assert _facet(body, "storage_modes") == {"full_snapshot": 3, "append_only": 2}
    assert body["total"] == 5


def test_facets_are_ordered_biggest_first(client):
    body = client.get("/api/admin/dataset-facets").json()
    counts = [f["count"] for f in body["sources"]]
    assert counts == sorted(counts, reverse=True)


def test_each_facet_is_counted_with_its_own_filter_lifted(client):
    """The rule that makes a count a promise.

    With mode=append_only applied, the SOURCE list must still show every source
    (narrowed by the mode), while the MODE list must still show both modes —
    otherwise picking one option would collapse its own dropdown to itself and
    there'd be no way back.
    """
    body = client.get("/api/admin/dataset-facets?mode=append_only").json()
    assert _facet(body, "sources") == {"ckan": 1, "jda": 1}
    assert _facet(body, "storage_modes") == {"full_snapshot": 3, "append_only": 2}
    assert body["total"] == 2

    # And the source axis behaves the same way in reverse.
    body = client.get("/api/admin/dataset-facets?source=jda").json()
    assert _facet(body, "sources") == {"ckan": 2, "govmap": 1, "jda": 1, "mankal": 1}
    assert _facet(body, "storage_modes") == {"append_only": 1}
    assert body["total"] == 1


def test_facet_counts_match_what_the_list_returns(client):
    """A count that doesn't equal the resulting page is a lie the admin acts on."""
    facets = client.get("/api/admin/dataset-facets").json()
    for f in facets["sources"]:
        listed = client.get(f"/api/admin/datasets?source={f['value']}").json()
        assert listed["total"] == f["count"], f["value"]
    for f in facets["storage_targets"]:
        listed = client.get(f"/api/admin/datasets?storage={f['value']}").json()
        assert listed["total"] == f["count"], f["value"]
