"""A dataset's quality banner (``import_warning``) reaches API and MCP callers.

The site shows it above the data. A caller reading rows through
/api/append/…/datastore_search, the GeoJSON features API or the MCP never sees
that page, and would read a partial version — a GovMap layer short wherever
more than 500 records share one point — as complete.
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402

from app.api.append import _attach_quality_warning  # noqa: E402
from app.mcp import server as mcp  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402

WARN = "קיים קושי טכני לאסוף את כל הפריטים; באזורים מרכזיים המידע חלקי."
AT = datetime(2026, 10, 31, 12, 0, tzinfo=timezone.utc)


def _ds(title, warning=None):
    return TrackedDataset(id=uuid.uuid4(), ckan_id=title, ckan_name=title, title=title,
                          source_type="govmap", poll_interval=86400, is_active=True,
                          status="active", storage_mode="full_snapshot", source_url="u",
                          import_warning=warning, import_warning_at=AT if warning else None)


class _Req:
    headers = {}
    url = type("U", (), {"scheme": "https", "netloc": "over.org.il"})()
    base_url = "https://over.org.il/"


def test_rows_answers_carry_the_warning_only_when_there_is_one():
    res = {}
    _attach_quality_warning(res, _ds("a", WARN))
    assert res == {"import_warning": WARN, "import_warning_at": AT.isoformat()}
    clean = {}
    _attach_quality_warning(clean, _ds("b"))
    assert clean == {}


def test_mcp_search_and_get_dataset_carry_the_warning(monkeypatch):
    monkeypatch.setattr(mcp, "base_url", lambda request: "https://over.org.il")

    async def go():
        engine = create_async_engine("sqlite+aiosqlite://")
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      VersionIndex.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))
        S = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        partial, whole = _ds("חברות בע\"מ", WARN), _ds("תחנות אוטובוס")
        async with S() as db:
            db.add_all([partial, whole])
            await db.commit()
            found, _n = await mcp._tool_search_datasets(_Req(), db, None, {})
            got, _n = await mcp._tool_get_dataset(_Req(), db, None,
                                                  {"dataset_id": str(partial.id)})
            clean, _n = await mcp._tool_get_dataset(_Req(), db, None,
                                                    {"dataset_id": str(whole.id)})
        by = {i["title"]: i for i in found["items"]}
        assert by['חברות בע"מ']["import_warning"] == WARN
        assert "import_warning" not in by["תחנות אוטובוס"]
        assert got["import_warning"] == WARN and got["import_warning_at"] == AT.isoformat()
        assert clean["import_warning"] is None
    asyncio.run(go())


def test_mcp_instructions_say_to_pass_the_warning_on():
    assert "import_warning" in mcp.SERVER_INSTRUCTIONS
