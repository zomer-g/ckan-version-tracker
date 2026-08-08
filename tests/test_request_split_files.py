"""One picked Excel → one independent dataset.

The scraper twin of ``split_resources``. A CBS publication page is a folder of
up to 110 unrelated tables, and tracking it as ONE dataset makes "אוכלוסייה
ביישובים, 2024" resource #37 of something named after a page — with no version
history of its own and, past a payload ceiling, no SQL table at all. With
``split_files`` each ticked file becomes its own TrackedDataset, classified by
the registry from its own URL.

Also pins the rules that make a fifty-file submit survivable: a file already
tracked is reported as a duplicate rather than created twice, and one
unrecognised URL does not throw away the rest.
"""
import asyncio
import os
import sys

os.environ.setdefault("JWT_SECRET_KEY", "test")
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import (  # noqa: E402
    AsyncSession, async_sessionmaker, create_async_engine,
)

from app.api import datasets as datasets_api  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.organization import Organization  # noqa: E402
from app.models.source_registry import SourceRegistry  # noqa: E402
from app.models.tag import Tag, dataset_tags  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.models.version_index import VersionIndex  # noqa: E402
from app.rate_limit import limiter, rate_limit_exceeded_handler  # noqa: E402
from app.services import source_registry as sr  # noqa: E402

PAGE = "https://toyfiles.example.org/he/pubs/Pages/2019/report.aspx"
F1 = "https://toyfiles.example.org/he/pubs/DocLib/2019/a/table-2024.xlsx"
F2 = "https://toyfiles.example.org/he/pubs/DocLib/2019/a/table-2023.xlsx"
F3 = "https://toyfiles.example.org/he/pubs/DocLib/2019/a/%D7%9C%D7%95%D7%97.xlsx"

MANIFEST = {
    "manifest_version": 1,
    "id": "toyfiles",
    "label_he": "מקור צעצוע", "label_en": "Toy Files",
    "site_url": "https://toyfiles.example.org/",
    "badge": {"bg": "#fae8ff", "fg": "#86198f", "accent": "#c026d3"},
    "neon_eligible": True,
    "file_picker": True,
    "default_config": {"download_files": True},
    "url_patterns": [
        {"regex": r"^https?://toyfiles\.example\.org/(?P<lang>he|en)/(?P<sec>.+?)"
                  r"/Pages/(?P<year>\d{4})/(?P<slug>[^/?#]+)\.aspx$",
         "page_type": "toyfiles_page",
         "config": {"page": "{lang}/{sec}/{year}/{slug|unquote}"}},
        {"regex": r"^https?://toyfiles\.example\.org/(?P<lang>he|en)/(?P<sec>.+?)"
                  r"/DocLib/(?P<file>.+\.xlsx)$",
         "page_type": "toyfiles_file",
         "title_he": "מקור צעצוע — קובץ",
         "config": {"doc": "{lang}/{sec}/{file|unquote}"}},
    ],
}


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def stack(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "min_poll_interval", 900)
    monkeypatch.setattr(settings, "odata_api_key", "")
    sr.invalidate_cache()

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{(tmp_path / 'split.sqlite').as_posix()}")

    async def _create():
        async with engine.begin() as conn:
            await conn.run_sync(lambda c: Organization.__table__.create(c))
            await conn.run_sync(lambda c: TrackedDataset.__table__.create(c))
            await conn.run_sync(lambda c: Tag.__table__.create(c))
            await conn.run_sync(lambda c: dataset_tags.create(c))
            await conn.run_sync(lambda c: SourceRegistry.__table__.create(c))
            # find_datasets_for_url counts versions per candidate, so the
            # duplicate check needs this table even when it is empty.
            await conn.run_sync(lambda c: VersionIndex.__table__.create(c))

    _run(_create())
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _seed():
        async with Session() as db:
            db.add(SourceRegistry(id=MANIFEST["id"], manifest=MANIFEST,
                                  enabled=True,
                                  manifest_hash=sr.manifest_hash(MANIFEST)))
            await db.commit()

    _run(_seed())

    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)
    app.include_router(datasets_api.router)

    async def _db():
        async with Session() as session:
            yield session

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    yield TestClient(app, raise_server_exceptions=False), Session
    sr.invalidate_cache()


def _rows(Session):
    async def _load():
        async with Session() as db:
            return (await db.execute(
                select(TrackedDataset).order_by(TrackedDataset.source_url)
            )).scalars().all()

    return _run(_load())


def _post(client, **extra):
    body = {"source_type": "scraper", "source_url": PAGE,
            "title": "עמוד פרסום", "preferred_interval": 86400}
    body.update(extra)
    return client.post("/api/datasets/requests", json=body)


def test_each_picked_file_becomes_its_own_dataset(stack):
    client, Session = stack
    r = _post(client, split_files=True, selected_files=[F1, F2, F3])
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 3

    rows = _rows(Session)
    assert len(rows) == 3
    # Each dataset points at ONE file — that is what gives it its own version
    # history and its own SQL table.
    assert {d.source_url for d in rows} == {F1, F2, F3}
    assert all((d.scraper_config or {}).get("kind") == "toyfiles" for d in rows)
    # A manifest that calls itself neon_eligible must carry the dual-write
    # opt-in into the config, or the rows are archived as files and never
    # reach the SQL console.
    assert all((d.scraper_config or {}).get("archive_neon") for d in rows)
    # Each carries the FILE's config, not the page's — the identity that keeps
    # them from collapsing back into one dataset.
    assert len({(d.scraper_config or {}).get("doc") for d in rows}) == 3
    assert all(d.status == "pending" for d in rows)
    assert all(d.poll_interval == 86400 for d in rows)


def test_each_dataset_is_named_so_the_queue_can_tell_them_apart(stack):
    """Fifty pending cards all reading "הלמ״ס — קובץ נתונים" are unusable: the
    only thing distinguishing them was the URL. The picker's own label wins;
    failing that the manifest at least renders the filename."""
    client, Session = stack
    r = _post(client, split_files=True, selected_files=[F1, F2],
              file_titles={F1: "קובץ נתונים לעיבוד - 2024"})
    assert r.status_code == 201, r.text
    titles = {d.source_url: d.title for d in _rows(Session)}
    assert titles[F1] == "קובץ נתונים לעיבוד - 2024"      # the picker's label
    assert titles[F2] == "מקור צעצוע — קובץ"               # the manifest fallback
    assert len(set(titles.values())) == 2


def test_a_blank_or_absent_label_falls_back_instead_of_blanking_the_title(stack):
    client, Session = stack
    _post(client, split_files=True, selected_files=[F1, F2],
          file_titles={F1: "   ", "https://unrelated.example.org/x": "ignored"})
    assert all(d.title for d in _rows(Session))


def test_the_page_itself_is_not_tracked_when_splitting(stack):
    """Splitting means the files are the datasets. Opening the page as well
    would re-import every one of them a second time."""
    client, Session = stack
    _post(client, split_files=True, selected_files=[F1, F2])
    assert all(d.source_url != PAGE for d in _rows(Session))


def test_a_file_already_tracked_is_reported_not_duplicated(stack):
    client, Session = stack
    _post(client, split_files=True, selected_files=[F1])
    r = _post(client, split_files=True, selected_files=[F1, F2])
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 1
    statuses = {x["url"]: x["status"] for x in r.json()["results"]}
    assert statuses[F1] == "duplicate" and statuses[F2] == "pending"
    assert len(_rows(Session)) == 2


def _set_status(Session, url, status):
    async def _go():
        async with Session() as db:
            row = (await db.execute(
                select(TrackedDataset).where(TrackedDataset.source_url == url)
            )).scalar_one()
            row.status = status
            await db.commit()

    _run(_go())


def test_rejecting_a_batch_does_not_make_those_files_unaddable(stack):
    """Rejecting is how someone says "not these" — often, as it happened, because
    27 identically-titled cards looked like junk. It used to leave every one of
    those files permanently un-addable: the queue was empty, and every new
    request was answered "already tracked" about a row nothing would ever
    scrape. Re-requesting re-opens that row."""
    client, Session = stack
    _post(client, split_files=True, selected_files=[F1, F2])
    _set_status(Session, F1, "rejected")

    r = _post(client, split_files=True, selected_files=[F1],
              file_titles={F1: "קובץ נתונים לעיבוד - 2024"})
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 1
    assert r.json()["results"][0]["reopened"] is True

    rows = {d.source_url: d for d in _rows(Session)}
    # Re-opened in place — a SECOND row for the same URL would be a real
    # duplicate, and the two would fight over the same corpus.
    assert len(rows) == 2
    assert rows[F1].status == "pending"
    # Renamed from this request: the bad name is usually why it was rejected.
    assert rows[F1].title == "קובץ נתונים לעיבוד - 2024"


def test_an_active_dataset_still_blocks(stack):
    """Only 'rejected' is revivable. A live dataset is a real duplicate."""
    client, Session = stack
    _post(client, split_files=True, selected_files=[F1])
    _set_status(Session, F1, "active")
    r = _post(client, split_files=True, selected_files=[F1])
    assert r.json()["created"] == 0
    assert r.json()["results"][0]["status"] == "duplicate"


def test_everything_already_tracked_is_an_answer_not_a_failure(stack):
    """Re-submitting a page whose files are sitting in the approval queue used
    to paint a red "None of the picked files could be opened" over a correct
    result, and read as the picker being broken. The per-file results say what
    happened; only a submit where nothing was even RECOGNISED still fails."""
    client, _ = stack
    _post(client, split_files=True, selected_files=[F1, F2])
    r = _post(client, split_files=True, selected_files=[F1, F2])
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 0
    assert {x["status"] for x in r.json()["results"]} == {"duplicate"}


def test_a_submit_of_nothing_recognisable_still_fails_loudly(stack):
    client, _ = stack
    r = _post(client, split_files=True,
              selected_files=["https://elsewhere.example.org/x.xlsx"])
    assert r.status_code == 400
    assert "know" in r.json()["detail"]


def test_one_bad_url_does_not_sink_the_rest(stack):
    """With fifty files ticked, a single unrecognised entry must not throw away
    the other forty-nine."""
    client, Session = stack
    r = _post(client, split_files=True,
              selected_files=[F1, "https://elsewhere.example.org/x.xlsx", "", F2])
    assert r.status_code == 201, r.text
    assert r.json()["created"] == 2
    assert len(_rows(Session)) == 2
    assert {x["status"] for x in r.json()["results"]} == {"pending", "invalid"}


def test_splitting_with_nothing_picked_is_refused(stack):
    """Rather than silently falling back to tracking the whole page, which is
    the shape the user just asked not to have."""
    client, _ = stack
    assert _post(client, split_files=True, selected_files=[]).status_code == 400
    assert _post(client, split_files=True).status_code == 400


def test_without_split_the_page_is_still_one_dataset(stack):
    """The old shape stays available: one dataset for the page, the picked
    files as resources inside it."""
    client, Session = stack
    r = _post(client, selected_files=["/he/pubs/DocLib/2019/a/table-2024.xlsx"])
    assert r.status_code == 201, r.text
    rows = _rows(Session)
    assert len(rows) == 1 and rows[0].source_url == PAGE
    assert rows[0].scraper_config["files"] == [
        "/he/pubs/DocLib/2019/a/table-2024.xlsx"]
