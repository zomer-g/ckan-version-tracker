"""One classifier decides what a failed resource download means.

Three ways a failure was misread, all measured on prod 2026-10-31:

  * walled files of DATASTORE-backed resources on data.gov.il's hosts (water
    quality history ×5, מדידות איכות במעיינות, אירועים לפי מחוז …) were raw
    "403 Forbidden" errors instead of being handed to the browser worker —
    the old inline rule only looked at non-datastore resources;
  * a resource that is a LINK to another site's web page (the Knesset's
    lawlaws.aspx) was read as walled, queued for the browser, refused by the
    worker every day, and aborted the version as 0/1;
  * a publisher host that no longer exists surfaced as "[Errno -2] Name or
    service not known".
"""
import asyncio
import os
import socket
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import httpx  # noqa: E402
import pytest  # noqa: E402

from app.services import blocked_resources as br  # noqa: E402
from app.services import version_detector as vd  # noqa: E402


def _403(url):
    req = httpx.Request("GET", url)
    return httpx.HTTPStatusError(f"Client error '403 Forbidden' for url '{url}'",
                                 request=req, response=httpx.Response(403, request=req))


def _html():
    return RuntimeError("Got HTML instead of data — likely blocked by IAP/auth")


def _dns(url):
    try:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
    except socket.gaierror as g:
        try:
            raise httpx.ConnectError("[Errno -2] Name or service not known",
                                     request=httpx.Request("GET", url)) from g
        except httpx.ConnectError as e:
            return e


def _detect(resources, fail, old=None):
    async def fake_download(url, resource_id="", max_bytes=None):
        raise fail(url)

    real = vd.ckan_client.download_resource
    vd.ckan_client.download_resource = fake_download
    external: set[str] = set()
    try:
        out = asyncio.run(vd.detect_resource_changes(old, resources, external_ids=external))
    finally:
        vd.ckan_client.download_resource = real
    return (*out, external)


# --- Problem 1: data.gov.il's wall, whatever the datastore flag says ----------

@pytest.mark.parametrize("url", [
    "https://e.data.gov.il/dataset/x/resource/y/download/water-1990.csv",
    "https://aws-e.data.gov.il/dataset/x/resource/y/download/water-2000.csv",
    "https://data.gov.il/dataset/x/resource/y/download/events.xlsx",
])
@pytest.mark.parametrize("datastore", [True, False])
def test_a_403_from_data_gov_il_is_the_wall(url, datastore):
    r = {"id": "r", "url": url, "format": "CSV", "datastore_active": datastore}
    assert br.classify_download_failure(r, _403(url)) == br.BLOCKED
    assert br.classify_download_failure(r, _html()) == br.BLOCKED


def test_a_redirect_onto_data_gov_il_counts_by_where_it_landed():
    r = {"id": "r", "url": "https://example.org/f.csv", "datastore_active": True}
    assert br.classify_download_failure(
        r, _403("https://aws-e.data.gov.il/f.csv")) == br.BLOCKED


def test_a_walled_datastore_resource_is_blocked_not_an_error():
    """ckan_client tried the datastore first; reaching the file's 403 means the
    rows did not come through, so this is a hole the worker must fill."""
    res = [{"id": "w", "name": "הסטוריית נתוני איכות מים בקידוחים 1990", "format": "CSV",
            "datastore_active": True,
            "url": "https://aws-e.data.gov.il/dataset/q/resource/w/download/w.csv"}]
    changed, hashes, errors, blocked, external = _detect(res, _403)
    assert blocked == {"w"}
    assert errors == []
    assert external == set()
    assert br.describe(res, blocked)[0]["id"] == "w"  # → blocked_resources → task


def test_an_ordinary_failure_is_still_an_error():
    r = {"id": "r", "url": "https://e.data.gov.il/x.csv"}
    assert br.classify_download_failure(r, httpx.ReadTimeout("timed out")) is None


# --- Problem 2: a link to another site's web page ------------------------------

KNESSET = {"id": "k", "name": "lawlaws.aspx?t=lawlaws&st=lawlaws", "format": "",
           "url": "https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/"
                  "lawlaws.aspx?t=lawlaws&st=lawlaws"}


def test_a_knesset_page_is_an_external_link_not_a_blocked_file():
    assert br.is_external_page(KNESSET)
    assert br.classify_download_failure(KNESSET, _html()) == br.EXTERNAL_LINK
    assert br.classify_download_failure(
        KNESSET, _403(KNESSET["url"])) == br.EXTERNAL_LINK


def test_a_gov_il_page_is_an_external_link():
    r = {"id": "g", "format": "HTML", "datastore_active": True,
         "url": "https://www.gov.il/he/pages/national_infrastructure_plans"}
    assert br.classify_download_failure(r, _403(r["url"])) == br.EXTERNAL_LINK


@pytest.mark.parametrize("r", [
    {"url": "https://e.data.gov.il/dataset/x/page.aspx"},          # data.gov.il is never external
    {"url": "https://www.mot.gov.il/files/roads.zip"},             # a file by extension
    {"url": "https://api.example.org/download?id=7", "format": "CSV"},  # a file by format
    {"url": ""},
])
def test_files_are_not_external_pages(r):
    assert not br.is_external_page({"id": "x", **r})


def test_detection_reports_the_link_without_blocking_or_failing():
    res = [KNESSET, {"id": "f", "name": "קובץ", "format": "ZIP",
                     "url": "https://e.data.gov.il/f.zip"}]
    changed, hashes, errors, blocked, external = _detect(
        res, lambda url: _html(), old={"_hashes": {"k": "prev"}})
    assert external == {"k"}
    assert blocked == {"f"}
    assert errors == []
    assert hashes["k"] == "prev"  # a skip is not a change
    note = br.external_note(res, external)
    assert br.EXTERNAL_LINK_LABEL in note and "main.knesset.gov.il" in note


def test_an_external_link_is_never_listed_for_the_worker():
    """The row-archive path lists file resources without downloading them;
    the Knesset link landed there too, and the worker refused it daily."""
    files = br.file_resources([KNESSET, {"id": "s", "url": "https://e.data.gov.il/s.shp"}])
    assert [r["id"] for r in files] == ["s"]


# --- Problem 3: the publisher's host is gone ------------------------------------

def test_a_dead_host_reads_as_a_dead_link_in_hebrew():
    res = [{"id": "n", "name": "אתרים לאומיים", "format": "XLSX",
            "url": "https://old-publisher.example.gov.il/sites.xlsx"}]
    changed, hashes, errors, blocked, external = _detect(res, _dns)
    assert blocked == set() and external == set()
    [err] = errors
    assert br.DEAD_HOST_MARKER in err
    assert "old-publisher.example.gov.il" in err
    assert "Errno" not in err
    assert hashes["n"] == "download_failed"


def test_only_permanent_resolver_answers_count_as_dead():
    assert br.is_dead_host(httpx.ConnectError("[Errno -2] Name or service not known"))
    assert not br.is_dead_host(
        httpx.ConnectError("[Errno -3] Temporary failure in name resolution"))
    assert br.classify_download_failure(
        {"url": "https://x.example"}, httpx.ConnectError("Temporary failure in name resolution")) is None


# --- Through a whole poll: a package whose only resource is a page link ---------

def test_a_package_whose_only_resource_is_a_page_link_still_versions(monkeypatch):
    """Was: 'Aborting version 1 for https-main-knesset-gov-il-… — 0/1' and a
    ckan_blocked_files task the worker refused, every day."""
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from app.config import settings
    from app.models.scrape_task import ScrapeTask
    from app.models.tag import Tag, dataset_tags
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex
    from app.services import conditional_archiver
    from app.worker import poll_job

    ds_id = uuid.uuid4()
    pkg = {"id": "pkg", "name": "https-main-knesset-gov-il-laws", "title": "חוקי מדינת ישראל",
           "metadata_modified": "2026-10-01T00:00:00", "resources": [KNESSET]}

    def _async(v):
        async def _c():
            return v
        return _c()

    engine = create_async_engine("sqlite+aiosqlite://")
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _setup():
        async with engine.begin() as conn:
            for t in (Tag.__table__, dataset_tags, TrackedDataset.__table__,
                      VersionIndex.__table__, ScrapeTask.__table__):
                await conn.run_sync(lambda c, t=t: t.create(c))
        async with Session() as s:
            s.add(TrackedDataset(
                id=ds_id, ckan_id="pkg", ckan_name=pkg["name"], title=pkg["title"],
                source_type="ckan", poll_interval=86400, is_active=True,
                status="active", storage_mode="full_snapshot"))
            await s.commit()

    asyncio.run(_setup())

    async def _no_datastore(_rid):
        raise RuntimeError("no datastore")

    async def _download(url, resource_id="", max_bytes=None):
        raise _html()

    async def _snapshot(**kw):
        return None, {"_hashes": kw["hash_map"], "_resource_ids": ["k"]}, []

    monkeypatch.setattr(poll_job, "async_session", Session)
    monkeypatch.setattr(settings, "ckan_blocked_files_enabled", True)
    monkeypatch.setattr(poll_job.ckan_client, "package_show", lambda _id: _async(pkg))
    monkeypatch.setattr(poll_job.ckan_client, "datastore_info", _no_datastore)
    monkeypatch.setattr(vd.ckan_client, "download_resource", _download)
    monkeypatch.setattr(poll_job, "create_version_snapshot", _snapshot)
    monkeypatch.setattr(conditional_archiver, "try_conditional_archive",
                        lambda ds, db: _async(conditional_archiver.Result.FALLBACK))
    poll_job._active_polls.clear()

    asyncio.run(poll_job.poll_dataset(str(ds_id)))

    async def _read():
        async with Session() as s:
            ds = await s.get(TrackedDataset, ds_id)
            versions = (await s.execute(select(VersionIndex))).scalars().all()
            tasks = (await s.execute(select(ScrapeTask))).scalars().all()
            return ds, versions, tasks

    ds, versions, tasks = asyncio.run(_read())
    assert [v.version_number for v in versions] == [1]
    assert tasks == []
    assert br.stored(ds) == []
    assert br.EXTERNAL_LINK_LABEL in (ds.last_error or "")
