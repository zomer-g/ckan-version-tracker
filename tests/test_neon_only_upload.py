"""A NEON-only dataset must be able to publish a version of ANY size.

The bug, as it reached production: מנהל התכנון — מאגר התוכניות (מבא"ת) scrapes
36,784 rows × 47 columns. The worker estimated the inline JSON at 67MB, over its
30MB threshold, so it routed the rows through the out-of-band CSV path — and
``/upload-csv`` answered ``404 "Dataset not found or no storage backend"``. The
dataset exists; it is on the NEON-only storage plan, where ``odata_dataset_id``
is null BY DESIGN and R2 is not the backend, so the endpoint's opening guard
(``ds.odata_dataset_id or _use_r2(ds)``) — written before NEON-only plans existed
— read "no backend" and refused. The worker retries only on 5xx, so the 4xx
failed instantly and was reported to the operator as a probable server OOM or a
deploy in progress. Two datasets sat at version_count 0 because of it.

The guard was only the first of two: ``push_version`` gates its whole tabular
block on the same condition, so a NEON-only dataset's rows had no path through
it at any size — inline or out-of-band — and every push fell through to the
empty-version guard.

What is locked here:

  1. ``/upload-csv`` ACCEPTS a NEON-only dataset, and touches neither ODATA nor
     R2 while doing it — there is no file to write, only rows;
  2. an ODATA dataset and an R2 dataset keep their existing answers;
  3. a dataset that stores files and genuinely has nowhere to put them is still
     refused — with a reason, and never as a 404, because it exists;
  4. a NEON-only version's ``append_table`` counts as content landing, so the
     empty-version guard doesn't discard a fully-loaded version.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from slowapi import _rate_limit_exceeded_handler  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402

from app.api import worker as worker_api  # noqa: E402
from app.config import settings  # noqa: E402
from app.database import get_db  # noqa: E402
from app.models.tracked_dataset import TrackedDataset  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import append_store  # noqa: E402

DS_ID = uuid.uuid4()
CSV = "שם,גוש\nתוכנית א,30001\nתוכנית ב,30002\n".encode("utf-8")


# ── plumbing ─────────────────────────────────────────────────────────────

class _DB:
    def __init__(self, ds):
        self.ds = ds

    async def execute(self, stmt):
        ds = self.ds

        class _Result:
            def scalar_one_or_none(self):
                return ds
        return _Result()

    async def commit(self):
        pass

    def add(self, obj):
        pass


def _ds(scraper_config, *, odata_dataset_id=None):
    return TrackedDataset(
        id=DS_ID, ckan_id="mavat-plans", ckan_name="mavat-plans",
        title="מאגר התוכניות", source_type="scraper",
        odata_dataset_id=odata_dataset_id, scraper_config=scraper_config,
    )


def _client(db):
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(worker_api.router)

    async def _db():
        yield db

    app.dependency_overrides[get_db] = _db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture(autouse=True)
def worker_key(monkeypatch):
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")


@pytest.fixture(autouse=True)
def no_backend_calls(monkeypatch, tmp_path):
    """Fail loudly if any file backend is touched, and keep temp files out of
    /tmp (which does not exist on Windows)."""
    async def _boom(*a, **kw):
        raise AssertionError("a file backend was called")

    monkeypatch.setattr(worker_api.storage_client, "upload_object", _boom)
    monkeypatch.setattr(worker_api.odata_client, "upload_resource", _boom)
    monkeypatch.setattr(worker_api.odata_client, "create_resource", _boom)
    monkeypatch.setattr(append_store, "is_configured", lambda: True)
    tmp_dir = tmp_path / "upload_csv"
    monkeypatch.setattr(worker_api, "_UPLOAD_TMP_DIR", str(tmp_dir), raising=False)
    return tmp_dir


def _upload(client, **form):
    return client.post(
        f"/api/worker/upload-csv/{DS_ID}",
        files={"file": ("data.csv", CSV, "text/csv")},
        data={"version_number": "1", "resource_name": "נתוני הסורק",
              "row_count": "36784", **form},
        headers={"Authorization": "Bearer workerkey"},
    )


# ── 1. a NEON-only dataset is accepted, and no file backend is touched ───

def test_a_neon_only_dataset_can_upload_its_csv():
    """The whole bug: this returned 404 for a dataset that exists."""
    db = _DB(_ds({"storage_backend": "neon"}))
    resp = _upload(_client(db))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["upload_mode"] == "neon"
    assert body["rows"] == 36784
    # The returned reference is NOT a storage value — there is no object.
    assert worker_api._is_neon_csv_ref(body["resource_id"])
    assert not worker_api.storage.is_storage_value(body["resource_id"])


def test_the_uploaded_rows_are_kept_for_the_push_that_loads_them():
    """A NEON-only upload has nowhere to put the bytes but the append table,
    and only push-version knows which table that is (one merged, or one per
    resource). So the CSV is held on disk and handed over by reference."""
    db = _DB(_ds({"storage_backend": "neon"}))
    ref = _upload(_client(db)).json()["resource_id"]
    path = worker_api._neon_csv_path(ref)
    assert os.path.exists(path)
    with open(path, "r", encoding="utf-8-sig") as fh:
        assert "תוכנית א" in fh.read()
    os.remove(path)


def test_the_held_csv_streams_into_the_append_table(monkeypatch):
    """What push-version then does with it: rows land, batched, and the temp
    file is dropped."""
    import asyncio

    ensured, appended = [], []

    async def _ensure_table(table, cols, *, key_col, keyless):
        ensured.append((table, cols))

    async def _append_rows(table, cols, rows, *, key_col, keyless):
        appended.extend(rows)
        return len(rows)

    monkeypatch.setattr(append_store, "ensure_table", _ensure_table)
    monkeypatch.setattr(append_store, "append_rows", _append_rows)

    db = _DB(_ds({"storage_backend": "neon"}))
    path = worker_api._neon_csv_path(_upload(_client(db)).json()["resource_id"])
    total = asyncio.run(
        worker_api._neon_stream_load_file("append_mavat", path, delete_after=True)
    )
    assert total == 2
    assert ensured == [("append_mavat", ["שם", "גוש"])]
    assert [r["שם"] for r in appended] == ["תוכנית א", "תוכנית ב"]
    assert not os.path.exists(path), "the temp file must not be left behind"


# ── 2. the other plans answer exactly as before ──────────────────────────

def test_an_odata_dataset_still_uploads_to_odata(monkeypatch):
    calls = []

    async def _upload_resource(**kw):
        calls.append(kw)
        return {"id": "8f5c1a0e-1111-2222-3333-444455556666"}

    async def _enqueue(**kw):
        pass

    monkeypatch.setattr(worker_api.odata_client, "upload_resource", _upload_resource)
    monkeypatch.setattr(
        "app.worker.datastore_push_runner.enqueue", _enqueue,
    )
    db = _DB(_ds({"storage_backend": "odata"}, odata_dataset_id="pkg-1"))
    resp = _upload(_client(db))
    assert resp.status_code == 200, resp.text
    assert resp.json()["upload_mode"] == "file+datastore"
    assert calls and calls[0]["dataset_id"] == "pkg-1"


def test_an_r2_dataset_still_uploads_to_r2(monkeypatch):
    keys = []

    async def _upload_object(key, **kw):
        keys.append(key)

    monkeypatch.setattr(worker_api.storage_client, "upload_object", _upload_object)
    monkeypatch.setattr(worker_api.storage_client, "is_configured", lambda: True)
    db = _DB(_ds({"storage_backend": "r2"}))
    resp = _upload(_client(db))
    assert resp.status_code == 200, resp.text
    assert resp.json()["upload_mode"] == "r2"
    assert worker_api.storage.is_storage_value(resp.json()["resource_id"])
    assert len(keys) == 1


# ── 3. a real absence of a backend is refused — with a reason ────────────

def test_a_dataset_with_no_backend_at_all_is_refused_and_told_why(monkeypatch):
    """Storage plan says files, but there is no ODATA mirror and R2 is not
    configured. Refused — and NOT as "not found", which is what sent a whole
    investigation looking for a missing dataset.

    The unconfigured half is ENFORCED, not assumed. This test used to rely on
    the developer's .env happening to carry no S3 credentials; the moment real
    ones were added it began failing with 502 — the code correctly took the R2
    path — which reads as a regression in the guard rather than in the fixture.
    """
    from app.config import settings
    for field in ("s3_endpoint", "s3_bucket", "s3_access_key",
                  "s3_secret_key", "s3_public_base_url"):
        monkeypatch.setattr(settings, field, "", raising=False)
    ds_no_backend = _ds({"storage_backend": "r2"})
    resp = _upload(_client(_DB(ds_no_backend)))
    assert resp.status_code == 409
    assert "no storage backend" in resp.json()["detail"]


def test_a_missing_dataset_is_the_only_404():
    resp = _upload(_client(_DB(None)))
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Dataset not found"


def test_a_neon_only_dataset_is_refused_a_geojson_in_words_a_human_can_act_on():
    """Geometry is a file, and a NEON-only plan has no file store. That is a
    legitimate refusal — but it has to SAY so, instead of the 404 the worker
    reported as "likely server OOM"."""
    ds = _ds({"storage_backend": "neon"})
    with pytest.raises(HTTPException) as e:
        worker_api._require_file_backend(ds, "a GeoJSON file")
    assert e.value.status_code == 409
    assert "NEON-only" in e.value.detail and "GeoJSON" in e.value.detail


def test_a_neon_only_upload_is_refused_when_the_append_db_is_gone(monkeypatch):
    monkeypatch.setattr(append_store, "is_configured", lambda: False)
    resp = _upload(_client(_DB(_ds({"storage_backend": "neon"}))))
    assert resp.status_code == 409
    assert "append DB" in resp.json()["detail"]


# ── 4. a row archive IS content ──────────────────────────────────────────

def test_a_neon_only_version_is_not_read_as_empty():
    """A NEON-only version carries no file mapping — there is no file store —
    so its table key is the only evidence anything landed. Counting only files
    would make the guard discard a complete 36,784-row version."""
    single = {"_hashes": {"scraper": "h"}, "_resource_ids": [],
              "append_table": "append_mavat_plans_510e8644"}
    multi = {"_hashes": {"scraper": "h"}, "_resource_ids": [],
             "_append_tables": [{"resource": "נתוני הסורק", "table": "append_a"},
                                {"resource": "מסמכים", "table": "append_b"}]}
    assert worker_api._landed_resource_count(single) >= 1
    assert worker_api._landed_resource_count(multi) >= 1


def test_an_actually_empty_version_is_still_read_as_empty():
    assert worker_api._landed_resource_count(
        {"_hashes": {"scraper": "h"}, "_resource_ids": [], "_symbology": []}
    ) == 0


# ── 5. push-version: the other half of the same guard ────────────────────
# ``push_version`` gates its whole tabular block on the same
# ``odata_dataset_id or _use_r2(ds)`` condition, so fixing only the upload
# would have moved the failure one call later: the rows would arrive and then
# fall through to the empty-version guard with nothing recorded.

class _PushDB(_DB):
    """Serves the dataset, a running task, and no prior versions."""

    def __init__(self, ds):
        super().__init__(ds)
        self.added = []

    async def execute(self, stmt):
        text = str(stmt)
        db = self

        class _Result:
            def scalar_one_or_none(self):
                if "tracked_datasets" in text and "scrape_task" not in text:
                    return db.ds
                if "scrape_task" in text:
                    return _Task()
                return None  # no previous version

            def scalars(self):
                class _S:
                    def all(self_inner):
                        return []
                return _S()
        return _Result()

    def add(self, obj):
        self.added.append(obj)


class _Task:
    status = "running"
    phase = None
    message = None
    progress = 0
    completed_at = None
    error = None


def _push(client, resources, **extra):
    return client.post(
        "/api/worker/push-version",
        json={
            "tracked_dataset_id": str(DS_ID),
            "metadata_modified": "2026-08-01T17:18:00",
            "resources": resources,
            **extra,
        },
        headers={"Authorization": "Bearer workerkey"},
    )


def test_push_version_loads_a_neon_only_dataset_rows_and_records_the_table(monkeypatch):
    """Inline rows (under the worker's out-of-band threshold): they go to the
    append table, the version records it, and no file backend is touched."""
    loaded = []

    async def _ensure_table(table, cols, *, key_col, keyless):
        pass

    async def _append_rows(table, cols, rows, *, key_col, keyless):
        loaded.append((table, len(rows)))
        return len(rows)

    monkeypatch.setattr(append_store, "ensure_table", _ensure_table)
    monkeypatch.setattr(append_store, "append_rows", _append_rows)

    db = _PushDB(_ds({"storage_backend": "neon"}))
    resp = _push(_client(db), [{
        "name": "נתוני הסורק", "format": "CSV", "row_count": 2,
        "fields": [{"id": "שם", "type": "text"}, {"id": "גוש", "type": "text"}],
        "records": [{"שם": "תוכנית א", "גוש": "30001"},
                    {"שם": "תוכנית ב", "גוש": "30002"}],
    }])
    assert resp.status_code == 200, resp.text
    assert resp.json()["version_number"] == 1
    assert loaded and loaded[0][1] == 2
    version = db.added[-1]
    assert version.resource_mappings["append_table"] == loaded[0][0]
    assert version.change_summary["total_rows"] == 2


def test_push_version_queues_the_out_of_band_csv_of_a_neon_only_dataset(monkeypatch):
    """The >30MB path end to end: /upload-csv's reference reaches push-version,
    which streams it into the append table off the request path and records the
    table — with no file mapping, because there is no file."""
    scheduled = []

    def _fake_create_task(coro):
        # Patched globally, so the session's own housekeeping tasks land here
        # too — keep only the load we're asserting on.
        if getattr(coro, "__name__", "") == "_neon_only_load_csv":
            scheduled.append(coro)
        coro.close()
        return _Dummy()

    class _Dummy:
        def add_done_callback(self, cb):
            pass

    monkeypatch.setattr(worker_api.asyncio, "create_task", _fake_create_task)

    db = _PushDB(_ds({"storage_backend": "neon"}))
    resp = _push(
        _client(db),
        [{"name": "נתוני הסורק", "format": "CSV", "row_count": 36784,
          "fields": [], "records": []}],
        csv_resource_ids={"נתוני הסורק": worker_api._neon_csv_ref("/tmp/x.csv")},
    )
    assert resp.status_code == 200, resp.text
    assert len(scheduled) == 1, "the row load must be scheduled off-request"
    mappings = db.added[-1].resource_mappings
    assert mappings["append_table"].startswith("append_")
    # The disk reference is plumbing between two calls — it must never be
    # published as if it were a resource.
    assert "נתוני הסורק" not in mappings
    assert not any(worker_api._is_neon_csv_ref(v) for v in mappings.values())


def test_push_version_still_refuses_a_neon_only_dataset_with_no_append_db(monkeypatch):
    """No file store AND no append DB means nothing can hold the data. That has
    to fail loudly, with the reason — not create an empty version."""
    monkeypatch.setattr(append_store, "is_configured", lambda: False)
    db = _PushDB(_ds({"storage_backend": "neon"}))
    resp = _push(_client(db), [{
        "name": "נתוני הסורק", "format": "CSV", "row_count": 1,
        "fields": [{"id": "שם", "type": "text"}],
        "records": [{"שם": "תוכנית א"}],
    }])
    assert resp.status_code == 502
    assert "append DB" in resp.json()["detail"]["message"]
    assert db.added == []
