"""A file too big for the edge is uploaded in slices, and must reassemble EXACTLY.

/odata/import-file carries the file as one request body. over.org.il sits behind
Cloudflare, whose body ceiling is ~100MB, so גזטיר נכסים — a 413MB CSV — was
rejected at the edge before FastAPI saw it: nothing reached the server, nothing
could report a failure, and the button appeared to do nothing.

Presigned PUTs straight to R2 were tried first and are worse here: a
cross-origin PUT needs a bucket CORS policy that the application cannot set, and
its absence surfaces as an opaque "Failed to fetch". Slicing the file and POSTing
the slices to our OWN origin removes CORS from the problem entirely.

What these lock in is the part that would fail quietly. A CSV assembled out of
order, or short by one slice, still parses — it just yields wrong rows, in a
table nobody would think to re-check. So the bytes are compared exactly.
"""
import asyncio
import io
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.api.admin import router as admin_router  # noqa: E402
from app.auth.dependencies import get_admin_user  # noqa: E402
from app.models.user import User  # noqa: E402
from app.rate_limit import limiter  # noqa: E402


@pytest.fixture
def client():
    app = FastAPI()
    app.state.limiter = limiter
    app.include_router(admin_router)
    app.dependency_overrides[get_admin_user] = lambda: User(
        id=uuid.uuid4(), email="admin@test", is_admin=True
    )
    limiter.reset()
    return TestClient(app)


def _upload(client, payload: bytes, chunk: int):
    began = client.post("/api/admin/odata/upload/begin")
    assert began.status_code == 200, began.text
    uid = began.json()["upload_id"]
    for i in range(0, len(payload), chunk):
        r = client.post(
            "/api/admin/odata/upload/chunk",
            data={"upload_id": uid},
            files={"chunk": ("part", io.BytesIO(payload[i:i + chunk]),
                             "application/octet-stream")},
        )
        assert r.status_code == 200, r.text
    return uid


def _staged_bytes(uid: str) -> bytes:
    import tempfile
    path = os.path.join(tempfile.gettempdir(), f"odata-stage-{uid}")
    with open(path, "rb") as fh:
        return fh.read()


def test_the_slices_reassemble_byte_for_byte(client):
    """The whole point. A CSV short by one slice still parses; it is simply
    wrong, in a table nobody would re-check."""
    payload = bytes(range(256)) * 4096          # 1 MB, every byte value
    uid = _upload(client, payload, 64 * 1024)   # 16 slices
    assert _staged_bytes(uid) == payload
    os.remove(os.path.join(__import__("tempfile").gettempdir(), f"odata-stage-{uid}"))


def test_a_final_short_slice_is_not_padded_or_truncated(client):
    """File sizes are not multiples of the chunk size, so the last slice is
    almost always short — the easiest place to be off by a few bytes."""
    payload = b"a" * (64 * 1024 * 3 + 7)
    uid = _upload(client, payload, 64 * 1024)
    got = _staged_bytes(uid)
    assert len(got) == len(payload) and got == payload
    os.remove(os.path.join(__import__("tempfile").gettempdir(), f"odata-stage-{uid}"))


def test_the_upload_id_cannot_escape_the_temp_directory(client):
    """It goes straight into a filesystem path. Server-generated and validated
    as 32 hex characters, so a crafted one is refused rather than sanitised."""
    for bad in ("../../etc/passwd", "..", "abc", ""):
        r = client.post("/api/admin/odata/upload/chunk",
                        data={"upload_id": bad},
                        files={"chunk": ("p", io.BytesIO(b"x"), "text/plain")})
        assert r.status_code in (400, 404, 422), f"{bad!r} was accepted"


def test_a_chunk_for_an_unopened_upload_is_refused(client):
    """Says start over, rather than silently creating a file that no begin call
    ever authorised."""
    r = client.post("/api/admin/odata/upload/chunk",
                    data={"upload_id": uuid.uuid4().hex},
                    files={"chunk": ("p", io.BytesIO(b"x"), "text/plain")})
    assert r.status_code == 404


def test_importing_nothing_is_refused(client):
    """An upload that opened and received no bytes must not create an empty
    table and call it an import."""
    began = client.post("/api/admin/odata/upload/begin")
    uid = began.json()["upload_id"]
    r = client.post("/api/admin/odata/import-staged",
                    data={"upload_id": uid, "resource_id": "r1",
                          "filename": "x.csv"})
    assert r.status_code == 404


def test_an_unsupported_format_is_refused_and_the_staging_cleaned(client):
    """Otherwise a rejected upload leaves 413MB on the dyno's disk."""
    import tempfile
    uid = _upload(client, b"hello", 64 * 1024)
    r = client.post("/api/admin/odata/import-staged",
                    data={"upload_id": uid, "resource_id": "r1",
                          "filename": "notes.pdf"})
    assert r.status_code == 422
    assert not os.path.exists(
        os.path.join(tempfile.gettempdir(), f"odata-stage-{uid}"))


# ── the other half: parsing must not hold the file either ────────────────────
#
# The chunked upload fixed the TRANSPORT. Parsing was still eager for CSV — the
# whole file as bytes, again as a str, again as a list of millions of small
# lists, all alive at once — which is what OOM-killed the 2GB dyno on a 413MB
# file. XLSX had a streaming reader; CSV, the format that actually gets big,
# did not.

def _big_csv(tmp_path, rows: int):
    p = tmp_path / "big.csv"
    with open(p, "w", encoding="utf-8", newline="") as fh:
        fh.write("שם,כתובת,ערך\n")
        for i in range(rows):
            fh.write(f"ישוב {i},רחוב הרצל {i},{i}\n")
    return str(p)


def test_csv_is_streamed_not_held(tmp_path):
    """Peak heap must track the BATCH size, not the file size — otherwise the
    ceiling is just moved, not removed."""
    import tracemalloc
    from app.services import odata_import as oi

    path = _big_csv(tmp_path, 200_000)
    size_mb = os.path.getsize(path) / 1024 / 1024

    tracemalloc.start()
    columns, batches = oi._open_for_load(path, "CSV")
    seen = 0
    peak_seen = 0
    for b in batches:
        seen += len(b)
        _, peak = tracemalloc.get_traced_memory()
        peak_seen = max(peak_seen, peak)
    tracemalloc.stop()

    assert columns == ["שם", "כתובת", "ערך"]
    assert seen == 200_000, "rows were lost in the stream"
    # Generous, and still far under the several-times-the-file the eager path
    # cost. The point is that this does not scale with the input.
    assert peak_seen / 1024 / 1024 < size_mb * 2, (
        f"peak {peak_seen/1024/1024:.0f}MB on a {size_mb:.0f}MB file — "
        "the parse is holding the file")


def test_the_windows_1255_sniff_survives_streaming(tmp_path):
    """Many Israeli gov CSVs are win-1255. The eager parser could look at every
    byte to decide; a streaming one decides from a prefix, and getting that
    wrong corrupts every Hebrew value in the table."""
    from app.services import odata_import as oi

    p = tmp_path / "cp1255.csv"
    with open(p, "wb") as fh:
        fh.write("עיר,ערך\nחיפה,1\nתל אביב,2\n".encode("windows-1255"))
    columns, rows = oi._stream_csv(str(p))
    assert columns == ["עיר", "ערך"]
    assert [list(r) for r in rows] == [["חיפה", "1"], ["תל אביב", "2"]]


def test_the_file_is_closed_when_the_generator_is_exhausted(tmp_path):
    """It stays open for the life of the generator; a leak here would strand a
    handle per import."""
    import gc
    from app.services import odata_import as oi

    path = _big_csv(tmp_path, 10)
    _, rows = oi._stream_csv(path)
    list(rows)
    gc.collect()
    # On Windows an open handle blocks removal outright, which is the assertion.
    os.remove(path)
