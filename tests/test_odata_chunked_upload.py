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
