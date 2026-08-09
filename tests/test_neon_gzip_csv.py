"""A CSV that reaches NEON may or may not be gzipped, and both are correct.

/upload-csv receives gzip and decompresses on the way to storage, so its stored
object is plain. A CSV too large for that endpoint (Cloudflare will not carry
894MB) goes straight from the worker to R2 and stays compressed — which is not
merely acceptable but required: the national parcel layer is 2.8 GB plain and
894 MB gzipped, and the dyno's /tmp is capped at 2 GB. Downloading the plain
form in order to read it exceeds the disk before anything else.

The loader read every file as UTF-8 text, so the compressed route would have
been fed gzip bytes and failed on the first line.
"""
import gzip
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api.worker import _open_maybe_gzip  # noqa: E402

ROWS = 'גוש,חלקה\n30338,266\n30590,16\n'


def _write(data: bytes) -> str:
    fd, p = tempfile.mkstemp()
    with os.fdopen(fd, "wb") as fh:
        fh.write(data)
    return p


def test_a_plain_csv_still_reads():
    p = _write(ROWS.encode("utf-8-sig"))
    with _open_maybe_gzip(p) as fh:
        assert fh.read().splitlines()[1] == "30338,266"
    os.remove(p)


def test_a_gzipped_csv_reads_too():
    p = _write(gzip.compress(ROWS.encode("utf-8-sig")))
    with _open_maybe_gzip(p) as fh:
        assert fh.read().splitlines()[1] == "30338,266"
    os.remove(p)


def test_it_sniffs_the_bytes_not_the_name():
    """The key an object is stored under is not a promise about its contents —
    the same R2 path has held both forms depending on which upload route the
    file's size sent it down."""
    misnamed = _write(gzip.compress(ROWS.encode("utf-8-sig")))
    os.rename(misnamed, misnamed + ".csv")          # gzip bytes, plain name
    with _open_maybe_gzip(misnamed + ".csv") as fh:
        assert "30590" in fh.read()
    os.remove(misnamed + ".csv")


def test_hebrew_headers_survive_the_bom_either_way():
    """utf-8-sig matters on both branches: the worker writes the BOM for Excel,
    and a stripped BOM would leave the first column named '\ufeffגוש'."""
    for data in (ROWS.encode("utf-8-sig"), gzip.compress(ROWS.encode("utf-8-sig"))):
        p = _write(data)
        with _open_maybe_gzip(p) as fh:
            assert fh.readline().strip().split(",")[0] == "גוש"
        os.remove(p)
