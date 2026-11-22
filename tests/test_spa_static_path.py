"""The SPA 404 fallback serves a file only from inside frontend/dist.

Regression: the check compared unresolved path strings, so a request whose
path uvicorn had decoded to ``/../../.env`` (sent as ``/..%2f..%2f.env``)
still "started with" dist and returned any file the process could read.
"""
from pathlib import Path

import pytest

from app.main import _dist_file, _dist_file_headers


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    dist = tmp_path / "frontend" / "dist"
    (dist / "sub").mkdir(parents=True)
    (dist / "favicon.svg").write_text("<svg/>")
    (dist / "sub" / "a.txt").write_text("a")
    (tmp_path / ".env").write_text("JWT_SECRET_KEY=x")
    (tmp_path / "frontend" / "distX").mkdir()
    (tmp_path / "frontend" / "distX" / "s.txt").write_text("sibling")
    (dist / "out").symlink_to(tmp_path / ".env")
    return dist


def test_serves_files_inside_dist(tree: Path):
    assert _dist_file("/favicon.svg", tree) == (tree / "favicon.svg").resolve()
    assert _dist_file("/sub/a.txt", tree) == (tree / "sub" / "a.txt").resolve()


@pytest.mark.parametrize(
    "path",
    [
        "/../../.env",          # what uvicorn hands over for /..%2f..%2f.env
        "/sub/../../../.env",
        "/../distX/s.txt",      # sibling directory sharing the "dist" prefix
        "/etc/passwd/../../../../../../etc/passwd",
        "//etc/passwd",
        "/out",                 # symlink inside dist pointing outside it
        "/sub",                 # a directory, not a file
        "/",
        "/fav\x00icon.svg",
        "/missing.js",
    ],
)
def test_refuses_anything_else(tree: Path, path: str):
    assert _dist_file(path, tree) is None


# The PWA files come through the same fallback. The manifest's type must not
# depend on the host's mime.types, and sw.js must not sit in a CDN cache, or a
# deploy that changes the caching rules would not reach browsers.
def test_pwa_files_get_their_headers(tree: Path):
    (tree / "manifest.webmanifest").write_text("{}")
    (tree / "sw.js").write_text("")
    manifest = _dist_file("/manifest.webmanifest", tree)
    sw = _dist_file("/sw.js", tree)
    assert manifest is not None and sw is not None
    assert _dist_file_headers(manifest) == {"media_type": "application/manifest+json"}
    assert _dist_file_headers(sw) == {"headers": {"Cache-Control": "no-cache"}}
    assert _dist_file_headers(tree / "favicon.svg") == {}
