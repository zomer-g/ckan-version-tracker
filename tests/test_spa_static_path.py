"""The SPA 404 fallback serves a file only from inside frontend/dist.

Regression: the check compared unresolved path strings, so a request whose
path uvicorn had decoded to ``/../../.env`` (sent as ``/..%2f..%2f.env``)
still "started with" dist and returned any file the process could read.
"""
from pathlib import Path

import pytest

from app.main import _dist_file


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
