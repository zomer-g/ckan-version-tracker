"""Rows that land in NEON get offered to the geometry step — on EVERY loader.

push-version has three ways to put rows in an append table, and only the
streaming one (`_neon_stream_load_file`, reached by the >50MB out-of-band CSV
path) called `append_store.fill_geometry`. Everything under that threshold —
which is every spatial layer of the CBS GIS catalog except land_use2014 and
land_use2003 — went through the inline loader, which loaded `geometry_wkt` into
Postgres and never built a `geom` column, with nothing in the logs saying so.

The gap was invisible because each half looked right on its own: the WKT branch
of `append_geometry.fill` is tested, and the streaming loader does call it.
Nothing checked that a NEW loader had to.

So what is pinned here is the invariant rather than the one call site:
**a function that appends rows must also offer them to the geometry step.**

No DB, no network — the loaders are read structurally, and the routing is
exercised against a fake connection.
"""
import ast
import inspect
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

import app.api.worker as worker  # noqa: E402
from app.services import append_geometry as ag  # noqa: E402
from app.services import index_mirror as idx  # noqa: E402


# ── the invariant: rows in ⇒ geometry offered ──────────────────────────


def _own_calls(fn) -> set[str]:
    """Dotted call names made by this function itself, not by nested ones.

    Nested at ANY depth: `_neon_load_from_csv` sits inside a block within
    push_version, so looking only at direct children credited its append to the
    handler and hid the loader this test exists to watch.
    """
    nested = set()
    for node in ast.walk(fn):
        if node is not fn and isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            nested.update(id(n) for n in ast.walk(node))
    names: set[str] = set()
    for node in ast.walk(fn):
        if id(node) in nested or not isinstance(node, ast.Call):
            continue
        f = node.func
        parts = []
        while isinstance(f, ast.Attribute):
            parts.append(f.attr)
            f = f.value
        if isinstance(f, ast.Name):
            parts.append(f.id)
        if parts:
            names.add(".".join(reversed(parts)))
    return names


def _loaders() -> list[tuple[str, set[str]]]:
    tree = ast.parse(inspect.getsource(worker))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        calls = _own_calls(node)
        if "append_store.append_rows" in calls:
            out.append((node.name, calls))
    return out


def test_the_loaders_are_still_the_ones_this_test_knows_about():
    # If a fourth path appears, this fails first and names it — better than the
    # invariant below passing because the new loader was never looked at.
    assert {name for name, _ in _loaders()} == {
        "_neon_stream_load_file", "_neon_load_from_csv",
    }


@pytest.mark.parametrize("name", ["_neon_stream_load_file", "_neon_load_from_csv"])
def test_every_loader_offers_its_rows_to_the_geometry_step(name):
    calls = dict(_loaders())[name]
    assert "append_store.fill_geometry" in calls, (
        f"{name} appends rows without offering them to the geometry step — "
        "a spatial table loaded through it would keep geometry_wkt and never "
        "get a geom column"
    )


# ── routing: a WKT table is the idx shape, in `public` too ─────────────


class _Conn:
    """Enough asyncpg surface for the routing decision."""

    def __init__(self):
        self.queries: list[str] = []
        self.args: list[tuple] = []

    async def fetch(self, sql, *args):
        self.queries.append(sql)
        self.args.append(args)
        return []

    async def fetchval(self, sql, *args):
        self.queries.append(sql)
        self.args.append(args)
        return None


def test_a_wkt_table_is_routed_to_the_tested_idx_path(monkeypatch):
    """`geometry_wkt` is idx's shape — including the ITM reprojection — so it
    must go to that code and NOT fall through to the coordinate-pair reader,
    which would report "no coordinate columns" on a perfectly spatial table."""
    seen = {}

    async def _fake_fill(conn, table, columns, schema):
        seen.update(table=table, columns=columns, schema=schema)
        return {"rows": 7}

    monkeypatch.setattr(idx, "_fill_geometry", _fake_fill)
    monkeypatch.setattr(idx, "_postgis_available",
                        lambda conn: _true())

    async def _true():
        return True

    cols = ["_id", "OBJECTID", "SEMEL_YISHUV", "geometry_wkt"]
    out = _run(ag.fill(_Conn(), "append_localities_2024_ab12cd34", cols))
    assert out == {"rows": 7}
    assert seen["columns"] == cols
    # public, not idx: these are append tables.
    assert seen["schema"] == ag.SCHEMA == "public"


def test_the_candidate_query_asks_for_wkt_tables_not_only_coordinate_pairs():
    """candidates() is what the admin backfill walks. A table carrying WKT and
    no geom has to be IN that list — the whole CBS corpus is that shape."""
    conn = _Conn()
    _run(ag.candidates(conn, limit=25))
    wanted = conn.args[0][1]
    assert idx.WKT_COLUMN in wanted
    assert "public" in conn.args[0]


def _run(coro):
    import asyncio

    return asyncio.run(coro)
