"""PostGIS's schema is read from the database, so the code works both before
and after xhostd moves the extension from `extensions` to `postgis`."""
import asyncio
import pathlib
import re

import pytest

from app.services import postgis_schema as ps


@pytest.fixture(autouse=True)
def _reset():
    saved = (ps._schema, ps._resolved)
    yield
    ps._schema, ps._resolved = saved


class _Conn:
    def __init__(self, value=None, exc=None):
        self.value, self.exc, self.sql = value, exc, None

    async def fetchval(self, sql):
        self.sql = sql
        if self.exc:
            raise self.exc
        return self.value


def test_default_is_extensions():
    ps._schema, ps._resolved = ps.DEFAULT_SCHEMA, False
    assert ps.schema() == "extensions"
    assert ps.qschema() == '"extensions"'


def test_resolve_reads_pg_extension_and_follows_the_move():
    conn = _Conn("postgis")
    assert asyncio.run(ps.resolve(conn)) == "postgis"
    assert "pg_extension" in conn.sql and "extnamespace::regnamespace::text" in conn.sql
    assert ps.qschema() == '"postgis"' and ps.is_resolved()


def test_no_postgis_or_failed_lookup_keeps_the_name():
    ps._schema = "extensions"
    assert asyncio.run(ps.resolve(_Conn(None))) == "extensions"
    assert asyncio.run(ps.resolve(_Conn(exc=RuntimeError("down")))) == "extensions"


def test_quoted_and_bad_names():
    assert ps.set_schema('"postgis"') == "postgis"
    assert ps.set_schema("x; DROP TABLE y") == "postgis"


def test_sql_builders_follow_the_resolved_schema():
    from app.services import append_geometry, nadlan_index
    ps.set_schema("postgis")
    assert "postgis.ST_MakePoint" in append_geometry.point_expr("lon", "lat").replace('"', "")
    ddl = "\n".join(nadlan_index._ddl())
    assert '"postgis".geometry' in ddl and '"extensions"' not in ddl


def test_no_hard_coded_extensions_qualifier_left():
    root = pathlib.Path(__file__).resolve().parents[1] / "app"
    pat = re.compile(r'\bextensions"?\.(ST_|geometry|geography|spatial_ref_sys)')
    hits = []
    for p in root.rglob("*.py"):
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            if pat.search(line) and not line.lstrip().startswith(("#", "--")) \
                    and "postgis_schema.py" not in str(p):
                hits.append(f"{p.name}:{i}")
    assert not hits, hits


def test_search_paths_carry_postgis_after_extensions():
    from app.services import data_catalog, ocal_db, ocoi_db
    for path in (data_catalog.CONSOLE_SEARCH_PATH, ocal_db._SEARCH_PATH, ocoi_db._SEARCH_PATH):
        parts = [p.strip() for p in path.split(",")]
        assert parts.index("postgis") == parts.index("extensions") + 1, path
