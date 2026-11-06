"""Where PostGIS lives is read from the database, not hard-coded."""
import asyncio

import pytest

from app.services import address_municipal, append_geometry, pg_ext


class _Conn:
    def __init__(self, answer=None, boom=False):
        self.answer, self.boom, self.sql = answer, boom, []

    async def fetchval(self, sql):
        self.sql.append(sql)
        if self.boom:
            raise RuntimeError("no catalog")
        return self.answer


@pytest.fixture(autouse=True)
def _reset():
    pg_ext._schema, pg_ext._detected = pg_ext.DEFAULT_SCHEMA, False
    yield
    pg_ext._schema, pg_ext._detected = pg_ext.DEFAULT_SCHEMA, False


def test_default_is_extensions_until_a_pool_answers():
    assert pg_ext.schema() == "extensions"
    assert pg_ext.q() == '"extensions"'


def test_detect_reads_the_schema_from_pg_extension():
    conn = _Conn("postgis")
    assert asyncio.run(pg_ext.detect(conn)) == "postgis"
    assert conn.sql == [pg_ext.DETECT_SQL]
    assert "extnamespace::regnamespace::text" in pg_ext.DETECT_SQL
    assert pg_ext.q() == '"postgis"'
    assert append_geometry.point_expr("lon", "lat").count('"postgis".ST_MakePoint') == 3
    assert '"extensions"' not in append_geometry.point_expr("lon", "lat")
    assert '"postgis".ST_Centroid' in address_municipal._layer_cte("t", "x", "s", "h", None)


def test_old_layout_keeps_extensions():
    asyncio.run(pg_ext.detect(_Conn("extensions")))
    assert pg_ext.q() == '"extensions"'


def test_no_postgis_or_failed_probe_keeps_the_current_value():
    asyncio.run(pg_ext.detect(_Conn(None)))
    assert pg_ext.schema() == "extensions"
    asyncio.run(pg_ext.detect(_Conn("postgis")))
    asyncio.run(pg_ext.detect(_Conn(boom=True)))
    assert pg_ext.schema() == "postgis"


def test_quoted_regnamespace_is_unquoted():
    asyncio.run(pg_ext.detect(_Conn('"Geo ""X"""')))
    assert pg_ext.schema() == 'Geo "X"'
    assert pg_ext.q() == '"Geo ""X"""'


def test_ensure_asks_once_per_pool_start():
    conn = _Conn("postgis")
    asyncio.run(pg_ext.ensure(conn))
    asyncio.run(pg_ext.ensure(conn))
    assert len(conn.sql) == 1
    pg_ext.forget()
    asyncio.run(pg_ext.ensure(conn))
    assert len(conn.sql) == 2


def test_search_paths_list_postgis_after_extensions():
    from app.services import data_catalog, ocal_db, ocoi_db
    for path in (data_catalog.CONSOLE_SEARCH_PATH, ocal_db._SEARCH_PATH, ocoi_db._SEARCH_PATH):
        parts = [p.strip() for p in path.split(",")]
        assert parts.index("postgis") == parts.index("extensions") + 1, path


def test_import_time_ddl_is_filled_when_it_runs():
    from app.services import govmap_abroad, nadlan_index
    ddl = "\n".join(nadlan_index._DDL + govmap_abroad._DDL
                    + [t for _a, _b, t in nadlan_index._COLUMN_ADDITIONS])
    assert pg_ext.TOKEN in ddl and '"extensions"' not in ddl
    asyncio.run(pg_ext.detect(_Conn("postgis")))
    filled = pg_ext.fill(ddl)
    assert pg_ext.TOKEN not in filled
    assert '"postgis".geometry(Point, 4326)' in filled
