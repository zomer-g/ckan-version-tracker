"""A selected geometry comes back as WKT, not as the hex nobody can use.

asyncpg has no codec for PostGIS types, so `SELECT geom` returns what Postgres
prints for a type it cannot describe: hex EWKB, `0106000020E6100000…`. Every
consumer of the read-only SQL path then gets a blob — the /data map decides
mappability by the SHAPE of the value and refuses it, the table view shows 800
characters of hex, and CSV export writes it out as such.

Three assistants in a row wrote a correct multi-layer map query that could not
be drawn for exactly this reason, and the fix kept being aimed at whoever wrote
the SQL: the console's help says ST_AsText, the copy-to-AI schema says it, the
MCP's instructions say it, and the MCP now says it again on the result. None of
that reaches a person who pastes a query written somewhere else.

So the projection happens where the value is read. Pinned here because it is a
rewrite of user-supplied SQL: it must fire on geometry, leave everything else
byte-identical, and refuse the one case where projecting by name would produce
a WRONG result rather than an ugly one.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services.append_store import wkt_projection  # noqa: E402


def test_a_geometry_column_is_rendered_as_wkt():
    proj = wkt_projection([("layer", "text"), ("geom", "geometry")])
    assert proj == '"layer", "extensions".ST_AsText("geom") AS "geom"'


def test_geography_too():
    # ST_DWithin work casts to geography; a result can carry one.
    assert "ST_AsText" in wkt_projection([("g", "geography")])


def test_a_query_with_no_geometry_is_not_rewritten():
    """The rewrite costs a second prepare, and touching a query that does not
    need it is how an unrelated result changes shape."""
    assert wkt_projection([("a", "text"), ("n", "int4")]) is None
    assert wkt_projection([]) is None


def test_hebrew_and_quoted_names_survive_the_projection():
    proj = wkt_projection([('שם יישוב', "text"), ('the "geom"', "geometry")])
    assert '"שם יישוב"' in proj
    assert '"the ""geom"""' in proj


def test_duplicate_column_names_are_left_alone():
    """`SELECT a.geom, b.geom` returns two columns called geom. Projecting them
    by name would emit the same expression twice and silently hand back one
    column's geometry under both names — worse than the hex it replaces."""
    assert wkt_projection([("geom", "geometry"), ("geom", "geometry")]) is None
    assert wkt_projection([("x", "text"), ("x", "text"), ("g", "geometry")]) is None


def test_every_column_keeps_its_name_and_position():
    attrs = [("z", "int4"), ("geom", "geometry"), ("label", "text")]
    proj = wkt_projection(attrs)
    # order preserved, one entry per column, aliases restore the original names
    assert proj.count(",") == 2
    assert proj.index('"z"') < proj.index('"geom"') < proj.index('"label"')
