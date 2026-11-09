"""current.<table> views (app/services/current_views.py) and the catalog's
choice of which version names a dataset's tables. No DB needed."""
import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
os.environ.setdefault("JWT_SECRET_KEY", "test")

from sqlalchemy.dialects import postgresql  # noqa: E402

from app.services import current_views as V  # noqa: E402
from app.services import data_catalog as C  # noqa: E402

COLS = ["work_id", "site_name", "first_seen", "row_hash", "last_seen"]


def test_view_keeps_source_columns_and_first_seen_only():
    assert V.view_columns(COLS) == ["work_id", "site_name", "first_seen"]


def test_view_sql_filters_to_open_rows():
    sql = V.view_sql("append_x", COLS)
    assert sql == ('SELECT "work_id", "site_name", "first_seen"\n'
                   'FROM public."append_x"\n'
                   'WHERE "last_seen" IS NULL')


def test_no_view_without_a_validity_column():
    assert V.view_sql("append_x", ["a", "first_seen", "row_hash"]) is None


def test_signature_follows_the_column_list():
    assert V.signature(COLS) == V.signature(list(COLS))
    assert V.signature(COLS) != V.signature(COLS + ["new_col"])


def test_view_ref_is_schema_qualified():
    assert V.view_ref("append_x") == "current.append_x"


def test_latest_mappings_only_reads_versions_that_name_tables():
    pred = C._has_table_mapping().compile(dialect=postgresql.dialect())
    text = str(pred)
    assert text.count("?") == 2 and " OR " in text
    assert set(pred.params.values()) == {"append_table", "_append_tables"}
