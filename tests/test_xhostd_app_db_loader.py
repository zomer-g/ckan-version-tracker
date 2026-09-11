"""The pure parts of scripts/xhostd_app_db_loader.py."""
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location("app_db_loader", ROOT / "scripts" / "xhostd_app_db_loader.py")
A = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(A)


def test_trigram_operator_classes_move_to_the_schema_pg_trgm_lives_in():
    sql = "CREATE INDEX ix_cbs_index_title_trgm ON app.cbs_index USING gin (title public.gin_trgm_ops);"
    assert A.adapt_post_data(sql) == (
        "CREATE INDEX ix_cbs_index_title_trgm ON app.cbs_index USING gin (title ocal.gin_trgm_ops);")
    assert "ocal.gist_trgm_ops" in A.adapt_post_data("USING gist (x public.gist_trgm_ops)")
    assert A.adapt_post_data("public.tracked_datasets") == "public.tracked_datasets"


def test_pre_data_leaves_extensions_alone():
    dump = "CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;\nCREATE SCHEMA app;\nCREATE TABLE app.tags (id uuid);"
    out = A.clean_pre_data(dump)
    assert "EXTENSION" not in out
    assert "CREATE SCHEMA app;" in out and "CREATE TABLE app.tags (id uuid);" in out


def test_it_knows_when_the_app_is_using_the_channel_database():
    local = "postgres://app:pw@10.77.1.5:5432/over"
    assert A.same_host("postgresql+asyncpg://app:pw@10.77.1.5:5432/over", local)
    assert not A.same_host("postgresql+asyncpg://u:p@ep-x.aws.neon.tech/neondb?sslmode=require", local)
