"""The pure parts of scripts/xhostd_archive_loader.py.

The loader itself runs against Neon and the xhostd channel database; these pin
the text transformations it depends on, where a mistake would fail hours into a
copy or silently skip objects.
"""
import importlib.util
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("loader", ROOT / "scripts" / "xhostd_archive_loader.py")
L = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(L)


def test_source_uses_the_direct_neon_endpoint_not_the_pooler():
    url = "postgresql+asyncpg://u:p@ep-quiet-1-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    assert L.libpq_url(url, direct=True) == (
        "postgresql://u:p@ep-quiet-1.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
    assert "-pooler" in L.libpq_url(url)


def test_asyncpg_dsn_drops_libpq_only_parameters():
    dsn = L.asyncpg_dsn("postgres://u:p@10.200.2.2:5432/over?sslmode=disable&channel_binding=prefer&application_name=x")
    assert dsn == "postgresql://u:p@10.200.2.2:5432/over?application_name=x"


def test_identifiers_are_quoted_including_embedded_quotes():
    assert L.qualified("public", 'append_"odd"') == '"public"."append_""odd"""'
    assert L.qualified("idx", "govmap_52_Mixed") == '"idx"."govmap_52_Mixed"'


def test_pre_data_drops_the_blocked_extension_and_tolerates_existing_schemas():
    dump = "\n".join([
        "CREATE SCHEMA extensions;",
        "CREATE SCHEMA IF NOT EXISTS ocal;",
        "CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;",
        "COMMENT ON EXTENSION postgres_fdw IS 'foreign-data wrapper for remote PostgreSQL servers';",
        "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA extensions;",
        "COMMENT ON SCHEMA public IS 'standard public schema';",
        "CREATE TABLE public.append_x (id integer);",
    ])
    out = L.clean_pre_data(dump)
    assert "postgres_fdw" not in out
    assert "COMMENT ON SCHEMA public" not in out
    assert "CREATE SCHEMA IF NOT EXISTS extensions;" in out
    assert out.count("CREATE SCHEMA IF NOT EXISTS ocal;") == 1
    assert "CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA extensions;" in out
    assert "CREATE TABLE public.append_x (id integer);" in out


def test_pre_data_drops_the_leftover_foreign_server_and_its_user_mapping():
    """The first real run stopped here: a server and a user mapping built on
    postgres_fdw survived a line filter that only knew the extension."""
    dump = """SET statement_timeout = 0;

--
-- Name: postgres_fdw; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA public;


--
-- Name: EXTENSION postgres_fdw; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION postgres_fdw IS 'foreign-data wrapper for remote PostgreSQL servers';


--
-- Name: ocal_srv; Type: SERVER; Schema: -; Owner: -
--

CREATE SERVER ocal_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (
    dbname 'neondb',
    host 'example.neon.tech'
);


--
-- Name: USER MAPPING public SERVER ocal_srv; Type: USER MAPPING; Schema: -; Owner: -
--

CREATE USER MAPPING FOR public SERVER ocal_srv OPTIONS (
    password 'secret',
    "user" 'x'
);


--
-- Name: ocal; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA ocal;


--
-- Name: diary_events; Type: TABLE; Schema: ocal; Owner: -
--

CREATE TABLE ocal.diary_events (id integer);
"""
    out = L.clean_pre_data(dump)
    assert "postgres_fdw" not in out
    assert "ocal_srv" not in out
    assert "USER MAPPING" not in out
    assert "password" not in out
    assert out.startswith("SET statement_timeout = 0;")
    assert "CREATE SCHEMA IF NOT EXISTS ocal;" in out
    assert "CREATE TABLE ocal.diary_events (id integer);" in out


def test_the_copy_pipe_is_one_target_transaction_that_fails_with_its_source():
    s = L.copy_script()
    assert s.startswith("set -o pipefail;")
    assert '-c "COPY $LOADER_TABLE TO STDOUT (FORMAT binary)"' in s
    assert "--single-transaction" in s and "ON_ERROR_STOP=1" in s
    assert s.index('-c "TRUNCATE $LOADER_TABLE"') < s.index('-c "COPY $LOADER_TABLE FROM STDIN (FORMAT binary)"')
    # the name reaches psql only through the variable, inside double quotes
    assert "public" not in s and '"$LOADER_TABLE' not in s


def _fake_bin(tmp_path, src_exit: int, dst_exit: int):
    """A stand-in psql: the source prints bytes and exits src_exit, the target
    drains stdin and exits dst_exit."""
    psql = tmp_path / "psql"
    psql.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$*" == *"TO STDOUT"* ]]; then printf PGCOPY; exit ' + str(src_exit) + "; fi\n"
        "cat >/dev/null; exit " + str(dst_exit) + "\n", encoding="utf-8", newline="\n")
    psql.chmod(0o755)
    return str(tmp_path).replace("\\", "/")


import shutil  # noqa: E402

import pytest  # noqa: E402


@pytest.mark.skipif(shutil.which("bash") is None, reason="no bash")
@pytest.mark.parametrize("src_exit,dst_exit,ok", [(0, 0, True), (1, 0, False), (0, 3, False)])
def test_a_failure_on_either_side_fails_the_table(tmp_path, monkeypatch, src_exit, dst_exit, ok):
    import asyncio
    monkeypatch.setattr(L, "BIN", _fake_bin(tmp_path, src_exit, dst_exit))
    coro = L.copy_table("postgresql://src/db", "postgresql://dst/db", "public", "t")
    if ok:
        asyncio.run(coro)
    else:
        with pytest.raises(RuntimeError):
            asyncio.run(coro)


def test_post_data_is_split_into_typed_statements_in_order():
    dump = """
\\restrict abc123
--
-- Name: append_x append_x_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.append_x
    ADD CONSTRAINT append_x_pkey PRIMARY KEY (id);


--
-- Name: append_x_geom_gix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX append_x_geom_gix ON public.append_x USING gist (geom);


--
-- Name: diary_events trg_events_search; Type: TRIGGER; Schema: ocal; Owner: -
--

CREATE TRIGGER trg_events_search BEFORE INSERT OR UPDATE ON ocal.diary_events FOR EACH ROW EXECUTE FUNCTION ocal.diary_events_search_trigger();


--
-- Name: mv; Type: MATERIALIZED VIEW DATA; Schema: ocal; Owner: -
--

REFRESH MATERIALIZED VIEW ocal.mv;

\\unrestrict abc123
"""
    items = L.split_post_data(dump)
    assert [t for t, _ in items] == ["CONSTRAINT", "INDEX", "TRIGGER", "MATERIALIZED VIEW DATA"]
    assert items[0][1].startswith("ALTER TABLE ONLY public.append_x")
    assert "ADD CONSTRAINT append_x_pkey" in items[0][1]
    assert all("\\restrict" not in s and "\\unrestrict" not in s for _, s in items)
    assert items[3][1] == "REFRESH MATERIALIZED VIEW ocal.mv;"
