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
