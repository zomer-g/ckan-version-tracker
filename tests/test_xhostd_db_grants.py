"""The statements scripts/xhostd_db_grants.py applies at every xhostd boot."""
import importlib.util
import os
import pathlib

os.environ.setdefault("JWT_SECRET_KEY", "test")

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("grants", ROOT / "scripts" / "xhostd_db_grants.py")
G = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(G)

RO = "r_77ac5c28731046659e6e35dbb904bede_ro"


def test_functions_become_executable_in_every_present_data_schema_and_by_default():
    out = G.statements(RO, {"public", "ocal", "idx", "app", "auth"}, [])
    assert f'GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "public" TO "{RO}"' in out
    assert f'ALTER DEFAULT PRIVILEGES IN SCHEMA "ocal" GRANT EXECUTE ON FUNCTIONS TO "{RO}"' in out
    assert not any('"knesset"' in s for s in out), "absent schemas are skipped"


def test_private_schemas_never_get_anything():
    out = G.statements(RO, {"public", "app", "auth", "_loader"}, [])
    assert not any(s for s in out if '"app"' in s or '"auth"' in s or '"_loader"' in s)


def test_hidden_tables_are_revoked_after_the_grants():
    out = G.statements(RO, {"public"}, ["over_re_geocode"])
    assert out[-1] == f'REVOKE ALL ON public."over_re_geocode" FROM "{RO}"'


def test_the_hidden_list_is_the_catalog_list():
    from app.services.data_catalog import _OVER_HIDDEN
    assert "over_re_geocode" in _OVER_HIDDEN
