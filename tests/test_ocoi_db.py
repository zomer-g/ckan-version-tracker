"""Unit tests for the OCOI access layer (app/services/ocoi_db.py).

Pure-function coverage only — no database required. What is worth pinning here
is the handful of ocoi-specific quirks that a future edit could silently undo:
the DSN normalisation asyncpg depends on, the search_path/timezone that keep
writes landing on the right schema and the right clock, and the tolerant
``aliases`` decoding (the column is TEXT-holding-JSON, and is inconsistent in
practice).
"""
from app.services import ocoi_db


class TestDsnNormalisation:
    def test_strips_dialect_suffix_and_libpq_only_params(self):
        # asyncpg takes plain postgresql:// and gets SSL via a kwarg; leaving
        # sslmode/channel_binding in the query string makes it raise.
        got = ocoi_db._dsn_from(
            "postgresql+asyncpg://u:p@h.neon.tech/db"
            "?sslmode=require&channel_binding=require"
        )
        assert got == "postgresql://u:p@h.neon.tech/db"

    def test_keeps_unrelated_query_params(self):
        got = ocoi_db._dsn_from("postgresql://u:p@h/db?application_name=over")
        assert got == "postgresql://u:p@h/db?application_name=over"

    def test_tolerates_whitespace_and_missing_scheme_parts(self):
        assert ocoi_db._dsn_from("  postgres://u@h/db  ") == "postgres://u@h/db"

    def test_drops_options_param(self):
        # `options` is libpq-only too and asyncpg rejects it.
        got = ocoi_db._dsn_from("postgresql://u@h/db?options=-csearch_path%3Dx")
        assert got == "postgresql://u@h/db"


class TestConnectionInvariants:
    def test_search_path_puts_ocoi_first(self):
        # Reads AND writes must land on the co-located schema, not public.
        assert ocoi_db._SEARCH_PATH.split(",")[0].strip() == "ocoi"

    def test_session_timezone_matches_the_corpus_clock(self):
        # ocoi's naive timestamps hold Israel wall clock. Writing UTC into them
        # would interleave rows 2-3 hours off with no way to tell them apart.
        assert ocoi_db._TIMEZONE == "Asia/Jerusalem"


class TestDecodeAliases:
    def test_json_array_string(self):
        assert ocoi_db.decode_aliases('["א", "ב"]') == ["א", "ב"]

    def test_none_and_empty_string_are_empty(self):
        assert ocoi_db.decode_aliases(None) == []
        assert ocoi_db.decode_aliases("") == []

    def test_malformed_json_degrades_to_empty(self):
        # An alias list is decoration; it must never fail an entity read.
        assert ocoi_db.decode_aliases("not json at all") == []

    def test_already_a_list_passes_through(self):
        assert ocoi_db.decode_aliases(["x", "y"]) == ["x", "y"]

    def test_non_list_json_is_empty(self):
        assert ocoi_db.decode_aliases('{"a": 1}') == []
        assert ocoi_db.decode_aliases("42") == []

    def test_falsy_members_dropped(self):
        assert ocoi_db.decode_aliases('["a", "", null]') == ["a"]


class TestFeatureGate:
    def test_is_configured_reflects_the_setting(self, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "ocoi_database_url", "", raising=False)
        assert ocoi_db.is_configured() is False
        monkeypatch.setattr(
            settings, "ocoi_database_url", "postgresql://u@h/db", raising=False
        )
        assert ocoi_db.is_configured() is True
