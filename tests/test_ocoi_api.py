"""Unit tests for the OCOI public API helpers (app/api/ocoi.py).

Covers the pure logic without a database. This is deliberately where the effort
goes: every bug found while writing this router was in exactly these functions —
SQL placeholder numbering, the hidden-entity prune, and a closure variable that
FastAPI would have published as a client-settable query parameter.
"""
import pytest
from fastapi.testclient import TestClient

from app.api import ocoi


# ---------------------------------------------------------------------------
# Origin filtering — a whitelist, because it shapes a SQL predicate
# ---------------------------------------------------------------------------

class TestParseOrigins:
    def test_empty_and_none(self):
        assert ocoi._parse_origins(None) == []
        assert ocoi._parse_origins("") == []

    def test_known_values_pass(self):
        assert ocoi._parse_origins("mk_expense") == ["mk_expense"]
        got = ocoi._parse_origins("mk_expense,coi_declaration")
        assert set(got) == {"mk_expense", "coi_declaration"}

    def test_unknown_values_dropped_not_rejected(self):
        # A stale bookmark should degrade to "no filter", not 400.
        assert ocoi._parse_origins("bogus") == []
        assert ocoi._parse_origins("bogus,mk_expense") == ["mk_expense"]

    def test_injection_attempt_is_dropped(self):
        assert ocoi._parse_origins("x') OR 1=1 --") == []

    def test_whitespace_tolerated(self):
        assert ocoi._parse_origins("  mk_expense  ") == ["mk_expense"]


class TestOriginClause:
    def test_empty_produces_no_sql_and_no_params(self):
        sql, params = ocoi._origin_clause([])
        assert sql == "" and params == []

    def test_placeholders_start_at_requested_index(self):
        sql, params = ocoi._origin_clause(["mk_expense"], 3)
        assert "$3" in sql and params == ["mk_expense"]

    def test_multiple_placeholders_are_contiguous(self):
        sql, params = ocoi._origin_clause(["mk_expense", "coi_declaration"], 4)
        assert "$4" in sql and "$5" in sql
        assert len(params) == 2

    def test_default_start_index_is_one(self):
        # ministries() calls this with no index; a required arg was a TypeError.
        sql, _ = ocoi._origin_clause(["mk_expense"])
        assert "$1" in sql

    def test_alias_is_honoured(self):
        sql, _ = ocoi._origin_clause(["mk_expense"], 1, alias="w")
        assert "w.origin_kind" in sql

    def test_values_are_bound_never_inlined(self):
        sql, params = ocoi._origin_clause(["mk_expense"], 1)
        assert "mk_expense" not in sql
        assert params == ["mk_expense"]


# ---------------------------------------------------------------------------
# Hidden pruning — nodes AND the edges that touch them
# ---------------------------------------------------------------------------

def _edge(st, sid, tt, tid):
    return {
        "source_entity_type": st, "source_entity_id": sid,
        "target_entity_type": tt, "target_entity_id": tid,
        "relationship_type": "holds", "details": None,
        "origin_kind": "coi_declaration", "verified": False,
        "document_id": "d1", "doc_title": "t", "doc_url": "u",
    }


class TestPruneHidden:
    def test_keeps_edges_between_visible_entities(self):
        edges = [_edge("person", "p1", "company", "c1")]
        assert ocoi._prune_hidden(edges, {"person": set(), "company": set()}) == edges

    def test_drops_edge_when_source_hidden(self):
        edges = [_edge("person", "p1", "company", "c1")]
        assert ocoi._prune_hidden(edges, {"person": {"p1"}}) == []

    def test_drops_edge_when_target_hidden(self):
        edges = [_edge("person", "p1", "company", "c1")]
        assert ocoi._prune_hidden(edges, {"company": {"c1"}}) == []

    def test_hidden_id_of_another_type_does_not_match(self):
        # Ids are type-scoped; a hidden company must not hide a person sharing an id.
        edges = [_edge("person", "x", "company", "c1")]
        assert ocoi._prune_hidden(edges, {"company": {"x"}}) == edges

    def test_mixed_set_keeps_only_clean_edges(self):
        edges = [
            _edge("person", "p1", "company", "c1"),
            _edge("person", "p2", "company", "c1"),
        ]
        kept = ocoi._prune_hidden(edges, {"person": {"p1"}})
        assert len(kept) == 1 and kept[0]["source_entity_id"] == "p2"


# ---------------------------------------------------------------------------
# Subgraph assembly
# ---------------------------------------------------------------------------

class TestSubgraph:
    def test_nodes_are_deduplicated_across_edges(self):
        edges = [
            _edge("person", "p1", "company", "c1"),
            _edge("person", "p1", "company", "c2"),
        ]
        g = ocoi._subgraph(edges, {})
        assert len(g["edges"]) == 2
        ids = sorted(n["id"] for n in g["nodes"])
        assert ids == ["c1", "c2", "p1"]

    def test_unhydrated_node_gets_empty_name_not_a_crash(self):
        g = ocoi._subgraph([_edge("person", "p1", "company", "c1")], {})
        assert all(n["name"] == "" for n in g["nodes"])

    def test_hydrated_names_are_used(self):
        names = {("person", "p1"): {"id": "p1", "entity_type": "person", "name": "דנה"}}
        g = ocoi._subgraph([_edge("person", "p1", "company", "c1")], names)
        assert any(n.get("name") == "דנה" for n in g["nodes"])

    def test_edge_carries_document_provenance(self):
        g = ocoi._subgraph([_edge("person", "p1", "company", "c1")], {})
        e = g["edges"][0]
        assert e["document_id"] == "d1"
        assert e["document_title"] == "t" and e["document_url"] == "u"

    def test_truncated_flag_propagates(self):
        assert ocoi._subgraph([], {}, truncated=True)["truncated"] is True
        assert ocoi._subgraph([], {})["truncated"] is False


# ---------------------------------------------------------------------------
# Validation + envelope
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_uuid_shape_accepted_and_returned_as_str(self):
        u = "123e4567-e89b-12d3-a456-426614174000"
        assert ocoi._require_id(u) == u
        assert isinstance(ocoi._require_id(u), str)

    @pytest.mark.parametrize("bad", ["", "abc", "1; DROP TABLE persons", None])
    def test_bad_ids_rejected(self, bad):
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as e:
            ocoi._require_id(bad)
        assert e.value.status_code == 400

    def test_entity_type_whitelist(self):
        from fastapi import HTTPException
        for t in ("person", "company", "association", "domain"):
            assert ocoi._require_entity_type(t) == t
        with pytest.raises(HTTPException) as e:
            ocoi._require_entity_type("users")
        assert e.value.status_code == 400

    def test_every_entity_type_has_columns_defined(self):
        # A new type added to one dict and not the other would KeyError at runtime.
        assert set(ocoi._ENTITY_TABLES) == set(ocoi._ENTITY_COLUMNS)


class TestEnvelope:
    def test_ok_without_meta_omits_the_key(self):
        assert ocoi._ok([1]) == {"status": "ok", "data": [1]}

    def test_ok_with_meta(self):
        out = ocoi._ok([], {"total": 0})
        assert out["status"] == "ok" and out["meta"] == {"total": 0}

    def test_page_meta_rounds_pages_up(self):
        assert ocoi._page_meta(21, 1, 20)["pages"] == 2
        assert ocoi._page_meta(20, 1, 20)["pages"] == 1
        assert ocoi._page_meta(0, 1, 20)["pages"] == 0

    def test_entity_row_decodes_aliases_and_stamps_type(self):
        row = ocoi._entity_row({"id": "1", "name_hebrew": "x", "aliases": '["a"]'},
                               "person")
        assert row["aliases"] == ["a"] and row["entity_type"] == "person"


# ---------------------------------------------------------------------------
# Routing surface — guards that a refactor could silently break
# ---------------------------------------------------------------------------

class TestRoutingSurface:
    @pytest.fixture(scope="class")
    def client(self):
        from app.main import app
        return TestClient(app, raise_server_exceptions=False)

    def test_generated_entity_routes_expose_no_internal_params(self, client):
        """`etype` must stay a closure, never a query parameter.

        Written as a signature test because the failure mode is silent: FastAPI
        would happily accept ?_etype=users and read another table.
        """
        from app.main import app
        schema = app.openapi()
        for path in ("/api/ocoi/persons", "/api/ocoi/companies",
                     "/api/ocoi/associations", "/api/ocoi/domains"):
            names = [p["name"] for p in schema["paths"][path]["get"].get("parameters", [])]
            assert names == ["page", "limit", "q"], f"{path} exposes {names}"

    def test_all_four_entity_kinds_are_registered(self, client):
        from app.main import app
        paths = {r.path for r in app.routes if getattr(r, "path", "").startswith("/api/ocoi")}
        for p in ("persons", "companies", "associations", "domains"):
            assert f"/api/ocoi/{p}" in paths
            assert f"/api/ocoi/{p}/{{entity_id}}" in paths
            assert f"/api/ocoi/{p}/{{entity_id}}/documents" in paths

    def test_unconfigured_returns_503_not_500(self, client, monkeypatch):
        from app.config import settings
        monkeypatch.setattr(settings, "ocoi_database_url", "", raising=False)
        assert client.get("/api/ocoi/stats").status_code == 503

    def test_no_db_health_endpoint_was_ported(self, client):
        # OCOI's /api/db-health leaks 3000 chars of traceback in a 500.
        from app.main import app
        paths = {r.path for r in app.routes if getattr(r, "path", "")}
        assert "/api/ocoi/db-health" not in paths
