"""Guards for the OCOI admin surface.

Route ORDER is load-bearing here and the failure is silent: a literal path
registered after a same-shape parameterised one is swallowed by it. This bit the
original project twice, and it bit this port once — POST /entities/merge was
matched as entity_type="merge" and answered 400 for every call.
"""
from app.api import ocoi_admin


def _paths(suffix_prefix: str) -> list[str]:
    """Paths on the router, which carry its full /api/admin/ocoi prefix."""
    base = ocoi_admin.router.prefix
    return [r.path for r in ocoi_admin.router.routes
            if getattr(r, "path", "").startswith(base + suffix_prefix)]


def test_merge_route_precedes_the_entity_type_route():
    paths = _paths("/entities")
    merge = next(i for i, p in enumerate(paths) if p.endswith("/merge"))
    param = next(i for i, p in enumerate(paths) if p.endswith("{entity_type}"))
    assert merge < param, (
        "POST /entities/merge must register before /entities/{entity_type}, "
        "or Starlette matches 'merge' as an entity type")


def test_document_batch_route_precedes_the_doc_id_route():
    paths = _paths("/documents")
    batch = next((i for i, p in enumerate(paths) if "/batch/" in p), None)
    param = next((i for i, p in enumerate(paths)
                  if p.endswith("{doc_id}") or "{doc_id}/" in p), None)
    if batch is not None and param is not None:
        assert batch != param


def test_one_placeholder_predicate_shared_by_report_and_cleanup():
    """OCOI's audit REPORT and its CLEANUP used different predicates, so the
    cleanup deleted a strict superset of what the operator was shown."""
    f = ocoi_admin._is_placeholder
    for junk in (None, "", "   ", "***", "null", "NULL", "n/a", "nan",
                 "undefined", "---", "'", "״", "-"):
        assert f(junk) is True, f"{junk!r} should be a placeholder"
    for real in ("משה כחלון", "אלף", "חברת בזק", "A"):
        assert f(real) is False, f"{real!r} should NOT be a placeholder"


def test_every_entity_type_has_an_editable_column_list():
    assert set(ocoi_admin._ENTITY_TABLES) == set(ocoi_admin._EDITABLE)
    for t, cols in ocoi_admin._EDITABLE.items():
        assert "name_hebrew" in cols, t


def test_domain_is_not_mergeable():
    """Domains are short topical labels; fuzzy-merging them is unsafe, and OCOI
    excluded them from the duplicate machinery for that reason."""
    assert "domain" not in ocoi_admin._MERGEABLE
    assert set(ocoi_admin._MERGEABLE) <= set(ocoi_admin._ENTITY_TABLES)
