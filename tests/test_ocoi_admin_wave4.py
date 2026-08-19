"""Wave 4 admin surface: registry, ignore list, suggestions, site content.

These are structural tests — route shape, guards, and the two traps that cost
real debugging time (route ordering, and reusing one bind parameter at two
different SQL types).
"""
import inspect
import pathlib

from app.api import ocoi_admin, worker


def _routes(router):
    out = {}
    for r in router.routes:
        for m in (getattr(r, "methods", None) or set()) - {"HEAD", "OPTIONS"}:
            out.setdefault(r.path, set()).add(m)
    return out


B = "/api/admin/ocoi"
ADMIN = _routes(ocoi_admin.router)


# ── the routes exist, with the right verbs ────────────────────────────────────

def test_registry_routes():
    assert "GET" in ADMIN[f"{B}/registry/sources"]
    assert "POST" in ADMIN[f"{B}/registry/sync"]
    assert "POST" in ADMIN[f"{B}/registry/match"]
    assert "GET" in ADMIN[f"{B}/registry/records"]


def test_ignore_list_routes():
    verbs = ADMIN[f"{B}/ignored"]
    assert {"GET", "POST", "DELETE"} <= verbs


def test_suggestion_routes():
    assert "GET" in ADMIN[f"{B}/suggestions"]
    assert {"PATCH", "DELETE"} <= ADMIN[f"{B}/suggestions/{{suggestion_id}}"]


def test_content_routes():
    assert {"GET", "PUT"} <= ADMIN[f"{B}/content/{{key}}"]


def test_worker_serves_the_extraction_prompt():
    """OCOI kept the prompt on an ephemeral disk, so every admin edit was
    reverted by the next deploy. The worker must read it over HTTP instead."""
    assert "/api/worker/ocoi-config" in _routes(worker.router)


# ── guards ────────────────────────────────────────────────────────────────────

def test_every_wave4_route_requires_an_admin():
    from app.auth.dependencies import get_admin_user
    wave4 = ("/registry", "/ignored", "/suggestions", "/content")
    for r in ocoi_admin.router.routes:
        if not any(r.path.startswith(B + p) for p in wave4):
            continue
        deps = [d.call for d in r.dependant.dependencies]
        assert get_admin_user in deps, f"{r.path} is not admin-gated"


def test_content_keys_are_whitelisted():
    """An unknown key must 404 rather than create a row nothing renders."""
    assert "extraction_prompt" in ocoi_admin._CONTENT_KEYS
    src = inspect.getsource(ocoi_admin.put_content)
    assert "_CONTENT_KEYS" in src and "404" in src


def test_registry_records_count_is_capped():
    """797k rows: an exact count over a substring scan is the 39-second query
    the public API already learned about."""
    src = inspect.getsource(ocoi_admin.registry_records)
    assert "cap" in src and "LIMIT {cap + 1}" in src


def test_suggestion_review_does_not_reuse_one_param_at_two_types():
    """`status = $2` binds varchar while `$2 = 'pending'` binds text, and
    Postgres refuses the statement ("inconsistent types deduced for $2"). The
    resolved_at decision is made in Python for exactly that reason."""
    src = inspect.getsource(ocoi_admin.review_suggestion)
    assert "CASE WHEN $2" not in src
    assert "resolved = None if body.status" in src


def test_approving_a_suggestion_does_not_apply_it():
    """A submitted correction is a claim, not a fact — review only flags it."""
    src = inspect.getsource(ocoi_admin.review_suggestion)
    for table in ("persons", "companies", "associations", "entity_relationships"):
        assert f"UPDATE {table}" not in src


# ── ordering (the trap that broke /entities/merge) ────────────────────────────

def test_registry_subroutes_precede_no_wildcard_that_could_swallow_them():
    """`/registry/records` must not be shadowed by a `/registry/{something}`."""
    paths = [r.path for r in ocoi_admin.router.routes
             if r.path.startswith(f"{B}/registry")]
    assert not any("{" in p.split("/registry/")[1] for p in paths)


# ── the ignore list is what makes discovery converge ──────────────────────────

def test_discovery_reads_the_same_table_the_admin_writes():
    text = pathlib.Path("app/services/ocoi_ingest.py").read_text(encoding="utf-8")
    assert "ignored_resources" in text
    src = inspect.getsource(ocoi_admin.add_ignored)
    assert "ignored_resources" in src
    # Re-adding a URL must not raise: the duplicate-content path calls this
    # on every repeat push.
    assert "ON CONFLICT (file_url) DO NOTHING" in src
