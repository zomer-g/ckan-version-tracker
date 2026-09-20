"""Every route annotation must be resolvable where FastAPI looks for it.

`/openapi.json` was 500 in production, and the cause is a trap this codebase is
uniquely exposed to because every route is wrapped by `@limiter.limit`.

`functools.wraps` copies a function's metadata but NOT its globals: the wrapper
slowapi returns carries `__globals__` pointing at `slowapi.extension`. FastAPI's
`get_typed_signature` resolves a STRING annotation against
`call.__globals__` — so in a module with `from __future__ import annotations`,
where every annotation IS a string, none of that module's own names can be
found. `app/api/ocoi.py` and `app/api/ocoi_admin.py` had it, and 63 parameters
across them were left as unresolved ForwardRefs. One of them was
`body: SuggestionIn` on an anonymous POST, which FastAPI then treated as a
QUERY parameter, and the unresolvable ForwardRef broke schema generation for
the whole app.

This test does exactly what FastAPI does, so it does not depend on the FastAPI
or Python version: the failure was invisible locally because the dev machine
runs fastapi 0.135 / pydantic 2.12 / py3.14 against a pin of 0.115 / 2.10 /
3.13, and a version-specific reproduction was not available. An annotation that
needs no resolution cannot be resolved wrongly by any version.
"""
import inspect
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from fastapi.routing import APIRoute  # noqa: E402

from app.main import app  # noqa: E402


def _unresolvable() -> list[str]:
    out = []
    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue
        fn = route.endpoint
        globalns = getattr(fn, "__globals__", {})
        for name, param in inspect.signature(fn).parameters.items():
            annotation = param.annotation
            if not isinstance(annotation, str):
                continue  # a real object needs no resolution
            try:
                eval(annotation, globalns, globalns)  # noqa: S307 — what FastAPI does
            except Exception as exc:  # noqa: BLE001
                out.append(
                    f"{sorted(route.methods)} {route.path} → {name}: {annotation} "
                    f"({type(exc).__name__}; globals are "
                    f"{globalns.get('__name__')!r})"
                )
    return out


def test_no_route_annotation_is_unresolvable():
    bad = _unresolvable()
    assert bad == [], (
        "These annotations are strings that FastAPI cannot resolve, because the "
        "endpoint's __globals__ belong to slowapi rather than to the module that "
        "wrote them. Drop `from __future__ import annotations` from the module, "
        "or import the name into a place the wrapper can see:\n  "
        + "\n  ".join(bad)
    )


def test_the_openapi_schema_can_be_built():
    """The symptom the annotations caused: /openapi.json, and so /docs, 500."""
    schema = app.openapi()
    assert schema.get("paths"), "the schema has no paths"


def test_a_model_body_is_a_body_parameter_not_a_query_parameter():
    """An unresolved annotation does not merely omit a schema: FastAPI falls
    back to treating the parameter as a query string, so the endpoint stops
    accepting the JSON it documents."""
    route = next(r for r in app.routes
                 if isinstance(r, APIRoute) and r.path == "/api/ocoi/suggestions")
    assert [f.name for f in route.dependant.body_params] == ["body"]
    assert [f.name for f in route.dependant.query_params] == []
