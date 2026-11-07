"""/openapi.json documents the public API, not the admin or worker routes."""
from fastapi.testclient import TestClient

from app.main import app


def test_schema_has_public_routes_and_no_internal_ones():
    paths = TestClient(app).get("/openapi.json").json()["paths"]
    assert "/api/datasets" in paths
    assert "/api/tables/sql" in paths
    leaked = [p for p in paths if p.startswith(("/api/admin", "/api/worker"))]
    assert leaked == []
    # Admin routes that live outside /api/admin are caught by their dependency.
    assert "/api/datasets/{dataset_id}" not in paths or "delete" not in paths["/api/datasets/{dataset_id}"]
