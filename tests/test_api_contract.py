from fastapi.testclient import TestClient


def test_api_routes_are_wired():
    from trees_api.app.server import app

    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/health" in paths
    assert "/version" in paths
    assert "/jobs" in paths
    assert "/downloads" in paths
    assert "/upload/multipart/create" in paths


def test_jobs_endpoint_returns_503_when_dependencies_unavailable():
    from trees_api.app.server import app
    from trees_api.routes.jobs.router import (
        get_galaxy_client,
        get_storage_client,
        get_supabase_client,
    )

    app.dependency_overrides[get_galaxy_client] = lambda: None
    app.dependency_overrides[get_supabase_client] = lambda: None
    app.dependency_overrides[get_storage_client] = lambda: None

    try:
        client = TestClient(app)
        response = client.post(
            "/jobs",
            params={"dataset_id": "1", "workflow_name": "noop"},
        )
        assert response.status_code == 503
    finally:
        app.dependency_overrides.clear()

