from fastapi.testclient import TestClient


def test_api_routes_are_wired():
    from trees_api.app.server import app

    paths = {route.path for route in app.routes}
    assert "/" in paths
    assert "/health" in paths
    assert "/version" in paths
    assert "/jobs" in paths
    assert "/downloads" in paths
    assert "/ingestions" in paths
    assert "/upload/multipart/create" not in paths


def test_jobs_endpoint_returns_503_when_dependencies_unavailable():
    from trees_api.app.server import app
    from trees_api.routes.downloads.router import AuthenticatedUser
    from trees_api.routes.jobs.router import (
        get_authenticated_user,
        get_galaxy_client,
        get_storage_client,
        get_supabase_client,
    )

    app.dependency_overrides[get_authenticated_user] = lambda: AuthenticatedUser(
        id="user-1",
        email="user@example.com",
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


def test_jobs_endpoint_requires_authorization_header():
    from trees_api.app.server import app
    from trees_api.routes.downloads.router import get_supabase_client as get_downloads_supabase_client

    app.dependency_overrides[get_downloads_supabase_client] = lambda: object()

    try:
        client = TestClient(app)
        response = client.post(
            "/jobs",
            params={"dataset_id": "1", "workflow_name": "noop"},
            json={},
        )
        assert response.status_code == 401
    finally:
        app.dependency_overrides.clear()


def test_ingestions_endpoint_requires_authorization_header():
    from trees_api.app.server import app
    from trees_api.routes.downloads.router import get_supabase_client as get_downloads_supabase_client
    from trees_api.routes.ingestions.router import get_supabase_client as get_ingestions_supabase_client

    app.dependency_overrides[get_downloads_supabase_client] = lambda: object()
    app.dependency_overrides[get_ingestions_supabase_client] = lambda: object()

    try:
        client = TestClient(app)
        response = client.get("/ingestions")
        assert response.status_code == 401
    finally:
        app.dependency_overrides.clear()


def test_cors_allows_idempotency_key_header():
    from trees_api.app.server import ALLOWED_HEADERS

    normalized = {header.lower() for header in ALLOWED_HEADERS}
    assert "idempotency-key" in normalized

