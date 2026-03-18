from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
import time
from datetime import datetime

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import get_supabase_client as get_downloads_supabase_client
from trees_api.routes.jobs.router import (
    get_galaxy_client,
    get_storage_client,
    get_supabase_client,
    router,
)
from tests.supabase_auth_test_utils import (
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping jobs authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(
            f"Skipping jobs authz integration test: Supabase is not reachable/authenticated: {error}"
        )
    return client


@pytest.fixture(scope="module")
def auth_tokens() -> tuple[str, str]:
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping jobs authz integration test"
    )

    owner_token = password_login_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=owner_email,
        password=owner_password,
    )

    outsider_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=f"jobs-outsider-{int(time.time())}@example.test",
        password="JobsAuthzPassw0rd!",
    )
    return owner_token, outsider_token


@pytest.fixture
def jobs_api_client(local_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_downloads_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_galaxy_client] = lambda: object()
    app.dependency_overrides[get_storage_client] = lambda: object()
    return TestClient(app)


def test_create_job_denies_non_owner(jobs_api_client: TestClient, local_supabase_client: SupabaseClient, auth_tokens: tuple[str, str]):
    _, outsider_token = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.post(
            "/jobs",
            params={"dataset_id": str(dataset.id), "workflow_name": "EndToEndPipeline"},
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={},
        )
        assert response.status_code == 403
        assert "dataset owner" in response.json()["detail"]
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_list_jobs_returns_empty_for_foreign_dataset(jobs_api_client: TestClient, local_supabase_client: SupabaseClient, auth_tokens: tuple[str, str]):
    _, outsider_token = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs List Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.get(
            "/jobs",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert response.status_code == 200
        assert response.json() == []
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_list_jobs_allows_owner_on_own_dataset(
    jobs_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/jobs-authz/{int(time.time())}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Jobs Owner List Authz {int(time.time())}",
        file_name="raw.laz",
        visibility="private",
    )

    try:
        response = jobs_api_client.get(
            "/jobs",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert response.status_code == 200, response.text
        assert isinstance(response.json(), list)
    finally:
        local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()


def test_jobs_requires_authorization_header(jobs_api_client: TestClient):
    response = jobs_api_client.post(
        "/jobs",
        params={"dataset_id": "1", "workflow_name": "EndToEndPipeline"},
        json={},
    )
    assert response.status_code == 401
