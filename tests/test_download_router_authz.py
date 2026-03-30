import time
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import get_supabase_client, router
from tests.supabase_auth_test_utils import (
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping downloads authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(
            f"Skipping downloads authz integration test: Supabase is not reachable/authenticated: {error}"
        )
    return client


@pytest.fixture(scope="module")
def service_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping downloads authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
    except Exception as error:
        pytest.skip(
            f"Skipping downloads authz integration test: Supabase service-role client unavailable: {error}"
        )
    if not client.using_service_role:
        pytest.skip(
            "Skipping downloads authz integration test: SUPABASE_SERVICE_KEY is required for fixture setup"
        )
    return client


@pytest.fixture(scope="module")
def auth_tokens() -> tuple[str, str]:
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping downloads authz integration test"
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
        email=f"downloads-outsider-{int(time.time())}@example.test",
        password="DownloadsAuthzPassw0rd!",
    )
    return owner_token, outsider_token


def _resolve_user_identity(supabase_url: str, supabase_key: str, token: str) -> tuple[str, str]:
    response = httpx.get(
        f"{supabase_url}/auth/v1/user",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {token}",
        },
        timeout=20.0,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["id"], payload["email"]


@pytest.fixture
def downloads_api_client(local_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_supabase_client] = lambda: local_supabase_client
    return TestClient(app)


def _create_dataset(local_supabase_client: SupabaseClient, visibility: str, title_prefix: str):
    suffix = int(time.time() * 1000)
    return local_supabase_client.create_dataset(
        bucket_path=f"RAW/downloads-authz/{suffix}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"{title_prefix} {suffix}",
        file_name="raw.laz",
        visibility=visibility,
    )


def _cleanup_dataset(local_supabase_client: SupabaseClient, dataset_id: int):
    local_supabase_client.client.table("download_requests").delete().eq(
        "dataset_id", dataset_id
    ).execute()
    local_supabase_client.client.table("datasets").delete().eq("id", dataset_id).execute()


@pytest.mark.parametrize(
    ("visibility", "expected_status"),
    [
        ("public", 200),
        ("view_only", 403),
        ("private", 403),
    ],
)
def test_download_request_access_for_non_owner(
    downloads_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
    visibility: str,
    expected_status: int,
):
    _, outsider_token = auth_tokens
    dataset = _create_dataset(
        local_supabase_client=local_supabase_client,
        visibility=visibility,
        title_prefix=f"Downloads non-owner {visibility}",
    )

    try:
        response = downloads_api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert response.status_code == expected_status, response.text
        if expected_status == 200:
            payload = response.json()
            assert payload["dataset_id"] == dataset.id
            assert payload["status"] in {"pending", "processing"}
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)


@pytest.mark.parametrize("visibility", ["private", "view_only"])
def test_download_request_allows_owner_for_restricted_visibility(
    downloads_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
    visibility: str,
):
    owner_token, _ = auth_tokens
    dataset = _create_dataset(
        local_supabase_client=local_supabase_client,
        visibility=visibility,
        title_prefix=f"Downloads owner {visibility}",
    )

    try:
        response = downloads_api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["dataset_id"] == dataset.id
        assert payload["status"] in {"pending", "processing"}
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)


def test_download_request_requires_authorization_header(
    downloads_api_client: TestClient,
):
    response = downloads_api_client.post(
        "/downloads",
        json={
            "dataset_id": 1,
            "include_raw": True,
            "include_segmentation": False,
        },
    )
    assert response.status_code == 401


def test_download_list_and_get_are_user_scoped(
    downloads_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, outsider_token = auth_tokens
    dataset = _create_dataset(
        local_supabase_client=local_supabase_client,
        visibility="public",
        title_prefix="Downloads scoped list/get",
    )

    request_id = None
    try:
        create_response = downloads_api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert create_response.status_code == 200, create_response.text
        request_id = create_response.json()["id"]

        outsider_list = downloads_api_client.get(
            "/downloads",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_list.status_code == 200
        assert all(row["id"] != request_id for row in outsider_list.json())

        outsider_get = downloads_api_client.get(
            f"/downloads/{request_id}",
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_get.status_code == 404
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)


def test_core_team_member_can_request_download_for_private_dataset(
    downloads_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, _, _ = require_supabase_auth_env(
        skip_prefix="Skipping downloads authz integration test"
    )
    core_email = f"downloads-core-team-{int(time.time())}@example.test"
    core_password = "DownloadsCoreTeamPassw0rd!"
    core_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=core_email,
        password=core_password,
    )
    core_user_id, core_user_email = _resolve_user_identity(
        supabase_url, supabase_key, core_token
    )

    dataset = _create_dataset(
        local_supabase_client=local_supabase_client,
        visibility="private",
        title_prefix="Downloads core team private",
    )
    service_supabase_client.client.table("core_team_members").upsert(
        {"user_id": core_user_id, "email": core_user_email}
    ).execute()

    try:
        response = downloads_api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {core_token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert response.status_code == 200, response.text
        payload = response.json()
        assert payload["dataset_id"] == dataset.id
        assert payload["status"] in {"pending", "processing"}
    finally:
        service_supabase_client.client.table("core_team_members").delete().eq(
            "user_id", core_user_id
        ).execute()
        _cleanup_dataset(local_supabase_client, dataset.id)


def test_shared_read_user_can_request_download_for_private_dataset(
    downloads_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
):
    supabase_url, supabase_key, _, _ = require_supabase_auth_env(
        skip_prefix="Skipping downloads authz integration test"
    )
    shared_email = f"downloads-shared-read-{int(time.time())}@example.test"
    shared_password = "DownloadsSharedReadPassw0rd!"
    shared_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=shared_email,
        password=shared_password,
    )
    shared_user_id, _ = _resolve_user_identity(supabase_url, supabase_key, shared_token)

    dataset = _create_dataset(
        local_supabase_client=local_supabase_client,
        visibility="private",
        title_prefix="Downloads shared read private",
    )
    service_supabase_client.client.table("dataset_user_access").upsert(
        {
            "dataset_id": dataset.id,
            "grantee_user_id": shared_user_id,
            "permission": "read",
            "granted_by_user_id": dataset.user_id,
        }
    ).execute()

    try:
        response = downloads_api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {shared_token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert response.status_code == 200, response.text
    finally:
        service_supabase_client.client.table("dataset_user_access").delete().eq(
            "dataset_id", dataset.id
        ).eq("grantee_user_id", shared_user_id).execute()
        _cleanup_dataset(local_supabase_client, dataset.id)
