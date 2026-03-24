import time
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.collections.router import (
    get_supabase_client as get_collections_supabase_client,
    router,
)
from trees_api.routes.downloads.router import (
    get_supabase_client as get_downloads_supabase_client,
)
from tests.supabase_auth_test_utils import (
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


def _require_collections_schema(local_supabase_client: SupabaseClient) -> None:
    try:
        local_supabase_client.client.table("collections").select("id").limit(1).execute()
        local_supabase_client.client.table("datasets").select("id, collection_id").limit(1).execute()
    except Exception as error:
        pytest.skip(f"Skipping collections authz integration test: collections schema unavailable: {error}")


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping collections authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(
            f"Skipping collections authz integration test: Supabase is not reachable/authenticated: {error}"
        )
    _require_collections_schema(client)
    return client


@pytest.fixture(scope="module")
def auth_tokens() -> tuple[str, str]:
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping collections authz integration test"
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
        email=f"collections-outsider-{int(time.time())}@example.test",
        password="CollectionsAuthzPassw0rd!",
    )
    return owner_token, outsider_token


@pytest.fixture
def collections_api_client(local_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_collections_supabase_client] = lambda: local_supabase_client
    # Authentication dependency comes from downloads router.
    app.dependency_overrides[get_downloads_supabase_client] = lambda: local_supabase_client
    return TestClient(app)


def _create_private_dataset(local_supabase_client: SupabaseClient):
    suffix = int(time.time() * 1000)
    return local_supabase_client.create_dataset(
        bucket_path=f"RAW/collections-authz/{suffix}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Collections authz {suffix}",
        file_name="raw.laz",
        visibility="private",
    )


def _cleanup_dataset(local_supabase_client: SupabaseClient, dataset_id: int) -> None:
    local_supabase_client.client.table("download_requests").delete().eq("dataset_id", dataset_id).execute()
    local_supabase_client.client.table("datasets").delete().eq("id", dataset_id).execute()


def _cleanup_collection(local_supabase_client: SupabaseClient, collection_id: int) -> None:
    local_supabase_client.client.table("collections").delete().eq("id", collection_id).execute()


def _get_dataset_collection_id(local_supabase_client: SupabaseClient, dataset_id: int):
    response = (
        local_supabase_client.client.table("datasets")
        .select("collection_id")
        .eq("id", dataset_id)
        .limit(1)
        .execute()
    )
    if not response.data:
        return None
    return response.data[0].get("collection_id")


def test_collections_requires_authorization_header(collections_api_client: TestClient):
    response = collections_api_client.get("/collections")
    assert response.status_code == 401

    assignments_response = collections_api_client.get("/collections/datasets")
    assert assignments_response.status_code == 401

    assign_response = collections_api_client.put(
        "/collections/datasets/1/collection",
        json={"collection_id": None},
    )
    assert assign_response.status_code == 401


def test_collection_owner_crud_and_assignment(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    dataset = _create_private_dataset(local_supabase_client)
    collection_id = None

    try:
        create_response = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "name": f"Collection owner flow {int(time.time())}",
                "description": "Collection for integration testing",
            },
        )
        assert create_response.status_code == 200, create_response.text
        created = create_response.json()
        collection_id = created["id"]

        list_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert list_response.status_code == 200, list_response.text
        assert any(row["id"] == collection_id for row in list_response.json())

        assign_response = collections_api_client.put(
            f"/collections/datasets/{dataset.id}/collection",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={"collection_id": collection_id},
        )
        assert assign_response.status_code == 200, assign_response.text
        assert assign_response.json()["collection_id"] == collection_id
        assert _get_dataset_collection_id(local_supabase_client, dataset.id) == collection_id

        update_response = collections_api_client.patch(
            f"/collections/{collection_id}",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "description": "Updated description",
            },
        )
        assert update_response.status_code == 200, update_response.text
        updated = update_response.json()
        assert updated["description"] == "Updated description"

        archive_response = collections_api_client.delete(
            f"/collections/{collection_id}",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert archive_response.status_code == 200, archive_response.text
        archived = archive_response.json()
        assert archived["status"] == "archived"
        assert archived["unassigned_dataset_count"] >= 1
        assert _get_dataset_collection_id(local_supabase_client, dataset.id) is None

        list_active_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert list_active_response.status_code == 200
        assert all(row["id"] != collection_id for row in list_active_response.json())

        list_archived_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            params={"include_archived": "true"},
        )
        assert list_archived_response.status_code == 200
        archived_rows = [row for row in list_archived_response.json() if row["id"] == collection_id]
        assert len(archived_rows) == 1
        assert archived_rows[0]["archived"] is True
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)
        if collection_id is not None:
            _cleanup_collection(local_supabase_client, collection_id)


def test_collection_access_denied_for_outsider(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, outsider_token = auth_tokens
    dataset = _create_private_dataset(local_supabase_client)
    collection_id = None

    try:
        create_response = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "name": f"Collection outsider access {int(time.time())}",
                "description": "Only owner should see this",
            },
        )
        assert create_response.status_code == 200, create_response.text
        collection_id = create_response.json()["id"]

        outsider_list = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_list.status_code == 200, outsider_list.text
        assert all(row["id"] != collection_id for row in outsider_list.json())

        outsider_patch = collections_api_client.patch(
            f"/collections/{collection_id}",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={"description": "hijacked"},
        )
        assert outsider_patch.status_code == 404

        outsider_delete = collections_api_client.delete(
            f"/collections/{collection_id}",
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_delete.status_code == 404

        outsider_assign = collections_api_client.put(
            f"/collections/datasets/{dataset.id}/collection",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={"collection_id": collection_id},
        )
        assert outsider_assign.status_code == 404
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)
        if collection_id is not None:
            _cleanup_collection(local_supabase_client, collection_id)


def test_collection_name_must_be_unique_for_owner(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    name = f"Collection unique {int(time.time())}"
    created_ids: list[int] = []

    try:
        first = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={"name": name, "description": "first"},
        )
        assert first.status_code == 200, first.text
        created_ids.append(first.json()["id"])

        second = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={"name": name, "description": "second"},
        )
        assert second.status_code == 409
    finally:
        for collection_id in created_ids:
            _cleanup_collection(local_supabase_client, collection_id)


def test_collection_name_cannot_be_whitespace_only(
    collections_api_client: TestClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    response = collections_api_client.post(
        "/collections",
        headers={"Authorization": f"Bearer {owner_token}"},
        json={"name": "   ", "description": "invalid"},
    )
    assert response.status_code == 422


def test_collection_update_name_cannot_be_whitespace_only(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    create_response = collections_api_client.post(
        "/collections",
        headers={"Authorization": f"Bearer {owner_token}"},
        json={"name": f"Collection update validation {int(time.time())}", "description": "valid"},
    )
    assert create_response.status_code == 200, create_response.text
    collection_id = create_response.json()["id"]

    try:
        update_response = collections_api_client.patch(
            f"/collections/{collection_id}",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={"name": "   "},
        )
        assert update_response.status_code == 422
    finally:
        _cleanup_collection(local_supabase_client, collection_id)

