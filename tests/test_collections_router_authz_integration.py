import time
from datetime import datetime

import httpx
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
def service_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping collections authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
    except Exception as error:
        pytest.skip(
            f"Skipping collections authz integration test: Supabase service-role client unavailable: {error}"
        )
    if not client.using_service_role:
        pytest.skip(
            "Skipping collections authz integration test: SUPABASE_SERVICE_KEY is required for fixture setup"
        )
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
def collections_api_client(service_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_collections_supabase_client] = lambda: service_supabase_client
    # Authentication dependency comes from downloads router.
    app.dependency_overrides[get_downloads_supabase_client] = lambda: service_supabase_client
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


def test_platform_admin_can_manage_owner_collections_and_preserve_same_owner_assignment(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, outsider_token = auth_tokens
    supabase_url, supabase_key, _, _ = require_supabase_auth_env(
        skip_prefix="Skipping collections authz integration test"
    )
    platform_admin_email = f"collections-platform-admin-{int(time.time())}@example.test"
    platform_admin_password = "CollectionsPlatformAdminPassw0rd!"
    platform_admin_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=platform_admin_email,
        password=platform_admin_password,
    )
    platform_admin_user_id, platform_admin_email = _resolve_user_identity(
        supabase_url, supabase_key, platform_admin_token
    )

    dataset = _create_private_dataset(local_supabase_client)
    platform_admin_collection_id = None
    outsider_collection_id = None

    service_supabase_client.client.table("core_team_members").upsert(
        {"user_id": platform_admin_user_id, "email": platform_admin_email}
    ).execute()

    try:
        create_platform_admin_response = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            json={
                "name": f"Platform admin managed collection {int(time.time())}",
                "description": "Created on behalf of dataset owner",
                "owner_user_id": dataset.user_id,
            },
        )
        assert (
            create_platform_admin_response.status_code == 200
        ), create_platform_admin_response.text
        created_collection = create_platform_admin_response.json()
        platform_admin_collection_id = created_collection["id"]
        assert created_collection["owner_user_id"] == dataset.user_id

        owner_collections_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            params={"owner_user_id": dataset.user_id},
        )
        assert owner_collections_response.status_code == 200, owner_collections_response.text
        assert any(
            row["id"] == platform_admin_collection_id
            for row in owner_collections_response.json()
        )

        all_collections_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            params={"scope": "all"},
        )
        assert all_collections_response.status_code == 200, all_collections_response.text
        assert any(
            row["id"] == platform_admin_collection_id
            for row in all_collections_response.json()
        )

        assign_response = collections_api_client.put(
            f"/collections/datasets/{dataset.id}/collection",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            json={"collection_id": platform_admin_collection_id},
        )
        assert assign_response.status_code == 200, assign_response.text
        assert assign_response.json()["collection_id"] == platform_admin_collection_id
        assert (
            _get_dataset_collection_id(local_supabase_client, dataset.id)
            == platform_admin_collection_id
        )

        create_outsider_collection = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={
                "name": f"Outsider collection {int(time.time())}",
                "description": "Different owner collection",
            },
        )
        assert create_outsider_collection.status_code == 200, create_outsider_collection.text
        outsider_collection_id = create_outsider_collection.json()["id"]

        invalid_assign_response = collections_api_client.put(
            f"/collections/datasets/{dataset.id}/collection",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            json={"collection_id": outsider_collection_id},
        )
        assert invalid_assign_response.status_code == 400

        update_response = collections_api_client.patch(
            f"/collections/{platform_admin_collection_id}",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
            json={"description": "Updated by platform admin"},
        )
        assert update_response.status_code == 200, update_response.text
        assert update_response.json()["description"] == "Updated by platform admin"

        archive_response = collections_api_client.delete(
            f"/collections/{platform_admin_collection_id}",
            headers={"Authorization": f"Bearer {platform_admin_token}"},
        )
        assert archive_response.status_code == 200, archive_response.text
        assert archive_response.json()["status"] == "archived"
        assert _get_dataset_collection_id(local_supabase_client, dataset.id) is None
    finally:
        service_supabase_client.client.table("core_team_members").delete().eq(
            "user_id", platform_admin_user_id
        ).execute()
        _cleanup_dataset(local_supabase_client, dataset.id)
        if platform_admin_collection_id is not None:
            _cleanup_collection(local_supabase_client, platform_admin_collection_id)
        if outsider_collection_id is not None:
            _cleanup_collection(local_supabase_client, outsider_collection_id)


def test_dataset_shared_read_user_cannot_manage_other_users_collections(
    collections_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    service_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, _ = auth_tokens
    supabase_url, supabase_key, _, _ = require_supabase_auth_env(
        skip_prefix="Skipping collections authz integration test"
    )
    shared_email = f"collections-shared-read-{int(time.time())}@example.test"
    shared_password = "CollectionsSharedReadPassw0rd!"
    shared_token = ensure_user_token(
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        email=shared_email,
        password=shared_password,
    )
    shared_user_id, _ = _resolve_user_identity(supabase_url, supabase_key, shared_token)

    dataset = _create_private_dataset(local_supabase_client)
    collection_id = None

    service_supabase_client.client.table("dataset_user_access").upsert(
        {
            "dataset_id": dataset.id,
            "grantee_user_id": shared_user_id,
            "permission": "read",
            "granted_by_user_id": dataset.user_id,
        }
    ).execute()

    try:
        owner_collection_response = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {owner_token}"},
            json={
                "name": f"Owner collection for viewer restriction {int(time.time())}",
                "description": "Owned by dataset owner",
            },
        )
        assert owner_collection_response.status_code == 200, owner_collection_response.text
        collection_id = owner_collection_response.json()["id"]

        list_response = collections_api_client.get(
            "/collections",
            headers={"Authorization": f"Bearer {shared_token}"},
            params={"owner_user_id": dataset.user_id},
        )
        assert list_response.status_code == 403

        create_response = collections_api_client.post(
            "/collections",
            headers={"Authorization": f"Bearer {shared_token}"},
            json={
                "name": f"Shared read forbidden collection {int(time.time())}",
                "description": "Should not be allowed",
                "owner_user_id": dataset.user_id,
            },
        )
        assert create_response.status_code == 403

        assign_response = collections_api_client.put(
            f"/collections/datasets/{dataset.id}/collection",
            headers={"Authorization": f"Bearer {shared_token}"},
            json={"collection_id": collection_id},
        )
        assert assign_response.status_code == 404
    finally:
        service_supabase_client.client.table("dataset_user_access").delete().eq(
            "dataset_id", dataset.id
        ).eq("grantee_user_id", shared_user_id).execute()
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

