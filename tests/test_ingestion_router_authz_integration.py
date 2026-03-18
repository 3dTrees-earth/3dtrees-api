import time
from datetime import datetime

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trees_api.core.config import SupabaseConfig
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import get_supabase_client as get_downloads_supabase_client
from trees_api.routes.ingestions.router import (
    get_supabase_client as get_ingestions_supabase_client,
    get_uploader_storage,
    router,
)
from tests.supabase_auth_test_utils import (
    ensure_user_token,
    password_login_token,
    require_supabase_auth_env,
)


class _DummyUploaderStorage:
    bucket_name_raw = "3dtrees-raw"
    client = object()


def _require_ingestion_tables(local_supabase_client: SupabaseClient) -> None:
    try:
        local_supabase_client.client.table("ingestion_sessions").select("id").limit(1).execute()
        local_supabase_client.client.table("ingestion_session_items").select("id").limit(1).execute()
    except Exception as error:
        pytest.skip(f"Skipping ingestion authz integration test: ingestion tables unavailable: {error}")


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping ingestion authz integration test")
    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        client.authenticate_user(client.email, client.password)
    except Exception as error:
        pytest.skip(
            f"Skipping ingestion authz integration test: Supabase is not reachable/authenticated: {error}"
        )
    _require_ingestion_tables(client)
    return client


@pytest.fixture(scope="module")
def auth_tokens() -> tuple[str, str]:
    supabase_url, supabase_key, owner_email, owner_password = require_supabase_auth_env(
        skip_prefix="Skipping ingestion authz integration test"
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
        email=f"ingestion-outsider-{int(time.time())}@example.test",
        password="IngestionAuthzPassw0rd!",
    )
    return owner_token, outsider_token


@pytest.fixture
def ingestions_api_client(local_supabase_client: SupabaseClient) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_ingestions_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_downloads_supabase_client] = lambda: local_supabase_client
    app.dependency_overrides[get_uploader_storage] = lambda: _DummyUploaderStorage()
    return TestClient(app)


def _create_private_dataset_with_item(local_supabase_client: SupabaseClient):
    suffix = int(time.time() * 1000)
    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/ingestion-authz/{suffix}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Ingestion authz {suffix}",
        file_name="raw.laz",
        visibility="private",
    )
    item_rows = (
        local_supabase_client.client.table("dataset_items")
        .select("id")
        .eq("dataset_id", dataset.id)
        .order("id")
        .limit(1)
        .execute()
    )
    assert item_rows.data, "Expected dataset item for created dataset"
    return dataset, item_rows.data[0]["id"]


def _cleanup_dataset(local_supabase_client: SupabaseClient, dataset_id: int, session_id: int | None = None):
    if session_id is not None:
        local_supabase_client.client.table("ingestion_session_items").delete().eq(
            "ingestion_session_id", session_id
        ).execute()
        local_supabase_client.client.table("ingestion_sessions").delete().eq("id", session_id).execute()
    local_supabase_client.client.table("download_requests").delete().eq("dataset_id", dataset_id).execute()
    local_supabase_client.client.table("datasets").delete().eq("id", dataset_id).execute()


def _create_owner_ingestion_session_with_item(local_supabase_client: SupabaseClient):
    dataset, dataset_item_id = _create_private_dataset_with_item(local_supabase_client)
    session = local_supabase_client.create_or_get_active_ingestion_session(
        dataset_id=dataset.id,
        created_by=dataset.user_id,
        idempotency_key=f"owner-session-{int(time.time() * 1000)}",
        workflow_name="EndToEndPipeline",
        metadata={"source": "ingestion_authz_test"},
    )
    item = local_supabase_client.create_or_update_ingestion_session_item(
        session_id=session["id"],
        dataset_item_id=dataset_item_id,
        file_name="raw.laz",
        file_size_bytes=1024,
        content_type="application/octet-stream",
        key=f"RAW/{dataset.id}/{dataset_item_id}/raw.laz",
        upload_id=f"upload-{int(time.time() * 1000)}",
        status="uploading",
    )
    return dataset, session, item


def test_create_ingestion_denies_non_owner(
    ingestions_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    _, outsider_token = auth_tokens
    dataset, dataset_item_id = _create_private_dataset_with_item(local_supabase_client)

    try:
        response = ingestions_api_client.post(
            "/ingestions",
            headers={
                "Authorization": f"Bearer {outsider_token}",
                "Idempotency-Key": f"ingestion-authz-{int(time.time() * 1000)}",
            },
            json={
                "dataset_id": dataset.id,
                "items": [
                    {
                        "dataset_item_id": dataset_item_id,
                        "file_name": "raw.laz",
                        "file_size_bytes": 1024,
                        "content_type": "application/octet-stream",
                    }
                ],
            },
        )
        assert response.status_code == 403
        assert "dataset owner" in response.json()["detail"]
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id)


def test_get_and_list_ingestions_are_user_scoped(
    ingestions_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    owner_token, outsider_token = auth_tokens
    dataset, session, _ = _create_owner_ingestion_session_with_item(local_supabase_client)

    try:
        owner_get = ingestions_api_client.get(
            f"/ingestions/{session['id']}",
            headers={"Authorization": f"Bearer {owner_token}"},
        )
        assert owner_get.status_code == 200, owner_get.text
        assert owner_get.json()["id"] == session["id"]

        outsider_get = ingestions_api_client.get(
            f"/ingestions/{session['id']}",
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_get.status_code == 404

        outsider_list = ingestions_api_client.get(
            "/ingestions",
            params={"dataset_id": dataset.id},
            headers={"Authorization": f"Bearer {outsider_token}"},
        )
        assert outsider_list.status_code == 200
        assert outsider_list.json() == []
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id, session_id=session["id"])


def test_presign_ingestion_denies_non_owner(
    ingestions_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    _, outsider_token = auth_tokens
    dataset, session, item = _create_owner_ingestion_session_with_item(local_supabase_client)

    try:
        response = ingestions_api_client.post(
            f"/ingestions/{session['id']}/presign",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={
                "ingestion_item_id": item["id"],
                "part_numbers": [1],
            },
        )
        assert response.status_code == 404
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id, session_id=session["id"])


def test_complete_ingestion_denies_non_owner(
    ingestions_api_client: TestClient,
    local_supabase_client: SupabaseClient,
    auth_tokens: tuple[str, str],
):
    _, outsider_token = auth_tokens
    dataset, session, item = _create_owner_ingestion_session_with_item(local_supabase_client)

    try:
        response = ingestions_api_client.post(
            f"/ingestions/{session['id']}/complete",
            headers={"Authorization": f"Bearer {outsider_token}"},
            json={
                "items": [
                    {
                        "ingestion_item_id": item["id"],
                        "parts": [{"part_number": 1, "e_tag": "etag-1"}],
                    }
                ],
                "auto_process": False,
            },
        )
        assert response.status_code == 404
    finally:
        _cleanup_dataset(local_supabase_client, dataset.id, session_id=session["id"])


def test_ingestions_requires_authorization_header(ingestions_api_client: TestClient):
    response = ingestions_api_client.get("/ingestions")
    assert response.status_code == 401


def test_ingestion_presign_requires_authorization_header(ingestions_api_client: TestClient):
    response = ingestions_api_client.post(
        "/ingestions/1/presign",
        json={
            "ingestion_item_id": 1,
            "part_numbers": [1],
        },
    )
    assert response.status_code == 401


def test_ingestion_complete_requires_authorization_header(ingestions_api_client: TestClient):
    response = ingestions_api_client.post(
        "/ingestions/1/complete",
        json={
            "items": [
                {
                    "ingestion_item_id": 1,
                    "parts": [{"part_number": 1, "e_tag": "etag-1"}],
                }
            ],
            "auto_process": False,
        },
    )
    assert response.status_code == 401
