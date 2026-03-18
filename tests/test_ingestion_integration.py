import time
from datetime import datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from trees_api.core.config import StorageConfig, SupabaseConfig
from trees_api.integrations.storage.client import StorageClient, UploaderStorageClient
from trees_api.integrations.supabase.client import SupabaseClient
from tests.supabase_auth_test_utils import password_login_token, require_supabase_auth_env


def _get_access_token() -> str:
    supabase_url, supabase_key, email, password = require_supabase_auth_env(
        skip_prefix="Skipping integration test"
    )
    return password_login_token(supabase_url, supabase_key, email, password)


@pytest.fixture(scope="module")
def local_supabase_client() -> SupabaseClient:
    require_supabase_auth_env(skip_prefix="Skipping integration test")

    client = SupabaseClient(SupabaseConfig())
    try:
        client.connect()
        try:
            client.authenticate_user(client.email, client.password)
        except Exception as auth_error:
            auth_text = str(auth_error).lower()
            if (
                "authentication failed" in auth_text
                or "invalid login credentials" in auth_text
                or "invalid credentials" in auth_text
            ):
                try:
                    client.register_user(client.email, client.password)
                    client.authenticate_user(client.email, client.password)
                except Exception as reg_error:
                    pytest.skip(
                        "Skipping integration test: Supabase auth failed and registration fallback failed: "
                        f"{reg_error}"
                    )
            else:
                pytest.skip(f"Skipping integration test: Supabase authentication failed: {auth_error}")
    except Exception as error:
        pytest.skip(f"Skipping integration test: Supabase is not reachable/authenticated: {error}")
    return client


@pytest.fixture(scope="module")
def local_storage_client() -> StorageClient:
    client = StorageClient(StorageConfig())
    try:
        client.connect()
    except Exception as error:
        pytest.skip(f"Skipping integration test: Storage client not reachable: {error}")
    return client


@pytest.fixture(scope="module")
def uploader_storage_client() -> UploaderStorageClient:
    config = StorageConfig()
    client = UploaderStorageClient(config)
    try:
        client.connect()
    except Exception as error:
        pytest.skip(f"Skipping integration test: Uploader storage not reachable: {error}")
    return client


@pytest.fixture(scope="module")
def api_client(
    local_supabase_client: SupabaseClient,
    local_storage_client: StorageClient,
    uploader_storage_client: UploaderStorageClient,
) -> TestClient:
    """Create FastAPI test client with Supabase + storage dependencies available."""
    from trees_api.app.connection_manager import connection_manager
    from trees_api.app.server import app

    connection_manager.supabase.client = local_supabase_client
    connection_manager.supabase.connected = True

    connection_manager.storage.client = local_storage_client
    connection_manager.storage.connected = True

    connection_manager.uploader_storage.client = uploader_storage_client
    connection_manager.uploader_storage.connected = True

    return TestClient(app)


def _require_ingestion_tables(supabase_client: SupabaseClient) -> None:
    """Skip test when ingestion migration is not applied in local Supabase."""
    try:
        supabase_client.client.table("ingestion_sessions").select("id").limit(1).execute()
        supabase_client.client.table("ingestion_session_items").select("id").limit(1).execute()
    except Exception as error:
        pytest.skip(f"Ingestion tables not available. Apply migrations first. Error: {error}")


def test_ingestion_route_end_to_end_local_supabase_minio(
    api_client: TestClient,
    local_supabase_client: SupabaseClient,
    local_storage_client: StorageClient,
):
    """
    End-to-end integration test for authenticated ingestion route with local Supabase + MinIO.

    Flow:
    1) create dataset + dataset_item in Supabase
    2) create ingestion session via API
    3) presign one part and upload bytes directly to MinIO
    4) complete ingestion via API
    5) verify DB bucket_path update + object presence in MinIO
    """
    _require_ingestion_tables(local_supabase_client)
    token = _get_access_token()
    unique_suffix = int(time.time())

    dataset = local_supabase_client.create_dataset(
        bucket_path=f"RAW/test-ingestion/{unique_suffix}/placeholder.laz",
        acquisition_date=datetime.now(),
        title=f"Test Ingestion Dataset {unique_suffix}",
        file_name="placeholder.laz",
        visibility="private",
    )

    ingestion_id = None
    ingestion_item_id = None
    s3_key = None

    try:
        item_rows = (
            local_supabase_client.client.table("dataset_items")
            .select("id")
            .eq("dataset_id", dataset.id)
            .order("id")
            .limit(1)
            .execute()
        )
        assert item_rows.data, "Expected at least one dataset_item for created dataset"
        dataset_item_id = item_rows.data[0]["id"]

        # Reset bucket_path so we can assert ingestion updates it.
        local_supabase_client.client.table("dataset_items").update({"bucket_path": ""}).eq(
            "id", dataset_item_id
        ).execute()

        payload_bytes = b"laz-part-content-for-integration-test"

        create_response = api_client.post(
            "/ingestions",
            headers={
                "Authorization": f"Bearer {token}",
                "Idempotency-Key": f"ingestion-{unique_suffix}",
            },
            json={
                "dataset_id": dataset.id,
                "items": [
                    {
                        "dataset_item_id": dataset_item_id,
                        "file_name": "raw.laz",
                        "file_size_bytes": len(payload_bytes),
                        "content_type": "application/octet-stream",
                    }
                ],
                "metadata": {"source": "integration_test"},
            },
        )
        assert create_response.status_code == 200, create_response.text
        created = create_response.json()
        ingestion_id = created["id"]
        assert created["status"] == "uploading"
        assert len(created["items"]) == 1
        ingestion_item_id = created["items"][0]["id"]
        s3_key = created["items"][0]["s3_key"]

        presign_response = api_client.post(
            f"/ingestions/{ingestion_id}/presign",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "ingestion_item_id": ingestion_item_id,
                "part_numbers": [1],
            },
        )
        assert presign_response.status_code == 200, presign_response.text
        presigned_url = presign_response.json()["parts"][0]["url"]

        put_response = httpx.put(
            presigned_url,
            content=payload_bytes,
            headers={"Content-Type": "application/octet-stream"},
            timeout=30.0,
        )
        assert put_response.status_code in (200, 204), put_response.text
        etag = put_response.headers.get("etag")
        assert etag, "Expected ETag header from S3 upload_part response"

        complete_response = api_client.post(
            f"/ingestions/{ingestion_id}/complete",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "items": [
                    {
                        "ingestion_item_id": ingestion_item_id,
                        "parts": [{"part_number": 1, "e_tag": etag}],
                    }
                ],
                "auto_process": False,
            },
        )
        assert complete_response.status_code == 200, complete_response.text
        completed = complete_response.json()
        assert completed["status"] == "completed"
        assert completed["workflow_triggered"] is False

        updated_item = (
            local_supabase_client.client.table("dataset_items")
            .select("bucket_path")
            .eq("id", dataset_item_id)
            .limit(1)
            .execute()
        )
        assert updated_item.data
        assert updated_item.data[0]["bucket_path"] == s3_key

        local_storage_client.client.head_object(
            Bucket=local_storage_client.bucket_name_raw,
            Key=s3_key,
        )

    finally:
        # Cleanup ingestion rows first (FK to dataset_items/datasets)
        if ingestion_id is not None:
            local_supabase_client.client.table("ingestion_session_items").delete().eq(
                "ingestion_session_id", ingestion_id
            ).execute()
            local_supabase_client.client.table("ingestion_sessions").delete().eq(
                "id", ingestion_id
            ).execute()

        if s3_key:
            try:
                local_storage_client.client.delete_object(
                    Bucket=local_storage_client.bucket_name_raw,
                    Key=s3_key,
                )
            except Exception:
                pass

        if dataset and dataset.id is not None:
            local_supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()

