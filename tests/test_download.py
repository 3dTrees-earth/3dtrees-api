import time
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from trees_api.integrations.supabase.client import SupabaseClient
from tests.supabase_auth_test_utils import password_login_token, require_supabase_auth_env


def _get_access_token() -> str:
    supabase_url, supabase_key, email, password = require_supabase_auth_env(
        skip_prefix="Skipping download integration test"
    )
    return password_login_token(supabase_url, supabase_key, email, password)


@pytest.fixture(scope="module")
def api_client(supabase_client: SupabaseClient) -> TestClient:
    """Create FastAPI test client with Supabase dependency available."""
    from trees_api.app.connection_manager import connection_manager
    from trees_api.app.server import app

    connection_manager.supabase.client = supabase_client
    connection_manager.supabase.connected = True

    return TestClient(app)


def test_download_request_create_and_list(api_client: TestClient, supabase_client: SupabaseClient):
    """
    Minimal download feature integration test:
    1) create public dataset
    2) POST /downloads as authenticated user
    3) assert pending request is persisted and visible in GET /downloads
    """
    token = _get_access_token()
    unique_suffix = int(time.time())

    dataset = supabase_client.create_dataset(
        bucket_path=f"RAW/test-download/{unique_suffix}/raw.laz",
        acquisition_date=datetime.now(),
        title=f"Test Download Dataset {unique_suffix}",
        file_name="raw.laz",
        visibility="public",
    )

    created_request_id = None
    try:
        create_response = api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert create_response.status_code == 200, create_response.text
        created = create_response.json()
        created_request_id = created["id"]

        assert created["dataset_id"] == dataset.id
        assert created["include_raw"] is True
        assert created["include_segmentation"] is False
        assert created["status"] == "pending"

        duplicate_response = api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": False,
            },
        )
        assert duplicate_response.status_code == 200, duplicate_response.text
        duplicate_row = duplicate_response.json()
        assert duplicate_row["id"] == created_request_id

        active_rows = (
            supabase_client.client.table("download_requests")
            .select("id")
            .eq("requested_by", created["requested_by"])
            .eq("dataset_id", dataset.id)
            .eq("include_raw", True)
            .eq("include_segmentation", False)
            .in_("status", ["pending", "processing"])
            .execute()
        )
        assert len(active_rows.data or []) == 1

        list_response = api_client.get(
            "/downloads",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert list_response.status_code == 200, list_response.text
        rows = list_response.json()
        assert any(row["id"] == created_request_id for row in rows)

    finally:
        if created_request_id is not None:
            supabase_client.client.table("download_requests").delete().eq("id", created_request_id).execute()
        if dataset and dataset.id is not None:
            supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()

