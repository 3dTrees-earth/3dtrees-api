import os
import time
from datetime import datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from trees_api.supabase_client import SupabaseClient


def _get_access_token() -> str:
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    supabase_email = os.environ.get("SUPABASE_EMAIL")
    supabase_password = os.environ.get("SUPABASE_PASSWORD")

    if not all([supabase_url, supabase_key, supabase_email, supabase_password]):
        raise RuntimeError("SUPABASE_URL, SUPABASE_KEY, SUPABASE_EMAIL, and SUPABASE_PASSWORD must be set")

    response = httpx.post(
        f"{supabase_url}/auth/v1/token?grant_type=password",
        headers={
            "apikey": supabase_key,
            "Content-Type": "application/json",
        },
        json={
            "email": supabase_email,
            "password": supabase_password,
        },
        timeout=20.0,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token returned by Supabase auth endpoint")
    return token


@pytest.fixture(scope="module")
def api_client(supabase_client: SupabaseClient) -> TestClient:
    """Create FastAPI test client with Supabase dependency available."""
    from trees_api.server import app
    from trees_api.connection_manager import connection_manager

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

