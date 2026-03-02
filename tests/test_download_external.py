import os
import json
import tempfile
import time
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from trees_api.routes.downloads import worker as download_worker
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient


pytestmark = pytest.mark.external


def _get_access_token() -> str:
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    supabase_email = os.environ.get("SUPABASE_EMAIL")
    supabase_password = os.environ.get("SUPABASE_PASSWORD")
    if not all([supabase_url, supabase_key, supabase_email, supabase_password]):
        raise RuntimeError("SUPABASE_URL, SUPABASE_KEY, SUPABASE_EMAIL, SUPABASE_PASSWORD must be set")

    response = httpx.post(
        f"{supabase_url}/auth/v1/token?grant_type=password",
        headers={"apikey": supabase_key, "Content-Type": "application/json"},
        json={"email": supabase_email, "password": supabase_password},
        timeout=20.0,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token returned from Supabase auth")
    return token


def _brevo_logs_url() -> str:
    api_url = os.environ.get("BREVO_API_URL", "https://api.brevo.com/v3/smtp/email").rstrip("/")
    if api_url.endswith("/smtp/email"):
        return api_url[: -len("/smtp/email")] + "/smtp/emails"
    return "https://api.brevo.com/v3/smtp/emails"


@pytest.fixture(scope="module")
def api_client(supabase_client: SupabaseClient) -> TestClient:
    from trees_api.server import app
    from trees_api.connection_manager import connection_manager

    connection_manager.supabase.client = supabase_client
    connection_manager.supabase.connected = True

    return TestClient(app)


@pytest.mark.external
def test_download_external_brevo_email_delivery(
    api_client: TestClient,
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
):
    if os.environ.get("RUN_EXTERNAL_EMAIL_TESTS") != "1":
        pytest.skip("Set RUN_EXTERNAL_EMAIL_TESTS=1 to run external Brevo tests")

    recipient = os.environ.get("BREVO_TEST_RECIPIENT_EMAIL") or os.environ.get("SUPABASE_EMAIL")
    if not recipient:
        pytest.skip("Set BREVO_TEST_RECIPIENT_EMAIL or SUPABASE_EMAIL to run external Brevo tests")

    if not os.environ.get("BREVO_API_KEY"):
        pytest.skip("BREVO_API_KEY is required for external Brevo tests")

    # Ensure download bucket exists in local MinIO
    try:
        storage_client.client.head_bucket(Bucket=storage_client.bucket_name_download)
    except Exception:
        storage_client.client.create_bucket(Bucket=storage_client.bucket_name_download)

    token = _get_access_token()
    unique_suffix = int(time.time())
    raw_key = f"RAW/test-download-external/{unique_suffix}/raw.laz"

    dataset = None
    dataset_item_id = None
    seg_key = None
    request_id = None
    archive_key = None
    archive_local_path = None
    try:
        with tempfile.NamedTemporaryFile("wb", suffix=".laz", delete=False) as tmp:
            tmp.write(b"external email integration test laz data\n")
            tmp_path = Path(tmp.name)
        storage_client.upload_file(tmp_path, raw_key, bucket=storage_client.bucket_name_raw)
        tmp_path.unlink(missing_ok=True)

        dataset = supabase_client.create_dataset(
            bucket_path=raw_key,
            acquisition_date=datetime.now(),
            title=f"External Download Email Test {unique_suffix}",
            file_name="raw.laz",
            visibility="public",
        )

        dataset_with_items = supabase_client.get_dataset_with_items(dataset.id)
        assert dataset_with_items and dataset_with_items.get("dataset_items"), "dataset item missing"
        dataset_item_id = int(dataset_with_items["dataset_items"][0]["id"])

        seg_key = f"{dataset.id}/segmentation/{dataset_item_id}.laz"
        with tempfile.NamedTemporaryFile("wb", suffix=".laz", delete=False) as seg_tmp:
            seg_tmp.write(b"external email integration test segmentation laz data\n")
            seg_tmp_path = Path(seg_tmp.name)
        storage_client.upload_file(seg_tmp_path, seg_key, bucket=storage_client.bucket_name_products)
        seg_tmp_path.unlink(missing_ok=True)

        create_response = api_client.post(
            "/downloads",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "dataset_id": dataset.id,
                "include_raw": True,
                "include_segmentation": True,
            },
        )
        assert create_response.status_code == 200, create_response.text
        request_id = create_response.json()["id"]

        # Override recipient for this dedicated external test run.
        supabase_client.update_download_request(request_id, requester_email=recipient)

        # Process request immediately in-process (instead of waiting for worker loop).
        download_worker.run_once()

        terminal = {"completed", "failed", "failed_email", "expired"}
        row = None
        for _ in range(20):
            row = supabase_client.get_download_request(request_id)
            if row and row.get("status") in terminal:
                break
            time.sleep(2)

        assert row is not None, "download request not found after processing"
        archive_key = row.get("archive_key")
        assert row["status"] == "completed", f"expected completed, got {row['status']} ({row.get('failure_reason')})"
        assert row.get("email_sent_at"), "email_sent_at was not set"
        assert row.get("archive_key"), "archive_key missing despite completed status"
        processing = (row.get("metadata") or {}).get("processing") or {}
        assert "segmentation" in processing, "metadata.processing.segmentation missing"
        assert "used_model" in processing["segmentation"], "metadata.processing.segmentation.used_model missing"

        with tempfile.NamedTemporaryFile("wb", suffix=".zip", delete=False) as archive_tmp:
            archive_local_path = Path(archive_tmp.name)
        storage_client.download_file(
            key=row["archive_key"],
            file_path=archive_local_path,
            bucket=storage_client.bucket_name_download,
        )
        with zipfile.ZipFile(archive_local_path, "r") as zip_file:
            names = zip_file.namelist()
            seg_prefix = f"3dt_{dataset.id}/data/segmentation/3dtree_{dataset.id}_{dataset_item_id}_segmentation"
            assert any(name.startswith(seg_prefix) for name in names), "segmentation file missing from archive"
            metadata_from_archive = json.loads(zip_file.read(f"3dt_{dataset.id}/metadata.json").decode("utf-8"))
            archive_processing = metadata_from_archive.get("processing") or {}
            assert "segmentation" in archive_processing
            assert "used_model" in archive_processing["segmentation"]

        expected_subject = f"Your 3Dtrees download is ready: {row['archive_filename']}"
        logs_url = _brevo_logs_url()
        headers = {"api-key": os.environ["BREVO_API_KEY"], "accept": "application/json"}

        found = False
        matched_message_id = None
        for _ in range(30):
            logs_response = httpx.get(
                logs_url,
                headers=headers,
                params={
                    "email": recipient,
                    "subject": expected_subject,
                    "limit": 20,
                    "sort": "desc",
                },
                timeout=20.0,
            )
            logs_response.raise_for_status()
            payload = logs_response.json() if logs_response.text else {}
            emails = payload.get("transactionalEmails") or []
            for entry in emails:
                if entry.get("subject") == expected_subject and entry.get("email") == recipient:
                    matched_message_id = entry.get("messageId")
                    found = True
                    break
            if found:
                break
            time.sleep(2)

        assert found, f"No Brevo transactional log entry found for subject '{expected_subject}' to '{recipient}'"
        assert matched_message_id, "Brevo log entry found, but messageId is missing"

        # Verify this specific message has no hard delivery error.
        delivery_events_url = logs_url.replace("/smtp/emails", "/smtp/statistics/events")
        error_events = {
            "error",
            "blocked",
            "invalid",
            "bounces",
            "hardBounces",
            "softBounces",
            "spam",
        }
        seen_events: set[str] = set()
        has_request = False

        for _ in range(30):
            events_response = httpx.get(
                delivery_events_url,
                headers=headers,
                params={
                    "messageId": matched_message_id,
                    "days": 30,
                    "limit": 50,
                    "sort": "desc",
                },
                timeout=20.0,
            )
            events_response.raise_for_status()
            events_payload = events_response.json() if events_response.text else {}
            events = events_payload.get("events") or []
            seen_events = {str(e.get("event")) for e in events if e.get("event")}
            if "requests" in seen_events:
                has_request = True
            if seen_events & error_events:
                break
            if {"delivered", "opened", "clicks"} & seen_events:
                break
            time.sleep(2)

        assert has_request, f"No Brevo request event observed for messageId {matched_message_id}"
        assert not (seen_events & error_events), (
            f"Brevo delivery error for messageId {matched_message_id}: events={sorted(seen_events)}"
        )

    finally:
        if request_id is not None:
            supabase_client.client.table("download_requests").delete().eq("id", request_id).execute()
        if dataset and dataset.id is not None:
            supabase_client.client.table("datasets").delete().eq("id", dataset.id).execute()
        if archive_key:
            try:
                storage_client.delete_object(archive_key, bucket=storage_client.bucket_name_download)
            except Exception:
                pass
        if archive_local_path:
            archive_local_path.unlink(missing_ok=True)
        try:
            storage_client.delete_object(raw_key, bucket=storage_client.bucket_name_raw)
        except Exception:
            pass
        if seg_key:
            try:
                storage_client.delete_object(seg_key, bucket=storage_client.bucket_name_products)
            except Exception:
                pass

