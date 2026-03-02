import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from trees_api.core.config import StorageConfig, SupabaseConfig
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.workers.history_sync import sync_history_for_invocation


def _upload_json(storage_client: StorageClient, key: str, payload: dict, bucket: str) -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=True) as tmp:
        json.dump(payload, tmp)
        tmp.flush()
        storage_client.upload_file(tmp.name, key, bucket=bucket)


def _delete_prefix(storage_client: StorageClient, bucket: str, prefix: str) -> None:
    response = storage_client.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = response.get("Contents", [])
    if not contents:
        return
    delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in contents]}
    storage_client.client.delete_objects(Bucket=bucket, Delete=delete_payload)


def _ensure_bucket_exists(storage_client: StorageClient) -> None:
    buckets_to_check = [
        storage_client.bucket_name_raw,
        storage_client.bucket_name_products,
        storage_client.config.bucket_name_visualization,
    ]
    for bucket_name in buckets_to_check:
        try:
            storage_client.client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                storage_client.client.create_bucket(Bucket=bucket_name)
            else:
                raise


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


@pytest.fixture(scope="session")
def minio_storage_client() -> StorageClient:
    os.environ.setdefault("STORAGE_URL", "http://localhost:9500")
    os.environ.setdefault("STORAGE_ACCESS_KEY_PROCESSOR", "minioadmin")
    os.environ.setdefault("STORAGE_SECRET_KEY_PROCESSOR", "minioadmin")
    os.environ.setdefault("STORAGE_ACCESS_KEY", "minioadmin")
    os.environ.setdefault("STORAGE_SECRET_KEY", "minioadmin")

    config = StorageConfig()
    client = StorageClient(config)
    client.connect()
    _ensure_bucket_exists(client)
    return client


@pytest.fixture(scope="session")
def supabase_client() -> SupabaseClient:
    repo_root = Path(__file__).resolve().parents[2]
    _load_env_file(repo_root / ".env")

    config = SupabaseConfig()
    client = SupabaseClient(config)
    client.connect()

    if client.email and client.password:
        try:
            client.authenticate_user(client.email, client.password)
        except Exception:
            client.register_user(client.email, client.password)
            client.authenticate_user(client.email, client.password)
    else:
        pytest.skip("SUPABASE_EMAIL/PASSWORD not configured for local Supabase tests")

    return client


def test_history_sync_minio_supabase_integration(
    minio_storage_client: StorageClient,
    supabase_client: SupabaseClient,
):
    """
    Integration test: MinIO + Supabase only (no Galaxy).

    Validates that sync_history_outputs builds deterministic outputs,
    ingests metadata JSON from MinIO, and marks invocations as synced.
    """
    storage_config = StorageConfig()
    invocation_id = f"inv-test-{uuid4()}"
    history_id = f"hist-test-{uuid4()}"

    dataset = supabase_client.create_dataset(
        bucket_path=f"LAS/status_pooler_test/{uuid4()}.laz",
        acquisition_date=datetime.now(),
        title="Status Pooler MinIO Test",
        file_name="status_pooler_test.laz",
        visibility="public",
    )
    dataset_id = dataset.id

    items_resp = (
        supabase_client.client.table("dataset_items")
        .select("id")
        .eq("dataset_id", dataset_id)
        .order("id")
        .execute()
    )
    assert items_resp.data, "Expected at least one dataset_item"
    item_id = items_resp.data[0]["id"]

    history = supabase_client.get_or_create_galaxy_history(
        dataset_id=dataset_id,
        history_id=history_id,
        history_name="Status Pooler MinIO Test",
        s3_base_path=f"{dataset_id}/",
    )
    history_fk = history["id"]

    supabase_client.create_workflow_invocation(
        workflow_uuid=invocation_id,
        dataset_id=dataset_id,
        workflow_name="EndToEndPipeline-GalaxyEU",
        history_fk=history_fk,
    )
    supabase_client.update_workflow_invocation(
        invocation_id,
        status="ok",
        results_synced=False,
    )

    collection_summary = {
        "collection": {
            "n_tiles": 1,
            "multipolygon_wkt": "",
            "first_crs": {"epsg": 4326},
        },
        "files": {"item": {"n_points": 123}},
    }
    item_metadata = {"item_id": item_id, "name": "Test Tree"}
    item_geojson = {"type": "FeatureCollection", "features": []}

    base_prefix = f"{dataset_id}/metadata/"
    _upload_json(
        minio_storage_client,
        f"{base_prefix}collection_summary.json",
        collection_summary,
        bucket=storage_config.bucket_name_products,
    )
    _upload_json(
        minio_storage_client,
        f"{base_prefix}{item_id}.json",
        item_metadata,
        bucket=storage_config.bucket_name_products,
    )
    _upload_json(
        minio_storage_client,
        f"{base_prefix}{item_id}.geojson",
        item_geojson,
        bucket=storage_config.bucket_name_products,
    )

    try:
        success = sync_history_for_invocation(
            supabase_client,
            minio_storage_client,
            storage_config,
            invocation_id=invocation_id,
            workflow_name="EndToEndPipeline-GalaxyEU",
            history_fk=history_fk,
            dataset_id=dataset_id,
            galaxy_client=None,
            delete_history_after_sync=False,
        )
        assert success is True

        updated_invocation = supabase_client.get_workflow_invocation_by_id(invocation_id)
        assert updated_invocation is not None
        assert updated_invocation.results_synced is True

        history_resp = (
            supabase_client.client.table("galaxy_histories")
            .select("outputs")
            .eq("id", history_fk)
            .execute()
        )
        assert history_resp.data, "Expected galaxy_history outputs"
        outputs = history_resp.data[0]["outputs"]

        assert "metadata" in outputs
        assert outputs["metadata"]["collection_summary"]["total_point_count"] == 123
        assert outputs["metadata"]["collection_summary"]["epsg"] == 4326
        assert outputs["metadata"][str(item_id)]["name"] == "Test Tree"
        assert outputs["metadata"][str(item_id)]["convex_hull"]["type"] == "FeatureCollection"

        assert "potree" in outputs
        assert any(path.endswith("potree/metadata.json") for path in outputs["potree"])
        assert any(path.endswith("potree/hierarchy.bin") for path in outputs["potree"])
        assert any(path.endswith("potree/octree.bin") for path in outputs["potree"])

    finally:
        _delete_prefix(
            minio_storage_client,
            bucket=storage_config.bucket_name_products,
            prefix=f"{dataset_id}/",
        )
        supabase_client.client.table("galaxy_workflow_invocations").delete().eq(
            "invocation_id", invocation_id
        ).execute()
        supabase_client.client.table("galaxy_histories").delete().eq(
            "id", history_fk
        ).execute()
        supabase_client.client.table("dataset_items").delete().eq(
            "dataset_id", dataset_id
        ).execute()
        supabase_client.client.table("datasets").delete().eq("id", dataset_id).execute()
