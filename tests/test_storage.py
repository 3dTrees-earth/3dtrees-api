from pathlib import Path

import pytest
import datetime from datetime

from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient
from trees_api.models import Dataset


@pytest.fixture(scope="session")
def storage_client() -> StorageClient:
    client


def test_storage_client(storage_client: StorageClient):
    assert storage_client.connect()

@pytest.fixture(scope="session")
def test_remote_file(storage_client: StorageClient, supabase_client: SupabaseClient) -> Dataset:
    key = "LAS/Example_Platane.laz"
    file_path = Path().cwd() / "Example_Platane.laz"
    storage_client.upload_file(file_path, key)

    dataset = supabase_client.create_dataset(
        bucket_path=key,
        acquisition_data=datetime.now(),
        title="Test Platane",
        file_name=file_path.name,
        visibility="public"
    )

    return dataset