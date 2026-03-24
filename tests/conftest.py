import pytest
import logging
import os
from pathlib import Path
from typing import Generator, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

from trees_api.core.config import GalaxyConfig, SupabaseConfig, StorageConfig
from trees_api.core.models import Dataset
from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _load_repo_env() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    env_path = repo_root / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)


_load_repo_env()


@pytest.fixture(scope="session")
def storage_client() -> StorageClient:
    config = StorageConfig()
    client = StorageClient(config)
    
    try:
        # Connect to storage service
        client.connect()
        logger.info("Storage client connected successfully")
        
        # Ensure bucket exists
        _ensure_bucket_exists(client)
        
        return client
        
    except Exception as e:
        logger.error(f"Failed to setup storage client: {e}")
        pytest.skip(f"Skipping integration test: storage client unavailable: {e}")


def _ensure_bucket_exists(storage_client: StorageClient) -> None:
    """Ensure the required buckets exist, create if they don't (for local MinIO only)."""
    # Check/create raw, products, and visualization buckets for local dev
    buckets_to_check = [
        storage_client.bucket_name_raw,
        storage_client.bucket_name_products,
        storage_client.config.bucket_name_visualization,
        storage_client.bucket_name_download,
    ]
    
    for bucket_name in buckets_to_check:
        try:
            # Check if bucket exists
            storage_client.client.head_bucket(Bucket=bucket_name)
            logger.info(f"✅ Bucket '{bucket_name}' already exists")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Bucket doesn't exist, try to create it (only works for local MinIO with write access)
                try:
                    logger.info(f"Creating bucket '{bucket_name}'...")
                    storage_client.client.create_bucket(Bucket=bucket_name)
                    logger.info(f"✅ Bucket '{bucket_name}' created successfully")
                except ClientError as create_error:
                    logger.warning(f"Could not create bucket '{bucket_name}': {create_error}")
                    logger.info(f"Assuming bucket will be created externally or using read-only access")
            else:
                logger.error(f"❌ Error checking bucket: {e}")
                raise RuntimeError(f"Failed to check bucket '{bucket_name}': {e}")


@pytest.fixture(scope="session")
def supabase_client() -> SupabaseClient:
    config = SupabaseConfig()
    if not config.url or not (config.key or config.service_key):
        pytest.skip("Skipping integration test: Supabase environment is not configured")

    client = SupabaseClient(config)
    try:
        client.connect()
    except Exception as e:
        pytest.skip(f"Skipping integration test: Supabase is not reachable: {e}")
    
    # Authenticate with processor user for testing using environment variables
    try:
        client.authenticate_user(client.email, client.password)
        logger.info("✅ Authenticated with processor user")
    except Exception as e:
        logger.warning(f"Failed to authenticate with processor user: {e}")
        logger.info("Attempting to create processor user...")
        try:
            client.register_user(client.email, client.password)
            logger.info("✅ Created processor user")
            # Now try to authenticate again
            client.authenticate_user(client.email, client.password)
            logger.info("✅ Authenticated with newly created processor user")
        except Exception as reg_e:
            logger.error(f"Failed to create processor user: {reg_e}")
            pytest.skip(f"Skipping integration test: processor user auth/registration failed: {reg_e}")
    
    return client


@pytest.fixture(scope="session")
def test_remote_file(storage_client: StorageClient, supabase_client: SupabaseClient) -> Dataset:
    key = "LAS/mikro.laz"
    file_path = Path(__file__).parent / "test_data" / "mikro.laz"
    
    if not file_path.exists():
        raise FileNotFoundError(f"Test file not found: {file_path}")
    
    # Upload to RAW bucket (input data)
    # Use bucket_name_raw to match production two-bucket setup
    raw_bucket = storage_client.bucket_name_raw
    
    # Check if file already exists in storage
    if not _file_exists_in_storage(storage_client, key, raw_bucket):
        logger.info(f"Uploading test file to RAW bucket ({raw_bucket}): {key}")
        storage_client.upload_file(file_path, key, bucket=raw_bucket)
        logger.info(f"✅ File uploaded to RAW bucket: {key}")
    else:
        logger.info(f"✅ File already exists in RAW bucket: {key}")

    # Check if dataset already exists in Supabase
    existing_dataset = _find_existing_dataset(supabase_client, key)
    if existing_dataset:
        logger.info(f"✅ Dataset already exists in Supabase: {existing_dataset.id}")
        return existing_dataset
    
    # Create new dataset (only if user is authenticated)
    current_user = supabase_client.get_current_user()
    if not current_user:
        logger.error("No authenticated user - cannot create dataset")
        raise RuntimeError("No authenticated user - cannot create dataset")
    
    logger.info("Creating new dataset in Supabase...")
    dataset = supabase_client.create_dataset(
        bucket_path=key,
        acquisition_date=datetime.now(),
        title="Test Mikro",
        file_name=file_path.name,
        visibility="public"
    )
    logger.info(f"✅ Dataset created in Supabase: {dataset.id}")
    return dataset


def _file_exists_in_storage(storage_client: StorageClient, key: str, bucket: Optional[str] = None) -> bool:
    """Check if a file exists in storage."""
    bucket_name = bucket or storage_client.bucket_name
    try:
        storage_client.client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            return False
        else:
            logger.warning(f"Error checking file existence: {e}")
            return False


def _find_existing_dataset(supabase_client: SupabaseClient, bucket_path: str) -> Optional[Dataset]:
    """Find existing dataset by bucket path."""
    try:
        # This would need to be implemented in the SupabaseClient
        # For now, we'll assume it doesn't exist and create a new one
        # TODO: Implement dataset lookup by bucket_path
        return None
    except Exception as e:
        logger.debug(f"Error checking for existing dataset: {e}")
        return None


@pytest.fixture(scope="session")
def test_collection_dataset(storage_client: StorageClient, supabase_client: SupabaseClient) -> Dataset:
    """
    Fixture that creates a dataset with multiple LAZ files (simulating segmented tiles).
    
    Uses multi-file test data (multiple LAZ tiles).
    Creates multiple dataset_items under a single dataset - exactly what Py3DTiles expects.
    """
    # Test files - use the multi-file test data from tests/test_data/multi-file/
    test_data_dir = Path(__file__).parent / "test_data" / "multi-file"
    test_files = [
        ("LAS/collection_test/tile_1.laz", test_data_dir / "tile_1.laz"),
        ("LAS/collection_test/tile_2.laz", test_data_dir / "tile_2.laz"),
    ]
    
    raw_bucket = storage_client.bucket_name_raw
    
    # Upload all test files
    for s3_key, local_path in test_files:
        if not local_path.exists():
            raise FileNotFoundError(f"Test file not found: {local_path}")
        
        if not _file_exists_in_storage(storage_client, s3_key, raw_bucket):
            logger.info(f"Uploading test file to RAW bucket ({raw_bucket}): {s3_key}")
            storage_client.upload_file(local_path, s3_key, bucket=raw_bucket)
            logger.info(f"✅ File uploaded to RAW bucket: {s3_key}")
        else:
            logger.info(f"✅ File already exists in RAW bucket: {s3_key}")
    
    # Check if dataset already exists
    # For simplicity, we'll look for a dataset with title "Test Collection"
    try:
        response = supabase_client.client.table("datasets").select("*").eq("title", "Test Collection (Py3DTiles)").limit(1).execute()
        if response.data:
            existing_dataset = Dataset(**response.data[0])
            logger.info(f"✅ Dataset already exists in Supabase: {existing_dataset.id}")
            
            # Verify it has the expected number of items
            items_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", existing_dataset.id).execute()
            if len(items_resp.data) >= 2:
                logger.info(f"✅ Dataset has {len(items_resp.data)} items")
                return existing_dataset
            else:
                logger.warning(f"Dataset has only {len(items_resp.data)} items, creating new items...")
    except Exception as e:
        logger.debug(f"Error checking for existing dataset: {e}")
    
    # Check if user is authenticated
    current_user = supabase_client.get_current_user()
    if not current_user:
        logger.error("No authenticated user - cannot create dataset")
        raise RuntimeError("No authenticated user - cannot create dataset")
    
    # Create new dataset with first file
    # create_dataset() creates one dataset_item automatically
    first_s3_key, first_local_path = test_files[0]
    logger.info("Creating new multi-file dataset in Supabase...")
    dataset = supabase_client.create_dataset(
        bucket_path=first_s3_key,  # First file's path
        acquisition_date=datetime.now(),
        title="Test Collection (Py3DTiles)",
        file_name=first_local_path.name,
        visibility="public"
    )
    logger.info(f"✅ Dataset created in Supabase: {dataset.id}")
    
    # Create dataset_items for remaining files (first already created by create_dataset)
    # dataset_items table has: id, bucket_path, file_name, dataset_id
    for s3_key, local_path in test_files[1:]:  # Skip first file
        try:
            item_resp = supabase_client.client.table("dataset_items").insert({
                "dataset_id": dataset.id,
                "bucket_path": s3_key,
                "file_name": local_path.name
            }).execute()
            logger.info(f"✅ Created dataset_item: {item_resp.data[0]['id']} for {s3_key}")
        except Exception as e:
            logger.warning(f"Failed to create dataset_item for {s3_key}: {e}")
    
    return dataset


@pytest.fixture(scope="session")
def test_single_file_dataset(storage_client: StorageClient, supabase_client: SupabaseClient) -> Dataset:
    """
    Fixture that creates a dataset with a single LAZ file.
    
    Uses mikro.laz as test file - a small point cloud for fast testing.
    Creates a single dataset_item - tests single-file Py3DTiles conversion.
    """
    # Use mikro.laz from test_data
    test_file_path = Path(__file__).parent / "test_data" / "mikro.laz"
    s3_key = "LAS/single_file_test/mikro.laz"
    
    if not test_file_path.exists():
        raise FileNotFoundError(f"Test file not found: {test_file_path}")
    
    raw_bucket = storage_client.bucket_name_raw
    
    # Upload test file
    if not _file_exists_in_storage(storage_client, s3_key, raw_bucket):
        logger.info(f"Uploading test file to RAW bucket ({raw_bucket}): {s3_key}")
        storage_client.upload_file(test_file_path, s3_key, bucket=raw_bucket)
        logger.info(f"✅ File uploaded to RAW bucket: {s3_key}")
    else:
        logger.info(f"✅ File already exists in RAW bucket: {s3_key}")
    
    # Check if dataset already exists
    try:
        response = supabase_client.client.table("datasets").select("*").eq("title", "Test Single File (Py3DTiles)").limit(1).execute()
        if response.data:
            existing_dataset = Dataset(**response.data[0])
            logger.info(f"✅ Single-file dataset already exists in Supabase: {existing_dataset.id}")
            return existing_dataset
    except Exception as e:
        logger.debug(f"Error checking for existing dataset: {e}")
    
    # Check if user is authenticated
    current_user = supabase_client.get_current_user()
    if not current_user:
        logger.error("No authenticated user - cannot create dataset")
        raise RuntimeError("No authenticated user - cannot create dataset")
    
    # Create new dataset with single file
    logger.info("Creating new single-file dataset in Supabase...")
    dataset = supabase_client.create_dataset(
        bucket_path=s3_key,
        acquisition_date=datetime.now(),
        title="Test Single File (Py3DTiles)",
        file_name=test_file_path.name,
        visibility="public"
    )
    logger.info(f"✅ Single-file dataset created in Supabase: {dataset.id}")
    
    return dataset


@pytest.fixture(scope="session")
def galaxy_client() -> Generator[GalaxyClient, None, None]:
    """
    Fixture that provides an authenticated and connected Galaxy client.
    
    This fixture handles:
    - User setup using bootstrap admin API key (if needed)
    - Authentication (if needed)
    - Connection to Galaxy
    - Workflow registry loading
    
    Returns:
        GalaxyClient: Authenticated and connected client
    """
    config = GalaxyConfig()
    client = GalaxyClient(config)
    
    try:
        # First try to set up user with bootstrap admin API key
        try:
            client.setup_user_with_bootstrap()
            logger.info("User setup with bootstrap admin API key successful")
        except Exception as e:
            logger.debug(f"Bootstrap setup failed, trying normal authentication: {e}")
            # Fall back to normal authentication
            client.authenticate()
        
        # Connect to Galaxy
        client.connect()
        
        logger.info("Galaxy client authenticated and connected")
        yield client
        
    except Exception as e:
        logger.error(f"Failed to setup Galaxy client: {e}")
        pytest.skip(f"Galaxy is unavailable for integration tests: {e}")


@pytest.fixture
def test_dataset_id(galaxy_client: GalaxyClient) -> str:
    """
    Fixture that uploads a test dataset and returns its ID.
    
    This fixture:
    - Checks if a test history already exists
    - Creates a test history only if needed
    - Checks if the test file is already uploaded
    - Uploads a test LAS/LAZ file only if needed
    - Waits for upload completion
    - Returns the dataset ID
    
    Returns:
        str: Dataset ID in Galaxy
    """
    # Test file path
    test_file_path = Path("./Example_Platane.laz")
    
    if not test_file_path.exists():
        pytest.skip(f"Test file not found: {test_file_path}")
    
    try:
        # Check if test history already exists
        history_name = "Test - Overviews Workflow"
        existing_history = _find_existing_history(galaxy_client, history_name)
        
        if existing_history:
            logger.info(f"✅ Using existing test history: {existing_history.id}")
            history = existing_history
        else:
            logger.info(f"Creating new test history: {history_name}")
            history = galaxy_client.create_history(history_name)
            logger.info(f"✅ Test history created: {history.id}")
        
        # Check if test file is already uploaded in this history
        existing_dataset = _find_existing_dataset_in_history(galaxy_client, history, test_file_path.name)
        
        if existing_dataset:
            logger.info(f"✅ Using existing test dataset: {existing_dataset.id}")
            return existing_dataset.id
        
        # Upload test file
        logger.info(f"Uploading test file: {test_file_path.name}")
        dataset = galaxy_client.upload_file(history, test_file_path)
        
        # Wait for upload to complete
        galaxy_client.wait_for_upload(dataset)
        
        logger.info(f"Test dataset uploaded with ID: {dataset.id}")
        return dataset.id
        
    except Exception as e:
        logger.error(f"Failed to upload test dataset: {e}")
        raise


def _find_existing_history(galaxy_client: GalaxyClient, history_name: str):
    """Find existing history by name."""
    try:
        # This would need to be implemented in the GalaxyClient
        # For now, we'll assume it doesn't exist and create a new one
        # TODO: Implement history lookup by name
        return None
    except Exception as e:
        logger.debug(f"Error checking for existing history: {e}")
        return None


def _find_existing_dataset_in_history(galaxy_client: GalaxyClient, history, filename: str):
    """Find existing dataset in history by filename."""
    try:
        # This would need to be implemented in the GalaxyClient
        # For now, we'll assume it doesn't exist and create a new one
        # TODO: Implement dataset lookup by filename in history
        return None
    except Exception as e:
        logger.debug(f"Error checking for existing dataset: {e}")
        return None
