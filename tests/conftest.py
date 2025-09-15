import pytest
import logging
from pathlib import Path
from typing import Generator, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient
from trees_api.models import Dataset

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def storage_client() -> StorageClient:
    client = StorageClient()
    
    try:
        # Connect to storage service
        client.connect()
        logger.info("Storage client connected successfully")
        
        # Ensure bucket exists
        _ensure_bucket_exists(client)
        
        return client
        
    except Exception as e:
        logger.error(f"Failed to setup storage client: {e}")
        raise


def _ensure_bucket_exists(storage_client: StorageClient) -> None:
    """Ensure the required bucket exists, create if it doesn't."""
    bucket_name = storage_client.bucket_name
    
    try:
        # Check if bucket exists
        storage_client.client.head_bucket(Bucket=bucket_name)
        logger.info(f"✅ Bucket '{bucket_name}' already exists")
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket doesn't exist, create it
            logger.info(f"Creating bucket '{bucket_name}'...")
            storage_client.client.create_bucket(Bucket=bucket_name)
            logger.info(f"✅ Bucket '{bucket_name}' created successfully")
        else:
            logger.error(f"❌ Error checking bucket: {e}")
            raise RuntimeError(f"Failed to check bucket '{bucket_name}': {e}")


@pytest.fixture(scope="session")
def supabase_client() -> SupabaseClient:
    client = SupabaseClient()
    client.connect()
    
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
            logger.warning("Tests will use anonymous access - this may cause issues with dataset creation")
    
    return client


@pytest.fixture(scope="session")
def test_remote_file(storage_client: StorageClient, supabase_client: SupabaseClient) -> Dataset:
    key = "LAS/Example_Platane.laz"
    file_path = Path(__file__).parent / "Example_Platane.laz"
    
    if not file_path.exists():
        raise FileNotFoundError(f"Test file not found: {file_path}")
    
    # Check if file already exists in storage
    if not _file_exists_in_storage(storage_client, key):
        logger.info(f"Uploading test file to storage: {key}")
        storage_client.upload_file(file_path, key)
        logger.info(f"✅ File uploaded to storage: {key}")
    else:
        logger.info(f"✅ File already exists in storage: {key}")

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
        title="Test Platane",
        file_name=file_path.name,
        visibility="public"
    )
    logger.info(f"✅ Dataset created in Supabase: {dataset.id}")
    return dataset


def _file_exists_in_storage(storage_client: StorageClient, key: str) -> bool:
    """Check if a file exists in storage."""
    try:
        storage_client.client.head_object(Bucket=storage_client.bucket_name, Key=key)
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
    client = GalaxyClient()
    
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
        raise


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
