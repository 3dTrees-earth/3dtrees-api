

import pytest
import logging
from pathlib import Path
from typing import Generator

from trees_api.galaxy_client import GalaxyClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    - Creates a test history
    - Uploads a test LAS/LAZ file
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
        # Create history for test
        history = galaxy_client.create_history("Test - Overviews Workflow")
        
        # Upload test file
        dataset = galaxy_client.upload_file(history, test_file_path)
        
        # Wait for upload to complete
        galaxy_client.wait_for_upload(dataset)
        
        logger.info(f"Test dataset uploaded with ID: {dataset.id}")
        return dataset.id
        
    except Exception as e:
        logger.error(f"Failed to upload test dataset: {e}")
        raise


def test_overviews_invocation(galaxy_client: GalaxyClient, test_dataset_id: str):
    """
    End-to-end test for the Overviews workflow.
    
    This test:
    1. Ensures the Overviews workflow is available
    2. Invokes the workflow with the test dataset
    3. Verifies the workflow started successfully
    """
    workflow_name = "Overviews"
    
    try:
        # Ensure workflow is available
        workflow = galaxy_client.ensure_workflow_available(workflow_name)
        assert workflow is not None, f"Workflow '{workflow_name}' not available"
        
        # Invoke workflow with test dataset
        result = galaxy_client.invoke_workflow_with_dataset(
            workflow_name=workflow_name,
            dataset_id=test_dataset_id,
            history_name="Test - Overviews Results"
        )
        
        # Verify workflow started successfully
        assert result is not None, "Workflow invocation failed"
        assert "invocation_id" in result, "No invocation ID returned"
        assert "workflow_id" in result, "No workflow ID returned"
        
        logger.info(f"Workflow invoked successfully!")
        logger.info(f"  Invocation ID: {result['invocation_id']}")
        logger.info(f"  Workflow ID: {result['workflow_id']}")
        logger.info(f"  History ID: {result.get('history_id', 'N/A')}")
        logger.info(f"  State: {result.get('state', 'N/A')}")
        
    except Exception as e:
        logger.error(f"Workflow test failed: {e}")
        raise
