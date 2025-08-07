"""
End-to-end tests for Galaxy integration.
Tests the complete workflow from authentication to tool execution.
"""

import os
import pytest
import logging
from pathlib import Path
from typing import Dict, Any

from trees_api.galaxy_client import GalaxyClient

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestGalaxyIntegration:
    """Test class for Galaxy integration end-to-end tests."""
    
    @pytest.fixture(scope="class")
    def galaxy_client(self) -> GalaxyClient:
        """Create a Galaxy client instance for testing."""
        galaxy_url = "http://127.0.0.1:9090"
        api_key = os.getenv("GALAXY_API_KEY")
        return GalaxyClient(galaxy_url, api_key)
    
    @pytest.fixture(scope="class")
    def test_file_path(self) -> Path:
        """Get the path to the test file."""
        # Try multiple possible locations for the test file
        possible_paths = [
            Path("./Example_Platane.laz"),
            Path("../Example_Platane.laz"),
            Path("../galaxy/tools/test-data/Example_Platane.laz"),
            Path("../../Example_Platane.laz"),
        ]
        
        for path in possible_paths:
            if path.exists():
                return path
                
        pytest.skip("Test file Example_Platane.laz not found")
    
    @pytest.fixture(scope="class")
    def overviews_tool_params(self) -> Dict[str, Any]:
        """Get the parameters for the overviews tool test."""
        return {
            'max_points': 1000000,  # 1M points for testing
            'section_width': 5,
            'image_width': 512,
            'image_height': 384,
            'top_views_deg': 45,
            'cmap': 'viridis',
            'camera_distance': 25.0
        }
    
    def test_galaxy_authentication(self, galaxy_client: GalaxyClient):
        """Test Galaxy authentication."""
        # Test credentials for local development
        email = "processor@3dtrees.earth"
        password = "processor"
        
        # Authenticate
        assert galaxy_client.authenticate(email, password), "Authentication failed"
        assert galaxy_client.api_key is not None, "API key not obtained"
    
    def test_galaxy_connection(self, galaxy_client: GalaxyClient):
        """Test connection to Galaxy instance."""
        assert galaxy_client.connect(), "Failed to connect to Galaxy"
        assert galaxy_client.gi is not None, "Galaxy instance not created"
    
    def test_find_overviews_tool(self, galaxy_client: GalaxyClient):
        """Test finding the overviews tool."""
        tool = galaxy_client.find_tool("3D Trees Overview Generator")
        assert tool is not None, "Overviews tool not found"
        assert tool.name == "3D Trees Overview Generator", "Wrong tool found"
    
    def test_create_history(self, galaxy_client: GalaxyClient):
        """Test creating a new history."""
        history = galaxy_client.create_history("Pytest Test - Overviews")
        assert history is not None, "Failed to create history"
        assert history.name == "Pytest Test - Overviews", "Wrong history name"
    
    def test_upload_file(self, galaxy_client: GalaxyClient, test_file_path: Path):
        """Test uploading a file to Galaxy."""
        # Create a history for this test
        history = galaxy_client.create_history("Pytest Test - File Upload")
        assert history is not None, "Failed to create history for upload test"
        
        # Upload the file
        dataset = galaxy_client.upload_file(history, test_file_path)
        assert dataset is not None, "Failed to upload file"
        assert dataset.name == test_file_path.name, "Wrong dataset name"
        
        # Wait for upload to complete
        assert galaxy_client.wait_for_upload(dataset), "Upload failed or timed out"
    
    def test_overviews_tool_execution(self, galaxy_client: GalaxyClient, 
                                    test_file_path: Path, 
                                    overviews_tool_params: Dict[str, Any]):
        """Test complete overviews tool execution workflow."""
        # Find the tool
        tool = galaxy_client.find_tool("3D Trees Overview Generator")
        assert tool is not None, "Overviews tool not found"
        
        # Create a history
        history = galaxy_client.create_history("Pytest Test - Tool Execution")
        assert history is not None, "Failed to create history"
        
        # Upload the test file
        dataset = galaxy_client.upload_file(history, test_file_path)
        assert dataset is not None, "Failed to upload file"
        
        # Wait for upload to complete
        assert galaxy_client.wait_for_upload(dataset), "Upload failed or timed out"
        
        # Prepare tool inputs
        tool_inputs = {
            'input': dataset,
            **overviews_tool_params
        }
        
        # Run the tool
        job = galaxy_client.run_tool(tool, history, tool_inputs)
        assert job is not None, "Tool execution failed to start"
        
        # Wait for job completion
        assert galaxy_client.wait_for_job_completion(history, test_file_path.name), "Job failed or timed out"
        
        # Get results
        results = galaxy_client.get_job_results(history)
        assert len(results) > 1, "No output datasets produced"
        
        # Check that we have output files
        output_datasets = [r for r in results if r['name'] != test_file_path.name]
        assert len(output_datasets) > 0, "No output datasets found"
        
        # Check for expected output types
        output_names = [r['name'] for r in output_datasets]
        output_extensions = [r['file_ext'] for r in output_datasets]
        
        # Should have at least one output file
        assert any(ext in ['gif', 'png', 'collection'] for ext in output_extensions), \
            f"Expected output files not found. Got: {output_extensions}"
        
        logger.info(f"Tool execution successful. Outputs: {output_names}")
    
    def test_end_to_end_workflow(self, galaxy_client: GalaxyClient, 
                                test_file_path: Path, 
                                overviews_tool_params: Dict[str, Any]):
        """Test the complete end-to-end workflow."""
        logger.info("Starting end-to-end workflow test...")
        
        # Step 1: Authentication
        email = "processor@3dtrees.earth"
        password = "processor"
        assert galaxy_client.authenticate(email, password), "Authentication failed"
        
        # Step 2: Connection
        assert galaxy_client.connect(), "Connection failed"
        
        # Step 3: Find tool
        tool = galaxy_client.find_tool("3D Trees Overview Generator")
        assert tool is not None, "Tool not found"
        
        # Step 4: Create history
        history = galaxy_client.create_history("Pytest E2E Test")
        assert history is not None, "History creation failed"
        
        # Step 5: Upload file
        dataset = galaxy_client.upload_file(history, test_file_path)
        assert dataset is not None, "File upload failed"
        
        # Step 6: Wait for upload
        assert galaxy_client.wait_for_upload(dataset), "Upload completion failed"
        
        # Step 7: Run tool
        tool_inputs = {
            'input': dataset,
            **overviews_tool_params
        }
        job = galaxy_client.run_tool(tool, history, tool_inputs)
        assert job is not None, "Tool execution failed"
        
        # Step 8: Wait for completion
        assert galaxy_client.wait_for_job_completion(history, test_file_path.name), "Job completion failed"
        
        # Step 9: Get results
        results = galaxy_client.get_job_results(history)
        assert len(results) > 1, "No results produced"
        
        logger.info("End-to-end workflow test completed successfully!")
        logger.info(f"Results: {[r['name'] for r in results]}")


# Integration test that can be run independently
def test_galaxy_integration():
    """Integration test that can be run as a standalone function."""
    logger.info("Running Galaxy integration test...")
    
    # Create client
    galaxy_url = "http://127.0.0.1:9090"
    client = GalaxyClient(galaxy_url)
    
    # Test file path
    test_file_path = Path("./Example_Platane.laz")
    if not test_file_path.exists():
        logger.error(f"Test file not found: {test_file_path}")
        return False
    
    try:
        # Authenticate
        assert client.authenticate("processor@3dtrees.earth", "processor")
        
        # Connect
        assert client.connect()
        
        # Find tool
        tool = client.find_tool("3D Trees Overview Generator")
        assert tool is not None
        
        # Create history
        history = client.create_history("Integration Test")
        assert history is not None
        
        # Upload and process
        dataset = client.upload_file(history, test_file_path)
        assert dataset is not None
        
        assert client.wait_for_upload(dataset)
        
        tool_inputs = {
            'input': dataset,
            'max_points': 1000000,
            'section_width': 5,
            'image_width': 512,
            'image_height': 384,
            'top_views_deg': 45,
            'cmap': 'viridis',
            'camera_distance': 25.0
        }
        
        job = client.run_tool(tool, history, tool_inputs)
        assert job is not None
        
        assert client.wait_for_job_completion(history, test_file_path.name)
        
        results = client.get_job_results(history)
        assert len(results) > 1
        
        logger.info("Integration test completed successfully!")
        
    except Exception as e:
        logger.error(f"Integration test failed: {e}")
        raise


if __name__ == "__main__":
    # Run the integration test
    success = test_galaxy_integration()
    exit(0 if success else 1) 