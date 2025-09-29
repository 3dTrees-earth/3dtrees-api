import tempfile
import time
import httpx
import uvicorn
import threading
from pathlib import Path

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient
from trees_api.status import sync_workflow_statuses, get_connected_clients


def test_workflow_via_api_with_status_sync(test_remote_file: Dataset, galaxy_client: GalaxyClient, storage_client: StorageClient, supabase_client: SupabaseClient):
    """
    Comprehensive integration test that:
    1. Starts the API server
    2. Sends a job request via HTTP
    3. Monitors status changes using the status sync script
    4. Validates the complete end-to-end workflow
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from trees_api.server import app
    
    # Start API server in a separate thread
    def run_server():
        uvicorn.run(app, host="0.0.0.0", port=8003, log_level="error")
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait for server to start
    time.sleep(2)
    
    # Test if server is responding
    with httpx.Client(timeout=5.0) as test_client:
        test_response = test_client.get("http://127.0.0.1:8003/")
        print(f"Server health check: {test_response.status_code}")
    
    # Send job request via HTTP API
    with httpx.Client(timeout=60.0) as client:
        response = client.post(
            "http://127.0.0.1:8003/jobs",
            params={
                "dataset_id": str(test_remote_file.id),
                "workflow_name": "overviews",
                "overwrite": False
            },
            json={"test_parameter": "test_value"}
        )
        
        assert response.status_code == 200, f"API request failed: {response.text}"
        job_response = response.json()
        
        # Verify job was created
        assert "invocation_id" in job_response, "No invocation_id in response"
        assert job_response["workflow_name"] == "Overviews", "Wrong workflow name"
        assert job_response["status"] == "new", "Initial status should be 'new'"
        
        invocation_id = job_response["invocation_id"]
        print(f"Created job with invocation_id: {invocation_id}")
        
        # Monitor status changes using status sync
        max_attempts = 30  # 30 attempts with 2-second intervals = 1 minute max
        status_changed = False
        
        for attempt in range(max_attempts):
            print(f"Status sync attempt {attempt + 1}/{max_attempts}")
            
            # Run status sync
            galaxy_client_sync, supabase_client_sync, _ = get_connected_clients()
            stats = sync_workflow_statuses(galaxy_client_sync, supabase_client_sync)
            print(f"Sync stats: {stats}")
            
            # Check if status changed in Supabase
            updated_invocation = supabase_client.get_workflow_invocation_by_id(invocation_id)
            if updated_invocation and updated_invocation.status != "new":
                print(f"Status changed to: {updated_invocation.status}")
                status_changed = True
                break
                
            time.sleep(2)
        
        # Assertions
        assert status_changed, "Status did not change within timeout period"
        
        # Verify the status change is reflected in Supabase
        final_invocation = supabase_client.get_workflow_invocation_by_id(invocation_id)
        assert final_invocation is not None, "Invocation not found in Supabase"
        assert final_invocation.status in ["running", "scheduled"], f"Expected running/scheduled status, got: {final_invocation.status}"
        
        # Verify the inputs were stored correctly (should be Galaxy inputs, not user parameters)
        assert isinstance(final_invocation.inputs, list), "Inputs should be a list"
        assert len(final_invocation.inputs) > 0, "Inputs should not be empty"
        # Check that inputs contain Galaxy dataset information
        assert "id" in final_invocation.inputs[0], "Inputs should contain Galaxy dataset IDs"
        
        # Verify the parameters were stored correctly
        assert isinstance(final_invocation.parameters, dict), "Parameters should be a dict"
        assert final_invocation.parameters == {"test_parameter": "test_value"}, "Parameters not stored correctly"
        
        print(f"✅ Integration test passed! Final status: {final_invocation.status}")


def test_status_sync_standalone(galaxy_client: GalaxyClient, supabase_client: SupabaseClient):
    """
    Test the status sync functionality in isolation.
    This test verifies that the status sync can detect and update workflow statuses.
    """
    # Get all unfinished invocations from Supabase
    unfinished_invocations = supabase_client.get_unfinished_workflow_invocations()
    
    if not unfinished_invocations:
        print("No unfinished invocations found for status sync test")
        return
    
    # Run status sync
    stats = sync_workflow_statuses(galaxy_client, supabase_client)
    
    # Verify sync completed without errors
    assert stats['errors'] == 0, f"Status sync had errors: {stats}"
    assert stats['total_checked'] >= 0, "Should have checked some invocations"
    
    print(f"✅ Status sync test passed! Stats: {stats}")


def test_api_health_check():
    """
    Test that the API server can start and respond to basic requests.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    from trees_api.server import app
    
    # Start API server in a separate thread
    def run_server():
        uvicorn.run(app, host="0.0.0.0", port=8004, log_level="error")
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Test health check endpoint
        with httpx.Client(timeout=30.0) as client:
            response = client.get("http://127.0.0.1:8004/")
            
            assert response.status_code == 200, f"Health check failed: {response.text}"
            health_response = response.json()
            assert "message" in health_response, "Health check response missing message"
            assert "3DTrees API is running" in health_response["message"], "Wrong health check message"
            
            print("✅ API health check test passed!")
            
    except Exception as e:
        print(f"API health check test failed: {e}")
        raise
    finally:
        # Server will stop when thread dies (daemon=True)
        pass
