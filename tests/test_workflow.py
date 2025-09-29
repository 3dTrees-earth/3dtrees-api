import tempfile
from pathlib import Path

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient


def test_workflow(test_remote_file: Dataset, galaxy_client: GalaxyClient, storage_client: StorageClient, supabase_client: SupabaseClient):
    # create the workflow and the history for this test
    workflow_name = "Overviews"
    workflow = galaxy_client.ensure_workflow_available(workflow_name)
    history = galaxy_client.create_history("Test - Workflow")
    
    # download the S3 stored file
    with tempfile.NamedTemporaryFile(suffix=".laz") as temp_file:
        storage_client.download_file(test_remote_file.bucket_path, temp_file.name)
        dataset = galaxy_client.upload_file(history, Path(temp_file.name))
        galaxy_client.wait_for_upload(dataset)
    
    # now the file is in the history, we can invoke the workflow
    result = galaxy_client.invoke_workflow_with_dataset(
        workflow_name=workflow_name,
        dataset_id=dataset.id,
        history_name="Test - Workflow Results"
    )

    # create the workflow invocation in Supabase
    workflow_invocation = supabase_client.create_workflow_invocation(
        workflow_uuid=workflow.latest_workflow_uuid,
        dataset_id=test_remote_file.id,  # Use Supabase dataset ID, not Galaxy dataset ID
        workflow_name=workflow_name
    )

    assert result is not None, "Workflow invocation failed"
    assert "invocation_id" in result, "No invocation ID returned"
    assert "workflow_id" in result, "No workflow ID returned"