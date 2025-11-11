import tempfile
import time
import logging
import pytest
from pathlib import Path

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


def test_segmentation_workflow(test_remote_file: Dataset, galaxy_client: GalaxyClient, storage_client: StorageClient, supabase_client: SupabaseClient):
    """
    Test the Segmentation workflow end-to-end:
    1. Upload test file to Galaxy
    2. Invoke segmentation workflow (includes: Standard → Overviews → SegmentAnyTree → Py3DTiles)
    3. Monitor execution until completion
    4. Check for GPU-related errors
    5. Verify successful completion OR expected GPU failure
    
    Note: This workflow includes SegmentAnyTree which REQUIRES GPU.
    Without GPU support in Planemo, this test will fail with GPU-related errors.
    """
    # Create the workflow and the history for this test
    workflow_name = "Segmentation"
    workflow = galaxy_client.ensure_workflow_available(workflow_name)
    history = galaxy_client.create_history("Test - Segmentation Workflow")
    
    # Download the S3 stored file
    with tempfile.NamedTemporaryFile(suffix=".laz") as temp_file:
        storage_client.download_file(test_remote_file.bucket_path, temp_file.name)
        dataset = galaxy_client.upload_file(history, Path(temp_file.name))
        galaxy_client.wait_for_upload(dataset)
    
    # Now the file is in the history, we can invoke the workflow
    result = galaxy_client.invoke_workflow_with_dataset(
        workflow_name=workflow_name,
        dataset_id=dataset.id,
        history_name="Test - Segmentation Results"
    )

    assert result is not None, "Workflow invocation failed"
    assert "invocation_id" in result, "No invocation ID returned"
    assert "workflow_id" in result, "No workflow ID returned"
    
    invocation_id = result["invocation_id"]
    logger.info(f"✅ Workflow invoked successfully: {invocation_id}")

    # Create the workflow invocation in Supabase
    workflow_invocation = supabase_client.create_workflow_invocation(
        workflow_uuid=invocation_id,
        dataset_id=test_remote_file.id,
        workflow_name=workflow_name
    )
    logger.info(f"✅ Created workflow invocation record in Supabase")
    
    # Monitor workflow execution until completion
    logger.info("⏳ Monitoring workflow execution...")
    max_attempts = 360  # 360 attempts × 5 seconds = 30 minutes max (full workflow with GPU)
    workflow_completed = False
    final_status = None
    gpu_error_detected = False
    
    for attempt in range(max_attempts):
        time.sleep(5)  # Wait 5 seconds between checks
        
        try:
            # Get invocation from Galaxy
            galaxy_invocations = galaxy_client.get_workflow_invocations(invocation_ids=[invocation_id])
            
            if not galaxy_invocations:
                logger.warning(f"Attempt {attempt + 1}/{max_attempts}: Invocation not found in Galaxy")
                continue
            
            galaxy_inv = galaxy_invocations[0]
            current_status = galaxy_inv['state']
            jobs = galaxy_inv.get('jobs', [])
            
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Workflow status = {current_status}, Jobs = {len(jobs)}")
            
            # Check if workflow is in terminal state
            if current_status in ['ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning']:
                workflow_completed = True
                final_status = current_status
                logger.info(f"✅ Workflow reached terminal state: {final_status}")
                break
            
            # Check if all jobs are completed
            if jobs:
                all_jobs_finished = True
                all_jobs_successful = True
                job_details = []
                
                for job in jobs:
                    job_state = job.get('state', '')
                    job_label = job.get('step_label', 'unknown')
                    job_details.append(f"{job_label}={job_state}")
                    
                    # Terminal states: ok, error, failed, cancelled, paused (paused means workflow stopped due to errors)
                    if job_state not in ['ok', 'error', 'failed', 'cancelled', 'paused']:
                        all_jobs_finished = False
                        all_jobs_successful = False
                        break
                    if job_state != 'ok':
                        all_jobs_successful = False
                
                logger.info(f"  Job states: {', '.join(job_details)}")
                
                if all_jobs_finished:
                    workflow_completed = True
                    final_status = 'ok' if all_jobs_successful else 'error'
                    logger.info(f"✅ All jobs completed. Final status: {final_status}")
                    break
                    
        except Exception as e:
            logger.warning(f"Error checking workflow status: {e}")
    
    # Verify completion
    assert workflow_completed, f"Workflow did not complete within {max_attempts * 5} seconds (last status: {final_status})"
    
    # Get detailed invocation to check for errors
    logger.info("📋 Checking for errors in workflow execution...")
    galaxy_invocations = galaxy_client.get_workflow_invocations(invocation_ids=[invocation_id])
    galaxy_inv = galaxy_invocations[0]
    
    # Check jobs for GPU-related errors
    segmentation_job_failed = False
    gpu_error_message = None
    
    for job in galaxy_inv.get('jobs', []):
        job_id = job.get('id')
        job_state = job.get('state')
        job_label = job.get('step_label', 'unknown')
        job_tool_id = job.get('tool_id', '')
        
        logger.info(f"📝 Checking job: ID={job_id}, State={job_state}, Label={job_label}, Tool={job_tool_id}")
        
        if job_state in ['error', 'failed']:
            # Try to get detailed error message
            try:
                # Get the Job object and access its wrapped data (like in galaxy_client.py)
                job_obj = galaxy_client.gi.jobs.get(job_id)
                error_msg = str(getattr(job_obj, 'stderr', '')) if hasattr(job_obj, 'stderr') else ''
                stdout_msg = str(getattr(job_obj, 'stdout', '')) if hasattr(job_obj, 'stdout') else ''
                tool_stderr = str(getattr(job_obj, 'tool_stderr', '')) if hasattr(job_obj, 'tool_stderr') else ''
                tool_stdout = str(getattr(job_obj, 'tool_stdout', '')) if hasattr(job_obj, 'tool_stdout') else ''
                
                # Combine all error sources
                combined_error = error_msg + tool_stderr
                combined_stdout = stdout_msg + tool_stdout
                logger.info(f"📋 Combined error length: {len(combined_error)}, stdout length: {len(combined_stdout)}")
                
                # Check for GPU-related errors (expected for SegmentAnyTree without GPU)
                gpu_keywords = ['nvidia driver', 'cuda', 'gpu', 'no gpu', 'cuda available: false']
                if 'segmentanytree' in job_tool_id.lower() and any(keyword in error_msg.lower() or keyword in stdout_msg.lower() for keyword in gpu_keywords):
                    segmentation_job_failed = True
                    gpu_error_message = error_msg or stdout_msg
                    logger.warning(f"⚠️ Expected GPU error in SegmentAnyTree job: {gpu_error_message[:200]}")
                else:
                    logger.error(f"❌ Job '{job_label}' ({job_tool_id}) failed: {error_msg[:200]}")
                    raise AssertionError(f"Unexpected job failure in '{job_label}': {error_msg[:200]}")
            except AssertionError:
                raise
            except Exception as e:
                logger.warning(f"Could not get detailed error for job {job_id}: {e}")
    
    # If SegmentAnyTree failed due to GPU (expected), mark test as expected failure
    if segmentation_job_failed and gpu_error_message:
        pytest.skip(f"SegmentAnyTree requires GPU. Test cannot complete with Planemo (GPU not available). "
                   f"This is expected - deploy to production Galaxy with GPU support. "
                   f"Error: {gpu_error_message[:100]}")
    
    # If we got here, workflow completed successfully (GPU was available)
    assert final_status in ['ok', 'success'], f"Workflow failed with status: {final_status}"
    
    # Verify outputs were created (optional - workflow may not have workflow_outputs marked)
    outputs = galaxy_inv.get('outputs', {})
    output_collections = galaxy_inv.get('output_collections', {})
    
    logger.info(f"📦 Workflow outputs: {len(outputs)} files, {len(output_collections)} collections")
    
    # If no workflow outputs, that's OK as long as all jobs completed successfully
    if len(outputs) == 0 and len(output_collections) == 0:
        logger.info("⚠️  No workflow outputs marked, but all jobs completed successfully")
    
    logger.info("✅ Test passed! Segmentation workflow completed successfully with GPU support.")


def test_segmentanytree_tool_direct(test_remote_file: Dataset, galaxy_client: GalaxyClient, storage_client: StorageClient):
    """
    Direct test of SegmentAnyTree tool (not full workflow).
    This test focuses specifically on the segmentation step.
    
    Expected to fail/skip without GPU support.
    """
    history = galaxy_client.create_history("Test - SegmentAnyTree Direct")
    
    # Download and upload test file
    with tempfile.NamedTemporaryFile(suffix=".laz") as temp_file:
        storage_client.download_file(test_remote_file.bucket_path, temp_file.name)
        dataset = galaxy_client.upload_file(history, Path(temp_file.name))
        galaxy_client.wait_for_upload(dataset)
    
    # Run SegmentAnyTree tool directly
    logger.info("🔬 Running SegmentAnyTree tool directly...")
    
    try:
        tool_inputs = {
            'input': {'id': dataset.id, 'src': 'hda'},
            'log_file': True
        }
        
        job = galaxy_client.gi.tools.run_tool(
            history_id=history.id,
            tool_id='3dtrees_segmentanytree',
            tool_inputs=tool_inputs
        )
        
        job_id = job['jobs'][0]['id']
        logger.info(f"📝 Job submitted: {job_id}")
        
        # Monitor job completion
        max_wait = 300  # 5 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            time.sleep(10)
            job_details = galaxy_client.gi.jobs.get(job_id)
            job_state = job_details['state']
            
            logger.info(f"Job state: {job_state}")
            
            if job_state in ['ok', 'error', 'failed']:
                break
        
        # Check results
        if job_state == 'error' or job_state == 'failed':
            stderr = job_details.get('stderr', '')
            stdout = job_details.get('stdout', '')
            
            # Check for expected GPU error
            if 'nvidia driver' in stderr.lower() or 'nvidia driver' in stdout.lower():
                pytest.skip(f"SegmentAnyTree requires GPU (expected failure with Planemo). "
                           f"Deploy to production Galaxy with GPU support.")
            else:
                raise AssertionError(f"SegmentAnyTree failed unexpectedly: {stderr[:200]}")
        
        assert job_state == 'ok', f"SegmentAnyTree job failed with state: {job_state}"
        logger.info("✅ SegmentAnyTree completed successfully with GPU!")
        
    except Exception as e:
        logger.error(f"❌ SegmentAnyTree test error: {e}")
        raise


