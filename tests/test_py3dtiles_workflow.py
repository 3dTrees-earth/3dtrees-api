"""
End-to-End Test for Py3DTiles Workflow

Tests the complete production workflow:
1. Dataset exists in S3 (raw-storage) and Supabase
2. Call API endpoint /jobs to start workflow
3. Monitor workflow status via status.py poller → updates Supabase DB
4. Verify workflow completion

This test simulates actual production flow (~3 minutes for py3dtiles).
"""
import logging
import time

import pytest
import requests

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


def test_py3dtiles_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for Py3DTiles workflow.
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify outputs in Galaxy history
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database (~2-3 minutes for py3dtiles).
    """
    logger.info("🧪 Testing Py3DTiles workflow (End-to-End)")
    
    # Step 1: Verify dataset exists in S3 and DB
    logger.info(f"📦 Dataset ID: {test_remote_file.id}")
    logger.info(f"📍 S3 Path: s3://3dtrees-tool-raw/{test_remote_file.bucket_path}")
    
    # Verify file exists in raw bucket
    try:
        storage_client.client.head_object(
            Bucket="3dtrees-tool-raw",
            Key=test_remote_file.bucket_path
        )
        logger.info(f"✅ File exists in S3 raw-storage")
    except Exception as e:
        pytest.fail(f"Test file not found in S3: {e}")
    
    # Step 2: Call API endpoint to start workflow
    api_url = "http://localhost:8000/jobs"
    payload = {
        "dataset_id": str(test_remote_file.id),
        "workflow_name": "Py3DTiles",
        "overwrite": False,
        "parameters": {}  # Can add srs_out, extra_fields here if needed
    }
    
    logger.info(f"🚀 Calling API: POST {api_url}")
    logger.info(f"📋 Payload: {payload}")
    
    try:
        response = requests.post(api_url, params=payload, timeout=30)
        response.raise_for_status()
        workflow_invocation = response.json()
        invocation_id = workflow_invocation["invocation_id"]
        logger.info(f"✅ Workflow invoked via API: {invocation_id}")
    except requests.exceptions.RequestException as e:
        pytest.fail(f"API call failed: {e}")
    
    # Step 3: Monitor workflow status using status.py poller (production-like)
    logger.info("⏳ Monitoring workflow status via status.py poller...")
    max_attempts = 60  # 60 × 5 seconds = 5 minutes (py3dtiles can be slow)
    workflow_finished = False
    final_status = None
    supabase_inv = None
    expected_jobs = 4  # Py3DTiles workflow: 1 tool + 3 exports (tileset.json + preview.pnts + points_tiles collection)
    
    for attempt in range(max_attempts):
        time.sleep(5)
        
        try:
            # Run status sync (simulates the cronjob)
            from trees_api.status import sync_workflow_statuses
            sync_workflow_statuses(galaxy_client, supabase_client)
            
            # Check Supabase DB for updated status (production approach)
            supabase_inv = supabase_client.get_workflow_invocation_by_id(invocation_id)
            
            if not supabase_inv:
                logger.warning(f"Attempt {attempt + 1}/{max_attempts}: Invocation not found in DB")
                continue
            
            current_status = supabase_inv.status
            jobs = supabase_inv.jobs or []
            
            # Count job states
            job_states = {}
            for job in jobs:
                state = job.get('state', 'unknown')
                job_states[state] = job_states.get(state, 0) + 1
            
            job_summary = ', '.join([f"{state}={count}" for state, count in job_states.items()]) if job_states else "no jobs"
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: status={current_status}, jobs={len(jobs)}/{expected_jobs} ({job_summary})")
            
            # Check if finished (using same logic as status.py)
            if current_status in ['ok', 'success', 'error', 'failed', 'cancelled']:
                workflow_finished = True
                final_status = current_status
                logger.info(f"✅ Workflow reached terminal state: {final_status}")
                break
            
            # Check if we have all expected jobs and they're all finished
            elif len(jobs) >= expected_jobs:
                all_jobs_finished = all(
                    job.get('state') in ['ok', 'error', 'failed', 'cancelled'] 
                    for job in jobs
                )
                if all_jobs_finished:
                    workflow_finished = True
                    all_jobs_successful = all(job.get('state') == 'ok' for job in jobs)
                    final_status = 'ok' if all_jobs_successful else 'error'
                    logger.info(f"✅ All {len(jobs)} jobs completed: {final_status}")
                    break
        
        except Exception as e:
            if "AssertionError" in str(type(e).__name__):
                raise
            logger.warning(f"Error in status sync: {e}")
            continue
    
    # Step 4: Verify workflow completed successfully
    if not workflow_finished:
        timeout_mins = (max_attempts * 5) // 60
        last_state = f"status={current_status}, jobs={len(jobs)}/{expected_jobs}" if supabase_inv else "no data"
        pytest.fail(f"Workflow did not complete within {timeout_mins} minutes (last state: {last_state})")
    
    assert final_status in ['ok', 'success'], f"Workflow failed with status: {final_status}"
    
    # Step 5: Verify outputs exist in DB
    outputs = supabase_inv.outputs or {}
    logger.info(f"📦 Workflow produced {len(outputs)} outputs (from DB)")
    
    if not outputs:
        logger.warning("⚠️ No outputs found in Supabase (this is expected if Galaxy doesn't export them yet)")
    else:
        logger.info(f"✅ Output keys: {list(outputs.keys())}")
    
    # Step 6: Verify invocation details in Supabase
    logger.info(f"📊 Supabase status: {supabase_inv.status}")
    logger.info(f"📊 Finished at: {supabase_inv.finished_at}")
    logger.info(f"📊 Jobs count: {len(supabase_inv.jobs or [])}")
    
    # Step 7: Verify job details
    if supabase_inv.jobs:
        logger.info("📋 Job details:")
        for i, job in enumerate(supabase_inv.jobs or [], 1):
            tool_id = job.get('tool_id', 'unknown')
            state = job.get('state', 'unknown')
            logger.info(f"  Job {i}: {tool_id} → {state}")
            
            # Verify no failed jobs
            if state in ['error', 'failed']:
                pytest.fail(f"Job {i} ({tool_id}) failed with state: {state}")
    
    # Step 8: Verify exported 3D Tiles in S3 products bucket
    logger.info("🔍 Verifying exported 3D Tiles in S3 products bucket...")
    
    # Get dataset_item_id to construct actual export path
    try:
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            pytest.fail(f"Could not get dataset_item_id for dataset {test_remote_file.id}")
        dataset_item_id = dataset_item_resp.data[0]["id"]
    except Exception as e:
        pytest.fail(f"Error getting dataset_item_id: {e}")

    # Define all expected outputs for the Py3DTiles workflow
    # Path structure:
    #   Root: 3dtiles/{dataset_id}/{dataset_item_id}/
    #   Points: 3dtiles/{dataset_id}/{dataset_item_id}/points/
    # NOTE: With separate outputs + separate export_remote steps, we can control the directory structure!
    dataset_id = str(test_remote_file.id)
    export_base_path = f"3dtiles/{dataset_id}/{dataset_item_id}"

    # Expected outputs at root level:
    # 1. tileset.json - Metadata file
    # 2. preview.pnts - Thumbnail/preview tile
    expected_root_outputs = {
        'tileset.json': 'Cesium 3D Tileset JSON metadata',
        'preview.pnts': '3D Tiles preview/thumbnail',
    }

    missing_outputs = []
    found_outputs = []

    # Check for root level files (tileset.json and preview.pnts)
    logger.info("  Checking root level outputs...")
    for output_name, description in expected_root_outputs.items():
        # Construct the full S3 key
        s3_key = f"{export_base_path}/{output_name}"

        try:
            # Check if file exists in products-storage bucket using direct S3 API
            response = storage_client.client.list_objects_v2(
                Bucket='3dtrees-tool-products',
                Prefix=s3_key,
                MaxKeys=1
            )

            if 'Contents' in response and len(response['Contents']) > 0:
                found_outputs.append(output_name)
                logger.info(f"    ✅ {output_name}: {description}")
            else:
                missing_outputs.append(output_name)
                logger.error(f"    ❌ {output_name}: NOT FOUND at {s3_key}")

        except Exception as e:
            missing_outputs.append(output_name)
            logger.error(f"    ❌ {output_name}: ERROR checking - {e}")

    # Check for tile files in points/ subdirectory
    logger.info("  Checking points/ subdirectory...")
    try:
        # Check for .pnts files in the points/ subdirectory
        points_path = f"{export_base_path}/points/"
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-tool-products',
            Prefix=points_path,
            MaxKeys=1000  # Need more to count all .pnts files
        )

        if 'Contents' in response:
            # Look for files with .pnts extension in the points/ directory
            tile_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pnts')]
            if tile_files:
                logger.info(f"    ✅ Tile files: Found {len(tile_files)} .pnts files in points/ subdirectory")
                found_outputs.append(f'{len(tile_files)} tile files')
            else:
                missing_outputs.append('points_tiles')
                logger.error(f"    ❌ Tile files: NO .pnts files found at {points_path}")
        else:
            missing_outputs.append('points_tiles')
            logger.error(f"    ❌ Tile files: No files found at {points_path}")

    except Exception as e:
        missing_outputs.append('points_tiles')
        logger.error(f"    ❌ Tile files: ERROR checking - {e}")

    # CRITICAL ASSERTION: All outputs must be present
    if missing_outputs:
        error_msg = (
            f"\n❌ FAILED: Missing {len(missing_outputs)} expected outputs!\n"
            f"   Expected path: {export_base_path}/\n"
            f"   Missing: {missing_outputs}\n"
            f"   Found: {found_outputs}\n"
            f"   This indicates that not all export jobs completed successfully."
        )
        pytest.fail(error_msg)

    logger.info(f"✅ All expected outputs verified in S3!")
    
    logger.info("✅ Py3DTiles workflow End-to-End test PASSED!")
