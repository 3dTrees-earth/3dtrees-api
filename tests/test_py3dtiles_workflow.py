"""
End-to-End Tests for Py3DTiles Workflow

Tests the complete production workflow with both single and multi-file inputs:
1. Dataset exists in S3 (raw-storage) and Supabase
2. Call API endpoint /jobs to start workflow
3. Monitor workflow status via status.py poller → updates Supabase DB
4. Verify workflow completion and outputs in S3

Two test scenarios:
- Single file: One LAZ file → 3D Tiles (fast, ~1 minute)
- Multi-file: Multiple LAZ files merged → 3D Tiles (~2 minutes)
"""
import logging
import os
import time

import pytest
import requests

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)
API_PORT = os.getenv("API_SERVER_PORT", "8001")
API_BASE_URL = os.getenv("API_SERVER_URL", f"http://localhost:{API_PORT}")


def _run_py3dtiles_workflow_test(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    dataset: Dataset,
    expected_item_count: int,
    test_name: str,
):
    """
    Shared test logic for Py3DTiles workflow.
    
    Args:
        dataset: The dataset to process
        expected_item_count: Number of dataset_items expected (1 for single, 2+ for multi)
        test_name: Description for logging
    """
    logger.info(f"🧪 Testing Py3DTiles workflow ({test_name})")
    
    # Step 1: Verify dataset exists with expected items
    logger.info(f"📦 Dataset ID: {dataset.id}")
    
    # Get dataset items
    items_resp = supabase_client.client.table("dataset_items").select("id, bucket_path").eq("dataset_id", dataset.id).execute()
    dataset_items = items_resp.data
    logger.info(f"📦 Dataset has {len(dataset_items)} items")
    
    if len(dataset_items) < expected_item_count:
        pytest.fail(f"Expected at least {expected_item_count} dataset_items, got {len(dataset_items)}")
    
    # Verify files exist in raw bucket
    raw_bucket = storage_client.bucket_name_raw
    for item in dataset_items:
        try:
            storage_client.client.head_object(Bucket=raw_bucket, Key=item["bucket_path"])
            logger.info(f"  ✅ File exists: {item['bucket_path']}")
        except Exception as e:
            pytest.fail(f"Test file not found in S3: {item['bucket_path']} - {e}")
    
    # Step 2: Call API endpoint to start workflow
    api_url = f"{API_BASE_URL}/jobs"
    payload = {
        "dataset_id": str(dataset.id),
        "workflow_name": "Py3DTiles",
        "overwrite": False,
        "parameters": {}
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
    
    # Step 3: Monitor workflow status
    logger.info("⏳ Monitoring workflow status...")
    max_attempts = 60  # 60 × 5 seconds = 5 minutes
    workflow_finished = False
    final_status = None
    supabase_inv = None
    expected_jobs = 4  # 1 py3dtiles + 3 exports
    current_status = None
    jobs = []
    
    for attempt in range(max_attempts):
        time.sleep(5)
        
        try:
            # Run status sync (simulates the cronjob)
            from trees_api.status import sync_workflow_statuses
            sync_workflow_statuses(galaxy_client, supabase_client)
            
            # Check Supabase DB for updated status
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
            
            # Check if workflow is in terminal state AND all tracked jobs are finished
            # Note: Galaxy may report fewer jobs than expected if some exports don't run
            if current_status in ['ok', 'success', 'error', 'failed', 'cancelled'] and len(jobs) > 0:
                all_jobs_finished = all(
                    job.get('state') in ['ok', 'error', 'failed', 'cancelled'] 
                    for job in jobs
                )
                if all_jobs_finished:
                    workflow_finished = True
                    all_jobs_successful = all(job.get('state') == 'ok' for job in jobs)
                    final_status = 'ok' if all_jobs_successful else current_status
                    logger.info(f"✅ Workflow completed: {final_status} ({len(jobs)} jobs)")
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
    
    # Step 5: Verify outputs in DB
    outputs = supabase_inv.outputs or {}
    logger.info(f"📦 Workflow produced {len(outputs)} outputs (from DB)")
    
    # Step 6: Verify job details
    if supabase_inv.jobs:
        logger.info("📋 Job details:")
        for i, job in enumerate(supabase_inv.jobs or [], 1):
            tool_id = job.get('tool_id', 'unknown')
            state = job.get('state', 'unknown')
            logger.info(f"  Job {i}: {tool_id} → {state}")
            
            if state in ['error', 'failed']:
                pytest.fail(f"Job {i} ({tool_id}) failed with state: {state}")
    
    # Step 7: Verify exported 3D Tiles in S3 products bucket
    logger.info("🔍 Verifying exported 3D Tiles in S3 products bucket...")
    
    # Path structure: {dataset_id}/3dtiles/
    dataset_id = str(dataset.id)
    export_base_path = f"{dataset_id}/3dtiles"
    
    expected_outputs = {
        'tileset.json': 'Cesium 3D Tileset JSON metadata',
        'preview.pnts': '3D Tiles preview/thumbnail',
    }
    
    missing_outputs = []
    found_outputs = []
    products_bucket = storage_client.bucket_name_products
    
    # Check for root level files
    logger.info("  Checking root level outputs...")
    for output_name, description in expected_outputs.items():
        s3_key = f"{export_base_path}/{output_name}"
        
        try:
            response = storage_client.client.list_objects_v2(
                Bucket=products_bucket,
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
        points_path = f"{export_base_path}/points/"
        response = storage_client.client.list_objects_v2(
            Bucket=products_bucket,
            Prefix=points_path,
            MaxKeys=1000
        )
        
        if 'Contents' in response:
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
    logger.info(f"✅ Py3DTiles workflow ({test_name}) End-to-End test PASSED!")


def test_py3dtiles_single_file(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_single_file_dataset: Dataset
):
    """
    Test Py3DTiles workflow with a single LAZ file.
    
    This tests the basic conversion path: one LAZ file → 3D Tiles.
    Fast test (~1 minute) using mikro.laz test file.
    """
    _run_py3dtiles_workflow_test(
        galaxy_client=galaxy_client,
        storage_client=storage_client,
        supabase_client=supabase_client,
        dataset=test_single_file_dataset,
        expected_item_count=1,
        test_name="Single File",
    )


def test_py3dtiles_multi_file(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_collection_dataset: Dataset
):
    """
    Test Py3DTiles workflow with multiple LAZ files (collection).
    
    This tests the merge+conversion path: multiple LAZ files → merged 3D Tiles.
    Tests the collection-based workflow where segmented tiles are merged.
    Uses subsampled test tiles (~40KB each) for fast execution (~2 minutes).
    """
    _run_py3dtiles_workflow_test(
        galaxy_client=galaxy_client,
        storage_client=storage_client,
        supabase_client=supabase_client,
        dataset=test_collection_dataset,
        expected_item_count=2,
        test_name="Multi-File Collection",
    )
