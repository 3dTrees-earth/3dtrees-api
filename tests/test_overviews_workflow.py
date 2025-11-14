"""
Test Overviews workflow - End-to-End Test

This test verifies the complete production workflow:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify exported overview images in S3 products bucket

Workflow: Overview Generator → Export to S3

This is a fast test (~1-2 minutes) compared to the full Segmentation workflow.
"""
import logging
import time
import requests
from typing import Dict, Any

import pytest

from trees_api.models import Dataset
from trees_api.galaxy_client import GalaxyClient
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)


def test_overviews_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for Overviews workflow.
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify exported overview images in S3 products bucket
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database (~1-2 minutes for overviews).
    """
    logger.info("🧪 Testing Overviews workflow (End-to-End)")
    
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
        "workflow_name": "Overviews",
        "overwrite": False
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
    max_attempts = 60  # 60 × 5 seconds = 5 minutes (allows for backfill delays)
    workflow_finished = False
    final_status = None
    supabase_inv = None
    expected_jobs = 4  # Overviews workflow: 1 tool + 3 exports
    
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
        logger.warning("⚠️ No outputs found in Supabase")
    else:
        logger.info(f"✅ Output keys: {list(outputs.keys())}")
    
    # Step 6: Verify invocation details in Supabase
    logger.info(f"📊 Supabase status: {supabase_inv.status}")
    logger.info(f"📊 Finished at: {supabase_inv.finished_at}")
    logger.info(f"📊 Jobs count: {len(supabase_inv.jobs or [])}")
    
    # Step 7: Verify ALL expected outputs in S3/MinIO products bucket
    logger.info("🔍 Verifying ALL expected outputs in S3 products bucket...")
    
    # Get dataset_item_id to construct actual export path
    try:
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            pytest.fail(f"Could not get dataset_item_id for dataset {test_remote_file.id}")
        dataset_item_id = dataset_item_resp.data[0]["id"]
    except Exception as e:
        pytest.fail(f"Error getting dataset_item_id: {e}")
    
    # Define all expected outputs for the Overviews workflow
    # Path structure: overviews/{dataset_id}/{dataset_item_id}/{filename}
    # NOTE: Galaxy's export_remote adds .{extension} to collection elements, causing double extensions
    dataset_id = str(test_remote_file.id)
    export_base_path = f"overviews/{dataset_id}/{dataset_item_id}"
    
    expected_outputs = {
        'top_view_00.png.png': 'Top view perspective 0°',  # Note: double .png from Galaxy export
        'top_view_01.png.png': 'Top view perspective 180°',
        'section_ew.png.png': 'East-West section view',
        'section_ns.png.png': 'North-South section view',
        'Overview Animation.gif': 'Rotating overview animation',  # Note: Label name, not file name
    }
    
    missing_outputs = []
    found_outputs = []
    
    for output_name, description in expected_outputs.items():
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
                logger.info(f"  ✅ {output_name}: {description}")
            else:
                missing_outputs.append(output_name)
                logger.error(f"  ❌ {output_name}: NOT FOUND at {s3_key}")
        
        except Exception as e:
            missing_outputs.append(output_name)
            logger.error(f"  ❌ {output_name}: ERROR checking - {e}")
    
    # CRITICAL ASSERTION: All outputs must be present
    if missing_outputs:
        error_msg = (
            f"\n❌ FAILED: Missing {len(missing_outputs)}/{len(expected_outputs)} expected outputs!\n"
            f"   Expected path: {export_base_path}/\n"
            f"   Missing: {missing_outputs}\n"
            f"   Found: {found_outputs}\n"
            f"   This indicates that not all export jobs completed successfully."
        )
        pytest.fail(error_msg)
    
    logger.info(f"✅ All {len(expected_outputs)} expected outputs verified in S3!")
    
    logger.info("✅ Overviews workflow End-to-End test PASSED!")


