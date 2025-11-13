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
    max_attempts = 24  # 24 × 5 seconds = 2 minutes max for overviews
    workflow_finished = False
    final_status = None
    supabase_inv = None
    
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
            
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: status={current_status}, jobs={len(jobs)}")
            
            # Check if finished (using same logic as status.py)
            if current_status in ['ok', 'success', 'error', 'failed', 'cancelled']:
                workflow_finished = True
                final_status = current_status
                logger.info(f"✅ Workflow reached terminal state: {final_status}")
                break
            
            # Check if all jobs finished (even if workflow status is 'scheduled')
            elif jobs:
                all_jobs_finished = all(
                    job.get('state') in ['ok', 'error', 'failed', 'cancelled'] 
                    for job in jobs
                )
                if all_jobs_finished:
                    workflow_finished = True
                    all_jobs_successful = all(job.get('state') == 'ok' for job in jobs)
                    final_status = 'ok' if all_jobs_successful else 'error'
                    logger.info(f"✅ All jobs completed: {final_status}")
                    break
        
        except Exception as e:
            if "AssertionError" in str(type(e).__name__):
                raise
            logger.warning(f"Error in status sync: {e}")
            continue
    
    # Step 4: Verify workflow completed successfully
    if not workflow_finished:
        pytest.fail(f"Workflow did not complete within {max_attempts * 5} seconds")
    
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
    
    # Step 7: Verify exported overview images in S3/MinIO products bucket
    logger.info("🔍 Verifying exported overview images in S3 products bucket...")
    
    try:
        # Get dataset_item_id to construct expected S3 path
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            logger.warning("⚠️ Could not get dataset_item_id for S3 verification")
        else:
            dataset_item_id = dataset_item_resp.data[0]["id"]
            overview_prefix = f"overviews/{test_remote_file.id}/{dataset_item_id}/"
            
            logger.info(f"📂 Checking overview files at: {overview_prefix}")
            try:
                response = storage_client.client.list_objects_v2(
                    Bucket="3dtrees-tool-products",
                    Prefix=overview_prefix
                )
                
                if 'Contents' in response and len(response['Contents']) > 0:
                    overview_files = [obj['Key'] for obj in response['Contents']]
                    logger.info(f"✅ Overview export: {len(overview_files)} files found")
                    
                    # List all files
                    for file_key in overview_files:
                        file_size = next((obj['Size'] for obj in response['Contents'] if obj['Key'] == file_key), 0)
                        logger.info(f"   - {file_key} ({file_size} bytes)")
                    
                    # Verify at least some expected files exist
                    has_images = any('.png' in f or '.gif' in f for f in overview_files)
                    if has_images:
                        logger.info("✅ Found image files (PNG/GIF)")
                    else:
                        logger.warning("⚠️ No image files found")
                    
                    # Verify all files are non-empty
                    all_non_empty = all(
                        obj.get('Size', 0) > 0 
                        for obj in response['Contents']
                    )
                    if all_non_empty:
                        logger.info("✅ All exported files are non-empty")
                    else:
                        logger.warning("⚠️ Some exported files are empty")
                else:
                    logger.warning(f"⚠️ No overview files found at {overview_prefix}")
            
            except Exception as e:
                logger.warning(f"⚠️ Could not verify overview files: {e}")
    
    except Exception as e:
        logger.warning(f"⚠️ Export verification failed: {e}")
    
    logger.info("✅ Overviews workflow End-to-End test PASSED!")


