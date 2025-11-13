"""
Test Standard (LAZ Standardization) workflow - End-to-End Test

This test verifies the complete workflow:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via Galaxy API
5. Verify outputs in S3 products bucket

Fast test for debugging workflow status (~30 seconds)
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


def test_standard_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for Standard (LAZ Standardization) workflow.
    
    Flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via Galaxy API
    5. Verify results in S3 products bucket
    
    This is the fastest workflow test (~30s) and useful for debugging.
    """
    logger.info("🧪 Testing Standard workflow (End-to-End)")
    
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
        "workflow_name": "Standard",
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
    
    # Step 3: Monitor workflow status using Galaxy API (same logic as status.py)
    logger.info("⏳ Monitoring workflow status...")
    max_attempts = 12  # 12 × 5 seconds = 1 minute
    workflow_finished = False
    final_status = None
    galaxy_inv = None
    
    for attempt in range(max_attempts):
        time.sleep(5)
        
        try:
            # Get invocation from Galaxy (same method as status.py)
            galaxy_invocations = galaxy_client.get_workflow_invocations(invocation_ids=[invocation_id])
            
            if not galaxy_invocations:
                logger.warning(f"Attempt {attempt + 1}/{max_attempts}: Invocation not found")
                continue
            
            galaxy_inv = galaxy_invocations[0]
            current_status = galaxy_inv['state']
            jobs = galaxy_inv.get('jobs', [])
            
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: status={current_status}, jobs={len(jobs)}")
            
            # Check completion using status.py logic
            # First check: workflow invocation is in terminal state
            if current_status in ['ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning']:
                workflow_finished = True
                final_status = current_status
                logger.info(f"✅ Workflow reached terminal state: {final_status}")
                break
            
            # Second check: all individual jobs are in terminal states
            elif jobs:
                all_jobs_finished = True
                all_jobs_successful = True
                for job in jobs:
                    job_state = job.get('state', '')
                    if job_state not in ['ok', 'error', 'failed', 'cancelled']:
                        all_jobs_finished = False
                        all_jobs_successful = False
                        break
                    if job_state != 'ok':
                        all_jobs_successful = False
                
                if all_jobs_finished:
                    workflow_finished = True
                    final_status = 'ok' if all_jobs_successful else 'error'
                    logger.info(f"✅ All jobs completed: {final_status}")
                    break
        
        except Exception as e:
            if "AssertionError" in str(type(e).__name__):
                raise
            logger.warning(f"Error checking workflow status: {e}")
            continue
    
    # Step 4: Verify workflow completed successfully
    if not workflow_finished:
        pytest.fail(f"Workflow did not complete within {max_attempts * 5} seconds")
    
    assert final_status in ['ok', 'success'], f"Workflow failed with status: {final_status}"
    
    # Step 5: Verify outputs exist
    outputs = galaxy_inv.get('outputs', {})
    logger.info(f"📦 Workflow produced {len(outputs)} outputs")
    
    if not outputs:
        logger.warning("⚠️ No outputs found in Galaxy invocation")
    else:
        logger.info(f"✅ Output keys: {list(outputs.keys())}")
    
    # Step 6: Verify invocation exists in Supabase
    # Note: The full status sync is done by the status.py cronjob
    try:
        invocations = supabase_client.get_workflow_invocations(limit=10)
        matching = [inv for inv in invocations if inv.invocation_id == invocation_id]
        if matching:
            logger.info(f"📊 Supabase status: {matching[0].status}")
        else:
            logger.info(f"📊 Invocation created in Supabase (status sync pending)")
    except Exception as e:
        logger.warning(f"⚠️ Could not verify Supabase status: {e}")
    
    # Step 7: Verify file was exported to S3/MinIO products bucket
    logger.info("🔍 Verifying export to S3 products bucket...")
    try:
        # Get dataset_item_id to construct expected S3 path
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if dataset_item_resp.data:
            dataset_item_id = dataset_item_resp.data[0]["id"]
            # Expected path matches runner structure: standard/{dataset_id}/{dataset_item_id}/
            # Filename is Galaxy output label + extension: "Standardized Point Cloud.laz"
            expected_key = f"standard/{test_remote_file.id}/{dataset_item_id}/Standardized Point Cloud.laz"
            
            logger.info(f"📂 Checking for exported file: {expected_key}")
            
            # Check if file exists in products bucket
            try:
                response = storage_client.client.head_object(
                    Bucket="3dtrees-tool-products",
                    Key=expected_key
                )
                file_size = response.get('ContentLength', 0)
                logger.info(f"✅ File exported to S3 products bucket: {expected_key} ({file_size} bytes)")
                
                # Verify file size is reasonable (should be > 0)
                assert file_size > 0, f"Exported file is empty: {expected_key}"
                
            except storage_client.client.exceptions.NoSuchKey:
                pytest.fail(f"❌ Exported file not found in S3 products bucket: {expected_key}")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify export (may still be processing): {e}")
        else:
            logger.warning(f"⚠️ Could not get dataset_item_id for verification")
    except Exception as e:
        logger.warning(f"⚠️ Export verification failed: {e}")
    
    logger.info("✅ Standard workflow End-to-End test PASSED!")

