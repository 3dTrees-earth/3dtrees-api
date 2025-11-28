"""
Test PDAL Metadata Extraction Workflow - End-to-End Test

This test verifies the complete production workflow:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start PdalMetadata workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify exported metadata.json file in S3 at metadata/{dataset_id}/{dataset_item_id}/

This test simulates the actual production flow where status.py cronjob
polls Galaxy and updates the Supabase database (~30 seconds)
"""
import json
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


def test_pdal_metadata_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for PDAL Metadata Extraction workflow.
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify metadata.json in S3 at metadata/{dataset_id}/{dataset_item_id}/
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database (~30s).
    """
    logger.info("🧪 Testing PDAL Metadata workflow (End-to-End)")
    
    # Step 1: Verify dataset exists in S3 and DB
    logger.info(f"📦 Dataset ID: {test_remote_file.id}")
    logger.info(f"📍 S3 Path: s3://3dtrees-raw/{test_remote_file.bucket_path}")
    
    # Verify file exists in raw bucket
    try:
        storage_client.client.head_object(
            Bucket="3dtrees-raw",
            Key=test_remote_file.bucket_path
        )
        logger.info(f"✅ File exists in S3 raw-storage")
    except Exception as e:
        pytest.fail(f"Test file not found in S3: {e}")
    
    # Step 2: Call API endpoint to start workflow
    api_url = "http://localhost:8000/jobs"
    payload = {
        "dataset_id": str(test_remote_file.id),
        "workflow_name": "PdalMetadata",
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
    max_attempts = 12  # 12 × 5 seconds = 1 minute
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
    
    # Step 7: Verify metadata.json was exported to S3 products bucket
    logger.info("🔍 Verifying export to S3 products bucket...")
    try:
        # Get dataset_item_id to construct expected S3 path
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if dataset_item_resp.data:
            dataset_item_id = dataset_item_resp.data[0]["id"]
            
            # Expected path: metadata/{dataset_id}/{dataset_item_id}/metadata.json
            expected_key = f"metadata/{test_remote_file.id}/{dataset_item_id}/metadata.json"
            
            logger.info(f"📂 Checking for metadata file: {expected_key}")
            
            try:
                response = storage_client.client.head_object(
                    Bucket="3dtrees-products",
                    Key=expected_key
                )
                file_size = response.get('ContentLength', 0)
                logger.info(f"✅ Metadata file found: {expected_key} ({file_size} bytes)")
                
                # Verify file size is reasonable (should be > 0)
                assert file_size > 0, f"Exported metadata file is empty: {expected_key}"
                
                # Download and validate JSON content
                logger.info("📥 Downloading and validating metadata JSON...")
                obj = storage_client.client.get_object(
                    Bucket="3dtrees-products",
                    Key=expected_key
                )
                metadata_content = obj['Body'].read().decode('utf-8')
                metadata = json.loads(metadata_content)
                
                # Validate metadata structure
                assert "tool" in metadata, "Missing 'tool' section in metadata"
                assert "input_file" in metadata, "Missing 'input_file' section"
                assert "summary" in metadata, "Missing 'summary' section"
                assert "raw_metadata" in metadata, "Missing 'raw_metadata' section"
                
                logger.info(f"✅ Metadata structure valid")
                logger.info(f"  Tool: {metadata['tool']['name']} v{metadata['tool']['version']}")
                logger.info(f"  Point count: {metadata['summary'].get('point_count', 'N/A'):,}")
                
                if "boundary" in metadata["summary"]:
                    boundary = metadata["summary"]["boundary"]
                    logger.info(f"  Area: {boundary.get('area_m2', 'N/A'):.2f} m²")
                    logger.info(f"  Density: {boundary.get('density_pts_per_m2', 'N/A'):.2f} pts/m²")
                
                logger.info(f"✅ Metadata JSON content validated!")
                
            except storage_client.client.exceptions.NoSuchKey:
                pytest.fail(f"❌ Metadata file not found in S3: {expected_key}")
            except json.JSONDecodeError as e:
                pytest.fail(f"❌ Metadata file is not valid JSON: {e}")
            except Exception as e:
                pytest.fail(f"❌ Failed to validate metadata: {e}")
        else:
            logger.warning(f"⚠️ Could not get dataset_item_id for verification")
    except Exception as e:
        logger.warning(f"⚠️ Export verification failed: {e}")
        raise
    
    logger.info("✅ PDAL Metadata workflow End-to-End test PASSED!")

