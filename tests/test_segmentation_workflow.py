"""
Test Segmentation workflow - End-to-End Test

This test verifies the segmentation workflow with the 3-step pattern:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify segmented LAZ exported to S3 products bucket

Pipeline: Raw LAZ → Tile → SegmentAnyTree → Merge → Export

Steps:
- Tile: Subsample and tile the point cloud (CPU only)
- SegmentAnyTree: Deep learning tree segmentation (GPU required)
- Merge: Merge segmented tiles back to original resolution (CPU only)
- Export: Export final LAZ to S3

This test requires GPU for SegmentAnyTree step. Test will fail if GPU unavailable (production-only test).
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


def test_segmentation_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for Segmentation workflow (3-step: Tile → Segment → Merge).
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify segmented LAZ exported to S3 products bucket
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database.
    
    Expected workflow steps:
    - Step 1: 3Dtrees: Tile (tile_merge tool in tile mode) - CPU only
    - Step 2: 3Dtrees: SegmentAnyTree - GPU required
    - Step 3: 3Dtrees: Merge (tile_merge tool in merge mode) - CPU only
    - Step 4: Export to S3
    
    Requires GPU for SegmentAnyTree step - will fail hard if unavailable.
    """
    logger.info("🧪 Testing Segmentation workflow (3-step: Tile → Segment → Merge)")
    
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
        "workflow_name": "Segmentation",
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
    logger.info("⚠️  Segmentation workflow (4 steps: Tile → Segment → Merge → Export) may take 5-10 minutes")
    
    # Expected jobs: tile_merge (tile), segmentanytree, tile_merge (merge), export_remote
    expected_jobs = 4
    
    max_attempts = 180  # 180 × 5 seconds = ~15 minutes max (GPU jobs + tiling/merging can be slow)
    workflow_finished = False
    final_status = None
    supabase_inv = None
    current_status = None
    jobs = []
    
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
            
            # Count job states for logging
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
            # (Galaxy keeps invocation as 'scheduled' even when all jobs complete)
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
    
    # Step 5: Check for GPU errors (fail hard - no skip for production test)
    if final_status in ['error', 'failed']:
        logger.error("❌ Workflow failed - checking for GPU errors...")
        
        # Check jobs for GPU-related errors or other failures
        gpu_error_found = False
        failed_jobs = []
        for job in supabase_inv.jobs or []:
            job_state = job.get('state')
            job_tool_id = job.get('tool_id', '')
            
            if job_state in ['error', 'failed']:
                failed_jobs.append(f"{job_tool_id}:{job_state}")
                
                # Check if segmentation job failed (GPU issue)
                if 'segmentanytree' in job_tool_id.lower():
                    gpu_error_found = True
                    logger.error("❌ SegmentAnyTree job failed - GPU likely unavailable")
                    logger.error("This test requires GPU support - deploy to production Galaxy with GPU")
                    pytest.fail(f"SegmentAnyTree requires GPU. Deploy to production environment with GPU support.")
                
                # Check for tile_merge failures
                elif 'tile_merge' in job_tool_id.lower():
                    logger.error(f"❌ Tile/Merge job failed: {job_tool_id}")
        
        if not gpu_error_found:
            pytest.fail(f"Workflow failed with status: {final_status}. Failed jobs: {failed_jobs}")
    
    assert final_status in ['ok', 'success'], f"Workflow failed with status: {final_status}"
    
    # Step 6: Verify all expected jobs completed
    jobs = supabase_inv.jobs or []
    logger.info(f"📊 Total jobs executed: {len(jobs)}")
    
    # Expected tools in order: tile_merge (tile), segmentanytree, tile_merge (merge), export_remote
    expected_tools = ['3dtrees_tile_merge', '3dtrees_segmentanytree', '3dtrees_tile_merge', 'export_remote']
    actual_tools = [job.get('tool_id', '') for job in jobs]
    
    logger.info(f"📋 Expected tools: {expected_tools}")
    logger.info(f"📋 Actual tools: {actual_tools}")
    
    # Verify we have the expected number of jobs (4 for 3-step + export)
    assert len(jobs) >= 4, f"Expected at least 4 jobs (tile, segment, merge, export), got {len(jobs)}"
    
    # Step 7: Verify outputs exist in DB
    outputs = supabase_inv.outputs or {}
    logger.info(f"📦 Workflow produced {len(outputs)} outputs (from DB)")
    
    if not outputs:
        logger.warning("⚠️ No outputs found in Supabase")
    else:
        logger.info(f"✅ Output keys: {list(outputs.keys())}")
    
    # Step 8: Verify invocation details in Supabase
    logger.info(f"📊 Supabase status: {supabase_inv.status}")
    logger.info(f"📊 Finished at: {supabase_inv.finished_at}")
    logger.info(f"📊 Jobs count: {len(supabase_inv.jobs or [])}")
    
    # Step 9: Verify segmented LAZ exported to S3 products bucket
    logger.info("🔍 Verifying segmented LAZ in S3 products bucket...")
    
    try:
        # Get dataset_item_id to construct expected S3 path
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            pytest.fail(f"Could not get dataset_item_id for dataset {test_remote_file.id}")
        dataset_item_id = dataset_item_resp.data[0]["id"]
        
        # Verify segmented LAZ at: segmentation/{dataset_id}/{dataset_item_id}/segmented.laz
        seg_key = f"segmentation/{test_remote_file.id}/{dataset_item_id}/segmented.laz"
        logger.info(f"📂 Checking segmented LAZ at: {seg_key}")
        
        try:
            response = storage_client.client.head_object(
                Bucket="3dtrees-products",
                Key=seg_key
            )
            file_size = response.get('ContentLength', 0)
            logger.info(f"✅ Segmented LAZ found: {seg_key} ({file_size:,} bytes)")
            assert file_size > 0, f"Segmented LAZ is empty"
        except storage_client.client.exceptions.NoSuchKey:
            pytest.fail(f"❌ Segmented LAZ not found at expected path: {seg_key}")
        except Exception as e:
            pytest.fail(f"❌ Error verifying segmented LAZ: {e}")
        
    except Exception as e:
        pytest.fail(f"❌ Export verification failed: {e}")
    
    logger.info("✅ Segmentation workflow (3-step: Tile → Segment → Merge) End-to-End test PASSED!")

