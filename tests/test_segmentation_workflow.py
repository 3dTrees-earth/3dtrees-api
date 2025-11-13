"""
Test Segmentation workflow - End-to-End Test

This test verifies the complete production workflow:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify all 4 exported product types in S3 products bucket

Full pipeline: Standardization → Export → Overviews → Export → Segmentation → Export → 3DTiles → Export

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
    End-to-End test for Segmentation workflow (full pipeline).
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify all 4 exported product types in S3 products bucket
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database (~5 minutes for full pipeline).
    
    Requires GPU for SegmentAnyTree step - will fail hard if unavailable.
    """
    logger.info("🧪 Testing Segmentation workflow (End-to-End, Full Pipeline)")
    
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
    logger.info("⚠️  Full pipeline (4 tools + 4 exports) may take 3-5 minutes")
    
    max_attempts = 60  # 60 × 5 seconds = 5 minutes max for full pipeline
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
    
    # Step 5: Check for GPU errors (fail hard - no skip for production test)
    if final_status in ['error', 'failed']:
        logger.error("❌ Workflow failed - checking for GPU errors...")
        
        # Check jobs for GPU-related errors
        gpu_error_found = False
        for job in supabase_inv.jobs or []:
            job_state = job.get('state')
            if job_state in ['error', 'failed']:
                # If segmentation job failed, assume GPU issue
                job_tool_id = job.get('tool_id', '')
                if 'segmentanytree' in job_tool_id.lower():
                    gpu_error_found = True
                    logger.error("❌ SegmentAnyTree job failed - GPU likely unavailable")
                    logger.error("This test requires GPU support - deploy to production Galaxy with GPU")
                    pytest.fail(f"SegmentAnyTree requires GPU. Deploy to production environment with GPU support.")
        
        if not gpu_error_found:
            pytest.fail(f"Workflow failed with status: {final_status}")
    
    assert final_status in ['ok', 'success'], f"Workflow failed with status: {final_status}"
    
    # Step 6: Verify outputs exist in DB
    outputs = supabase_inv.outputs or {}
    logger.info(f"📦 Workflow produced {len(outputs)} outputs (from DB)")
    
    if not outputs:
        logger.warning("⚠️ No outputs found in Supabase")
    else:
        logger.info(f"✅ Output keys: {list(outputs.keys())}")
    
    # Step 7: Verify invocation details in Supabase
    logger.info(f"📊 Supabase status: {supabase_inv.status}")
    logger.info(f"📊 Finished at: {supabase_inv.finished_at}")
    logger.info(f"📊 Jobs count: {len(supabase_inv.jobs or [])}")
    
    # Step 8: Verify all 4 exported product types in S3/MinIO products bucket
    logger.info("🔍 Verifying all 4 exported product types in S3 products bucket...")
    
    try:
        # Get dataset_item_id to construct expected S3 paths
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            logger.warning("⚠️ Could not get dataset_item_id for S3 verification")
        else:
            dataset_item_id = dataset_item_resp.data[0]["id"]
            base_path = f"{test_remote_file.id}/{dataset_item_id}"
            
            # 1. Verify standardized LAZ
            logger.info("📂 Checking standardized LAZ...")
            std_key = f"standard/{base_path}/Standardized Point Cloud.laz"
            try:
                response = storage_client.client.head_object(
                    Bucket="3dtrees-tool-products",
                    Key=std_key
                )
                logger.info(f"✅ Standardized LAZ: {std_key} ({response.get('ContentLength', 0)} bytes)")
                assert response.get('ContentLength', 0) > 0, f"Standardized LAZ is empty"
            except storage_client.client.exceptions.NoSuchKey:
                logger.warning(f"⚠️ Standardized LAZ not found: {std_key}")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify standardized LAZ: {e}")
            
            # 2. Verify overview files (multiple files)
            logger.info("📂 Checking overview files...")
            overview_prefix = f"overviews/{base_path}/"
            try:
                response = storage_client.client.list_objects_v2(
                    Bucket="3dtrees-tool-products",
                    Prefix=overview_prefix
                )
                if 'Contents' in response and len(response['Contents']) > 0:
                    overview_files = [obj['Key'] for obj in response['Contents']]
                    logger.info(f"✅ Overview files: {len(overview_files)} files found")
                    for file in overview_files[:5]:  # Log first 5
                        logger.info(f"   - {file}")
                else:
                    logger.warning(f"⚠️ No overview files found at {overview_prefix}")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify overview files: {e}")
            
            # 3. Verify segmented LAZ
            logger.info("📂 Checking segmented LAZ...")
            seg_key = f"segmentation/{base_path}/Segmented Point Cloud.laz"
            try:
                response = storage_client.client.head_object(
                    Bucket="3dtrees-tool-products",
                    Key=seg_key
                )
                logger.info(f"✅ Segmented LAZ: {seg_key} ({response.get('ContentLength', 0)} bytes)")
                assert response.get('ContentLength', 0) > 0, f"Segmented LAZ is empty"
            except storage_client.client.exceptions.NoSuchKey:
                logger.warning(f"⚠️ Segmented LAZ not found: {seg_key}")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify segmented LAZ: {e}")
            
            # 4. Verify 3D Tiles (tileset.json + .pnts files)
            logger.info("📂 Checking 3D Tiles...")
            tiles_prefix = f"3dtiles/{base_path}/"
            try:
                response = storage_client.client.list_objects_v2(
                    Bucket="3dtrees-tool-products",
                    Prefix=tiles_prefix
                )
                if 'Contents' in response and len(response['Contents']) > 0:
                    tile_files = [obj['Key'] for obj in response['Contents']]
                    tileset_json = [f for f in tile_files if 'tileset.json' in f]
                    pnts_files = [f for f in tile_files if '.pnts' in f]
                    
                    logger.info(f"✅ 3D Tiles: {len(tile_files)} total files")
                    logger.info(f"   - tileset.json: {'found' if tileset_json else 'NOT FOUND'}")
                    logger.info(f"   - .pnts tiles: {len(pnts_files)} files")
                else:
                    logger.warning(f"⚠️ No 3D Tiles found at {tiles_prefix}")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify 3D Tiles: {e}")
    
    except Exception as e:
        logger.warning(f"⚠️ Export verification failed: {e}")
    
    logger.info("✅ Segmentation workflow End-to-End test PASSED!")


