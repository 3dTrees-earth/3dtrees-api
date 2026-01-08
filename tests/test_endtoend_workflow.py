"""
Test EndToEndPipeline workflow - Complete End-to-End Test

This test verifies the complete production pipeline:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify ALL exported outputs in S3 products bucket across all stages:
   - Collection summary JSON
   - Standardized LAZ (per item)
   - Standardization metadata JSON (per item)
   - Convex hull GeoJSON (per item)
   - Overview images (4 files - top views and section views)
   - Segmented LAZ (per item)
   - 3D Tiles (tileset.json, preview.pnts, points/*.pnts)

Pipeline: CollectionCheck → Standardization → Exports → Overviews → Exports → Tile → SegmentAnyTree → Merge → Export → 3DTiles → Exports

Segmentation is now a 3-step process:
1. Tile (tile_merge in tile mode) - Subsamples and tiles the point cloud
2. SegmentAnyTree - Deep learning tree segmentation on tiles
3. Merge (tile_merge in merge mode) - Merges tiles back to original resolution

This is a comprehensive test (~10-15 minutes) that validates the entire 3DTrees processing pipeline.
Requires GPU for SegmentAnyTree step - will fail if GPU unavailable.
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


def test_endtoend_workflow(
    galaxy_client: GalaxyClient,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    test_remote_file: Dataset
):
    """
    End-to-End test for the complete EndToEndPipeline workflow.
    
    Production-like flow:
    1. Dataset already exists in S3 (raw-storage) and Supabase (via test_remote_file fixture)
    2. Call API endpoint /jobs to start workflow
    3. API imports file from S3 using Galaxy file sources (no download/upload)
    4. Monitor workflow status via status.py poller → updates Supabase DB
    5. Check workflow completion via database (not Galaxy directly)
    6. Verify ALL outputs exported to S3 products bucket across all stages
    
    Pipeline structure (17 jobs total):
    - CollectionCheck (1 tool) → 1 export (collection_summary.json)
    - Standard (1 tool) → 3 exports (LAZ, metadata, convex hull per item)
    - Overviews (1 tool) → 2 exports (top views, section views)
    - Tile → SegmentAnyTree → Merge (3 tools) → 1 export (segmented LAZ)
    - Py3DTiles (1 tool) → 3 exports (tileset.json, preview.pnts, points tiles)
    
    This test simulates the actual production flow where the status.py cronjob
    continuously syncs Galaxy status to the database (~10-15 minutes for full pipeline).
    
    Requires GPU for SegmentAnyTree step - will fail hard if unavailable.
    """
    logger.info("🧪 Testing EndToEndPipeline workflow (Complete End-to-End)")
    
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
        "workflow_name": "EndToEndPipeline",
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
        logger.error(f"API Response Status: {response.status_code}")
        logger.error(f"API Response: {response.text}")
        pytest.fail(f"API call failed: {e}")
    
    # Step 3: Monitor workflow status using status.py poller (production-like)
    logger.info("⏳ Monitoring workflow status via status.py poller...")
    logger.info("⚠️  EndToEndPipeline is comprehensive: 7 tools + 10 exports = 17 jobs total")
    logger.info("⚠️  Tools: CollectionCheck, Standard, Overviews, Tile, SegmentAnyTree, Merge, Py3DTiles")
    logger.info("⚠️  Expected duration: ~10-15 minutes (includes GPU segmentation + 3DTiles conversion)")
    
    max_attempts = 120  # 120 × 5 seconds = 10 minutes max
    workflow_finished = False
    final_status = None
    supabase_inv = None
    expected_jobs = 17  # 7 main tools + 10 export steps
    
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
            
            # Check if any job has errored and no jobs are still running
            # This means the workflow has failed and won't complete
            has_error = job_states.get('error', 0) > 0 or job_states.get('failed', 0) > 0
            has_running = job_states.get('running', 0) > 0 or job_states.get('new', 0) > 0 or job_states.get('queued', 0) > 0
            
            if has_error and not has_running:
                workflow_finished = True
                final_status = 'error'
                logger.info(f"❌ Workflow failed: {job_states.get('error', 0)} error job(s), {job_states.get('paused', 0)} paused")
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
    
    # Step 8: Get dataset_item_id for S3 path construction
    try:
        dataset_item_resp = supabase_client.client.table("dataset_items").select("id").eq("dataset_id", test_remote_file.id).limit(1).execute()
        if not dataset_item_resp.data:
            pytest.fail(f"Could not get dataset_item_id for dataset {test_remote_file.id}")
        dataset_item_id = dataset_item_resp.data[0]["id"]
    except Exception as e:
        pytest.fail(f"Error getting dataset_item_id: {e}")
    
    dataset_id = str(test_remote_file.id)
    
    # Step 9: Verify ALL expected outputs in S3/MinIO products bucket
    logger.info("🔍 Verifying ALL expected outputs across entire pipeline in S3 products bucket...")
    
    missing_outputs = []
    found_outputs = []
    
    # S3 path structure: {dataset_id}/{product_type}/{item_id}.ext
    s3_base_path = f"{dataset_id}/"
    
    # 9.0: Verify Collection Summary (from collection check tool)
    logger.info("\n  📁 Stage 0: Collection Check")
    collection_summary_key = f"{s3_base_path}standard/collection_summary.json"
    try:
        response = storage_client.client.head_object(
            Bucket="3dtrees-products",
            Key=collection_summary_key
        )
        file_size = response.get('ContentLength', 0)
        logger.info(f"    ✅ collection_summary.json: {file_size:,} bytes")
        found_outputs.append('collection_summary.json')
        assert file_size > 0, f"Collection summary is empty"
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('collection_summary.json')
        logger.error(f"    ❌ collection_summary.json: NOT FOUND at {collection_summary_key}")
    except Exception as e:
        missing_outputs.append('collection_summary.json')
        logger.error(f"    ❌ collection_summary.json: ERROR - {e}")
    
    # 9.1: Verify Standardized outputs (per item - uses item_id as filename)
    logger.info("\n  📁 Stage 1: Standardization")
    
    # Check for any .laz file in standard/
    standard_prefix = f"{s3_base_path}standard/"
    try:
        response = storage_client.client.list_objects_v2(
            Bucket="3dtrees-products",
            Prefix=standard_prefix,
            MaxKeys=100
        )
        
        if 'Contents' in response:
            laz_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.laz')]
            json_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.json') and 'collection_summary' not in obj['Key']]
            geojson_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.geojson')]
            
            if laz_files:
                logger.info(f"    ✅ Standardized LAZ: {len(laz_files)} file(s)")
                found_outputs.append('standardized_laz')
            else:
                missing_outputs.append('standardized_laz')
                logger.error(f"    ❌ Standardized LAZ: NOT FOUND")
            
            if json_files:
                logger.info(f"    ✅ Metadata JSON: {len(json_files)} file(s)")
                found_outputs.append('metadata_json')
            else:
                missing_outputs.append('metadata_json')
                logger.error(f"    ❌ Metadata JSON: NOT FOUND")
            
            if geojson_files:
                logger.info(f"    ✅ Convex Hull GeoJSON: {len(geojson_files)} file(s)")
                found_outputs.append('convex_hull')
            else:
                missing_outputs.append('convex_hull')
                logger.error(f"    ❌ Convex Hull GeoJSON: NOT FOUND")
        else:
            missing_outputs.extend(['standardized_laz', 'metadata_json', 'convex_hull'])
            logger.error(f"    ❌ No files found in {standard_prefix}")
    except Exception as e:
        missing_outputs.extend(['standardized_laz', 'metadata_json', 'convex_hull'])
        logger.error(f"    ❌ Error checking standard outputs: {e}")
    
    # 9.2: Verify Overview Images (4 files - top views and section views, no GIF)
    logger.info("\n  📁 Stage 2: Overviews")
    overviews_prefix = f"{s3_base_path}overviews/"
    
    try:
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-products',
            Prefix=overviews_prefix,
            MaxKeys=100
        )
        
        if 'Contents' in response:
            # Look for PNG files (may have double .png.png extension)
            png_files = [obj for obj in response['Contents'] if '.png' in obj['Key']]
            
            top_views = [f for f in png_files if 'top_view' in f['Key']]
            section_views = [f for f in png_files if 'section' in f['Key']]
            
            if top_views:
                logger.info(f"    ✅ Top view images: {len(top_views)} file(s)")
                found_outputs.append('top_views')
            else:
                missing_outputs.append('top_views')
                logger.error(f"    ❌ Top view images: NOT FOUND")
            
            if section_views:
                logger.info(f"    ✅ Section view images: {len(section_views)} file(s)")
                found_outputs.append('section_views')
            else:
                missing_outputs.append('section_views')
                logger.error(f"    ❌ Section view images: NOT FOUND")
        else:
            missing_outputs.extend(['top_views', 'section_views'])
            logger.error(f"    ❌ No files found in {overviews_prefix}")
    except Exception as e:
        missing_outputs.extend(['top_views', 'section_views'])
        logger.error(f"    ❌ Error checking overview outputs: {e}")
    
    # 9.3: Verify Segmented LAZ (uses item_id as filename)
    logger.info("\n  📁 Stage 3: Segmentation")
    segmentation_prefix = f"{s3_base_path}segmentation/"
    
    try:
        response = storage_client.client.list_objects_v2(
            Bucket="3dtrees-products",
            Prefix=segmentation_prefix,
            MaxKeys=100
        )
        
        if 'Contents' in response:
            laz_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.laz')]
            if laz_files:
                total_size = sum(obj.get('Size', 0) for obj in laz_files)
                logger.info(f"    ✅ Segmented LAZ: {len(laz_files)} file(s), {total_size:,} bytes total")
                found_outputs.append('segmented_laz')
            else:
                missing_outputs.append('segmented_laz')
                logger.error(f"    ❌ Segmented LAZ: NOT FOUND")
        else:
            missing_outputs.append('segmented_laz')
            logger.error(f"    ❌ No files found in {segmentation_prefix}")
    except Exception as e:
        missing_outputs.append('segmented_laz')
        logger.error(f"    ❌ Error checking segmentation outputs: {e}")
    
    # 9.4: Verify 3D Tiles outputs
    logger.info("\n  📁 Stage 4: 3D Tiles")
    tiles_base_path = f"{s3_base_path}3dtiles"
    
    # Check tileset.json
    tileset_key = f"{tiles_base_path}/tileset.json"
    try:
        response = storage_client.client.head_object(
            Bucket='3dtrees-products',
            Key=tileset_key
        )
        file_size = response.get('ContentLength', 0)
        logger.info(f"    ✅ tileset.json: {file_size:,} bytes")
        found_outputs.append('tileset.json')
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('tileset.json')
        logger.error(f"    ❌ tileset.json: NOT FOUND at {tileset_key}")
    except Exception as e:
        missing_outputs.append('tileset.json')
        logger.error(f"    ❌ tileset.json: ERROR - {e}")
    
    # Check preview.pnts
    preview_key = f"{tiles_base_path}/preview.pnts"
    try:
        response = storage_client.client.head_object(
            Bucket='3dtrees-products',
            Key=preview_key
        )
        file_size = response.get('ContentLength', 0)
        logger.info(f"    ✅ preview.pnts: {file_size:,} bytes")
        found_outputs.append('preview.pnts')
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('preview.pnts')
        logger.error(f"    ❌ preview.pnts: NOT FOUND at {preview_key}")
    except Exception as e:
        missing_outputs.append('preview.pnts')
        logger.error(f"    ❌ preview.pnts: ERROR - {e}")
    
    # Check for tile files in points/ subdirectory
    try:
        points_path = f"{tiles_base_path}/points/"
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-products',
            Prefix=points_path,
            MaxKeys=1000
        )
        
        if 'Contents' in response:
            tile_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.pnts')]
            if tile_files:
                logger.info(f"    ✅ Tile files: {len(tile_files)} .pnts files in points/")
                found_outputs.append('points_tiles')
            else:
                missing_outputs.append('points_tiles')
                logger.error(f"    ❌ Tile files: NO .pnts files found at {points_path}")
        else:
            missing_outputs.append('points_tiles')
            logger.error(f"    ❌ Tile files: No files found at {points_path}")
    except Exception as e:
        missing_outputs.append('points_tiles')
        logger.error(f"    ❌ Tile files: ERROR checking - {e}")
    
    # Step 10: CRITICAL ASSERTION - All outputs must be present
    # Expected: collection_summary + standard(3) + overviews(2) + segmented + 3dtiles(3) = 10
    total_expected = 10
    if missing_outputs:
        error_msg = (
            f"\n❌ FAILED: Missing {len(missing_outputs)}/{total_expected} expected outputs across pipeline!\n"
            f"   Missing: {missing_outputs}\n"
            f"   Found: {found_outputs}\n"
            f"   This indicates that not all stages of the pipeline completed successfully."
        )
        pytest.fail(error_msg)
    
    logger.info(f"\n✅ All {len(found_outputs)} expected outputs verified across entire pipeline!")
    
    # Step 11: Verify database metadata ingestion (optional - log warnings only)
    logger.info("\n📊 Step 11: Checking database metadata...")
    
    # Check galaxy_histories for outputs
    try:
        history_resp = supabase_client.client.table("galaxy_histories").select("outputs").eq("dataset_id", test_remote_file.id).execute()
        if history_resp.data and history_resp.data[0].get('outputs'):
            outputs = history_resp.data[0]['outputs']
            if 'metadata' in outputs:
                metadata = outputs['metadata']
                if 'collection_summary' in metadata:
                    logger.info("    ✅ collection_summary ingested to database")
                else:
                    logger.warning("    ⚠️ collection_summary not yet ingested")
            else:
                logger.warning("    ⚠️ No metadata in outputs yet")
        else:
            logger.warning("    ⚠️ No outputs in galaxy_histories yet")
    except Exception as e:
        logger.warning(f"    ⚠️ Could not check galaxy_histories: {e}")
    
    logger.info("✅ EndToEndPipeline workflow End-to-End test PASSED!")
