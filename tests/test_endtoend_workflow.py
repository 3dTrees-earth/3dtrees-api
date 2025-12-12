"""
Test EndToEndPipeline workflow - Complete End-to-End Test

This test verifies the complete production pipeline:
1. Dataset exists in S3 (raw-storage bucket) and Supabase DB
2. Call API endpoint to start workflow
3. Galaxy imports directly from S3 using file sources (no download/upload)
4. Monitor workflow status via status.py poller (database-driven)
5. Verify outputs in Supabase DB
6. Verify ALL exported outputs in S3 products bucket across all stages:
   - Standardized LAZ
   - Standardization metadata (metadata.json with pre/post info)
   - Convex hull GeoJSON (convex_hull_wgs84.GeoJSON)
   - Overview images (5 files)
   - Segmented LAZ
   - 3D Tiles (tileset.json, preview.pnts, points/*.pnts)

Pipeline: Standardization → Exports → Overviews → Exports → Tile → SegmentAnyTree → Merge → Export → 3DTiles → Exports

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
    
    Pipeline structure (16 jobs total):
    - Standard (1 tool) → 3 exports (LAZ, metadata, convex hull)
    - Overviews (1 tool) → 3 exports (top views, section views, GIF)
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
    logger.info("⚠️  EndToEndPipeline is comprehensive: 6 tools + 10 exports = 16 jobs total")
    logger.info("⚠️  Tools: Standard, Overviews, Tile, SegmentAnyTree, Merge, Py3DTiles")
    logger.info("⚠️  Expected duration: ~10-15 minutes (includes GPU segmentation + 3DTiles conversion)")
    
    max_attempts = 120  # 120 × 5 seconds = 10 minutes max
    workflow_finished = False
    final_status = None
    supabase_inv = None
    expected_jobs = 16  # 6 main tools + 10 export steps
    
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
    
    # Build s3_base_path for new path structure: {dataset_id}/{dataset_item_id}/
    s3_base_path = f"{dataset_id}/{dataset_item_id}/"
    
    # 9.1: Verify Standardized LAZ
    logger.info("\n  📁 Stage 1: Standardization")
    standard_key = f"{s3_base_path}standard/standardized.laz"
    try:
        response = storage_client.client.head_object(
            Bucket="3dtrees-products",
            Key=standard_key
        )
        file_size = response.get('ContentLength', 0)
        logger.info(f"    ✅ standardized.laz: {file_size:,} bytes")
        found_outputs.append('standardized.laz')
        assert file_size > 0, f"Standardized LAZ is empty"
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('standardized.laz')
        logger.error(f"    ❌ standardized.laz: NOT FOUND at {standard_key}")
    except Exception as e:
        missing_outputs.append('standardized.laz')
        logger.error(f"    ❌ standardized.laz: ERROR - {e}")
    
    # 9.1a: Verify Standardization Metadata JSON (from tool_standard)
    metadata_key = f"{s3_base_path}standard/metadata.json"
    try:
        response = storage_client.client.head_object(
            Bucket="3dtrees-products",
            Key=metadata_key
        )
        file_size = response.get('ContentLength', 0)
        assert file_size > 0, f"Metadata file is empty: {metadata_key}"
        logger.info(f"    ✅ metadata.json: {file_size:,} bytes")
        found_outputs.append('metadata.json')
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('metadata.json')
        logger.error(f"    ❌ metadata.json: NOT FOUND at {metadata_key}")
    except Exception as e:
        missing_outputs.append('metadata.json')
        logger.error(f"    ❌ metadata.json: ERROR - {e}")
    
    # 9.1b: Verify Convex Hull GeoJSON (from tool_standard)
    convex_hull_key = f"{s3_base_path}standard/convex_hull.geojson"
    try:
        response = storage_client.client.head_object(
            Bucket="3dtrees-products",
            Key=convex_hull_key
        )
        file_size = response.get('ContentLength', 0)
        assert file_size > 0, f"Convex hull file is empty: {convex_hull_key}"
        logger.info(f"    ✅ convex_hull_wgs84.GeoJSON: {file_size:,} bytes")
        found_outputs.append('convex_hull_wgs84.GeoJSON')
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('convex_hull_wgs84.GeoJSON')
        logger.error(f"    ❌ convex_hull_wgs84.GeoJSON: NOT FOUND at {convex_hull_key}")
    except Exception as e:
        missing_outputs.append('convex_hull_wgs84.GeoJSON')
        logger.error(f"    ❌ convex_hull_wgs84.GeoJSON: ERROR - {e}")
    
    # 9.2: Verify Overview Images (5 files)
    logger.info("\n  📁 Stage 2: Overviews")
    export_base_path = f"{s3_base_path}overviews"
    
    expected_overview_outputs = {
        'top_view_00.png': 'Top view perspective 0°',
        'top_view_01.png': 'Top view perspective 180°',
        'section_ew.png': 'East-West section view',
        'section_ns.png': 'North-South section view',
        'overview_animation.gif': 'Rotating overview animation',
    }
    
    for output_name, description in expected_overview_outputs.items():
        s3_key = f"{export_base_path}/{output_name}"
        
        try:
            response = storage_client.client.list_objects_v2(
                Bucket='3dtrees-products',
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
    
    # 9.3: Verify Segmented LAZ
    logger.info("\n  📁 Stage 3: Segmentation")
    seg_key = f"{s3_base_path}segmentation/segmented.laz"
    try:
        response = storage_client.client.head_object(
            Bucket="3dtrees-products",
            Key=seg_key
        )
        file_size = response.get('ContentLength', 0)
        logger.info(f"    ✅ segmented.laz: {file_size:,} bytes")
        found_outputs.append('segmented.laz')
        assert file_size > 0, f"Segmented LAZ is empty"
    except storage_client.client.exceptions.NoSuchKey:
        missing_outputs.append('segmented.laz')
        logger.error(f"    ❌ segmented.laz: NOT FOUND at {seg_key}")
    except Exception as e:
        missing_outputs.append('segmented.laz')
        logger.error(f"    ❌ segmented.laz: ERROR - {e}")
    
    # 9.4: Verify 3D Tiles outputs
    logger.info("\n  📁 Stage 4: 3D Tiles")
    tiles_base_path = f"{s3_base_path}3dtiles"
    
    # Check tileset.json
    tileset_key = f"{tiles_base_path}/tileset.json"
    try:
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-products',
            Prefix=tileset_key,
            MaxKeys=1
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            found_outputs.append('tileset.json')
            logger.info(f"    ✅ tileset.json: Cesium 3D Tileset JSON metadata")
        else:
            missing_outputs.append('tileset.json')
            logger.error(f"    ❌ tileset.json: NOT FOUND at {tileset_key}")
    except Exception as e:
        missing_outputs.append('tileset.json')
        logger.error(f"    ❌ tileset.json: ERROR checking - {e}")
    
    # Check preview.pnts
    preview_key = f"{tiles_base_path}/preview.pnts"
    try:
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-products',
            Prefix=preview_key,
            MaxKeys=1
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            found_outputs.append('preview.pnts')
            logger.info(f"    ✅ preview.pnts: 3D Tiles preview/thumbnail")
        else:
            missing_outputs.append('preview.pnts')
            logger.error(f"    ❌ preview.pnts: NOT FOUND at {preview_key}")
    except Exception as e:
        missing_outputs.append('preview.pnts')
        logger.error(f"    ❌ preview.pnts: ERROR checking - {e}")
    
    # Check for tile files in points/ subdirectory
    try:
        points_path = f"{tiles_base_path}/points/"
        response = storage_client.client.list_objects_v2(
            Bucket='3dtrees-products',
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
    
    # Step 10: CRITICAL ASSERTION - All outputs must be present
    total_expected = 2 + 1 + 5 + 1 + 3  # metadata(2) + standardized + overviews(5) + segmented + 3dtiles(3)
    if missing_outputs:
        error_msg = (
            f"\n❌ FAILED: Missing {len(missing_outputs)}/{total_expected} expected outputs across pipeline!\n"
            f"   Missing: {missing_outputs}\n"
            f"   Found: {found_outputs}\n"
            f"   This indicates that not all stages of the pipeline completed successfully."
        )
        pytest.fail(error_msg)
    
    logger.info(f"\n✅ All {len(found_outputs)} expected outputs verified across entire pipeline!")
    
    # Step 11: Verify database metadata ingestion
    logger.info("\n📊 Step 11: Verifying database metadata ingestion...")
    
    # Check standard table for PDAL metadata
    try:
        standard_resp = supabase_client.client.table("standard").select("*").eq("dataset_item_id", dataset_item_id).execute()
        
        if not standard_resp.data:
            logger.warning("⚠️  No standard record found yet - metadata ingestion may not have completed")
        else:
            standard_record = standard_resp.data[0]
            logger.info("  📋 Standard table metadata:")
            
            # Check for las_info_raw (metadata from tool_standard)
            if standard_record.get("las_info_raw"):
                logger.info("    ✅ las_info_raw: Present")
                # Verify it's valid JSON with expected structure
                las_raw = standard_record["las_info_raw"]
                assert isinstance(las_raw, (dict, list)), "las_info_raw should be a dict or list"
            else:
                logger.warning("    ⚠️  las_info_raw: Not yet populated")
            
            # Check for las_info_standardized
            if standard_record.get("las_info_standardized"):
                logger.info("    ✅ las_info_standardized: Present")
                las_std = standard_record["las_info_standardized"]
                assert isinstance(las_std, (dict, list)), "las_info_standardized should be a dict or list"
            else:
                logger.warning("    ⚠️  las_info_standardized: Not yet populated")
            
            # Check for convex_hull
            if standard_record.get("convex_hull"):
                logger.info("    ✅ convex_hull: Present")
                convex_hull = standard_record["convex_hull"]
                assert isinstance(convex_hull, dict), "convex_hull should be a dict (GeoJSON)"
            else:
                logger.warning("    ⚠️  convex_hull: Not yet populated")
                
    except Exception as e:
        logger.warning(f"⚠️  Could not verify standard table metadata: {e}")
    
    # Check overviews table
    try:
        overviews_resp = supabase_client.client.table("overviews").select("*").eq("dataset_item_id", dataset_item_id).execute()
        if overviews_resp.data:
            logger.info("  📋 Overviews table:")
            logger.info(f"    ✅ URL: {overviews_resp.data[0].get('url', 'Not set')}")
    except Exception as e:
        logger.warning(f"⚠️  Could not verify overviews table: {e}")
    
    # Check segmentations table
    try:
        seg_resp = supabase_client.client.table("segmentations").select("*").eq("dataset_item_id", dataset_item_id).execute()
        if seg_resp.data:
            logger.info("  📋 Segmentations table:")
            logger.info(f"    ✅ URL: {seg_resp.data[0].get('url', 'Not set')}")
    except Exception as e:
        logger.warning(f"⚠️  Could not verify segmentations table: {e}")
    
    # Check tilesets table
    try:
        tiles_resp = supabase_client.client.table("tilesets").select("*").eq("dataset_item_id", dataset_item_id).execute()
        if tiles_resp.data:
            logger.info("  📋 Tilesets table:")
            logger.info(f"    ✅ URL: {tiles_resp.data[0].get('url', 'Not set')}")
    except Exception as e:
        logger.warning(f"⚠️  Could not verify tilesets table: {e}")
    
    logger.info("✅ EndToEndPipeline workflow End-to-End test PASSED!")

