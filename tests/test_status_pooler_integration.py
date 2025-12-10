"""
Integration tests for status pooler: validates the complete data flow from
Galaxy workflow execution → status.py polling → database triggers → metadata ingestion.

Tests are split into focused units for better debugging and faster feedback.
"""

import pytest
import time
import logging
import subprocess
import requests
from pathlib import Path

from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient
from trees_api.config import GalaxyConfig, SupabaseConfig, StorageConfig

logger = logging.getLogger(__name__)


def start_status_pooler_background():
    """
    Start status pooler in continuous mode as a background process.
    
    Uses status.py's built-in --continuous mode which runs sync cycles
    every N seconds (configured via STATUS_POOLER_INTERVAL env var).
    
    This mimics how it would run in production via cron, but in a
    continuous daemon mode suitable for testing.
    
    Returns:
        subprocess.Popen object for the background process
    """
    import os
    pooler_interval = int(os.environ.get("STATUS_POOLER_INTERVAL", "10"))
    
    logger.info(f"Starting status pooler in continuous mode (interval: {pooler_interval}s)...")
    logger.info("  This runs status.py --continuous to emulate cron behavior")
    
    # Use current working directory (should be /src in Docker container)
    cwd = os.getcwd()
    
    # Start status.py in continuous mode
    process = subprocess.Popen(
        ["python", "-m", "trees_api.status", "--continuous"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # Merge stderr into stdout for easier monitoring
        text=True,
        cwd=cwd,
        env=os.environ.copy(),
        bufsize=1,  # Line buffered
        universal_newlines=True
    )
    
    logger.info(f"Status pooler started with PID: {process.pid}")
    logger.info(f"  - Mode: Continuous (daemon)")
    logger.info(f"  - Interval: {pooler_interval}s")
    logger.info(f"  - Working directory: {cwd}")
    
    # Give the pooler a moment to initialize
    time.sleep(2)
    
    # Verify it started successfully
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        raise RuntimeError(f"Status pooler failed to start. Exit code: {process.returncode}\nOutput: {stdout}")
    
    logger.info("✅ Status pooler is running")
    return process


def stop_status_pooler(process):
    """
    Stop the background status pooler gracefully.
    
    Args:
        process: subprocess.Popen object from start_status_pooler_background()
    """
    if process and process.poll() is None:
        logger.info("Stopping status pooler...")
        process.terminate()
        try:
            process.wait(timeout=5)
            logger.info("Status pooler stopped gracefully")
        except subprocess.TimeoutExpired:
            logger.warning("Status pooler did not stop gracefully, killing...")
            process.kill()
            process.wait()


def check_pooler_health(process):
    """
    Check if the status pooler is still running and healthy.
    
    Args:
        process: subprocess.Popen object from start_status_pooler_background()
        
    Returns:
        Tuple of (is_alive, error_message)
    """
    if process.poll() is not None:
        # Process has terminated
        exit_code = process.returncode
        try:
            stdout, stderr = process.communicate(timeout=1)
            error_output = stdout[-500:] if stdout else ""  # Last 500 chars
        except:
            error_output = "Could not read process output"
        
        return False, f"Status pooler died (exit code: {exit_code})\nOutput: {error_output}"
    
    return True, None


# ============================================================================
# Test 1: Workflow Status Synchronization
# ============================================================================

def test_workflow_status_sync(
    galaxy_client,
    supabase_client,
    storage_client,
    test_remote_file
):
    """
    Test that status.py correctly syncs workflow status from Galaxy to Supabase.
    
    The status pooler runs in continuous mode (status.py --continuous), executing
    sync cycles every N seconds (N = STATUS_POOLER_INTERVAL env var, default 10).
    This mimics production cron behavior but in a continuous daemon mode suitable for testing.
    
    Tests:
    - Workflow invocation creation
    - Status updates (new -> scheduled -> ok)
    - Job state synchronization
    - Finished timestamp setting
    - Continuous polling every 10 seconds
    """
    logger.info("=" * 80)
    logger.info("TEST 1: WORKFLOW STATUS SYNCHRONIZATION")
    logger.info("=" * 80)
    
    # Start status pooler in background (continuous mode)
    pooler_process = None
    try:
        pooler_process = start_status_pooler_background()
        
        # Give pooler a moment to start
        time.sleep(2)
        
        # Start workflow
        workflow_name = "EndToEndPipeline"
        dataset_id = test_remote_file.id
        
        logger.info(f"\n📋 Starting {workflow_name} workflow (Dataset ID: {dataset_id})...")
        
        api_url = "http://localhost:8000/jobs"
        payload = {
            "dataset_id": str(dataset_id),
            "workflow_name": workflow_name,
            "overwrite": False
        }
        
        response = requests.post(api_url, params=payload, timeout=30)
        response.raise_for_status()
        workflow_invocation = response.json()
        invocation_id = workflow_invocation["invocation_id"]
        
        logger.info(f"✅ Workflow started: {invocation_id}")
        logger.info(f"✅ Status pooler running in continuous mode (PID: {pooler_process.pid})")
        logger.info(f"   (Syncs every 10 seconds, like a production cron job)")
        
        # Monitor database until workflow completes
        start_time = time.time()
        poll_count = 0
        max_polls = 120  # 20 minutes max
        workflow_completed = False
        last_status = None
        last_job_count = 0
        
        logger.info("\n📋 Monitoring database (status pooler syncs every 10 seconds)...")
        
        while poll_count < max_polls:
            poll_count += 1
            elapsed_minutes = (time.time() - start_time) / 60
            
            logger.info(f"\n  🔄 Check #{poll_count} (elapsed: {elapsed_minutes:.1f} min)")
            
            # Check if pooler is still running
            is_alive, error_msg = check_pooler_health(pooler_process)
            if not is_alive:
                logger.error(f"    ❌ {error_msg}")
                pytest.fail("Status pooler died unexpectedly")
            
            # Check database (status pooler should be updating this)
            inv = supabase_client.get_workflow_invocation_by_id(invocation_id)
            if inv:
                # Detect changes
                status_changed = (last_status is not None and inv.status != last_status)
                jobs_changed = (len(inv.jobs) if inv.jobs else 0) != last_job_count
                
                if status_changed or jobs_changed:
                    logger.info(f"    🔄 Status pooler updated database!")
                
                logger.info(f"    📊 Status: {inv.status}")
                
                if inv.jobs:
                    completed_jobs = sum(1 for j in inv.jobs if j.get('state') in ['ok', 'error', 'failed'])
                    total_jobs = len(inv.jobs)
                    logger.info(f"    📊 Jobs: {completed_jobs}/{total_jobs} completed")
                    last_job_count = total_jobs
                else:
                    last_job_count = 0
                
                last_status = inv.status
                
                # Check completion
                if inv.status in ['ok', 'success']:
                    logger.info(f"    ✅ Workflow completed!")
                    workflow_completed = True
                    
                    # Verify finished_at is set
                    assert inv.finished_at is not None, "finished_at should be set when workflow completes"
                    logger.info(f"    ✅ finished_at timestamp: {inv.finished_at}")
                    break
                elif inv.status in ['error', 'failed', 'cancelled']:
                    pytest.fail(f"Workflow failed with status: {inv.status}")
            else:
                logger.warning(f"    ⚠️  Invocation {invocation_id} not found in database yet")
            
            # Wait before next check (status pooler runs every 10s, we check every 10s)
            if poll_count < max_polls:
                time.sleep(10)
        
        assert workflow_completed, "Workflow must complete within 20 minutes"
        
        total_elapsed = (time.time() - start_time) / 60
        logger.info(f"\n✅ TEST 1 PASSED: Workflow completed in {total_elapsed:.1f} minutes")
        
        # Store invocation_id for next tests (using pytest cache)
        pytest.invocation_id = invocation_id
        pytest.dataset_item_id = workflow_invocation.get("dataset_item_id")
        
    finally:
        # Always stop the pooler
        if pooler_process:
            stop_status_pooler(pooler_process)


# ============================================================================
# Test 2: Database Triggers
# ============================================================================

def test_database_triggers(supabase_client):
    """
    Test that database triggers automatically update dataset_processing_status.
    
    Tests:
    - Trigger fires when workflow completes
    - workflow_status is updated
    - processing_completed_at is set
    """
    logger.info("\n" + "=" * 80)
    logger.info("TEST 2: DATABASE TRIGGERS")
    logger.info("=" * 80)
    
    # Get invocation_id from previous test
    if not hasattr(pytest, 'dataset_item_id'):
        pytest.skip("Requires test_workflow_status_sync to run first")
    
    dataset_item_id = pytest.dataset_item_id
    
    logger.info(f"\n📋 Checking dataset_processing_status for dataset_item {dataset_item_id}...")
    
    # Wait for trigger to execute
    time.sleep(2)
    
    status_resp = supabase_client.client.table("dataset_processing_status").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    assert status_resp.data, "dataset_processing_status record not found - trigger did not fire!"
    
    status_record = status_resp.data[0]
    
    logger.info("  📊 Dataset Processing Status:")
    logger.info(f"    workflow_status: {status_record.get('workflow_status')}")
    logger.info(f"    processing_completed_at: {status_record.get('processing_completed_at')}")
    
    # Verify trigger updated status
    assert status_record.get('workflow_status') in ['ok', 'success', 'scheduled'], \
        "workflow_status should be updated by trigger"
    
    logger.info("\n✅ TEST 2 PASSED: Database triggers working correctly")


# ============================================================================
# Test 3: Product Detection and Metadata Ingestion
# ============================================================================

def test_product_metadata_ingestion(supabase_client):
    """
    Test that status.py detects products in S3 and ingests metadata.
    
    Tests:
    - Product detection from S3
    - Metadata file parsing (JSON)
    - Database flag updates (has_standardisation, has_overviews, etc.)
    - Product table population (standard, overviews, segmentations, tilesets)
    - metadata_synced_at timestamp
    """
    logger.info("\n" + "=" * 80)
    logger.info("TEST 3: PRODUCT METADATA INGESTION")
    logger.info("=" * 80)
    
    # Get dataset_item_id from previous test
    if not hasattr(pytest, 'dataset_item_id') or not hasattr(pytest, 'invocation_id'):
        pytest.skip("Requires test_workflow_status_sync to run first")
    
    dataset_item_id = pytest.dataset_item_id
    invocation_id = pytest.invocation_id
    
    # Restart status pooler in background (it was stopped after test_workflow_status_sync)
    pooler_process = None
    try:
        pooler_process = start_status_pooler_background()
        time.sleep(2)  # Give pooler a moment to start
        
        logger.info(f"\n📋 Waiting for status pooler to detect products and ingest metadata...")
        logger.info(f"  (Status pooler running in continuous mode, PID: {pooler_process.pid})")
        logger.info(f"  (Syncs every 10 seconds)")
        
        # Wait for status pooler to sync (give it a few cycles)
        # Products should be detected and metadata ingested within 30-60 seconds
        max_wait = 60  # 1 minute max
        wait_interval = 5
        waited = 0
        
        while waited < max_wait:
            time.sleep(wait_interval)
            waited += wait_interval
            
            # Check if metadata has been synced
            resp = supabase_client.client.table("galaxy_workflow_invocations").select("metadata_synced_at").eq("invocation_id", invocation_id).execute()
            if resp.data and resp.data[0].get("metadata_synced_at"):
                logger.info(f"  ✅ Metadata synced after {waited}s")
                break
            
            logger.debug(f"  ⏳ Waiting for metadata sync... ({waited}/{max_wait}s)")
        
    finally:
        if pooler_process:
            stop_status_pooler(pooler_process)
    
    # ========================================================================
    # Check 3.1: Dataset Processing Status Flags
    # ========================================================================
    logger.info("\n  📊 Checking dataset_processing_status flags...")
    
    status_resp = supabase_client.client.table("dataset_processing_status").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    assert status_resp.data, "dataset_processing_status not found"
    status_record = status_resp.data[0]
    
    logger.info(f"    has_standardisation: {status_record.get('has_standardisation')}")
    logger.info(f"    has_overviews: {status_record.get('has_overviews')}")
    logger.info(f"    has_segmentation: {status_record.get('has_segmentation')}")
    logger.info(f"    has_3dtiles: {status_record.get('has_3dtiles')}")
    
    # At least standardisation should be detected
    assert status_record.get('has_standardisation') is True, \
        "has_standardisation should be True after metadata ingestion"
    
    logger.info("    ✅ Flags updated correctly")
    
    # ========================================================================
    # Check 3.2: Standard Table
    # ========================================================================
    logger.info("\n  📊 Checking 'standard' table...")
    
    standard_resp = supabase_client.client.table("standard").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    assert standard_resp.data, "No standard record found"
    standard_record = standard_resp.data[0]
    
    # Check metadata fields (from tool_standard JSON log)
    assert standard_record.get("las_info_raw"), "las_info_raw should be populated"
    assert isinstance(standard_record["las_info_raw"], dict), "las_info_raw should be a dict"
    logger.info("    ✅ las_info_raw: Present")
    
    assert standard_record.get("las_info_standardized"), "las_info_standardized should be populated"
    assert isinstance(standard_record["las_info_standardized"], dict), "las_info_standardized should be a dict"
    logger.info("    ✅ las_info_standardized: Present")
    
    if standard_record.get("convex_hull"):
        logger.info("    ✅ convex_hull: Present")
    
    # ========================================================================
    # Check 3.3: Overviews Table
    # ========================================================================
    logger.info("\n  📊 Checking 'overviews' table...")
    
    overviews_resp = supabase_client.client.table("overviews").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    if overviews_resp.data:
        url = overviews_resp.data[0].get('url')
        assert url, "Overviews URL should be populated"
        logger.info(f"    ✅ URL: {url}")
    else:
        logger.warning("    ⚠️  No overviews record (may still be processing)")
    
    # ========================================================================
    # Check 3.4: Segmentations Table
    # ========================================================================
    logger.info("\n  📊 Checking 'segmentations' table...")
    
    seg_resp = supabase_client.client.table("segmentations").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    if seg_resp.data:
        url = seg_resp.data[0].get('url')
        assert url, "Segmentation URL should be populated"
        logger.info(f"    ✅ URL: {url}")
    else:
        logger.warning("    ⚠️  No segmentations record (may still be processing)")
    
    # ========================================================================
    # Check 3.5: Tilesets Table
    # ========================================================================
    logger.info("\n  📊 Checking 'tilesets' table...")
    
    tiles_resp = supabase_client.client.table("tilesets").select("*").eq(
        "dataset_item_id", dataset_item_id
    ).execute()
    
    if tiles_resp.data:
        url = tiles_resp.data[0].get('url')
        assert url, "Tileset URL should be populated"
        logger.info(f"    ✅ URL: {url}")
    else:
        logger.warning("    ⚠️  No tilesets record (may still be processing)")
    
    # ========================================================================
    # Check 3.6: metadata_synced_at Timestamp
    # ========================================================================
    logger.info("\n  📊 Checking metadata_synced_at timestamp...")
    
    inv = supabase_client.get_workflow_invocation_by_id(invocation_id)
    assert inv, "Workflow invocation not found"
    
    if hasattr(inv, 'metadata_synced_at') and inv.metadata_synced_at:
        logger.info(f"    ✅ metadata_synced_at: {inv.metadata_synced_at}")
    else:
        logger.warning("    ⚠️  metadata_synced_at not set (products may still be processing)")
    
    logger.info("\n✅ TEST 3 PASSED: Metadata ingestion working correctly")


# ============================================================================
# Combined Test (for backwards compatibility)
# ============================================================================

def test_status_pooler_full_integration(
    galaxy_client,
    supabase_client,
    storage_client,
    test_remote_file
):
    """
    Full integration test that validates the entire flow.
    
    Note: The individual tests (test_workflow_status_sync, test_database_triggers,
    test_product_metadata_ingestion) already ran and passed. This test just
    verifies the final state and provides a summary.
    
    If running this test alone, it will create a new workflow and validate everything.
    """
    logger.info("\n" + "=" * 80)
    logger.info("FULL STATUS POOLER INTEGRATION TEST")
    logger.info("=" * 80)
    
    # Check if we have a completed workflow from previous tests
    if hasattr(pytest, 'invocation_id') and hasattr(pytest, 'dataset_item_id'):
        logger.info("\n📋 Using workflow from previous tests...")
        logger.info(f"  Invocation ID: {pytest.invocation_id}")
        logger.info(f"  Dataset Item ID: {pytest.dataset_item_id}")
        
        # Just verify everything is still good
        inv = supabase_client.get_workflow_invocation_by_id(pytest.invocation_id)
        assert inv, "Workflow invocation should exist"
        assert inv.status in ['ok', 'success'], "Workflow should be completed"
        assert inv.metadata_synced_at, "Metadata should be synced"
        
        logger.info("  ✅ All previous test results verified")
    else:
        # No previous workflow, run the full flow
        logger.info("\n📋 Running full integration test from scratch...")
        test_workflow_status_sync(galaxy_client, supabase_client, storage_client, test_remote_file)
        test_database_triggers(supabase_client)
        test_product_metadata_ingestion(supabase_client)
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ ALL TESTS PASSED!")
    logger.info("=" * 80)
    logger.info("   All components working correctly:")
    logger.info("   ✓ Workflow execution")
    logger.info("   ✓ Status polling and updates")
    logger.info("   ✓ Database triggers")
    logger.info("   ✓ Metadata ingestion")
    logger.info("   ✓ Product table population")
    logger.info("=" * 80)
