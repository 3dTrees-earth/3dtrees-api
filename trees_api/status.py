#!/usr/bin/env python3
"""
Status synchronization module for 3DTrees API.

This module is designed to be run as a cronjob to:
1. Connect to Galaxy and check workflow invocations
2. Compare Galaxy status with Supabase database status  
3. Update Supabase when statuses differ
4. Detect products in S3 and ingest small metadata JSON files

Note: This module does NOT download/upload actual data files from/to S3.
Galaxy uses remote file sources to read input data directly from S3,
and uses the export tool to write outputs directly back to S3.
The status pooler only:
- Checks if files exist in S3 (to detect when products are ready)
- Downloads small JSON metadata files (~40KB) to extract fields for Supabase

Usage:
    # Run once (cron mode)
    python status.py
    
    # Run continuously (daemon mode for testing)
    python status.py --continuous
    python status.py --continuous --interval 10
    
    # Run from Docker container (as cronjob)
    docker compose run api python status.py

Environment Variables Required:
    - GALAXY_URL, GALAXY_EMAIL, GALAXY_PASSWORD (or GALAXY_API_KEY)
    - SUPABASE_URL, SUPABASE_KEY, SUPABASE_EMAIL, SUPABASE_PASSWORD
    - STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY, STORAGE_BUCKET_NAME, STORAGE_URL
    - STATUS_POOLER_INTERVAL (optional, for continuous mode)
"""

import sys
import logging
import time
import os
from pathlib import Path

# Add the current directory to Python path to import local modules
sys.path.insert(0, str(Path(__file__).parent))

from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient
from trees_api.config import GalaxyConfig, SupabaseConfig, StorageConfig
from trees_api.status_sync import sync_workflow_statuses
from trees_api.history_sync import sync_history_outputs
# Note: result_sync is NOT imported - Galaxy export tool writes outputs directly to S3
# so we don't need to download from Galaxy history and re-upload

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("status_pooler")


def get_connected_clients():
    """
    Initialize and connect all required clients.
    
    Returns:
        Tuple of (galaxy_client, supabase_client, storage_client)
        
    Raises:
        Exception: If any client connection fails
    """
    # Initialize configs
    galaxy_config = GalaxyConfig()
    supabase_config = SupabaseConfig()
    storage_config = StorageConfig()
    
    # Initialize clients with configs
    galaxy_client = GalaxyClient(galaxy_config)
    supabase_client = SupabaseClient(supabase_config)
    storage_client = StorageClient(storage_config)
    
    # Connect to services
    galaxy_client.authenticate()
    galaxy_client.connect()
    
    supabase_client.connect()
    try:
        supabase_client.authenticate_user(supabase_client.email, supabase_client.password)
    except Exception as e:
        if "Authentication failed" in str(e):
            supabase_client.register_user(supabase_client.email, supabase_client.password)
            logger.info(f"New user created: {supabase_client.email}")
        else:
            raise e
    
    storage_client.connect()
    
    return galaxy_client, supabase_client, storage_client


def run_sync_once():
    """
    Run a single synchronization cycle.
    
    This separates fast operations (status sync) from slow operations (S3 product detection + metadata ingestion).
    Product sync runs in a thread pool to handle multiple workflows concurrently.
    
    Returns:
        dict: Statistics from the sync cycle
    """
    try:
        # Get connected clients
        galaxy_client, supabase_client, storage_client = get_connected_clients()
        logger.info("All clients connected successfully")
        
        # Initialize storage config for product sync
        storage_config = StorageConfig()
        
        # FAST: Sync workflow statuses from Galaxy to Supabase (~1-2 seconds)
        logger.info("Syncing workflow statuses from Galaxy...")
        status_stats = sync_workflow_statuses(galaxy_client, supabase_client)
        
        # Sync history outputs for finished workflows
        # This stores output paths in galaxy_histories.outputs and ingests metadata JSON
        logger.info("Syncing history outputs...")
        history_stats = sync_history_outputs(
            supabase_client, storage_client, storage_config
        )
        
        # Note: We do NOT sync results from Galaxy history to S3 here because:
        # 1. Galaxy uses remote file sources to read input data directly from S3
        # 2. Galaxy uses the export tool to write outputs directly back to S3
        # 3. No need to download from Galaxy and re-upload - data is already in S3
        
        # Log final statistics
        logger.info("Status synchronization completed successfully")
        logger.info(f"Status sync stats: {status_stats}")
        logger.info(f"History sync stats: {history_stats}")
        
        # Note: Don't sign out - this would invalidate ALL sessions for this user
        # including browser sessions. The status pooler shares credentials with
        # the frontend user for RLS purposes.
        logger.info("Sync cycle completed (keeping session active)")
        
        return {
            "status_stats": status_stats,
            "history_stats": history_stats,
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Error during status synchronization: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def run_sync_with_clients(galaxy_client, supabase_client, storage_client):
    """
    Run a single synchronization cycle with pre-connected clients.
    
    Args:
        galaxy_client: Connected GalaxyClient
        supabase_client: Connected SupabaseClient
        storage_client: Connected StorageClient
        
    Returns:
        dict: Statistics from the sync cycle
    """
    # Initialize storage config for product sync
    storage_config = StorageConfig()
    
    # FAST: Sync workflow statuses from Galaxy to Supabase (~1-2 seconds)
    logger.info("Syncing workflow statuses from Galaxy...")
    status_stats = sync_workflow_statuses(galaxy_client, supabase_client)
    
    # Sync history outputs for finished workflows
    logger.info("Syncing history outputs...")
    history_stats = sync_history_outputs(
        supabase_client, storage_client, storage_config
    )
    
    logger.info("Status synchronization completed successfully")
    logger.info(f"Status sync stats: {status_stats}")
    logger.info(f"History sync stats: {history_stats}")
    logger.info("Sync cycle completed (keeping session active)")
    
    return {
        "status_stats": status_stats,
        "history_stats": history_stats,
        "success": True
    }


def run_continuous(interval: int = 10):
    """
    Run status synchronization continuously in a loop (daemon mode).
    
    This mode reuses HTTP connections across cycles to avoid file descriptor leaks.
    Clients are only recreated if a connection error occurs.
    
    Args:
        interval: Number of seconds between sync cycles (default: 10)
    """
    run_count = 0
    galaxy_client = None
    supabase_client = None
    storage_client = None
    
    logger.info("=" * 80)
    logger.info(f"Starting CONTINUOUS status pooler (interval: {interval}s)")
    logger.info("Clients will be reused across cycles to avoid file descriptor leaks")
    logger.info("=" * 80)
    
    while True:
        run_count += 1
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"SYNC CYCLE #{run_count} - Starting synchronization")
        logger.info("=" * 80)
        
        try:
            # Initialize clients on first run or after connection error
            if galaxy_client is None or supabase_client is None or storage_client is None:
                logger.info("Initializing clients...")
                galaxy_client, supabase_client, storage_client = get_connected_clients()
                logger.info("All clients connected successfully")
            
            stats = run_sync_with_clients(galaxy_client, supabase_client, storage_client)
            if stats["success"]:
                logger.info(f"✅ Sync cycle #{run_count} completed successfully")
            else:
                logger.error(f"❌ Sync cycle #{run_count} failed: {stats.get('error')}")
                
        except Exception as e:
            logger.error(f"❌ Unexpected error in sync cycle #{run_count}: {e}")
            # Reset clients on error to force reconnection on next cycle
            logger.info("Resetting clients due to error (will reconnect on next cycle)")
            galaxy_client = None
            supabase_client = None
            storage_client = None
        
        # Wait for next cycle
        logger.info(f"⏳ Waiting {interval}s until next sync cycle...")
        time.sleep(interval)


def main():
    """
    Main entry point for status synchronization.
    
    Supports two modes:
    1. One-shot mode (default): Run once and exit (for cron)
    2. Continuous mode (--continuous): Run in a loop with interval (for testing/k8s)
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="3DTrees Status Synchronization")
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run continuously in daemon mode (for testing/k8s)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Interval in seconds between sync cycles (default: from STATUS_POOLER_INTERVAL env var or 10)"
    )
    
    args = parser.parse_args()
    
    if args.continuous:
        # Continuous mode: run in a loop
        interval = args.interval or int(os.environ.get("STATUS_POOLER_INTERVAL", "10"))
        run_continuous(interval)
    else:
        # One-shot mode: run once and exit (for cron)
        logger.info("Starting 3DTrees status synchronization (one-shot mode)...")
        stats = run_sync_once()
        if not stats["success"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
