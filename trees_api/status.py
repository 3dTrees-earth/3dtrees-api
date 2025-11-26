#!/usr/bin/env python3
"""
Status synchronization module for 3DTrees API.

This module is designed to be run as a cronjob to:
1. Connect to Galaxy and check workflow invocations
2. Compare Galaxy status with Supabase database status
3. Update Supabase when statuses differ
4. Detect products in S3 and ingest metadata
5. Sync results from completed workflows to S3 storage

Usage:
    # Run once (cron mode)
    python status.py
    
    # Run continuously (daemon mode for testing)
    python status.py --continuous
    python status.py --continuous --interval 10
    
    # Run from Docker container (as cronjob)
    docker compose run api python status.py
    
    # Run sync_results function standalone for a specific invocation
    from result_sync import sync_results
    sync_results(galaxy_client, supabase_client, storage_client, "invocation_id")

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
from trees_api.product_sync import sync_workflow_products
from trees_api.result_sync import sync_results, sync_completed_workflows

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


def sync_results_for_invocation(invocation_id: str) -> bool:
    """
    Standalone function to sync results for a specific invocation.
    This can be called from other parts of the system.
    
    Args:
        invocation_id: ID of the workflow invocation to sync
        
    Returns:
        True if sync was successful, False otherwise
    """
    try:
        galaxy_client, supabase_client, storage_client = get_connected_clients()
        result = sync_results(galaxy_client, supabase_client, storage_client, invocation_id)
        supabase_client.sign_out()
        return result
    except Exception as e:
        logger.error(f"Error syncing results for invocation {invocation_id}: {e}")
        return False


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
        
        # SLOW: Sync products with threading (~5-30 seconds depending on S3 and workflows)
        logger.info("Syncing workflow products from S3...")
        product_stats = sync_workflow_products(
            galaxy_client, supabase_client, storage_client, storage_config
        )
        
        # Sync results for completed workflows (existing functionality)
        logger.info("Syncing results for completed workflows...")
        result_stats = sync_completed_workflows(galaxy_client, supabase_client, storage_client)
        
        # Log final statistics
        logger.info("Status synchronization completed successfully")
        logger.info(f"Status sync stats: {status_stats}")
        logger.info(f"Product sync stats: {product_stats}")
        logger.info(f"Result sync stats: {result_stats}")
        
        # Cleanup
        supabase_client.sign_out()
        logger.info("Supabase client signed out")
        
        return {
            "status_stats": status_stats,
            "product_stats": product_stats,
            "result_stats": result_stats,
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Error during status synchronization: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def run_continuous(interval: int = 10):
    """
    Run status synchronization continuously in a loop (daemon mode).
    
    This mode is useful for:
    - Testing environments where you want continuous monitoring
    - Development environments
    - Kubernetes deployments (vs cron jobs)
    
    Args:
        interval: Number of seconds between sync cycles (default: 10)
    """
    run_count = 0
    logger.info("=" * 80)
    logger.info(f"Starting CONTINUOUS status pooler (interval: {interval}s)")
    logger.info("This emulates a cron job running status.py every N seconds")
    logger.info("=" * 80)
    
    while True:
        run_count += 1
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"SYNC CYCLE #{run_count} - Starting synchronization")
        logger.info("=" * 80)
        
        try:
            stats = run_sync_once()
            if stats["success"]:
                logger.info(f"✅ Sync cycle #{run_count} completed successfully")
            else:
                logger.error(f"❌ Sync cycle #{run_count} failed: {stats.get('error')}")
        except Exception as e:
            logger.error(f"❌ Unexpected error in sync cycle #{run_count}: {e}")
        
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
