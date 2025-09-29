#!/usr/bin/env python3
"""
Status synchronization module for 3DTrees API.

This module is designed to be run as a cronjob to:
1. Connect to Galaxy and check workflow invocations
2. Compare Galaxy status with Supabase database status
3. Update Supabase when statuses differ
4. Sync results from completed workflows to S3 storage

Usage:
    # Run directly
    python status.py
    
    # Run from Docker container (as cronjob)
    docker compose run api python status.py
    
    # Run sync_results function standalone for a specific invocation
    from status import sync_results
    sync_results(galaxy_client, supabase_client, storage_client, "invocation_id")

Environment Variables Required:
    - GALAXY_URL, GALAXY_EMAIL, GALAXY_PASSWORD (or GALAXY_API_KEY)
    - SUPABASE_URL, SUPABASE_KEY, SUPABASE_EMAIL, SUPABASE_PASSWORD
    - STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY, STORAGE_BUCKET_NAME, STORAGE_URL
"""

import os
import sys
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

# Add the current directory to Python path to import local modules
sys.path.insert(0, str(Path(__file__).parent))

from galaxy_client import GalaxyClient
from supabase_client import SupabaseClient
from storage_client import StorageClient


# Configure logging to use uvicorn's logger as preferred by user
logger = logging.getLogger("uvicorn")


# No mapping needed! Galaxy states are used directly in the database enum


def sync_workflow_statuses(galaxy_client: GalaxyClient, supabase_client: SupabaseClient) -> Dict[str, int]:
    """
    Sync workflow statuses between Galaxy and Supabase using efficient filtering.
    
    Strategy:
    1. Get only unfinished invocations from Supabase
    2. Get only those specific invocations from Galaxy
    3. Compare and update only changed fields
    
    Args:
        galaxy_client: Connected Galaxy client
        supabase_client: Connected Supabase client
        
    Returns:
        Dictionary with sync statistics
    """
    stats = {
        'total_checked': 0,
        'status_updated': 0,
        'jobs_updated': 0,
        'messages_updated': 0,
        'outputs_updated': 0,
        'errors': 0
    }
    
    try:
        # Get only unfinished workflow invocations from Supabase
        logger.info("Getting unfinished workflow invocations from Supabase...")
        supabase_invocations = supabase_client.get_unfinished_workflow_invocations()
        
        if not supabase_invocations:
            logger.info("No unfinished workflow invocations found in Supabase")
            return stats
        
        logger.info(f"Found {len(supabase_invocations)} unfinished workflow invocations in Supabase")
        
        # Get only these specific invocations from Galaxy
        invocation_ids = [inv.invocation_id for inv in supabase_invocations]
        logger.info(f"Getting {len(invocation_ids)} specific invocations from Galaxy...")
        galaxy_invocations = galaxy_client.get_workflow_invocations(invocation_ids=invocation_ids)
        
        # Create a lookup dictionary for Galaxy invocations by ID
        galaxy_lookup = {inv['id']: inv for inv in galaxy_invocations}
        
        for supabase_inv in supabase_invocations:
            stats['total_checked'] += 1
            
            try:
                # Find corresponding Galaxy invocation
                galaxy_inv = galaxy_lookup.get(supabase_inv.invocation_id)
                
                if not galaxy_inv:
                    logger.warning(f"Galaxy invocation {supabase_inv.invocation_id} not found")
                    continue
                
                update_data = {}
                
                # Check if status needs updating (no mapping needed - use Galaxy state directly)
                galaxy_status = galaxy_inv['state']
                if supabase_inv.status != galaxy_status:
                    logger.info(f"Updating status for invocation {supabase_inv.invocation_id}: {supabase_inv.status} -> {galaxy_status}")
                    update_data['status'] = galaxy_status
                    
                    # If workflow is finished, set finished_at timestamp
                    if galaxy_status in ['ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning']:
                        update_data['finished_at'] = datetime.now()
                    
                    stats['status_updated'] += 1
                
                # Check if jobs have changed
                if supabase_inv.has_jobs_changed(galaxy_inv.get('jobs', [])):
                    logger.info(f"Updating jobs for invocation {supabase_inv.invocation_id}")
                    update_data['jobs'] = galaxy_inv.get('jobs', [])
                    stats['jobs_updated'] += 1
                
                # Check if messages have changed (append new ones)
                if supabase_inv.has_messages_changed(galaxy_inv.get('messages', [])):
                    logger.info(f"Updating messages for invocation {supabase_inv.invocation_id}")
                    update_data['messages'] = galaxy_inv.get('messages', [])
                    stats['messages_updated'] += 1
                
                # Check if outputs have changed (only when workflow completes)
                if galaxy_status in ['ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning']:
                    if supabase_inv.has_outputs_changed(galaxy_inv.get('outputs', {})):
                        logger.info(f"Updating outputs for invocation {supabase_inv.invocation_id}")
                        update_data['outputs'] = galaxy_inv.get('outputs', {})
                        stats['outputs_updated'] += 1
                    
                    if supabase_inv.has_output_collections_changed(galaxy_inv.get('output_collections', {})):
                        logger.info(f"Updating output collections for invocation {supabase_inv.invocation_id}")
                        update_data['output_collections'] = galaxy_inv.get('output_collections', {})
                        stats['outputs_updated'] += 1
                
                # Update steps if anything changed
                if update_data:
                    update_data['steps'] = galaxy_inv.get('steps', [])
                    update_data['inputs'] = galaxy_inv.get('inputs', {})
                    
                    
                    supabase_client.update_workflow_invocation(supabase_inv.invocation_id, **update_data)
                
            except Exception as e:
                logger.error(f"Error processing invocation {supabase_inv.invocation_id}: {e}")
                stats['errors'] += 1
                continue
        
        logger.info(f"Status sync completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error during status sync: {e}")
        stats['errors'] += 1
        return stats


def sync_results(galaxy_client: GalaxyClient, supabase_client: SupabaseClient, storage_client: StorageClient, invocation_id: str) -> bool:
    """
    Sync results from a completed Galaxy workflow to S3 storage.
    This function is idempotent and can be called multiple times safely.
    
    Args:
        galaxy_client: Connected Galaxy client
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        invocation_id: ID of the workflow invocation to sync
        
    Returns:
        True if sync was successful, False otherwise
    """
    try:
        logger.info(f"Starting result sync for invocation {invocation_id}")
        
        # Get invocation details from Supabase
        supabase_inv = supabase_client.get_workflow_invocation_by_id(invocation_id)
        if not supabase_inv:
            logger.error(f"Invocation {invocation_id} not found in Supabase")
            return False
        
        # Only sync if workflow is completed successfully
        if supabase_inv.status not in ['ok', 'success']:
            logger.info(f"Invocation {invocation_id} status is {supabase_inv.status}, skipping result sync")
            return True
        
        # Get detailed invocation information from Galaxy
        galaxy_inv_details = galaxy_client.get_invocation_details(invocation_id)
        
        if not galaxy_inv_details.get('history_id'):
            logger.error(f"No history ID found for invocation {invocation_id}")
            return False
        
        # Get all datasets from the Galaxy history
        history_datasets = galaxy_client.get_history_datasets(galaxy_inv_details['history_id'])
        
        if not history_datasets:
            logger.warning(f"No datasets found in history {galaxy_inv_details['history_id']}")
            return True
        
        # Create a temporary directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Download and upload each dataset
            for dataset in history_datasets:
                try:
                    # Skip datasets that are not in 'ok' state
                    if dataset['state'] != 'ok':
                        logger.info(f"Skipping dataset {dataset['id']} with state {dataset['state']}")
                        continue
                    
                    # Create a unique filename for the result
                    file_ext = dataset.get('file_ext', '')
                    if file_ext and not file_ext.startswith('.'):
                        file_ext = f".{file_ext}"
                    
                    result_filename = f"{invocation_id}_{dataset['id']}{file_ext}"
                    local_file_path = temp_path / result_filename
                    
                    # Download from Galaxy
                    logger.info(f"Downloading dataset {dataset['id']} from Galaxy...")
                    galaxy_client.download_dataset(
                        galaxy_inv_details['history_id'],
                        dataset['id'],
                        local_file_path
                    )
                    
                    # Upload to S3 storage
                    s3_key = f"results/{supabase_inv.dataset_id}/{supabase_inv.workflow_name.lower()}/{result_filename}"
                    logger.info(f"Uploading {result_filename} to S3 as {s3_key}...")
                    storage_client.upload_file(local_file_path, s3_key)
                    
                    logger.info(f"Successfully synced dataset {dataset['id']} to S3")
                    
                except Exception as e:
                    logger.error(f"Error syncing dataset {dataset['id']}: {e}")
                    continue
        
        logger.info(f"Result sync completed for invocation {invocation_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error during result sync for invocation {invocation_id}: {e}")
        return False


def sync_completed_workflows(galaxy_client: GalaxyClient, supabase_client: SupabaseClient, storage_client: StorageClient) -> Dict[str, int]:
    """
    Sync results for all completed workflows that haven't been synced yet.
    
    Args:
        galaxy_client: Connected Galaxy client
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        
    Returns:
        Dictionary with sync statistics
    """
    stats = {
        'completed_workflows': 0,
        'successfully_synced': 0,
        'sync_errors': 0
    }
    
    try:
        # Get all successful workflow invocations that haven't been synced yet
        # Galaxy success states: 'ok' and 'success'
        successful_invocations = []
        for status in ['ok', 'success']:
            invocations = supabase_client.get_workflow_invocations(
                status=status,
                results_synced=False
            )
            successful_invocations.extend(invocations)
        
        logger.info(f"Found {len(successful_invocations)} successful workflow invocations that need result sync")
        
        for invocation in successful_invocations:
            stats['completed_workflows'] += 1
            
            # Sync results
            if sync_results(galaxy_client, supabase_client, storage_client, invocation.invocation_id):
                # Mark as synced using the new results_synced field
                supabase_client.update_workflow_invocation(
                    invocation.invocation_id,
                    results_synced=True,
                    results_synced_at=datetime.now()
                )
                
                stats['successfully_synced'] += 1
                logger.info(f"Marked invocation {invocation.invocation_id} as synced")
            else:
                stats['sync_errors'] += 1
        
        logger.info(f"Completed workflow sync: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error during completed workflow sync: {e}")
        stats['sync_errors'] += 1
        return stats


def get_connected_clients():
    """
    Initialize and connect all required clients.
    
    Returns:
        Tuple of (galaxy_client, supabase_client, storage_client)
        
    Raises:
        Exception: If any client connection fails
    """
    # Initialize clients
    galaxy_client = GalaxyClient()
    supabase_client = SupabaseClient()
    storage_client = StorageClient()
    
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


def main():
    """
    Main function to run the status synchronization.
    """
    logger.info("Starting 3DTrees status synchronization...")
    
    try:
        # Get connected clients
        galaxy_client, supabase_client, storage_client = get_connected_clients()
        logger.info("All clients connected successfully")
        
        # Sync workflow statuses
        logger.info("Syncing workflow statuses...")
        status_stats = sync_workflow_statuses(galaxy_client, supabase_client)
        
        # Sync results for completed workflows
        logger.info("Syncing results for completed workflows...")
        result_stats = sync_completed_workflows(galaxy_client, supabase_client, storage_client)
        
        # Log final statistics
        logger.info("Status synchronization completed successfully")
        logger.info(f"Status sync stats: {status_stats}")
        logger.info(f"Result sync stats: {result_stats}")
        
        # Cleanup
        supabase_client.sign_out()
        logger.info("Supabase client signed out")
        
    except Exception as e:
        logger.error(f"Error during status synchronization: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
