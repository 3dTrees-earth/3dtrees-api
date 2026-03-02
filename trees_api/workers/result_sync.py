"""
Result syncing module for Galaxy workflows.

This module handles syncing workflow results from Galaxy history to S3 storage.
"""
import logging
import tempfile
from pathlib import Path
from typing import Dict
from datetime import datetime

from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.integrations.storage.client import StorageClient

logger = logging.getLogger("uvicorn")


def sync_results(
    galaxy_client: GalaxyClient,
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    invocation_id: str
) -> bool:
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
        
        # Get dataset_id (invocation has dataset_id for collection workflows)
        dataset_id = getattr(supabase_inv, "dataset_id", None)
        if dataset_id is None:
            dataset_item_id = getattr(supabase_inv, "dataset_item_id", None)
            if dataset_item_id is not None:
                dataset_item = supabase_client.get_dataset_item(dataset_item_id)
                if dataset_item:
                    dataset_id = dataset_item.get("dataset_id") if isinstance(dataset_item, dict) else dataset_item.dataset_id
        if dataset_id is None:
            logger.error(f"Invocation {invocation_id} has no dataset_id or dataset_item_id")
            return False
        
        # Get dataset details to access UUID
        supabase_dataset = supabase_client.get_dataset(dataset_id)
        if not supabase_dataset:
            logger.error(f"Dataset {dataset_id} not found in Supabase")
            return False
        
        # Only sync if workflow is completed successfully
        # Check both workflow status and job states to determine if truly completed
        workflow_completed = False
        
        # First check: workflow status is in success states
        if supabase_inv.status in ['ok', 'success']:
            workflow_completed = True
        # Second check: if workflow status is 'scheduled' but all jobs are completed
        elif supabase_inv.status == 'scheduled' and supabase_inv.jobs:
            all_jobs_completed = True
            for job in supabase_inv.jobs:
                job_state = job.get('state', '')
                if job_state not in ['ok', 'error', 'failed', 'cancelled']:
                    all_jobs_completed = False
                    break
            if all_jobs_completed:
                workflow_completed = True
        
        if not workflow_completed:
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
                    s3_key = f"results/{supabase_dataset.uuid}/{supabase_inv.workflow_name.lower()}/{result_filename}"
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


def sync_completed_workflows(
    galaxy_client: GalaxyClient,
    supabase_client: SupabaseClient,
    storage_client: StorageClient
) -> Dict[str, int]:
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
        # Get all workflow invocations that:
        # 1. Have status 'ok' or 'success', OR
        # 2. Have finished_at set (indicating completion via job states)
        # AND have not been synced yet
        
        # Query 1: Get by success status
        successful_invocations = []
        for status in ['ok', 'success']:
            invocations = supabase_client.get_workflow_invocations(
                status=status,
                results_synced=False
            )
            successful_invocations.extend(invocations)
        
        # Query 2: Get workflows with finished_at set but status still 'scheduled'
        # This catches workflows completed via job state detection
        if hasattr(supabase_client, 'get_finished_unsynced_workflows'):
            finished_invocations = supabase_client.get_finished_unsynced_workflows()
            successful_invocations.extend(finished_invocations)
        
        # Remove duplicates by invocation_id
        seen_ids = set()
        unique_invocations = []
        for inv in successful_invocations:
            if inv.invocation_id not in seen_ids:
                seen_ids.add(inv.invocation_id)
                unique_invocations.append(inv)
        
        logger.info(f"Found {len(unique_invocations)} workflow invocations that need result sync")
        
        for invocation in unique_invocations:
            stats['completed_workflows'] += 1
            
            # Sync results
            if sync_results(galaxy_client, supabase_client, storage_client, invocation.invocation_id):
                # Mark as synced
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

