"""
Galaxy workflow status synchronization module.

This module handles syncing workflow invocation statuses from Galaxy to Supabase,
including job states, messages, outputs, and completion detection.
"""
import json
import logging
from typing import Dict
from datetime import datetime

from trees_api.galaxy_client import GalaxyClient, count_tool_steps_from_workflow
from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger("uvicorn")


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
                    stats['status_updated'] += 1
                
                # Check if workflow is truly finished by examining both workflow state and job states
                workflow_finished = False
                should_update_status = False
                
                # First check: workflow invocation is in terminal state
                if galaxy_status in ['ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning']:
                    workflow_finished = True
                    logger.debug(f"Workflow {supabase_inv.invocation_id} finished: workflow state is {galaxy_status}")
                
                # Second check: Verify we have ALL expected jobs before marking as finished
                elif galaxy_inv.get('jobs'):
                    # Get workflow definition to count expected tool steps
                    try:
                        workflow_name = supabase_inv.workflow_name
                        workflow_file = galaxy_client.workflows_path / f"{workflow_name}.ga"
                        
                        expected_job_count = 0
                        if workflow_file.exists():
                            with open(workflow_file, 'r') as f:
                                workflow_data = json.load(f)
                                expected_job_count = count_tool_steps_from_workflow(workflow_data)
                        else:
                            logger.warning(f"Workflow file not found: {workflow_file}")
                            # Fallback: count workflow steps with job_id
                            expected_job_count = len([s for s in galaxy_inv.get('steps', []) 
                                                     if s.get('job_id') is not None or 
                                                        (hasattr(s, 'job_id') and s.job_id is not None)])
                        
                        actual_job_count = len(galaxy_inv['jobs'])
                        
                        # CRITICAL CHECK: Do we have all expected jobs?
                        if expected_job_count > 0 and actual_job_count < expected_job_count:
                            logger.warning(
                                f"Workflow {supabase_inv.invocation_id}: Only {actual_job_count}/{expected_job_count} "
                                f"jobs scheduled. Workflow not finished yet!"
                            )
                            workflow_finished = False
                        else:
                            # All jobs are scheduled, check if they're all finished
                            all_jobs_finished = True
                            all_jobs_successful = True
                            for job in galaxy_inv['jobs']:
                                job_state = job.get('state', '')
                                if job_state not in ['ok', 'error', 'failed', 'cancelled']:
                                    all_jobs_finished = False
                                    all_jobs_successful = False
                                    break
                                if job_state != 'ok':
                                    all_jobs_successful = False
                            
                            if all_jobs_finished:
                                workflow_finished = True
                                should_update_status = True
                                # Update workflow status based on job states
                                if all_jobs_successful:
                                    update_data['status'] = 'ok'
                                    stats['status_updated'] += 1
                                    logger.info(f"Updating workflow status to 'ok' for invocation {supabase_inv.invocation_id}")
                                else:
                                    update_data['status'] = 'error'
                                    stats['status_updated'] += 1
                                    logger.info(f"Updating workflow status to 'error' for invocation {supabase_inv.invocation_id}")
                                logger.debug(f"Workflow {supabase_inv.invocation_id} finished: all {actual_job_count} jobs completed")
                            else:
                                logger.debug(f"Workflow {supabase_inv.invocation_id}: {actual_job_count} jobs scheduled but not all finished")
                    
                    except Exception as e:
                        logger.error(f"Error checking job count for {supabase_inv.invocation_id}: {e}")
                        # Fallback to old logic
                        all_jobs_finished = all(job.get('state') in ['ok', 'error', 'failed', 'cancelled'] 
                                               for job in galaxy_inv['jobs'])
                        if all_jobs_finished:
                            workflow_finished = True
                
                # Set finished_at timestamp if workflow is finished
                if workflow_finished and not supabase_inv.finished_at:
                    update_data['finished_at'] = datetime.now()
                    logger.info(f"Marking workflow {supabase_inv.invocation_id} as finished")
                
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
                
                # Check if outputs have changed (when workflow is finished)
                if workflow_finished:
                    # Try to get outputs from Galaxy
                    galaxy_outputs = galaxy_inv.get('outputs', {})
                    galaxy_output_collections = galaxy_inv.get('output_collections', {})
                    
                    # If outputs are empty but we have a completed job, try to get outputs from job details
                    if not galaxy_outputs and not galaxy_output_collections and galaxy_inv.get('jobs'):
                        try:
                            # Get outputs from the first completed job
                            for job_data in galaxy_inv['jobs']:
                                if job_data.get('state') == 'ok' and job_data.get('id'):
                                    job_id = job_data['id']
                                    actual_job = galaxy_client.gi.jobs.get(job_id)
                                    if hasattr(actual_job, 'outputs'):
                                        galaxy_outputs = actual_job.outputs
                                    if hasattr(actual_job, 'output_collections'):
                                        galaxy_output_collections = actual_job.output_collections
                                    break
                        except Exception as e:
                            logger.warning(f"Could not extract outputs from job: {e}")
                    
                    if supabase_inv.has_outputs_changed(galaxy_outputs):
                        logger.info(f"Updating outputs for invocation {supabase_inv.invocation_id}")
                        update_data['outputs'] = galaxy_outputs
                        stats['outputs_updated'] += 1
                    
                    if supabase_inv.has_output_collections_changed(galaxy_output_collections):
                        logger.info(f"Updating output collections for invocation {supabase_inv.invocation_id}")
                        update_data['output_collections'] = galaxy_output_collections
                        stats['outputs_updated'] += 1
                
                # Update steps if anything changed
                if update_data:
                    # Update step states based on job states
                    updated_steps = []
                    for step in galaxy_inv.get('steps', []):
                        step_dict = dict(step) if isinstance(step, dict) else step.__dict__.copy()
                        # If this step has a job_id, update its state from the actual job
                        if step_dict.get('job_id') and galaxy_inv.get('jobs'):
                            for job_data in galaxy_inv['jobs']:
                                if job_data.get('id') == step_dict.get('job_id'):
                                    step_dict['state'] = job_data.get('state', step_dict.get('state'))
                                    break
                        updated_steps.append(step_dict)
                    
                    update_data['steps'] = updated_steps
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

