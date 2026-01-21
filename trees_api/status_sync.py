"""
Galaxy workflow status synchronization module.

This module handles syncing workflow invocation statuses from Galaxy to Supabase,
including job states, messages, outputs, and completion detection.

When workflows fail, this module also creates Linear issues automatically
(if LINEAR_ENABLED=true and LINEAR_API_KEY is set).
"""
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime

from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.linear_client import LinearClient, FailedJob, create_linear_client_if_enabled

logger = logging.getLogger("uvicorn")

# Global Linear client (initialized once, reused)
_linear_client: Optional[LinearClient] = None


def get_linear_client() -> Optional[LinearClient]:
    """Get or create the Linear client singleton."""
    global _linear_client
    if _linear_client is None:
        _linear_client = create_linear_client_if_enabled()
    return _linear_client


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
                
                # Second check: Check if ALL jobs are in terminal state
                # For collection workflows, job count = num_files × num_tool_steps
                # So we can't compare against expected_job_count from workflow file
                elif galaxy_inv.get('jobs'):
                    actual_job_count = len(galaxy_inv['jobs'])
                    
                    # Check if all jobs are in terminal state
                    all_jobs_finished = True
                    all_jobs_successful = True
                    running_count = 0
                    ok_count = 0
                    
                    for job in galaxy_inv['jobs']:
                        job_state = job.get('state', '')
                        if job_state == 'ok':
                            ok_count += 1
                        elif job_state in ['error', 'failed', 'cancelled']:
                            all_jobs_successful = False
                        else:
                            # Job is still running/queued
                            all_jobs_finished = False
                            all_jobs_successful = False
                            running_count += 1
                    
                    if all_jobs_finished and actual_job_count > 0:
                        workflow_finished = True
                        should_update_status = True
                        # Update workflow status based on job states
                        if all_jobs_successful:
                            update_data['status'] = 'ok'
                            stats['status_updated'] += 1
                            logger.info(f"Workflow {supabase_inv.invocation_id} completed: all {actual_job_count} jobs successful")
                        else:
                            update_data['status'] = 'error'
                            stats['status_updated'] += 1
                            logger.info(f"Workflow {supabase_inv.invocation_id} completed with errors: {ok_count}/{actual_job_count} jobs successful")
                            
                            # Create Linear issue for failed workflow
                            _create_linear_issue_for_failure(
                                galaxy_client=galaxy_client,
                                invocation_id=supabase_inv.invocation_id,
                                dataset_id=supabase_inv.dataset_id,
                                workflow_name=supabase_inv.workflow_name,
                                jobs=galaxy_inv.get('jobs', []),
                                messages=galaxy_inv.get('messages', []),
                            )
                    else:
                        logger.debug(f"Workflow {supabase_inv.invocation_id}: {ok_count}/{actual_job_count} jobs ok, {running_count} still running")
                
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


def _create_linear_issue_for_failure(
    galaxy_client: GalaxyClient,
    invocation_id: str,
    dataset_id: int,
    workflow_name: str,
    jobs: List[Dict],
    messages: List[Dict],
) -> None:
    """
    Create a Linear issue for a failed workflow.
    
    This function is resilient - it catches all exceptions and logs them,
    never allowing Linear failures to block workflow sync.
    
    Args:
        galaxy_client: Galaxy client for fetching job details
        invocation_id: The failed invocation ID
        dataset_id: The dataset ID
        workflow_name: Name of the workflow
        jobs: List of job dicts from Galaxy
        messages: List of message dicts from Galaxy
    """
    try:
        linear_client = get_linear_client()
        if not linear_client:
            logger.debug("Linear client not available, skipping issue creation")
            return
        
        # Collect detailed info for failed jobs
        failed_jobs: List[FailedJob] = []
        
        for job in jobs:
            job_state = job.get('state', '')
            if job_state in ['error', 'failed']:
                job_id = job.get('id')
                tool_id = job.get('tool_id', '')
                
                # Extract tool name from tool_id
                # e.g., "toolshed.g2.bx.psu.edu/.../3dtrees_overviews/1.2.0" -> "3dtrees_overviews"
                tool_name = tool_id.split("/")[-2] if "/" in tool_id else tool_id
                
                # Get detailed job info from Galaxy
                if job_id:
                    try:
                        details = galaxy_client.get_job_details(job_id)
                        failed_jobs.append(FailedJob(
                            tool_id=tool_id,
                            tool_name=tool_name,
                            exit_code=details.get('exit_code'),
                            stderr=details.get('tool_stderr', '')[:2000],  # Truncate
                            stdout=details.get('tool_stdout', '')[:500],   # Truncate
                            job_messages=details.get('job_messages', []),
                        ))
                    except Exception as e:
                        logger.warning(f"Could not get job details for {job_id}: {e}")
                        # Still add the job with minimal info
                        failed_jobs.append(FailedJob(
                            tool_id=tool_id,
                            tool_name=tool_name,
                            exit_code=None,
                            stderr="",
                            stdout="",
                            job_messages=[],
                        ))
        
        if not failed_jobs:
            logger.warning(f"No failed jobs found for invocation {invocation_id}, skipping Linear issue")
            return
        
        # Create the Linear issue
        issue_id = linear_client.create_workflow_failure_issue(
            dataset_id=dataset_id,
            invocation_id=invocation_id,
            workflow_name=workflow_name,
            failed_jobs=failed_jobs,
            messages=messages,
        )
        
        if issue_id:
            logger.info(f"Created Linear issue {issue_id} for failed workflow {invocation_id}")
        else:
            logger.warning(f"Failed to create Linear issue for invocation {invocation_id}")
            
    except Exception as e:
        # Never let Linear errors block workflow sync
        logger.error(f"Error creating Linear issue for invocation {invocation_id}: {e}")

