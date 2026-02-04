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


# Valid workflow status values in Supabase enum
VALID_WORKFLOW_STATUSES = {
    'pending', 'running', 'successful', 'warning', 'errored',
    'new', 'ready', 'queued', 'scheduled', 'ok', 'success',
    'error', 'failed', 'cancelled', 'paused', 'deleted', 'discarded'
}

# Map unknown Galaxy statuses to valid Supabase enum values
GALAXY_STATUS_MAPPING = {
    'requires_materialization': 'ready',  # Still processing
    'waiting': 'ready',
    'upload': 'running',
    'resubmitted': 'running',
}

TERMINAL_WORKFLOW_STATES = {
    'ok', 'success', 'error', 'failed', 'cancelled', 'deleted', 'discarded', 'warning'
}
TERMINAL_JOB_STATES = {'ok', 'error', 'failed', 'cancelled'}
ERROR_JOB_STATES = {'error', 'failed'}
RUNNING_JOB_STATES = {'new', 'queued', 'running'}
TERMINAL_STEP_STATES = {'ok', 'error', 'failed', 'cancelled', 'skipped', 'deleted', 'discarded'}


def _map_galaxy_status(galaxy_status: str) -> str:
    """
    Map Galaxy status to a valid Supabase workflow_status enum value.
    
    Galaxy may return new/unknown status values that don't exist in our enum.
    This function maps them to the closest valid status.
    """
    if galaxy_status in VALID_WORKFLOW_STATUSES:
        return galaxy_status
    
    if galaxy_status in GALAXY_STATUS_MAPPING:
        mapped = GALAXY_STATUS_MAPPING[galaxy_status]
        logger.debug(f"Mapped Galaxy status '{galaxy_status}' to '{mapped}'")
        return mapped
    
    # Unknown status - log warning and default to 'ready' (still processing)
    logger.warning(f"Unknown Galaxy status '{galaxy_status}', defaulting to 'ready'")
    return 'ready'


def _get_expected_tool_step_uuids(
    galaxy_client: GalaxyClient,
    workflow_name: str,
    cache: Dict[str, set]
) -> set:
    expected_step_uuids = cache.get(workflow_name)
    if expected_step_uuids is not None:
        return expected_step_uuids

    try:
        workflow_def = galaxy_client.get_workflow_structure(workflow_name)
        expected_step_uuids = {
            step.get("uuid")
            for step in workflow_def.get("steps", {}).values()
            if step.get("tool_id") and step.get("type") != "data_input"
        }
    except Exception as e:
        expected_step_uuids = set()
        logger.warning(f"Unable to resolve workflow steps for {workflow_name}: {e}")

    cache[workflow_name] = expected_step_uuids
    return expected_step_uuids


def _build_step_terminal_map(galaxy_inv: Dict, jobs: List[Dict]) -> Dict[str, bool]:
    job_state_by_id = {job.get("id"): job.get("state") for job in jobs if job.get("id")}
    step_terminal = {}

    for step in galaxy_inv.get("steps", []):
        step_uuid = step.get("workflow_step_uuid")
        if not step_uuid:
            continue

        step_state = step.get("state")
        is_terminal = step_state in TERMINAL_STEP_STATES

        job_id = step.get("job_id")
        if job_id and job_state_by_id.get(job_id) in TERMINAL_JOB_STATES:
            is_terminal = True

        step_jobs = step.get("jobs", [])
        if step_jobs:
            is_terminal = all(j.get("state") in TERMINAL_JOB_STATES for j in step_jobs)

        step_terminal[step_uuid] = is_terminal

    return step_terminal


def _all_expected_steps_terminal(expected_step_uuids: set, step_terminal_map: Dict[str, bool]) -> bool:
    if not expected_step_uuids:
        return False
    return all(step_terminal_map.get(step_uuid) is True for step_uuid in expected_step_uuids)


def _determine_workflow_completion(
    galaxy_status: str,
    jobs: List[Dict],
    expected_step_uuids: set,
    step_terminal_map: Dict[str, bool],
    invocation_id: str
) -> tuple[bool, Optional[str], bool]:
    if galaxy_status in TERMINAL_WORKFLOW_STATES:
        logger.debug(f"Workflow {invocation_id} finished: Galaxy state is {galaxy_status}")
        return True, galaxy_status, False

    if not jobs:
        logger.debug(f"Workflow {invocation_id}: Galaxy state={galaxy_status}, no jobs found yet")
        return False, None, False

    all_jobs_terminal = all(j.get('state') in TERMINAL_JOB_STATES for j in jobs)
    all_expected_steps_terminal = _all_expected_steps_terminal(expected_step_uuids, step_terminal_map)

    if all_jobs_terminal and all_expected_steps_terminal:
        error_jobs = [j for j in jobs if j.get('state') in ERROR_JOB_STATES]
        if error_jobs:
            logger.info(
                f"Workflow {invocation_id} finished (job+step-based): {len(error_jobs)} failed jobs"
            )
            return True, 'error', all_expected_steps_terminal
        logger.info(
            f"Workflow {invocation_id} finished (job+step-based): all {len(jobs)} jobs ok"
        )
        return True, 'ok', all_expected_steps_terminal

    return False, None, all_expected_steps_terminal


def _extract_outputs_from_jobs(galaxy_client: GalaxyClient, jobs: List[Dict]) -> tuple[Dict, Dict]:
    for job_data in jobs:
        if job_data.get('state') == 'ok' and job_data.get('id'):
            try:
                job_id = job_data['id']
                actual_job = galaxy_client.gi.jobs.get(job_id)
                outputs = actual_job.outputs if hasattr(actual_job, 'outputs') else {}
                output_collections = (
                    actual_job.output_collections if hasattr(actual_job, 'output_collections') else {}
                )
                return outputs, output_collections
            except Exception as e:
                logger.warning(f"Could not extract outputs from job: {e}")
                break
    return {}, {}


def _update_steps_from_jobs(steps: List[Dict], jobs: List[Dict]) -> List[Dict]:
    updated_steps = []
    for step in steps:
        step_dict = dict(step) if isinstance(step, dict) else step.__dict__.copy()
        if step_dict.get('job_id') and jobs:
            for job_data in jobs:
                if job_data.get('id') == step_dict.get('job_id'):
                    step_dict['state'] = job_data.get('state', step_dict.get('state'))
                    break
        updated_steps.append(step_dict)
    return updated_steps


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
        expected_tool_steps: Dict[str, set] = {}
        logger.info("Getting unfinished workflow invocations from Supabase...")
        supabase_invocations = supabase_client.get_unfinished_workflow_invocations()

        if not supabase_invocations:
            logger.info("No unfinished workflow invocations found in Supabase")
            return stats

        logger.info(f"Found {len(supabase_invocations)} unfinished workflow invocations in Supabase")

        invocation_ids = [inv.invocation_id for inv in supabase_invocations]
        logger.info(f"Getting {len(invocation_ids)} specific invocations from Galaxy...")
        galaxy_invocations = galaxy_client.get_workflow_invocations(invocation_ids=invocation_ids)

        galaxy_lookup = {inv['id']: inv for inv in galaxy_invocations}

        for supabase_inv in supabase_invocations:
            stats['total_checked'] += 1

            try:
                galaxy_inv = galaxy_lookup.get(supabase_inv.invocation_id)
                if not galaxy_inv:
                    logger.warning(f"Galaxy invocation {supabase_inv.invocation_id} not found")
                    continue

                update_data = {}

                galaxy_status_raw = galaxy_inv['state']
                galaxy_status = _map_galaxy_status(galaxy_status_raw)
                jobs = galaxy_inv.get('jobs', [])

                expected_step_uuids = set()
                step_terminal_map: Dict[str, bool] = {}
                if galaxy_status not in TERMINAL_WORKFLOW_STATES:
                    expected_step_uuids = _get_expected_tool_step_uuids(
                        galaxy_client,
                        supabase_inv.workflow_name,
                        expected_tool_steps,
                    )
                    step_terminal_map = _build_step_terminal_map(galaxy_inv, jobs)

                workflow_finished, final_status, all_expected_steps_terminal = _determine_workflow_completion(
                    galaxy_status,
                    jobs,
                    expected_step_uuids,
                    step_terminal_map,
                    supabase_inv.invocation_id,
                )

                if not workflow_finished and jobs:
                    ok_count = sum(1 for j in jobs if j.get('state') == 'ok')
                    running_count = sum(1 for j in jobs if j.get('state') in RUNNING_JOB_STATES)
                    logger.debug(
                        f"Workflow {supabase_inv.invocation_id}: Galaxy state={galaxy_status}, "
                        f"{ok_count}/{len(jobs)} jobs ok, {running_count} still running, "
                        f"expected_steps_terminal={all_expected_steps_terminal}"
                    )

                if workflow_finished and final_status:
                    if supabase_inv.status != final_status:
                        update_data['status'] = final_status
                        stats['status_updated'] += 1
                        logger.info(
                            f"Setting workflow {supabase_inv.invocation_id} status to {final_status} (finished)"
                        )
                elif supabase_inv.status != galaxy_status:
                    update_data['status'] = galaxy_status
                    stats['status_updated'] += 1
                    logger.debug(
                        f"Updating status for {supabase_inv.invocation_id}: {supabase_inv.status} -> {galaxy_status}"
                    )

                if workflow_finished and final_status in ['error', 'failed']:
                    _create_linear_issue_for_failure(
                        galaxy_client=galaxy_client,
                        invocation_id=supabase_inv.invocation_id,
                        dataset_id=supabase_inv.dataset_id,
                        workflow_name=supabase_inv.workflow_name,
                        jobs=jobs,
                        messages=galaxy_inv.get('messages', []),
                    )

                if workflow_finished and not supabase_inv.finished_at:
                    update_data['finished_at'] = datetime.now()
                    logger.info(f"Marking workflow {supabase_inv.invocation_id} as finished")

                if supabase_inv.has_jobs_changed(jobs):
                    logger.info(f"Updating jobs for invocation {supabase_inv.invocation_id}")
                    update_data['jobs'] = jobs
                    stats['jobs_updated'] += 1

                if supabase_inv.has_messages_changed(galaxy_inv.get('messages', [])):
                    logger.info(f"Updating messages for invocation {supabase_inv.invocation_id}")
                    update_data['messages'] = galaxy_inv.get('messages', [])
                    stats['messages_updated'] += 1

                if workflow_finished:
                    galaxy_outputs = galaxy_inv.get('outputs', {})
                    galaxy_output_collections = galaxy_inv.get('output_collections', {})

                    if not galaxy_outputs and not galaxy_output_collections and jobs:
                        galaxy_outputs, galaxy_output_collections = _extract_outputs_from_jobs(
                            galaxy_client,
                            jobs,
                        )

                    if supabase_inv.has_outputs_changed(galaxy_outputs):
                        logger.info(f"Updating outputs for invocation {supabase_inv.invocation_id}")
                        update_data['outputs'] = galaxy_outputs
                        stats['outputs_updated'] += 1

                    if supabase_inv.has_output_collections_changed(galaxy_output_collections):
                        logger.info(f"Updating output collections for invocation {supabase_inv.invocation_id}")
                        update_data['output_collections'] = galaxy_output_collections
                        stats['outputs_updated'] += 1

                if update_data:
                    update_data['steps'] = _update_steps_from_jobs(
                        galaxy_inv.get('steps', []),
                        jobs,
                    )
                    update_data['inputs'] = galaxy_inv.get('inputs', {})
                    supabase_client.update_workflow_invocation(
                        supabase_inv.invocation_id,
                        **update_data,
                    )

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
            logger.info("Linear client not configured or disabled, skipping issue creation")
            return
        
        logger.info(f"Creating Linear issue for failed workflow {invocation_id} (dataset {dataset_id})")
        
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

