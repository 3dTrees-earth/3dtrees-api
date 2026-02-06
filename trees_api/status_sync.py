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
from datetime import datetime, timedelta, timezone

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
TERMINAL_JOB_STATES = {'ok', 'error', 'failed', 'cancelled', 'paused'}
ERROR_JOB_STATES = {'error', 'failed'}
PAUSED_JOB_STATES = {'paused'}
RUNNING_JOB_STATES = {'new', 'queued', 'running'}
TERMINAL_STEP_STATES = {'ok', 'error', 'failed', 'cancelled', 'skipped', 'deleted', 'discarded', 'paused'}

# How long to wait before discarding invocations missing in Galaxy
MISSING_INVOCATION_DISCARD_AFTER = timedelta(hours=24)


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


def _extract_step_uuids_from_workflow_def(workflow_def: Dict) -> set:
    """Extract tool step UUIDs from a workflow definition (.ga file or API response)."""
    uuids = {
        step.get("uuid")
        for step in workflow_def.get("steps", {}).values()
        if step.get("tool_id") and step.get("type") != "data_input"
    }
    return {uuid for uuid in uuids if uuid}


def _extract_step_uuids_from_invocation(galaxy_inv: Dict) -> set:
    """
    Extract tool step UUIDs from invocation steps.
    
    This is a safe fallback when both Galaxy API and file-based lookup fail.
    The invocation always has workflow_step_uuid for each step.
    
    We only include steps that have jobs (tool steps that have been scheduled),
    filtering out input steps and steps that haven't been scheduled yet.
    """
    steps = galaxy_inv.get("steps", [])
    tool_step_uuids = set()
    
    for step in steps:
        step_uuid = step.get("workflow_step_uuid")
        # Include if step has a job_id or jobs array (indicates it's a scheduled tool step)
        has_job = step.get("job_id") or step.get("jobs")
        if step_uuid and has_job:
            tool_step_uuids.add(step_uuid)
    
    return tool_step_uuids


def _get_expected_tool_step_uuids(
    galaxy_client: GalaxyClient,
    workflow_name: str,
    cache: Dict[str, set],
    galaxy_inv: Optional[Dict] = None
) -> set:
    """
    Get expected tool step UUIDs for a workflow.
    
    Tries in order:
    1. Galaxy API (get_workflow_structure)
    2. Local .ga file (get_workflow_structure_from_file)
    3. Invocation steps (fallback when both above fail)
    
    Args:
        galaxy_client: Connected Galaxy client
        workflow_name: Name of the workflow
        cache: Cache dict to avoid repeated lookups
        galaxy_inv: Optional invocation dict for fallback extraction
    
    Returns:
        Set of expected tool step UUIDs
    """
    expected_step_uuids = cache.get(workflow_name)
    if expected_step_uuids is not None:
        return expected_step_uuids

    source = "unknown"
    
    try:
        # Method 1: Try Galaxy API
        workflow_def = galaxy_client.get_workflow_structure(workflow_name)
        expected_step_uuids = _extract_step_uuids_from_workflow_def(workflow_def)
        if expected_step_uuids:
            source = "galaxy_api"
        else:
            # Method 2: Try local .ga file
            logger.debug(
                f"Galaxy API returned no step UUIDs for {workflow_name}, "
                f"trying file fallback at {galaxy_client.workflows_path}"
            )
            try:
                workflow_def = galaxy_client.get_workflow_structure_from_file(workflow_name)
                expected_step_uuids = _extract_step_uuids_from_workflow_def(workflow_def)
                if expected_step_uuids:
                    source = "local_file"
                    logger.info(
                        f"Resolved {len(expected_step_uuids)} step UUIDs from local file for {workflow_name}"
                    )
            except FileNotFoundError as fe:
                logger.warning(
                    f"Workflow file not found for {workflow_name}: {fe}. "
                    f"workflows_path={galaxy_client.workflows_path}, "
                    f"registry={list(galaxy_client.workflow_registry.keys())}"
                )
            except KeyError as ke:
                logger.warning(
                    f"Workflow not in registry for {workflow_name}: {ke}. "
                    f"registry={list(galaxy_client.workflow_registry.keys())}"
                )
    except Exception as e:
        expected_step_uuids = set()
        logger.warning(f"Unable to resolve workflow steps for {workflow_name}: {e}")

    # Method 3: Fallback to invocation steps if still empty
    if not expected_step_uuids and galaxy_inv:
        expected_step_uuids = _extract_step_uuids_from_invocation(galaxy_inv)
        if expected_step_uuids:
            source = "invocation_fallback"
            logger.info(
                f"Using invocation-based fallback: {len(expected_step_uuids)} step UUIDs for {workflow_name}"
            )

    if expected_step_uuids:
        logger.info(
            f"Resolved {len(expected_step_uuids)} expected step UUIDs for {workflow_name} (source={source})"
        )
    else:
        logger.warning(
            f"No step UUIDs resolved for {workflow_name}. "
            f"workflows_path={galaxy_client.workflows_path}, "
            f"registry_count={len(galaxy_client.workflow_registry)}"
        )

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
    error_jobs = [j for j in jobs if j.get('state') in ERROR_JOB_STATES]
    paused_jobs = [j for j in jobs if j.get('state') in PAUSED_JOB_STATES]
    has_failures = bool(error_jobs or paused_jobs)

    # When all jobs are terminal and either all steps are terminal or there are
    # failures (error/paused jobs cause downstream steps to stay in scheduled/new
    # state forever - Galaxy never transitions them), the workflow is done.
    if all_jobs_terminal and (all_expected_steps_terminal or has_failures):
        if error_jobs:
            paused_info = f", {len(paused_jobs)} paused (blocked)" if paused_jobs else ""
            logger.info(
                f"Workflow {invocation_id} finished (job+step-based): "
                f"{len(error_jobs)} failed jobs{paused_info}"
            )
            return True, 'error', all_expected_steps_terminal
        if paused_jobs and not error_jobs:
            # All non-ok jobs are paused with no explicit errors - still a failure
            logger.warning(
                f"Workflow {invocation_id} has {len(paused_jobs)} paused jobs with no "
                f"explicit errors - marking as error (paused jobs won't resume)"
            )
            return True, 'error', all_expected_steps_terminal
        logger.info(
            f"Workflow {invocation_id} finished (job+step-based): all {len(jobs)} jobs ok"
        )
        return True, 'ok', all_expected_steps_terminal

    return False, None, all_expected_steps_terminal


def _should_discard_missing_invocation(supabase_inv) -> bool:
    if supabase_inv.status in TERMINAL_WORKFLOW_STATES:
        return False
    if supabase_inv.dataset_id is not None:
        return False
    created_at = supabase_inv.created_at
    if not created_at:
        return False
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    age = datetime.now(timezone.utc) - created_at
    return age >= MISSING_INVOCATION_DISCARD_AFTER


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
                    if _should_discard_missing_invocation(supabase_inv):
                        stats['status_updated'] += 1
                        supabase_client.update_workflow_invocation(
                            supabase_inv.invocation_id,
                            status="discarded",
                            finished_at=datetime.now(timezone.utc),
                        )
                        logger.info(
                            f"Discarded missing invocation {supabase_inv.invocation_id} "
                            f"(created_at={supabase_inv.created_at})"
                        )
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
                        galaxy_inv,  # Pass invocation for fallback extraction
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
                    error_count = sum(1 for j in jobs if j.get('state') in ERROR_JOB_STATES)
                    paused_count = sum(1 for j in jobs if j.get('state') in PAUSED_JOB_STATES)
                    logger.debug(
                        f"Workflow {supabase_inv.invocation_id}: Galaxy state={galaxy_status}, "
                        f"{ok_count}/{len(jobs)} jobs ok, {running_count} running, "
                        f"{error_count} error, {paused_count} paused, "
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
        
        # Collect detailed info for failed and paused jobs
        failed_jobs: List[FailedJob] = []
        paused_tool_names: List[str] = []
        
        for job in jobs:
            job_state = job.get('state', '')
            tool_id = job.get('tool_id', '')
            tool_name = tool_id.split("/")[-2] if "/" in tool_id else tool_id
            
            if job_state in ['error', 'failed']:
                job_id = job.get('id')
                
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
            elif job_state == 'paused':
                paused_tool_names.append(tool_name)
        
        if not failed_jobs and not paused_tool_names:
            logger.warning(f"No failed or paused jobs found for invocation {invocation_id}, skipping Linear issue")
            return
        
        # If only paused jobs (no explicit errors), create a synthetic FailedJob entry
        if not failed_jobs and paused_tool_names:
            failed_jobs.append(FailedJob(
                tool_id="unknown",
                tool_name="unknown (paused)",
                exit_code=None,
                stderr=f"All blocked jobs are paused with no explicit error. "
                       f"Paused tools: {', '.join(paused_tool_names)}",
                stdout="",
                job_messages=[],
            ))
        elif paused_tool_names and failed_jobs:
            # Add paused job context to the last failed job's stderr for visibility
            paused_note = (
                f"\n\n--- Blocked downstream jobs ({len(paused_tool_names)}) ---\n"
                f"The following tools were paused (blocked) due to upstream failures:\n"
                + "\n".join(f"  - {name}" for name in paused_tool_names)
            )
            last_job = failed_jobs[-1]
            failed_jobs[-1] = FailedJob(
                tool_id=last_job.tool_id,
                tool_name=last_job.tool_name,
                exit_code=last_job.exit_code,
                stderr=(last_job.stderr or "") + paused_note,
                stdout=last_job.stdout,
                job_messages=last_job.job_messages,
            )
        
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

