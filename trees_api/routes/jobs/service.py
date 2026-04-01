from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import HTTPException

from trees_api.core.config import SupabaseConfig
from trees_api.core.workflow_config import build_workflow_parameters
from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient

logger = logging.getLogger("trees_api.routes.jobs.service")
PROCESSOR_EMAIL = "processor@3dtrees.earth"


def _is_processor_user(
    *,
    requesting_user_id: str,
    requesting_user_email: str,
) -> bool:
    processor_user_id = (SupabaseConfig().processor_user_id or "").strip().lower()
    if processor_user_id and (requesting_user_id or "").strip().lower() == processor_user_id:
        return True

    # Keep the legacy email path during migration so existing environments continue to work.
    return (requesting_user_email or "").strip().lower() == PROCESSOR_EMAIL


def _can_access_dataset_jobs(
    *,
    dataset: Dict[str, Any],
    dataset_id: int,
    requesting_user_id: str,
    requesting_user_email: str,
    required_permission: str,
    supabase: SupabaseClient,
) -> bool:
    owner_id = dataset.get("user_id")
    if owner_id == requesting_user_id:
        return True
    if _is_processor_user(
        requesting_user_id=requesting_user_id,
        requesting_user_email=requesting_user_email,
    ):
        return True

    has_platform_admin = supabase.has_platform_dataset_admin(requesting_user_id)
    if has_platform_admin:
        return True

    return supabase.has_dataset_user_access(
        requesting_user_id,
        dataset_id,
        required_permission=required_permission,
    )


def invoke_workflow_with_collection(
    galaxy: GalaxyClient,
    supabase: SupabaseClient,
    workflow_name: str,
    dataset_id: int,
    history_name: str,
    workflow_parameters: Dict[int, Dict[str, str]],
    user_parameters: dict,
    preferred_object_store_id: Optional[str] = None,
    preferred_intermediate_object_store_id: Optional[str] = None,
    preferred_outputs_object_store_id: Optional[str] = None,
    history_id: Optional[str] = None,
    history_fk: Optional[int] = None,
):
    """
    Invoke Galaxy workflow with collection input and persist invocation metadata.
    """
    try:
        logger.info(
            "Invoking workflow '%s' with collection for dataset_id=%s (history_id=%s)",
            workflow_name,
            dataset_id,
            history_id,
        )
        invocation_result = galaxy.invoke_workflow_with_collection(
            workflow_name=workflow_name,
            dataset_id=dataset_id,
            supabase_client=supabase,
            history_name=history_name if not history_id else None,
            history_id=history_id,
            parameters=workflow_parameters if workflow_parameters else None,
            preferred_object_store_id=preferred_object_store_id,
            preferred_intermediate_object_store_id=preferred_intermediate_object_store_id,
            preferred_outputs_object_store_id=preferred_outputs_object_store_id,
        )
        logger.info(
            "Workflow invoked successfully: %s", invocation_result["invocation_id"]
        )
    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail=f"Invoking workflow {workflow_name} with collection failed: {error}",
        ) from error

    try:
        workflow_invocation = supabase.create_workflow_invocation(
            workflow_uuid=invocation_result["invocation_id"],
            dataset_id=dataset_id,
            workflow_name=workflow_name,
            history_fk=history_fk,
        )
        if user_parameters:
            supabase.update_workflow_invocation(
                workflow_invocation.invocation_id,
                parameters=user_parameters,
            )
        return workflow_invocation
    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail=f"Creating workflow invocation in Supabase failed: {error}",
        ) from error


def create_job(
    *,
    dataset_id: str,
    workflow_name: str,
    overwrite: bool,
    parameters: dict,
    requesting_user_id: str,
    requesting_user_email: str,
    galaxy: Optional[GalaxyClient],
    supabase: Optional[SupabaseClient],
    storage: Optional[StorageClient],
):
    """
    Create a workflow job for a dataset with collection-based invocation.
    """
    if not galaxy:
        raise HTTPException(
            status_code=503,
            detail="Galaxy service is unavailable. Please check /health for details.",
        )
    if not supabase:
        raise HTTPException(
            status_code=503,
            detail="Supabase service is unavailable. Please check /health for details.",
        )
    if not storage:
        logger.warning(
            "Storage service is unavailable - this is OK since Galaxy accesses S3 directly"
        )

    try:
        dataset_id_int = int(dataset_id)
    except (TypeError, ValueError) as error:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid dataset_id '{dataset_id}'",
        ) from error

    dataset = supabase.get_dataset_with_items(dataset_id_int)
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset {dataset_id_int} not found",
        )
    if dataset.get("archived"):
        raise HTTPException(
            status_code=400,
            detail=f"Dataset {dataset_id_int} is archived",
        )
    try:
        can_trigger_job = _can_access_dataset_jobs(
            dataset=dataset,
            dataset_id=dataset_id_int,
            requesting_user_id=requesting_user_id,
            requesting_user_email=requesting_user_email,
            required_permission="edit",
            supabase=supabase,
        )
    except Exception as error:
        logger.error("Failed to resolve job trigger permissions: %s", error)
        raise HTTPException(
            status_code=503,
            detail="Could not verify job trigger permissions",
        ) from error

    if not can_trigger_job:
        raise HTTPException(
            status_code=403,
            detail="Job trigger access denied for this dataset",
        )

    dataset_items = dataset.get("dataset_items") or []
    if not dataset_items:
        raise HTTPException(
            status_code=404,
            detail=f"No dataset_items found for dataset {dataset_id_int}",
        )
    first_item_id = dataset_items[0]["id"]

    logger.info(
        "Creating collection workflow job for dataset_id=%s (first_item_id=%s)",
        dataset_id_int,
        first_item_id,
    )

    if overwrite:
        logger.info("Overwrite mode: cleaning up existing data for dataset %s", dataset_id)
        deleted_count = supabase.delete_workflow_invocations_by_dataset(dataset_id_int)
        if deleted_count > 0:
            logger.info("Deleted %s old workflow invocation(s)", deleted_count)

        old_history_id = supabase.delete_galaxy_history_by_dataset(dataset_id_int)
        if old_history_id:
            if galaxy.delete_history(old_history_id, purge=True):
                logger.info("Deleted old Galaxy history %s", old_history_id)
            else:
                logger.warning(
                    "Failed to delete Galaxy history %s - may be orphaned",
                    old_history_id,
                )

    preferred_object_store_id = None
    preferred_intermediate_object_store_id = None
    preferred_outputs_object_store_id = None
    if workflow_name == "EndToEndPipeline-GalaxyEU":
        preferred_object_store_id = (
            galaxy.config.default_object_store_id or "s3_scratch_netapp01"
        )
        preferred_intermediate_object_store_id = (
            galaxy.config.default_intermediate_object_store_id
        )
        preferred_outputs_object_store_id = galaxy.config.default_outputs_object_store_id

    history_name = f"{workflow_name} - Dataset {dataset_id}"
    existing_history = supabase.get_galaxy_history_by_dataset(dataset_id_int)

    try:
        if existing_history:
            galaxy_history_id = existing_history["history_id"]
            galaxy_history_fk = existing_history["id"]
            s3_base_path = existing_history.get("s3_base_path", f"{dataset_id}/")
            logger.info(
                "Reusing existing Galaxy history %s for dataset %s",
                galaxy_history_id,
                dataset_id,
            )
            if preferred_object_store_id:
                galaxy.set_history_preferred_object_store(
                    galaxy_history_id, preferred_object_store_id
                )
        else:
            new_history = galaxy.create_history(
                name=history_name,
                preferred_object_store_id=preferred_object_store_id,
            )
            galaxy_history_id = new_history.id
            s3_base_path = f"{dataset_id}/"
            history_record = supabase.get_or_create_galaxy_history(
                dataset_id=dataset_id_int,
                history_id=galaxy_history_id,
                history_name=history_name,
                s3_base_path=s3_base_path,
            )
            galaxy_history_fk = history_record["id"]
            logger.info(
                "Created new Galaxy history %s for dataset %s",
                galaxy_history_id,
                dataset_id,
            )
    except Exception as error:
        raise HTTPException(
            status_code=500, detail=f"Failed to prepare Galaxy history: {error}"
        ) from error

    workflow_parameters = build_workflow_parameters(
        galaxy_client=galaxy,
        supabase_client=supabase,
        workflow_name=workflow_name,
        dataset_id=dataset_id_int,
        s3_base_path=s3_base_path,
    )

    return invoke_workflow_with_collection(
        galaxy=galaxy,
        supabase=supabase,
        workflow_name=workflow_name,
        dataset_id=dataset_id_int,
        history_name=history_name,
        workflow_parameters=workflow_parameters,
        user_parameters=parameters,
        preferred_object_store_id=preferred_object_store_id,
        preferred_intermediate_object_store_id=preferred_intermediate_object_store_id,
        preferred_outputs_object_store_id=preferred_outputs_object_store_id,
        history_id=galaxy_history_id,
        history_fk=galaxy_history_fk,
    )


def list_jobs(
    *,
    dataset_id: Optional[int],
    requesting_user_id: str,
    requesting_user_email: str,
    limit: int,
    offset: int,
    supabase: Optional[SupabaseClient],
):
    if not supabase:
        raise HTTPException(
            status_code=503,
            detail="Supabase service is unavailable. Please check /health for details.",
        )

    if dataset_id is not None:
        dataset = supabase.get_dataset_with_items(dataset_id)
        if not dataset:
            return []
        try:
            can_view_jobs = _can_access_dataset_jobs(
                dataset=dataset,
                dataset_id=dataset_id,
                requesting_user_id=requesting_user_id,
                requesting_user_email=requesting_user_email,
                required_permission="read",
                supabase=supabase,
            )
        except Exception as error:
            logger.error("Failed to resolve job listing permissions: %s", error)
            raise HTTPException(
                status_code=503,
                detail="Could not verify job listing permissions",
            ) from error

        if not can_view_jobs:
            return []
        return supabase.get_workflow_invocations_by_dataset_id(
            dataset_id, limit=limit, offset=offset
        )

    if _is_processor_user(
        requesting_user_id=requesting_user_id,
        requesting_user_email=requesting_user_email,
    ):
        return supabase.get_workflow_invocations(limit=limit, offset=offset)

    try:
        has_platform_admin = supabase.has_platform_dataset_admin(requesting_user_id)
    except Exception as error:
        logger.error("Failed to resolve platform admin permissions for job listing: %s", error)
        raise HTTPException(
            status_code=503,
            detail="Could not verify job listing permissions",
        ) from error

    if has_platform_admin:
        return supabase.get_workflow_invocations(limit=limit, offset=offset)

    datasets = supabase.get_datasets(user_id=requesting_user_id)
    dataset_ids = [dataset.id for dataset in datasets if dataset.id is not None]

    all_invocations = []
    for row_dataset_id in dataset_ids:
        user_invocations = supabase.get_workflow_invocations_by_dataset_id(
            row_dataset_id, limit=1000
        )
        all_invocations.extend(user_invocations)

    all_invocations.sort(key=lambda row: row.created_at, reverse=True)
    return all_invocations[offset : offset + limit]
