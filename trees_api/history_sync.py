"""
Galaxy history output synchronization module.

This module handles syncing outputs to galaxy_histories.outputs JSONB
when workflows complete. It uses deterministic paths based on s3_base_path
rather than scanning S3.

Also handles ingesting metadata JSON files from S3 into the outputs structure.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient
from trees_api.config import StorageConfig

logger = logging.getLogger("uvicorn")


# Expected output files for each product type
# These are the files that Galaxy exports for each workflow step
EXPECTED_OUTPUTS = {
    "EndToEndPipeline": {
        "standard": [
            "standardized.laz",
            "metadata.json",
            "convex_hull.geojson"
        ],
        "overviews": [
            "top_view_00.png",
            "top_view_01.png",
            "section_ew.png",
            "section_ns.png",
            "overview_animation.gif"
        ],
        "segmentation": [
            "segmented.laz"
        ],
        "3dtiles": [
            "tileset.json",
            "preview.pnts",
            "points/"  # Directory containing .pnts files
        ]
    },
    "Standard": {
        "standard": [
            "standardized.laz",
            "metadata.json",
            "convex_hull.geojson"
        ]
    },
    "Overviews": {
        "overviews": [
            "top_view_00.png",
            "top_view_01.png",
            "section_ew.png",
            "section_ns.png",
            "overview_animation.gif"
        ]
    },
    "Segmentation": {
        "segmentation": [
            "segmented.laz"
        ]
    },
    "Py3DTiles": {
        "3dtiles": [
            "tileset.json",
            "preview.pnts",
            "points/"
        ]
    }
}


def build_outputs_structure(
    workflow_name: str,
    s3_base_path: str,
    item_ids: Optional[List[int]] = None
) -> Dict[str, Any]:
    """
    Build the outputs structure for a workflow based on deterministic paths.
    
    For collection workflows (EndToEndPipeline):
    - s3_base_path = {dataset_id}/
    - Galaxy's collection export appends element identifier (item_id)
    - Final path: {s3_base_path}{product_type}/{item_id}/{filename}
    
    Args:
        workflow_name: Name of the workflow
        s3_base_path: Base S3 path (e.g., "324/" for collection workflows)
        item_ids: List of dataset_item IDs for collection workflows
        
    Returns:
        Dict with outputs structure for galaxy_histories.outputs
    """
    expected = EXPECTED_OUTPUTS.get(workflow_name, {})
    
    outputs = {}
    
    # Collection workflow with item_ids
    if item_ids:
        for product_type, files in expected.items():
            product_outputs = {}
            for item_id in item_ids:
                item_outputs = []
                for filename in files:
                    # Collection path: s3_base_path + product_type + item_id + filename
                    full_path = f"{s3_base_path}{product_type}/{item_id}/{filename}"
                    item_outputs.append(full_path)
                product_outputs[str(item_id)] = item_outputs
            outputs[product_type] = product_outputs
    else:
        # Legacy single-file workflow (backwards compatibility)
        for product_type, files in expected.items():
            product_outputs = []
            for filename in files:
                # Build full path: s3_base_path + product_type + filename
                full_path = f"{s3_base_path}{product_type}/{filename}"
                product_outputs.append(full_path)
            outputs[product_type] = product_outputs
    
    return outputs


def ingest_metadata_json(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    s3_base_path: str,
    outputs: Dict[str, Any],
    item_ids: Optional[List[int]] = None
) -> Dict[str, Any]:
    """
    Ingest metadata JSON files from S3 into the outputs structure.
    
    For collection workflows with item_ids:
    - Ingests metadata for each item from {s3_base_path}standard/{item_id}/metadata.json
    - Stores in outputs["metadata"][item_id]
    
    For legacy single-file workflows:
    - Ingests from {s3_base_path}standard/metadata.json
    - Stores in outputs["metadata"]
    
    Downloads the standard/metadata.json JSONL file and stores:
    - raw_las_info: full metadata record for original input (standardized=false)
    - standard_las_info: full metadata record after standardization (standardized=true)
    - logs: array of all log messages from the processing
    
    Args:
        storage_client: Connected storage client
        storage_config: Storage configuration
        s3_base_path: Base S3 path
        outputs: Existing outputs structure to update
        item_ids: List of dataset_item IDs for collection workflows
        
    Returns:
        Updated outputs with metadata content
    """
    if item_ids:
        # Collection workflow - ingest metadata for each item
        all_metadata = {}
        for item_id in item_ids:
            item_metadata = _ingest_single_metadata(
                storage_client, storage_config,
                f"{s3_base_path}standard/{item_id}/"
            )
            if item_metadata:
                all_metadata[str(item_id)] = item_metadata
        
        if all_metadata:
            outputs["metadata"] = all_metadata
            logger.info(f"Ingested metadata for {len(all_metadata)} items")
    else:
        # Legacy single-file workflow
        metadata = _ingest_single_metadata(
            storage_client, storage_config,
            f"{s3_base_path}standard/"
        )
        if metadata:
            outputs["metadata"] = metadata
    
    return outputs


def _ingest_single_metadata(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    standard_path: str
) -> Optional[Dict[str, Any]]:
    """
    Ingest metadata JSON files from a single path.
    
    Args:
        storage_client: Connected storage client
        storage_config: Storage configuration
        standard_path: Path to standard directory (ending with /)
        
    Returns:
        Dict with metadata content or None
    """
    metadata = {}
    
    # Try to ingest standard metadata.json (JSONL format)
    metadata_path = f"{standard_path}metadata.json"
    try:
        # Download and parse all records (logs + raw + standardized)
        logs, raw_record, standard_record = storage_client.download_jsonl_full(
            storage_config.products_bucket,
            metadata_path
        )
        
        # Store logs as array
        if logs:
            metadata["logs"] = logs
        
        # Store raw input las_info as-is
        if raw_record:
            metadata["raw_las_info"] = raw_record
        
        # Store standardized las_info as-is
        if standard_record:
            metadata["standard_las_info"] = standard_record
            
    except Exception as e:
        logger.debug(f"Could not ingest {metadata_path}: {e}")
    
    # Try to ingest convex hull GeoJSON
    convex_hull_path = f"{standard_path}convex_hull.geojson"
    try:
        convex_hull_json = storage_client.download_json(
            storage_config.products_bucket,
            convex_hull_path
        )
        if convex_hull_json:
            metadata["convex_hull"] = convex_hull_json
    except Exception as e:
        logger.debug(f"Could not ingest {convex_hull_path}: {e}")
    
    return metadata if metadata else None


def sync_history_for_invocation(
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    storage_config: StorageConfig,
    invocation_id: str,
    workflow_name: str,
    history_fk: int,
    dataset_id: int
) -> bool:
    """
    Sync outputs for a single workflow invocation to its galaxy_history.
    
    For collection workflows, queries all dataset_items for the dataset
    and builds outputs for each item.
    
    Args:
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: Storage configuration
        invocation_id: Galaxy invocation ID
        workflow_name: Name of the workflow
        history_fk: ID of the galaxy_histories record
        dataset_id: ID of the dataset (for querying items)
        
    Returns:
        True if sync was successful
    """
    try:
        # Get the galaxy_history record
        response = supabase_client.client.table("galaxy_histories").select(
            "id, history_id, s3_base_path, outputs"
        ).eq("id", history_fk).execute()
        
        if not response.data:
            logger.warning(f"Galaxy history {history_fk} not found")
            return False
        
        history = response.data[0]
        s3_base_path = history.get("s3_base_path", "")
        
        if not s3_base_path:
            logger.warning(f"No s3_base_path for history {history_fk}")
            return False
        
        # Get dataset_items for collection workflows
        item_ids = None
        if workflow_name in ["EndToEndPipeline"]:  # Collection-based workflows
            items_response = supabase_client.client.table("dataset_items").select(
                "id"
            ).eq("dataset_id", dataset_id).order("id").execute()
            
            if items_response.data:
                item_ids = [item["id"] for item in items_response.data]
                logger.info(f"Building outputs for {len(item_ids)} items in dataset {dataset_id}")
        
        # Build outputs structure from deterministic paths
        outputs = build_outputs_structure(workflow_name, s3_base_path, item_ids)
        
        # Optionally ingest metadata JSON files
        outputs = ingest_metadata_json(
            storage_client, storage_config, s3_base_path, outputs, item_ids
        )
        
        # Update galaxy_histories.outputs
        supabase_client.update_galaxy_history_outputs(
            history["history_id"],
            outputs
        )
        
        # Mark invocation as synced
        supabase_client.client.table("galaxy_workflow_invocations").update({
            "results_synced": True,
            "results_synced_at": datetime.now().isoformat()
        }).eq("invocation_id", invocation_id).execute()
        
        logger.info(f"Synced outputs for invocation {invocation_id} to history {history_fk}")
        return True
        
    except Exception as e:
        logger.error(f"Error syncing history for invocation {invocation_id}: {e}")
        return False


def sync_history_outputs(
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    storage_config: StorageConfig
) -> Dict[str, int]:
    """
    Sync outputs for all finished workflows that haven't been synced yet.
    
    Finds workflows where:
    - status is 'ok' (successfully completed)
    - results_synced is False
    - history_fk is not null
    
    Args:
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: Storage configuration
        
    Returns:
        Dict with sync statistics
    """
    stats = {
        'workflows_checked': 0,
        'outputs_synced': 0,
        'metadata_ingested': 0,
        'errors': 0
    }
    
    try:
        # Get finished workflows that need output sync (include dataset_id)
        response = supabase_client.client.table("galaxy_workflow_invocations").select(
            "invocation_id, workflow_name, history_fk, status, dataset_id"
        ).eq("status", "ok").eq("results_synced", False).not_.is_("history_fk", "null").execute()
        
        if not response.data:
            logger.info("No workflows need output sync")
            return stats
        
        workflows = response.data
        stats['workflows_checked'] = len(workflows)
        logger.info(f"Found {len(workflows)} workflows needing output sync")
        
        for workflow in workflows:
            invocation_id = workflow["invocation_id"]
            workflow_name = workflow["workflow_name"]
            history_fk = workflow["history_fk"]
            dataset_id = workflow["dataset_id"]
            
            success = sync_history_for_invocation(
                supabase_client,
                storage_client,
                storage_config,
                invocation_id,
                workflow_name,
                history_fk,
                dataset_id
            )
            
            if success:
                stats['outputs_synced'] += 1
            else:
                stats['errors'] += 1
        
        logger.info(f"History sync completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error during history sync: {e}")
        stats['errors'] += 1
        return stats
