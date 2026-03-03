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

from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.core.config import StorageConfig
from trees_api.integrations.galaxy.client import GalaxyClient

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
    },
    # Galaxy EU version - same outputs as EndToEndPipeline
    "EndToEndPipeline-GalaxyEU": {
        "standard": [
            "standardized.laz"
        ],
        "metadata": [
            "collection_summary.json",
            "{item_id}.json",
            "{item_id}.geojson"
        ],
        "overviews": [
            "top_view_00.png",
            "top_view_01.png",
            "section_ew.png",
            "section_ns.png"
        ],
        "segmentation": [
            "segmented.laz"
        ],
        "potree": [
            "metadata.json",
            "hierarchy.bin",
            "octree.bin"
        ]
    }
}


# Potree files that Galaxy exports with wrong extension
# Galaxy uses .binary extension instead of .bin required by Potree viewer
POTREE_FILE_RENAMES = {
    "hierarchy.binary": "hierarchy.bin",
    "octree.binary": "octree.bin",
}


def fix_potree_file_extensions(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    s3_base_path: str
) -> Dict[str, bool]:
    """
    Rename potree files from .binary to .bin extension.
    
    Galaxy outputs potree files with .binary extension due to its datatype system,
    but the Potree viewer expects .bin extension.
    
    Args:
        storage_client: Connected storage client
        storage_config: Storage configuration (for visualization bucket)
        s3_base_path: Base S3 path (e.g., "324/")
        
    Returns:
        Dict mapping filename to success status
    """
    results = {}
    potree_path = f"{s3_base_path}potree/"
    
    for old_name, new_name in POTREE_FILE_RENAMES.items():
        old_key = f"{potree_path}{old_name}"
        new_key = f"{potree_path}{new_name}"
        
        # Check if .binary file exists and .bin doesn't
        try:
            binary_exists = storage_client.file_exists(
                old_key, bucket=storage_config.bucket_name_visualization
            )
            bin_exists = storage_client.file_exists(
                new_key, bucket=storage_config.bucket_name_visualization
            )
            
            if binary_exists and not bin_exists:
                # Rename .binary to .bin
                success = storage_client.rename_object(
                    old_key, new_key,
                    bucket=storage_config.bucket_name_visualization
                )
                results[old_name] = success
                if success:
                    logger.info(f"Renamed potree file: {old_key} -> {new_key}")
                else:
                    logger.warning(f"Failed to rename potree file: {old_key}")
            elif bin_exists:
                # .bin already exists, no action needed
                results[old_name] = True
                logger.debug(f"Potree file already has correct extension: {new_key}")
            else:
                # Neither exists
                results[old_name] = False
                logger.debug(f"Potree file not found: {old_key}")
                
        except Exception as e:
            logger.warning(f"Error checking/renaming potree file {old_name}: {e}")
            results[old_name] = False
    
    return results


def build_outputs_structure(
    workflow_name: str,
    s3_base_path: str,
    item_ids: Optional[List[int]] = None
) -> Dict[str, Any]:
    """
    Build the outputs structure for a workflow based on deterministic paths.
    
    For collection workflows (EndToEndPipeline):
    - s3_base_path = {dataset_id}/
    - Most outputs: Galaxy's collection export appends element identifier (item_id)
      - Final path: {s3_base_path}{product_type}/{item_id}/{filename}
    - 3dtiles outputs: CONSOLIDATED per dataset (no item_id)
      - Final path: {s3_base_path}3dtiles/{filename}
      - All items are merged into a single 3D Tiles tileset
    - potree outputs: CONSOLIDATED per dataset (no item_id)
      - Final path: {s3_base_path}potree/{filename}
    
    Args:
        workflow_name: Name of the workflow
        s3_base_path: Base S3 path (e.g., "324/" for collection workflows)
        item_ids: List of dataset_item IDs for collection workflows
        
    Returns:
        Dict with outputs structure for galaxy_histories.outputs
    """
    expected = EXPECTED_OUTPUTS.get(workflow_name, {})
    
    # Product types that are consolidated per dataset (not per item)
    CONSOLIDATED_PRODUCTS = {"3dtiles", "potree"}
    
    outputs = {}
    
    # Collection workflow with item_ids
    if item_ids:
        for product_type, files in expected.items():
            if product_type in CONSOLIDATED_PRODUCTS:
                # Consolidated outputs: single output per dataset (no item_id in path)
                product_outputs = []
                for filename in files:
                    full_path = f"{s3_base_path}{product_type}/{filename}"
                    product_outputs.append(full_path)
                outputs[product_type] = product_outputs
            else:
                # Per-item outputs: Collection path with item_id
                product_outputs = {}
                for item_id in item_ids:
                    item_outputs = []
                    for filename in files:
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
    - Ingests collection_summary from {s3_base_path}standard/collection_summary.json
    - Ingests metadata for each item from {s3_base_path}standard/{item_id}/metadata.json
    - Stores collection_summary in outputs["metadata"]["collection_summary"]
    - Stores per-item metadata in outputs["metadata"][item_id]
    
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
    from trees_api.core.workflow_config import _process_collection_summary
    
    if item_ids:
        # Collection workflow - first try to ingest collection_summary.json
        all_metadata = {}
        
        # Try to ingest collection summary (collection-level metadata)
        collection_summary = _ingest_collection_summary(
            storage_client, storage_config, s3_base_path
        )
        if collection_summary:
            all_metadata["collection_summary"] = collection_summary
            logger.info(f"Ingested collection_summary for {s3_base_path}")
        
        # Then ingest per-item metadata
        for item_id in item_ids:
            # Try new path first (metadata/{item_id}.json), then legacy (standard/{item_id}/)
            item_metadata = _ingest_item_metadata(
                storage_client, storage_config,
                s3_base_path, item_id
            )
            if item_metadata:
                all_metadata[str(item_id)] = item_metadata
        
        if all_metadata:
            outputs["metadata"] = all_metadata
            logger.info(f"Ingested metadata for {len(all_metadata) - (1 if collection_summary else 0)} items")
    else:
        # Legacy single-file workflow
        metadata = _ingest_single_metadata(
            storage_client, storage_config,
            f"{s3_base_path}standard/"
        )
        if metadata:
            outputs["metadata"] = metadata
    
    return outputs


def _ingest_collection_summary(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    s3_base_path: str
) -> Optional[Dict[str, Any]]:
    """
    Ingest collection_summary.json from S3 and process it.
    
    Args:
        storage_client: Connected storage client
        storage_config: Storage configuration
        s3_base_path: Base S3 path (e.g., "369/")
        
    Returns:
        Processed collection summary dict or None
    """
    from trees_api.core.workflow_config import _process_collection_summary
    
    # Try both metadata/ and standard/ paths (metadata/ is the new location)
    paths_to_try = [
        f"{s3_base_path}metadata/collection_summary.json",  # New location
        f"{s3_base_path}standard/collection_summary.json",  # Legacy location
    ]
    
    for summary_path in paths_to_try:
        try:
            summary_data = storage_client.download_json(
                summary_path,
                bucket=storage_config.bucket_name_products
            )
            if summary_data:
                # Process the raw collection summary
                processed = _process_collection_summary(summary_data)
                if processed:
                    logger.info(f"Successfully ingested collection_summary from {summary_path}")
                    return processed
                # If processing fails, return raw data
                return summary_data
        except Exception as e:
            logger.debug(f"Could not ingest {summary_path}: {e}")
    
    return None


def _ingest_item_metadata(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    s3_base_path: str,
    item_id: int
) -> Optional[Dict[str, Any]]:
    """
    Ingest metadata for a single item, trying both new and legacy paths.
    
    New structure: {s3_base_path}metadata/{item_id}.json and {item_id}.geojson
    Legacy structure: {s3_base_path}standard/{item_id}/metadata.json and convex_hull.geojson
    """
    metadata = {}
    
    # Try new paths first (metadata/{item_id}.json)
    new_metadata_path = f"{s3_base_path}metadata/{item_id}.json"
    new_geojson_path = f"{s3_base_path}metadata/{item_id}.geojson"
    
    try:
        # Try new JSON metadata path
        json_data = storage_client.download_json(
            new_metadata_path,
            bucket=storage_config.bucket_name_products
        )
        if json_data:
            # New format stores all metadata in one file
            metadata = json_data
            logger.debug(f"Ingested item metadata from {new_metadata_path}")
    except Exception as e:
        logger.debug(f"Could not ingest {new_metadata_path}: {e}")
    
    try:
        # Try new GeoJSON path for convex hull
        geojson_data = storage_client.download_json(
            new_geojson_path,
            bucket=storage_config.bucket_name_products
        )
        if geojson_data:
            metadata["convex_hull"] = geojson_data
            logger.debug(f"Ingested convex_hull from {new_geojson_path}")
    except Exception as e:
        logger.debug(f"Could not ingest {new_geojson_path}: {e}")
    
    if metadata:
        return metadata
    
    # Fall back to legacy path
    legacy_metadata = _ingest_single_metadata(
        storage_client, storage_config,
        f"{s3_base_path}standard/{item_id}/"
    )
    return legacy_metadata


def _ingest_single_metadata(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    standard_path: str
) -> Optional[Dict[str, Any]]:
    """
    Ingest metadata JSON files from a single path (legacy format).
    
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
            metadata_path,
            bucket=storage_config.bucket_name_products
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
            convex_hull_path,
            bucket=storage_config.bucket_name_products
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
    dataset_id: int,
    galaxy_client: Optional[GalaxyClient] = None,
    delete_history_after_sync: bool = True
) -> bool:
    """
    Sync outputs for a single workflow invocation to its galaxy_history.
    
    For collection workflows, queries all dataset_items for the dataset
    and builds outputs for each item.
    
    After successful sync, optionally deletes the Galaxy history to prevent
    accumulation of datasets in Galaxy.
    
    Args:
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: Storage configuration
        invocation_id: Galaxy invocation ID
        workflow_name: Name of the workflow
        history_fk: ID of the galaxy_histories record
        dataset_id: ID of the dataset (for querying items)
        galaxy_client: Optional Galaxy client for history cleanup
        delete_history_after_sync: If True, delete Galaxy history after successful sync
        
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
        if workflow_name in ["EndToEndPipeline", "EndToEndPipeline-GalaxyEU"]:  # Collection-based workflows
            items_response = supabase_client.client.table("dataset_items").select(
                "id"
            ).eq("dataset_id", dataset_id).order("id").execute()
            
            if items_response.data:
                item_ids = [item["id"] for item in items_response.data]
                logger.info(f"Building outputs for {len(item_ids)} items in dataset {dataset_id}")
        
        # Build outputs structure from deterministic paths
        outputs = build_outputs_structure(workflow_name, s3_base_path, item_ids)
        
        # Fix potree file extensions (.binary -> .bin) for Galaxy EU workflows
        # Galaxy exports files with .binary extension, but Potree viewer expects .bin
        if workflow_name == "EndToEndPipeline-GalaxyEU" and "potree" in outputs:
            logger.info(f"Fixing potree file extensions for dataset {dataset_id}")
            rename_results = fix_potree_file_extensions(
                storage_client, storage_config, s3_base_path
            )
            if all(rename_results.values()):
                logger.info(f"Potree files renamed successfully: {rename_results}")
            else:
                logger.warning(f"Some potree files could not be renamed: {rename_results}")
        
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
        
        # Delete Galaxy history to prevent accumulation of datasets
        logger.info(f"History deletion check: delete_history_after_sync={delete_history_after_sync}, galaxy_client_available={galaxy_client is not None}")
        if delete_history_after_sync and galaxy_client:
            try:
                galaxy_history_id = history.get("history_id")
                logger.info(f"Galaxy history_id from record: {galaxy_history_id}")
                if galaxy_history_id:
                    logger.info(f"Deleting Galaxy history {galaxy_history_id} after successful sync")
                    deletion_result = galaxy_client.delete_history(galaxy_history_id, purge=True)
                    logger.info(f"Galaxy history deletion result: {deletion_result}")
                    if deletion_result:
                        logger.info(f"Galaxy history {galaxy_history_id} deleted successfully")
                    else:
                        logger.warning(f"Failed to delete Galaxy history {galaxy_history_id}")
                else:
                    logger.warning(f"No history_id in history record: {history}")
            except Exception as cleanup_error:
                # Don't fail the sync if cleanup fails
                logger.warning(f"Error during history cleanup for {invocation_id}: {cleanup_error}")
        elif not delete_history_after_sync:
            logger.info("History deletion disabled (delete_history_after_sync=False)")
        elif not galaxy_client:
            logger.warning("Cannot delete history: galaxy_client is None")
        
        return True
        
    except Exception as e:
        logger.error(f"Error syncing history for invocation {invocation_id}: {e}")
        return False


def sync_history_outputs(
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    storage_config: StorageConfig,
    galaxy_client: Optional[GalaxyClient] = None,
    delete_history_after_sync: bool = True
) -> Dict[str, int]:
    """
    Sync outputs for all finished workflows that haven't been synced yet.
    
    Finds workflows where:
    - status is 'ok' (successfully completed)
    - results_synced is False
    - history_fk is not null
    
    After successful sync, optionally deletes the Galaxy history to prevent
    accumulation of datasets in Galaxy.
    
    Args:
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: Storage configuration
        galaxy_client: Optional Galaxy client for history cleanup
        delete_history_after_sync: If True, delete Galaxy history after successful sync
        
    Returns:
        Dict with sync statistics
    """
    stats = {
        'workflows_checked': 0,
        'outputs_synced': 0,
        'histories_deleted': 0,
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
                dataset_id,
                galaxy_client=galaxy_client,
                delete_history_after_sync=delete_history_after_sync
            )
            
            if success:
                stats['outputs_synced'] += 1
                if delete_history_after_sync and galaxy_client:
                    stats['histories_deleted'] += 1
            else:
                stats['errors'] += 1
        
        logger.info(f"History sync completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error during history sync: {e}")
        stats['errors'] += 1
        return stats
