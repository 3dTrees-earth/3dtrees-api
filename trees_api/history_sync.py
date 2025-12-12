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
    s3_base_path: str
) -> Dict[str, Any]:
    """
    Build the outputs structure for a workflow based on deterministic paths.
    
    Args:
        workflow_name: Name of the workflow
        s3_base_path: Base S3 path (e.g., "322/3022/")
        
    Returns:
        Dict with outputs structure for galaxy_histories.outputs
    """
    expected = EXPECTED_OUTPUTS.get(workflow_name, {})
    
    outputs = {}
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
    outputs: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Ingest metadata JSON files from S3 into the outputs structure.
    
    Downloads small JSON files and stores their content in outputs["metadata"].
    
    The standard/metadata.json file is in JSONL format with log messages
    followed by actual metadata records. We extract the final standardized
    metadata record which contains point_count, bbox, crs, dimensions.
    
    Args:
        storage_client: Connected storage client
        storage_config: Storage configuration
        s3_base_path: Base S3 path
        outputs: Existing outputs structure to update
        
    Returns:
        Updated outputs with metadata content
    """
    metadata = {}
    
    # Try to ingest standard metadata.json (JSONL format)
    metadata_path = f"{s3_base_path}standard/metadata.json"
    try:
        metadata_record = storage_client.download_jsonl_metadata(
            storage_config.products_bucket,
            metadata_path
        )
        if metadata_record:
            # Extract key fields for easy access
            standard_meta = {
                "point_count": metadata_record.get("point_count", [None])[0],
                "standardized": metadata_record.get("standardized", [False])[0],
            }
            
            # Extract bbox if available
            bbox = metadata_record.get("bbox")
            if bbox:
                standard_meta["bbox"] = {
                    "xmin": bbox.get("xmin", [None])[0],
                    "ymin": bbox.get("ymin", [None])[0],
                    "xmax": bbox.get("xmax", [None])[0],
                    "ymax": bbox.get("ymax", [None])[0],
                }
            
            # Extract CRS info if available
            crs_data = metadata_record.get("crs")
            if crs_data and isinstance(crs_data, list) and len(crs_data) > 0:
                # CRS is a JSON string that needs parsing
                import json
                try:
                    crs_parsed = json.loads(crs_data[0])
                    # Try to extract EPSG from input WKT
                    input_wkt = crs_parsed.get("input", [[""]])[0][0] if crs_parsed.get("input") else ""
                    if "EPSG" in input_wkt:
                        # Extract EPSG code - find the LAST AUTHORITY["EPSG","..."] which is the CRS code
                        import re
                        epsg_matches = re.findall(r'AUTHORITY\["EPSG","(\d+)"\]', input_wkt)
                        if epsg_matches:
                            # Last match is the CRS EPSG (e.g., 32632 for UTM)
                            standard_meta["epsg"] = int(epsg_matches[-1])
                    standard_meta["crs_wkt"] = input_wkt[:200] if input_wkt else None  # Truncate for storage
                except (json.JSONDecodeError, TypeError, KeyError):
                    pass
            
            # Extract Z bounds from dimensions for height info
            dimensions = metadata_record.get("dimensions", [])
            for dim in dimensions:
                if isinstance(dim, dict) and dim.get("name", [None])[0] == "Z":
                    standard_meta["z_min"] = dim.get("minimum", [None])[0]
                    standard_meta["z_max"] = dim.get("maximum", [None])[0]
                    break
            
            metadata["standard_metadata"] = standard_meta
            logger.info(f"Ingested standard metadata: point_count={standard_meta.get('point_count')}")
    except Exception as e:
        logger.warning(f"Could not ingest {metadata_path}: {e}")
    
    # Try to ingest convex hull GeoJSON
    convex_hull_path = f"{s3_base_path}standard/convex_hull.geojson"
    try:
        convex_hull_json = storage_client.download_json(
            storage_config.products_bucket,
            convex_hull_path
        )
        if convex_hull_json:
            metadata["convex_hull"] = convex_hull_json
            logger.debug(f"Ingested convex hull from {convex_hull_path}")
    except Exception as e:
        logger.debug(f"Could not ingest {convex_hull_path}: {e}")
    
    if metadata:
        outputs["metadata"] = metadata
    
    return outputs


def sync_history_for_invocation(
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    storage_config: StorageConfig,
    invocation_id: str,
    workflow_name: str,
    history_fk: int
) -> bool:
    """
    Sync outputs for a single workflow invocation to its galaxy_history.
    
    Args:
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: Storage configuration
        invocation_id: Galaxy invocation ID
        workflow_name: Name of the workflow
        history_fk: ID of the galaxy_histories record
        
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
        
        # Build outputs structure from deterministic paths
        outputs = build_outputs_structure(workflow_name, s3_base_path)
        
        # Optionally ingest metadata JSON files
        outputs = ingest_metadata_json(
            storage_client, storage_config, s3_base_path, outputs
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
        # Get finished workflows that need output sync
        response = supabase_client.client.table("galaxy_workflow_invocations").select(
            "invocation_id, workflow_name, history_fk, status"
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
            
            success = sync_history_for_invocation(
                supabase_client,
                storage_client,
                storage_config,
                invocation_id,
                workflow_name,
                history_fk
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
