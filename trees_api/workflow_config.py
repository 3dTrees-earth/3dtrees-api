"""
Workflow configuration and dynamic step resolution for 3DTrees API.

This module provides centralized workflow export configuration using annotation-based
step resolution to handle Galaxy's dynamic step ID renumbering.

Key Insight:
    Galaxy renumbers workflow steps after import, so hardcoded step IDs don't work.
    We use annotation matching to find the actual Galaxy step IDs at runtime.
    See: docs/issues/galaxy-dynamic-step-id-mapping.md
"""
import re
import logging
from typing import Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from trees_api.galaxy_client import GalaxyClient
    from trees_api.supabase_client import SupabaseClient

logger = logging.getLogger("uvicorn")


def _extract_las_info(data: any, standardized: bool) -> dict | None:
    """
    Extract las_info entry from tool_standard JSON log based on standardized flag.
    
    The tool_standard R script writes two JSON entries to the log file:
    - las_info_pre with standardized=false (raw file metadata)
    - las_info_post with standardized=true (standardized file metadata)
    
    Args:
        data: Parsed JSON data (can be list of entries or single dict)
        standardized: If True, extract post-standardization entry; if False, pre-standardization
        
    Returns:
        The matching las_info entry, or None if not found
    """
    if data is None:
        return None
    
    # Handle list of JSON entries (typical output format)
    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict) and entry.get("standardized") == standardized:
                return entry
        # If no match by flag, return first entry for raw or last for standardized
        if data:
            return data[0] if not standardized else data[-1]
        return None
    
    # Handle single dict (fallback)
    if isinstance(data, dict):
        # If it has the standardized flag, check it matches
        if "standardized" in data:
            return data if data.get("standardized") == standardized else None
        # Otherwise return as-is for raw, None for standardized
        return data if not standardized else None
    
    return None


# Annotation patterns to match export steps
# These must match the annotations in the .ga workflow files!
# Pattern keys are used for matching (case-insensitive, supports regex)
WORKFLOW_EXPORT_ANNOTATIONS = {
    "Standard": {
        "export standardized laz": "standard/{dataset_id}/{dataset_item_id}/",
        "export metadata json": "standard/{dataset_id}/{dataset_item_id}/",
        "export convex hull": "standard/{dataset_id}/{dataset_item_id}/"
    },
    "Segmentation": {
        "export segmented": "segmentation/{dataset_id}/{dataset_item_id}/"
    },
    "Overviews": {
        "export top view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export section view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export.*animation|export.*gif": "overviews/{dataset_id}/{dataset_item_id}/"
    },
    "Py3DTiles": {
        "export.*tileset": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export.*preview": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export.*points.*tiles|points.*subdirectory": "3dtiles/{dataset_id}/{dataset_item_id}/points/"
    },
    "EndToEndPipeline": {
        "export standardized laz": "standard/{dataset_id}/{dataset_item_id}/",
        "export metadata json": "standard/{dataset_id}/{dataset_item_id}/",
        "export convex hull": "standard/{dataset_id}/{dataset_item_id}/",
        "export top view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export section view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export overview.*gif|export overview animation": "overviews/{dataset_id}/{dataset_item_id}/",
        "export segmented laz": "segmentation/{dataset_id}/{dataset_item_id}/",
        "export tileset": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export preview": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export points tiles": "3dtiles/{dataset_id}/{dataset_item_id}/points/"
    },
    # Galaxy EU version - no metadata_json or convex_hull (not available in Galaxy EU tool version)
    "EndToEndPipeline-GalaxyEU": {
        "export standardized laz": "standard/{dataset_id}/{dataset_item_id}/",
        "export top view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export section view": "overviews/{dataset_id}/{dataset_item_id}/",
        "export overview.*gif|export overview animation": "overviews/{dataset_id}/{dataset_item_id}/",
        "export segmented laz": "segmentation/{dataset_id}/{dataset_item_id}/",
        "export tileset": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export preview": "3dtiles/{dataset_id}/{dataset_item_id}/",
        "export points tiles": "3dtiles/{dataset_id}/{dataset_item_id}/points/"
    }
}


# Workflow metadata ingestion configuration
# Maps workflow names to metadata extraction and database ingestion specs
WORKFLOW_METADATA_INGESTION = {
    "EndToEndPipeline": {
        "standard": {
            "metadata_files": ["metadata.json", "convex_hull_wgs84.GeoJSON"],
            "s3_path_template": "standard/{dataset_id}/{dataset_item_id}/",
            "target_table": "standard",
            "field_mappings": {
                "metadata.json": {
                    # Parse JSON log and extract pre/post standardization entries
                    "las_info_raw": lambda data: _extract_las_info(data, standardized=False),
                    "las_info_standardized": lambda data: _extract_las_info(data, standardized=True),
                },
                "convex_hull_wgs84.GeoJSON": {
                    "convex_hull": lambda data: data  # Store GeoJSON directly
                }
            },
            "detection": {
                "files": [
                    "standard/{dataset_id}/{dataset_item_id}/standardized.laz",
                    "standard/{dataset_id}/{dataset_item_id}/metadata.json"
                ],
                "flag": "has_standardisation"
            }
        },
        "overviews": {
            "s3_path_template": "overviews/{dataset_id}/{dataset_item_id}/",
            "target_table": "overviews",
            "url_template": "{storage_endpoint}/{bucket_name}/overviews/{dataset_id}/{dataset_item_id}/",
            "detection": {
                "files": ["overviews/{dataset_id}/{dataset_item_id}/top_view_00.png"],
                "flag": "has_overviews"
            }
        },
        "segmentation": {
            "s3_path_template": "segmentation/{dataset_id}/{dataset_item_id}/",
            "target_table": "segmentations",
            "url_template": "{storage_endpoint}/{bucket_name}/segmentation/{dataset_id}/{dataset_item_id}/segmented.laz",
            "detection": {
                "files": ["segmentation/{dataset_id}/{dataset_item_id}/segmented.laz"],
                "flag": "has_segmentation"
            }
        },
        "3dtiles": {
            "s3_path_template": "3dtiles/{dataset_id}/{dataset_item_id}/",
            "target_table": "tilesets",
            "url_template": "{storage_endpoint}/{bucket_name}/3dtiles/{dataset_id}/{dataset_item_id}/",
            "detection": {
                "files": ["3dtiles/{dataset_id}/{dataset_item_id}/tileset.json"],
                "flag": "has_3dtiles"
            }
        }
    },
    "Standard": {
        "standard": {
            "metadata_files": ["metadata.json", "convex_hull_wgs84.GeoJSON"],
            "s3_path_template": "standard/{dataset_id}/{dataset_item_id}/",
            "target_table": "standard",
            "field_mappings": {
                "metadata.json": {
                    # Parse JSON log and extract pre/post standardization entries
                    "las_info_raw": lambda data: _extract_las_info(data, standardized=False),
                    "las_info_standardized": lambda data: _extract_las_info(data, standardized=True),
                },
                "convex_hull_wgs84.GeoJSON": {
                    "convex_hull": lambda data: data  # Store GeoJSON directly
                }
            },
            "detection": {
                "files": [
                    "standard/{dataset_id}/{dataset_item_id}/standardized.laz",
                    "standard/{dataset_id}/{dataset_item_id}/metadata.json"
                ],
                "flag": "has_standardisation"
            }
        }
    },
    # Galaxy EU version - no metadata files from standardization (tool version lacks those outputs)
    "EndToEndPipeline-GalaxyEU": {
        "standard": {
            "metadata_files": [],  # Galaxy EU tool doesn't produce metadata.json or convex_hull
            "s3_path_template": "standard/{dataset_id}/{dataset_item_id}/",
            "target_table": "standard",
            "field_mappings": {},
            "detection": {
                "files": ["standard/{dataset_id}/{dataset_item_id}/standardized.laz"],
                "flag": "has_standardisation"
            }
        },
        "overviews": {
            "s3_path_template": "overviews/{dataset_id}/{dataset_item_id}/",
            "target_table": "overviews",
            "url_template": "{storage_endpoint}/{bucket_name}/overviews/{dataset_id}/{dataset_item_id}/",
            "detection": {
                "files": ["overviews/{dataset_id}/{dataset_item_id}/top_view_00.png"],
                "flag": "has_overviews"
            }
        },
        "segmentation": {
            "s3_path_template": "segmentation/{dataset_id}/{dataset_item_id}/",
            "target_table": "segmentations",
            "url_template": "{storage_endpoint}/{bucket_name}/segmentation/{dataset_id}/{dataset_item_id}/segmented.laz",
            "detection": {
                "files": ["segmentation/{dataset_id}/{dataset_item_id}/segmented.laz"],
                "flag": "has_segmentation"
            }
        },
        "3dtiles": {
            "s3_path_template": "3dtiles/{dataset_id}/{dataset_item_id}/",
            "target_table": "tilesets",
            "url_template": "{storage_endpoint}/{bucket_name}/3dtiles/{dataset_id}/{dataset_item_id}/",
            "detection": {
                "files": ["3dtiles/{dataset_id}/{dataset_item_id}/tileset.json"],
                "flag": "has_3dtiles"
            }
        }
    }
}


def resolve_export_step_ids(galaxy_client: "GalaxyClient", workflow_name: str) -> Dict[str, int]:
    """
    Query Galaxy for actual step IDs by matching export tool annotations.
    
    Args:
        galaxy_client: Connected Galaxy client
        workflow_name: Name of the workflow to resolve
        
    Returns:
        Dict mapping annotation pattern to actual Galaxy step ID (as int)
        
    Raises:
        RuntimeError: If workflow structure cannot be retrieved
    """
    try:
        workflow_structure = galaxy_client.get_workflow_structure(workflow_name)
        galaxy_steps = workflow_structure.get('steps', {})
        
        annotation_patterns = WORKFLOW_EXPORT_ANNOTATIONS.get(workflow_name, {})
        if not annotation_patterns:
            logger.warning(f"No export annotations defined for workflow '{workflow_name}'")
            return {}
        
        resolved_steps = {}
        
        # Iterate through Galaxy steps and match export tools by annotation
        for step_id, step in galaxy_steps.items():
            if step.get('tool_id') == 'export_remote':
                annotation = (step.get('annotation') or '').lower()
                
                # Match against defined patterns
                for pattern_key, path_template in annotation_patterns.items():
                    pattern_lower = pattern_key.lower()
                    
                    # Try simple substring match first
                    if pattern_lower in annotation:
                        resolved_steps[pattern_key] = int(step_id)
                        logger.debug(f"Matched export step {step_id} by substring: '{pattern_key}' in '{annotation}'")
                        break
                    
                    # Try regex match if pattern contains regex metacharacters
                    if any(char in pattern_key for char in ['|', '.*', '.+', '[', ']']):
                        try:
                            if re.search(pattern_key, annotation, re.I):
                                resolved_steps[pattern_key] = int(step_id)
                                logger.debug(f"Matched export step {step_id} by regex: '{pattern_key}' matches '{annotation}'")
                                break
                        except re.error as e:
                            logger.warning(f"Invalid regex pattern '{pattern_key}': {e}")
        
        logger.info(f"Resolved {len(resolved_steps)}/{len(annotation_patterns)} export steps for workflow '{workflow_name}'")
        return resolved_steps
        
    except Exception as e:
        logger.error(f"Failed to resolve export steps for workflow '{workflow_name}': {e}")
        raise RuntimeError(f"Failed to resolve export steps: {e}") from e


def build_workflow_parameters(
    galaxy_client: "GalaxyClient",
    supabase_client: "SupabaseClient", 
    workflow_name: str,
    dataset_id: int,
    dataset_item_id: Optional[int] = None,
) -> Dict[int, Dict[str, str]]:
    """
    Build workflow parameters with dynamic step ID resolution.
    
    This function:
    1. Uses provided dataset_item_id or queries Supabase for it
    2. Queries Galaxy for actual workflow step IDs (annotation-based)
    3. Builds parameter dict with integer keys (Galaxy step IDs) and export paths
    4. Uses galaxy_client.config for file source scheme and bucket ID
    
    Args:
        galaxy_client: Connected Galaxy client (provides file source config)
        supabase_client: Connected Supabase client
        workflow_name: Name of the workflow
        dataset_id: ID of the dataset to process
        dataset_item_id: Optional specific dataset_item_id (for multi-file datasets)
        
    Returns:
        Dict with integer keys (Galaxy step IDs) and parameter dicts containing 'd_uri'
        Returns empty dict if no export steps found or dataset_item not found
        
    Example:
        # Local Galaxy:
        >>> params = build_workflow_parameters(galaxy, supabase, "Standard", 123)
        >>> params
        {3: {"d_uri": "gxfiles://products-storage/standard/123/456/"}}
        
        # Galaxy EU (with config: GALAXY_FILE_SOURCE_SCHEME=gxuserfiles, GALAXY_FILE_SOURCE_PRODUCTS=uuid):
        >>> params = build_workflow_parameters(galaxy, supabase, "Standard", 123)
        >>> params
        {3: {"d_uri": "gxuserfiles://e1d3f62b-2abb.../standard/123/456/"}}
    """
    # Get file source config from galaxy client
    file_source_products = galaxy_client.config.file_source_products
    file_source_scheme = galaxy_client.config.file_source_scheme
    
    try:
        # Use provided dataset_item_id or get first one from Supabase
        if dataset_item_id is None:
            dataset_item_resp = supabase_client.client.table("dataset_items")\
                .select("id").eq("dataset_id", dataset_id).limit(1).execute()
            
            if not dataset_item_resp.data:
                logger.warning(f"No dataset_item found for dataset {dataset_id}")
                return {}
            
            dataset_item_id = dataset_item_resp.data[0]["id"]
        
        logger.debug(f"Using dataset_item_id {dataset_item_id} for dataset {dataset_id}")
        
    except Exception as e:
        logger.error(f"Failed to get dataset_item_id for dataset {dataset_id}: {e}")
        return {}
    
    # Resolve actual Galaxy step IDs by annotation
    export_steps = resolve_export_step_ids(galaxy_client, workflow_name)
    
    if not export_steps:
        logger.warning(f"No export steps found for workflow '{workflow_name}'")
        return {}
    
    # Build parameters with resolved step IDs
    annotation_patterns = WORKFLOW_EXPORT_ANNOTATIONS.get(workflow_name, {})
    workflow_parameters = {}
    
    for pattern, path_template in annotation_patterns.items():
        step_id = export_steps.get(pattern)
        if step_id:
            # Format the path template with actual IDs
            path = path_template.format(
                dataset_id=dataset_id,
                dataset_item_id=dataset_item_id
            )
            
            # Build the full URI using config (gxfiles:// for local, gxuserfiles:// for Galaxy EU)
            uri = f"{file_source_scheme}://{file_source_products}/{path}"
            workflow_parameters[step_id] = {
                "d_uri": uri
            }
            logger.debug(f"Step {step_id}: d_uri = {uri}")
    
    logger.info(
        f"Built parameters for workflow '{workflow_name}' with {len(workflow_parameters)} export steps "
        f"(dataset_id={dataset_id}, dataset_item_id={dataset_item_id}, scheme={file_source_scheme})"
    )
    
    return workflow_parameters
