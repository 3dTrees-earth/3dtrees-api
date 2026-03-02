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
import json
import logging
from typing import Dict, Optional, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from trees_api.integrations.galaxy.client import GalaxyClient
    from trees_api.integrations.supabase.client import SupabaseClient

logger = logging.getLogger("uvicorn")


def _wkt_to_geojson(wkt_string: str) -> dict | None:
    """
    Convert WKT geometry to GeoJSON format using Shapely.
    
    Args:
        wkt_string: WKT string (e.g., "MULTIPOLYGON (((x1 y1, x2 y2, ...)))")
        
    Returns:
        GeoJSON dict or None if conversion fails
    """
    if not wkt_string or not isinstance(wkt_string, str):
        return None
    
    try:
        from shapely import wkt
        from shapely.geometry import mapping
        
        geom = wkt.loads(wkt_string)
        return mapping(geom)
    except Exception as e:
        logger.warning(f"Failed to parse WKT: {e}")
        return None


def _process_collection_summary(data: dict) -> dict | None:
    """
    Process collection_summary.json data and extract dataset-level metadata.
    
    Performs:
    - Calculates total_point_count from per-file n_points
    - Converts multipolygon_wkt to GeoJSON
    - Extracts EPSG from first_crs
    
    Args:
        data: Parsed collection_summary.json
        
    Returns:
        Processed collection summary with aggregated fields
    """
    if not data or not isinstance(data, dict):
        return None
    
    collection = data.get("collection", {})
    files = data.get("files", {})
    
    # Calculate total point count from files
    total_point_count = 0
    for file_info in files.values():
        if isinstance(file_info, dict):
            n_points = file_info.get("n_points", 0)
            if isinstance(n_points, (int, float)):
                total_point_count += int(n_points)
    
    # Convert WKT to GeoJSON
    multipolygon_wkt = collection.get("multipolygon_wkt", "")
    convex_hull_geojson = _wkt_to_geojson(multipolygon_wkt)
    
    # Extract EPSG from first_crs
    epsg = None
    first_crs = collection.get("first_crs", {})
    if isinstance(first_crs, dict):
        epsg = first_crs.get("epsg")
    # Also check crs_epsg_values (can be single value or array)
    if epsg is None:
        epsg_values = collection.get("crs_epsg_values")
        if isinstance(epsg_values, (int, float)):
            epsg = int(epsg_values)
        elif isinstance(epsg_values, list) and len(epsg_values) > 0:
            epsg = epsg_values[0]
    
    return {
        "n_tiles": collection.get("n_tiles", len(files)),
        "total_point_count": total_point_count,
        "homogeneous_crs": collection.get("homogeneous_crs", False),
        "epsg": epsg,
        "convex_hull": convex_hull_geojson,
        "multipolygon_wkt": multipolygon_wkt,
        "tiles_any_overlap": collection.get("tiles_any_overlap", False),
        "tiles_all_disjoint": collection.get("tiles_all_disjoint", True),
        "homogeneous_attribute_names": collection.get("homogeneous_attribute_names", False),
        "homogeneous_attribute_types": collection.get("homogeneous_attribute_types", False),
        "all_attribute_names": collection.get("all_attribute_names", []),
        "common_attribute_names": collection.get("common_attribute_names", []),
        "removeable_attributes": collection.get("removeable_attributes", []),
        "global_attribute_stats": collection.get("global_attribute_stats", []),
        # Keep raw data for reference
        "files": files
    }


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
# 
# Path structure: {s3_base_path}{product_type}/
# For collection workflows (EndToEndPipeline):
#   - s3_base_path = {dataset_id}/
#   - Galaxy's collection_auto export appends element identifier (item_id)
#   - Final path: {dataset_id}/{product_type}/{item_id}/
WORKFLOW_EXPORT_ANNOTATIONS = {
    "Standard": {
        "export_standardized_laz": "{s3_base_path}standard/",
        "export_metadata_json": "{s3_base_path}standard/",
        "export_convex_hull": "{s3_base_path}standard/"
    },
    "Segmentation": {
        "export_segmented": "{s3_base_path}segmentation/"
    },
    "Overviews": {
        "export_top_views": "{s3_base_path}overviews/",
        "export_section_views": "{s3_base_path}overviews/",
        "export_overview_gif": "{s3_base_path}overviews/"
    },
    "Py3DTiles": {
        "export_tileset": "{s3_base_path}3dtiles/",
        "export_preview": "{s3_base_path}3dtiles/",
        "export_points_tiles": "{s3_base_path}3dtiles/points/"
    },
    "EndToEndPipeline": {
        # Collection workflow - Galaxy appends element identifier to paths
        # Collection-level outputs (single file, no element identifier)
        "export_collection_summary": "{s3_base_path}metadata/",
        # Per-file outputs (mapped over collection)
        "export_standardized_laz": "{s3_base_path}standard/",
        "export_metadata_json": "{s3_base_path}metadata/",
        "export_convex_hull": "{s3_base_path}metadata/",
        "export_top_views": "{s3_base_path}overviews/",
        "export_section_views": "{s3_base_path}overviews/",
        "export_overview_gif": "{s3_base_path}overviews/",
        "export_segmented_laz": "{s3_base_path}segmentation/",
        "export_tileset": "{s3_base_path}3dtiles/",
        "export_preview": "{s3_base_path}3dtiles/",
        "export_points_tiles": "{s3_base_path}3dtiles/points/",
        "export_child_tilesets": "{s3_base_path}3dtiles/points/"
    },
    # Galaxy EU version - same as EndToEndPipeline (full outputs available)
    "EndToEndPipeline-GalaxyEU": {
        # Collection-level outputs (single file, no element identifier)
        "export_collection_summary": "{s3_base_path}metadata/",
        # Per-file outputs (mapped over collection)
        "export_standardized_laz": "{s3_base_path}standard/",
        "export_metadata_json": "{s3_base_path}metadata/",
        "export_convex_hull": "{s3_base_path}metadata/",
        "export_top_views": "{s3_base_path}overviews/",
        "export_section_views": "{s3_base_path}overviews/",
        "export_segmented_laz": "{s3_base_path}segmentation/",
        "export_potree": "{s3_base_path}potree/"
    }
}


# Workflow metadata ingestion configuration
# Maps workflow names to metadata extraction and database ingestion specs
# Path structure: {s3_base_path}/{product_type}/ where s3_base_path = {dataset_id}/{dataset_item_id}/
WORKFLOW_METADATA_INGESTION = {
    "EndToEndPipeline": {
        # Collection-level metadata (stored at dataset level)
        "collection_summary": {
            "metadata_files": ["collection_summary.json"],
            "s3_path_template": "{s3_base_path}metadata/",
            "target_table": "galaxy_histories",  # Store in outputs.metadata.collection_summary
            "field_mappings": {
                "collection_summary.json": {
                    # Process and aggregate collection summary
                    "collection_summary": _process_collection_summary
                }
            },
            "detection": {
                "files": ["{s3_base_path}metadata/collection_summary.json"],
                "flag": "has_collection_summary"
            }
        },
        # Per-file metadata (stored at dataset_item level)
        "metadata": {
            "metadata_files": ["{item_id}.json", "{item_id}.geojson"],
            "s3_path_template": "{s3_base_path}metadata/",
            "target_table": "standard",
            "field_mappings": {
                "{item_id}.json": {
                    # Parse JSON log and extract pre/post standardization entries
                    "las_info_raw": lambda data: _extract_las_info(data, standardized=False),
                    "las_info_standardized": lambda data: _extract_las_info(data, standardized=True),
                },
                "{item_id}.geojson": {
                    "convex_hull": lambda data: data  # Store GeoJSON directly
                }
            },
            "detection": {
                "files": ["{s3_base_path}metadata/{item_id}.json"],
                "flag": "has_metadata"
            }
        },
        "standard": {
            "s3_path_template": "{s3_base_path}standard/",
            "target_table": "standard",
            "field_mappings": {},
            "detection": {
                "files": ["{s3_base_path}standard/{item_id}.laz"],
                "flag": "has_standardisation"
            }
        },
        "overviews": {
            "s3_path_template": "{s3_base_path}overviews/",
            "target_table": "overviews",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}overviews/",
            "detection": {
                "files": ["{s3_base_path}overviews/top_view_00.png"],
                "flag": "has_overviews"
            }
        },
        "segmentation": {
            "s3_path_template": "{s3_base_path}segmentation/",
            "target_table": "segmentations",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}segmentation/segmented.laz",
            "detection": {
                "files": ["{s3_base_path}segmentation/segmented.laz"],
                "flag": "has_segmentation"
            }
        },
        "3dtiles": {
            "s3_path_template": "{s3_base_path}3dtiles/",
            "target_table": "tilesets",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}3dtiles/",
            "detection": {
                "files": ["{s3_base_path}3dtiles/tileset.json"],
                "flag": "has_3dtiles"
            }
        }
    },
    "Standard": {
        "standard": {
            "metadata_files": ["metadata.json", "convex_hull_wgs84.GeoJSON"],
            "s3_path_template": "{s3_base_path}standard/",
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
                    "{s3_base_path}standard/standardized.laz",
                    "{s3_base_path}standard/metadata.json"
                ],
                "flag": "has_standardisation"
            }
        }
    },
    # Galaxy EU version - same outputs as EndToEndPipeline
    "EndToEndPipeline-GalaxyEU": {
        # Collection-level metadata (stored at dataset level)
        "collection_summary": {
            "metadata_files": ["collection_summary.json"],
            "s3_path_template": "{s3_base_path}metadata/",
            "target_table": "galaxy_histories",
            "field_mappings": {
                "collection_summary.json": {
                    "collection_summary": _process_collection_summary
                }
            },
            "detection": {
                "files": ["{s3_base_path}metadata/collection_summary.json"],
                "flag": "has_collection_summary"
            }
        },
        # Per-file metadata (stored at dataset_item level)
        # Note: metadata.json and convex_hull.geojson use element identifier as filename
        "metadata": {
            "metadata_files": ["{item_id}.json", "{item_id}.geojson"],
            "s3_path_template": "{s3_base_path}metadata/",
            "target_table": "standard",
            "field_mappings": {
                "{item_id}.json": {
                    "las_info_raw": lambda data: _extract_las_info(data, standardized=False),
                    "las_info_standardized": lambda data: _extract_las_info(data, standardized=True),
                },
                "{item_id}.geojson": {
                    "convex_hull": lambda data: data
                }
            },
            "detection": {
                # Uses element identifier pattern: {item_id}.json
                "files": ["{s3_base_path}metadata/{item_id}.json"],
                "flag": "has_metadata"
            }
        },
        "standard": {
            "s3_path_template": "{s3_base_path}standard/",
            "target_table": "standard",
            "field_mappings": {},
            "detection": {
                # Uses element identifier pattern: {item_id}.laz
                "files": ["{s3_base_path}standard/{item_id}.laz"],
                "flag": "has_standardisation"
            }
        },
        "overviews": {
            "s3_path_template": "{s3_base_path}overviews/",
            "target_table": "overviews",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}overviews/",
            "detection": {
                # Note: Galaxy export adds extra extension: top_view_00.png.png
                "files": ["{s3_base_path}overviews/top_view_00.png.png"],
                "flag": "has_overviews"
            }
        },
        "segmentation": {
            "s3_path_template": "{s3_base_path}segmentation/",
            "target_table": "segmentations",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}segmentation/{item_id}.laz",
            "detection": {
                # Uses element identifier pattern: {item_id}.laz
                "files": ["{s3_base_path}segmentation/{item_id}.laz"],
                "flag": "has_segmentation"
            }
        },
        "3dtiles": {
            "s3_path_template": "{s3_base_path}3dtiles/",
            "target_table": "tilesets",
            "url_template": "{storage_endpoint}/{bucket_name}/{s3_base_path}3dtiles/",
            "detection": {
                "files": ["{s3_base_path}3dtiles/tileset.json"],
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
    s3_base_path: Optional[str] = None,
) -> Dict[int, Dict[str, str]]:
    """
    Build workflow parameters with dynamic step ID resolution.
    
    For collection-based workflows (EndToEndPipeline):
    - s3_base_path = {dataset_id}/
    - Galaxy's collection_auto export appends element identifier (item_id)
    - Final path: {dataset_id}/{product_type}/{item_id}/
    
    This function:
    1. Gets s3_base_path from galaxy_histories or falls back to {dataset_id}/
    2. Queries Galaxy for actual workflow step IDs (annotation-based)
    3. Builds parameter dict with integer keys (Galaxy step IDs) and export paths
    4. Uses galaxy_client.config for file source scheme and bucket ID
    
    Args:
        galaxy_client: Connected Galaxy client (provides file source config)
        supabase_client: Connected Supabase client
        workflow_name: Name of the workflow
        dataset_id: ID of the dataset to process
        s3_base_path: Optional S3 base path (from galaxy_histories). Falls back to {dataset_id}/
        
    Returns:
        Dict with integer keys (Galaxy step IDs) and parameter dicts containing 'd_uri'
        Returns empty dict if no export steps found
        
    Example:
        # Local Galaxy with s3_base_path="324/":
        >>> params = build_workflow_parameters(galaxy, supabase, "EndToEndPipeline", 324, s3_base_path="324/")
        >>> params
        {3: {"d_uri": "gxfiles://products-storage/324/standard/"}}
        # Galaxy collection export appends element ID: 324/standard/3024/
    """
    # Get file source config from galaxy client
    file_source_products = galaxy_client.config.file_source_products
    file_source_visualization = galaxy_client.config.file_source_visualization
    file_source_scheme = galaxy_client.config.file_source_scheme
    
    # Exports that should go to visualization bucket (overviews, 3dtiles, potree)
    visualization_exports = {
        "export_top_views",
        "export_section_views",
        "export_overview_gif",
        "export_tileset",
        "export_preview",
        "export_points_tiles",
        "export_child_tilesets",
        "export_potree",
    }
    
    # Get s3_base_path from galaxy_histories or fall back to default
    if s3_base_path is None:
        history = supabase_client.get_galaxy_history_by_dataset(dataset_id)
        if history and history.get("s3_base_path"):
            s3_base_path = history["s3_base_path"]
        else:
            # Fall back to collection-based format: {dataset_id}/
            s3_base_path = f"{dataset_id}/"
        logger.debug(f"Using s3_base_path: {s3_base_path}")
    
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
            # Format the path template with s3_base_path
            path = path_template.format(
                s3_base_path=s3_base_path,
                dataset_id=dataset_id
            )
            
            # Determine which bucket to use based on export type
            # Visualization exports (overviews, 3dtiles) go to visualization bucket
            # All other exports (standard, metadata, segmentation) go to products bucket
            if pattern in visualization_exports:
                file_source = file_source_visualization
                bucket_type = "visualization"
            else:
                file_source = file_source_products
                bucket_type = "products"
            
            # Build the full URI using config (gxfiles:// for local, gxuserfiles:// for Galaxy EU)
            uri = f"{file_source_scheme}://{file_source}/{path}"
            workflow_parameters[step_id] = {
                "d_uri": uri
            }
            logger.debug(f"Step {step_id} ({bucket_type}): d_uri = {uri}")
    
    logger.info(
        f"Built parameters for workflow '{workflow_name}' with {len(workflow_parameters)} export steps "
        f"(dataset_id={dataset_id}, s3_base_path={s3_base_path})"
    )
    
    return workflow_parameters
