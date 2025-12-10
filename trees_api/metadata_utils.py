"""Utilities for parsing LAS metadata from tool_standard and extracting database fields."""

from typing import Dict, Any, Optional, List
import json


def parse_las_metadata(metadata_json: Any) -> Dict[str, Any]:
    """
    Extract standard fields from tool_standard JSON metadata.
    
    The tool_standard outputs a JSON log file with multiple entries including
    las_info_pre and las_info_post containing bbox, point_count, crs, dimensions.
    
    Args:
        metadata_json: Full metadata JSON (could be list of log entries or single object)
        
    Returns:
        Dictionary with extracted metadata fields
    """
    # Handle case where metadata is a list of log entries
    if isinstance(metadata_json, list):
        # Find las_info entries in the log
        las_info = None
        for entry in metadata_json:
            if isinstance(entry, dict):
                if "bbox" in entry and "point_count" in entry:
                    las_info = entry
                    break
        if not las_info:
            return {}
        metadata_json = las_info
    
    return {
        "point_count": metadata_json.get("point_count"),
        "bbox": metadata_json.get("bbox"),
        "crs": metadata_json.get("crs"),
        "dimensions": metadata_json.get("dimensions")
    }


def extract_convex_hull_geojson(geojson_data: Any) -> Optional[Dict[str, Any]]:
    """
    Extract convex hull as GeoJSON from tool_standard output.
    
    The tool_standard outputs a separate convex_hull_wgs84.GeoJSON file
    containing a Polygon geometry in WGS84.
    
    Args:
        geojson_data: GeoJSON object (Polygon or Feature)
        
    Returns:
        GeoJSON object or None if invalid
    """
    if not geojson_data:
        return None
    
    # Handle both direct geometry and Feature wrapper
    if isinstance(geojson_data, dict):
        geojson_type = geojson_data.get("type")
        
        # Direct Polygon geometry
        if geojson_type == "Polygon":
            return geojson_data
        
        # Feature with geometry
        if geojson_type == "Feature":
            geometry = geojson_data.get("geometry", {})
            if geometry.get("type") == "Polygon":
                return geometry
        
        # FeatureCollection - take first polygon
        if geojson_type == "FeatureCollection":
            features = geojson_data.get("features", [])
            for feature in features:
                geometry = feature.get("geometry", {})
                if geometry.get("type") == "Polygon":
                    return geometry
    
    return None


def extract_point_count(metadata_json: Any) -> Optional[int]:
    """
    Extract point count from tool_standard metadata.
    
    Args:
        metadata_json: Metadata JSON object or list
        
    Returns:
        Point count as integer or None if not present
    """
    # Handle list of log entries
    if isinstance(metadata_json, list):
        for entry in metadata_json:
            if isinstance(entry, dict) and "point_count" in entry:
                try:
                    return int(entry["point_count"])
                except (ValueError, TypeError):
                    continue
        return None
    
    # Handle single object
    if isinstance(metadata_json, dict):
        count = metadata_json.get("point_count")
        if count is not None:
            try:
                return int(count)
            except (ValueError, TypeError):
                return None
    
    return None


def extract_bbox(metadata_json: Any) -> Optional[Dict[str, Any]]:
    """
    Extract bounding box from tool_standard metadata.
    
    Args:
        metadata_json: Metadata JSON object or list
        
    Returns:
        Bbox dictionary with xmin/xmax/ymin/ymax/zmin/zmax or None
    """
    # Handle list of log entries
    if isinstance(metadata_json, list):
        for entry in metadata_json:
            if isinstance(entry, dict) and "bbox" in entry:
                return entry["bbox"]
        return None
    
    # Handle single object
    if isinstance(metadata_json, dict):
        return metadata_json.get("bbox")
    
    return None


def extract_crs_info(metadata_json: Any) -> Optional[str]:
    """
    Extract Coordinate Reference System information from tool_standard metadata.
    
    Args:
        metadata_json: Metadata JSON object or list
        
    Returns:
        CRS string (JSON or WKT) or None if not present
    """
    # Handle list of log entries
    if isinstance(metadata_json, list):
        for entry in metadata_json:
            if isinstance(entry, dict) and "crs" in entry:
                return entry["crs"]
        return None
    
    # Handle single object
    if isinstance(metadata_json, dict):
        return metadata_json.get("crs")
    
    return None


def extract_dimensions(metadata_json: Any) -> Optional[List[Dict[str, Any]]]:
    """
    Extract dimension/attribute statistics from tool_standard metadata.
    
    Args:
        metadata_json: Metadata JSON object or list
        
    Returns:
        List of dimension statistics or None
    """
    # Handle list of log entries
    if isinstance(metadata_json, list):
        for entry in metadata_json:
            if isinstance(entry, dict) and "dimensions" in entry:
                return entry["dimensions"]
        return None
    
    # Handle single object
    if isinstance(metadata_json, dict):
        return metadata_json.get("dimensions")
    
    return None


# Legacy function names for backwards compatibility
def parse_pdal_metadata(metadata_json: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy alias for parse_las_metadata."""
    return parse_las_metadata(metadata_json)


def extract_bounds(metadata_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Legacy alias for extract_bbox."""
    return extract_bbox(metadata_json)


def extract_srs_info(metadata_json: Dict[str, Any]) -> Optional[Any]:
    """Legacy alias for extract_crs_info."""
    return extract_crs_info(metadata_json)
