"""Utilities for parsing PDAL metadata and extracting database fields."""

from typing import Dict, Any, Optional
import json


def parse_pdal_metadata(metadata_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract standard fields from PDAL metadata JSON.
    
    Args:
        metadata_json: Full PDAL metadata JSON object
        
    Returns:
        Dictionary with extracted metadata fields
    """
    metadata_section = metadata_json.get("metadata", {})
    
    return {
        "point_count": metadata_section.get("count"),
        "bounds": metadata_section.get("bounds"),
        "srs": metadata_section.get("srs", {}).get("wkt"),
        "dimensions": metadata_section.get("dimensions")
    }


def extract_convex_hull_geojson(metadata_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract convex hull as GeoJSON from PDAL metadata.
    
    Args:
        metadata_json: Full PDAL metadata JSON object from tool_pdal_metadata
        
    Returns:
        GeoJSON boundary object (Polygon in WGS84) or None if not present
    """
    # PDAL metadata structure: metadata_json["summary"]["boundary"]["boundary_wgs84_geojson"]
    boundary_dict = metadata_json.get("summary", {}).get("boundary", {})
    geojson_polygon = boundary_dict.get("boundary_wgs84_geojson")
    
    if geojson_polygon and isinstance(geojson_polygon, dict) and geojson_polygon.get("type") == "Polygon":
        return geojson_polygon
    
    return None


def extract_point_count(metadata_json: Dict[str, Any]) -> Optional[int]:
    """
    Extract point count from PDAL metadata.
    
    Args:
        metadata_json: Full PDAL metadata JSON object
        
    Returns:
        Point count as integer or None if not present
    """
    count = metadata_json.get("metadata", {}).get("count")
    if count is not None:
        try:
            return int(count)
        except (ValueError, TypeError):
            return None
    return None


def extract_bounds(metadata_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract spatial bounds from PDAL metadata.
    
    Args:
        metadata_json: Full PDAL metadata JSON object
        
    Returns:
        Bounds dictionary with min/max x/y/z values or None if not present
    """
    return metadata_json.get("metadata", {}).get("bounds")


def extract_srs_info(metadata_json: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extract Spatial Reference System information from PDAL metadata.
    
    Args:
        metadata_json: Full PDAL metadata JSON object
        
    Returns:
        SRS dictionary with wkt, epsg, proj4, etc. or None if not present
    """
    return metadata_json.get("metadata", {}).get("srs")

