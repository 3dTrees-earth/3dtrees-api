"""
Product detection and metadata ingestion module.

This module handles detecting products in S3, downloading metadata files,
and ingesting them into Supabase using configuration-driven logic with threading.
"""
import logging
from typing import Dict
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient
from trees_api.galaxy_client import GalaxyClient
from trees_api.models import WorkflowInvocation
from trees_api.config import StorageConfig
from trees_api.workflow_config import WORKFLOW_METADATA_INGESTION

logger = logging.getLogger("uvicorn")


def detect_products_from_config(
    storage_client: StorageClient,
    storage_config: StorageConfig,
    workflow_name: str,
    dataset_id: int,
    dataset_item_id: int
) -> Dict[str, bool]:
    """
    Detect products in S3 using WORKFLOW_METADATA_INGESTION configuration.
    
    Args:
        storage_client: Connected storage client
        storage_config: StorageConfig instance
        workflow_name: Name of the workflow (e.g., "EndToEndPipeline")
        dataset_id: Dataset ID
        dataset_item_id: Dataset item ID
        
    Returns:
        Dict mapping product_type to detection status
        Example: {"standard": True, "overviews": False, ...}
    """
    workflow_config = WORKFLOW_METADATA_INGESTION.get(workflow_name, {})
    detected = {}
    
    for product_type, config in workflow_config.items():
        detection_config = config.get("detection", {})
        if not detection_config:
            continue
        
        files_to_check = detection_config.get("files", [])
        if not files_to_check:
            continue
        
        # Format file paths with actual IDs
        formatted_files = [
            f.format(dataset_id=dataset_id, dataset_item_id=dataset_item_id)
            for f in files_to_check
        ]
        
        # Check if at least one file exists
        product_detected = False
        for file_path in formatted_files:
            if storage_client.object_exists(storage_config.products_bucket, file_path):
                product_detected = True
                logger.debug(f"Detected product file: {file_path}")
                break
        
        detected[product_type] = product_detected
        
        if product_detected:
            logger.info(f"Product '{product_type}' detected for workflow {workflow_name}, dataset_item {dataset_item_id}")
    
    return detected


def ingest_product_metadata(
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    storage_config: StorageConfig,
    workflow_name: str,
    product_type: str,
    dataset_id: int,
    dataset_item_id: int
):
    """
    Ingest metadata for a specific product type using configuration.
    
    Args:
        storage_client: Connected storage client
        supabase_client: Connected Supabase client
        storage_config: StorageConfig instance
        workflow_name: Name of the workflow
        product_type: Type of product (e.g., "standard", "overviews")
        dataset_id: Dataset ID
        dataset_item_id: Dataset item ID
    """
    workflow_config = WORKFLOW_METADATA_INGESTION.get(workflow_name, {})
    config = workflow_config.get(product_type, {})
    
    if not config:
        logger.warning(f"No config found for product type '{product_type}' in workflow '{workflow_name}'")
        return
    
    target_table = config.get("target_table")
    if not target_table:
        logger.warning(f"No target table defined for product type '{product_type}'")
        return
    
    data_to_upsert = {}
    
    # Handle metadata files (JSON ingestion)
    metadata_files = config.get("metadata_files", [])
    field_mappings = config.get("field_mappings", {})
    
    for metadata_file in metadata_files:
        file_path = config.get("s3_path_template", "").format(
            dataset_id=dataset_id,
            dataset_item_id=dataset_item_id
        )
        full_path = f"{file_path}{metadata_file}"
        
        try:
            metadata_json = storage_client.download_json(storage_config.products_bucket, full_path)
            
            # Apply field mappings
            if metadata_file in field_mappings:
                for field_name, mapping_func in field_mappings[metadata_file].items():
                    try:
                        data_to_upsert[field_name] = mapping_func(metadata_json)
                    except Exception as e:
                        logger.warning(f"Error applying mapping for field '{field_name}': {e}")
            
            logger.info(f"Ingested metadata from {full_path}")
        
        except Exception as e:
            logger.warning(f"Could not ingest metadata file {full_path}: {e}")
    
    # Handle URL template (for overviews, segmentations, tilesets)
    url_template = config.get("url_template")
    if url_template:
        url = url_template.format(
            storage_endpoint=storage_config.public_endpoint,
            dataset_id=dataset_id,
            dataset_item_id=dataset_item_id
        )
        data_to_upsert["url"] = url
        logger.info(f"Generated URL for {product_type}: {url}")
    
    # Upsert data if we have anything to insert
    if data_to_upsert:
        try:
            supabase_client.upsert_product_metadata(
                table=target_table,
                dataset_item_id=dataset_item_id,
                data=data_to_upsert
            )
            logger.info(f"Upserted {len(data_to_upsert)} fields into {target_table} for dataset_item {dataset_item_id}")
        except Exception as e:
            logger.error(f"Error upserting metadata into {target_table}: {e}")
    
    # Update processing status flag if configured
    detection_config = config.get("detection", {})
    flag_name = detection_config.get("flag")
    if flag_name:
        try:
            supabase_client.client.table("dataset_processing_status").update({
                flag_name: True,
                "updated_at": datetime.now().isoformat()
            }).eq("dataset_item_id", dataset_item_id).execute()
            logger.info(f"Set flag {flag_name}=True for dataset_item {dataset_item_id}")
        except Exception as e:
            logger.error(f"Error updating flag {flag_name}: {e}")


def process_workflow_products(
    workflow_invocation: WorkflowInvocation,
    storage_client: StorageClient,
    supabase_client: SupabaseClient,
    storage_config: StorageConfig
) -> Dict[str, int]:
    """
    Process products for a single workflow invocation.
    This function is designed to run in a thread.
    
    Args:
        workflow_invocation: WorkflowInvocation object
        storage_client: Connected storage client
        supabase_client: Connected Supabase client
        storage_config: StorageConfig instance
        
    Returns:
        Dict with statistics
    """
    stats = {
        'products_detected': 0,
        'metadata_ingested': 0,
        'flags_updated': 0
    }
    
    try:
        # Get dataset_id from dataset_item
        dataset_item = supabase_client.get_dataset_item(workflow_invocation.dataset_item_id)
        if not dataset_item:
            logger.warning(f"Dataset item {workflow_invocation.dataset_item_id} not found")
            return stats
        
        # Handle both dict and object returns
        dataset_id = dataset_item.get('dataset_id') if isinstance(dataset_item, dict) else dataset_item.dataset_id
        dataset_item_id = workflow_invocation.dataset_item_id
        workflow_name = workflow_invocation.workflow_name
        
        # Detect which products exist in S3
        detected_products = detect_products_from_config(
            storage_client,
            storage_config,
            workflow_name,
            dataset_id,
            dataset_item_id
        )
        
        # Ingest metadata for each detected product
        for product_type, is_detected in detected_products.items():
            if is_detected:
                stats['products_detected'] += 1
                try:
                    ingest_product_metadata(
                        storage_client,
                        supabase_client,
                        storage_config,
                        workflow_name,
                        product_type,
                        dataset_id,
                        dataset_item_id
                    )
                    stats['metadata_ingested'] += 1
                    stats['flags_updated'] += 1
                except Exception as e:
                    logger.error(f"Error ingesting metadata for {product_type}: {e}")
        
        # Check if workflow is fully complete and all products detected
        workflow_complete = workflow_invocation.status in ['ok', 'success', 'error', 'failed', 'cancelled']
        all_detected = all(detected_products.values()) if detected_products else False
        
        if workflow_complete and all_detected:
            # Mark as fully synced
            supabase_client.client.table("galaxy_workflow_invocations").update({
                "metadata_synced_at": datetime.now().isoformat()
            }).eq("invocation_id", workflow_invocation.invocation_id).execute()
            logger.info(f"Workflow {workflow_invocation.invocation_id} fully synced")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error processing workflow {workflow_invocation.invocation_id}: {e}")
        return stats


def sync_workflow_products(
    galaxy_client: GalaxyClient,
    supabase_client: SupabaseClient,
    storage_client: StorageClient,
    storage_config: StorageConfig
) -> Dict[str, int]:
    """
    Sync products for workflows using ThreadPoolExecutor.
    This separates fast database queries from slow S3 operations.
    
    Args:
        galaxy_client: Connected Galaxy client
        supabase_client: Connected Supabase client
        storage_client: Connected storage client
        storage_config: StorageConfig instance
        
    Returns:
        Dict with sync statistics
    """
    stats = {
        'workflows_checked': 0,
        'products_detected': 0,
        'metadata_ingested': 0,
        'flags_updated': 0,
        'errors': 0
    }
    
    try:
        # Fast: Get workflows that haven't been fully synced yet
        response = supabase_client.client.table("galaxy_workflow_invocations").select(
            "*"
        ).is_("metadata_synced_at", "null").execute()
        
        if not response.data:
            logger.info("No workflows need product sync")
            return stats
        
        workflows = [WorkflowInvocation.model_validate(inv_data) for inv_data in response.data]
        stats['workflows_checked'] = len(workflows)
        
        logger.info(f"Processing products for {len(workflows)} workflows with thread pool")
        
        # Slow: Process workflows in thread pool (max 5 concurrent)
        with ThreadPoolExecutor(max_workers=5, thread_name_prefix="product-sync") as executor:
            futures = {
                executor.submit(
                    process_workflow_products,
                    workflow, storage_client, supabase_client, storage_config
                ): workflow
                for workflow in workflows
            }
            
            for future in as_completed(futures):
                workflow = futures[future]
                try:
                    result = future.result()
                    stats['products_detected'] += result.get('products_detected', 0)
                    stats['metadata_ingested'] += result.get('metadata_ingested', 0)
                    stats['flags_updated'] += result.get('flags_updated', 0)
                except Exception as e:
                    logger.error(f"Error processing workflow {workflow.invocation_id}: {e}")
                    stats['errors'] += 1
        
        logger.info(f"Product sync completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error during product sync: {e}")
        stats['errors'] += 1
        return stats

