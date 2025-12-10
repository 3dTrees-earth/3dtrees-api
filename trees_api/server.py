import os
import json
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from trees_api.config import AppConfig
from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient
from trees_api.connection_manager import ConnectionManager
from trees_api.upload_router import router as upload_router
from trees_api.workflow_config import build_workflow_parameters


logger = logging.getLogger("uvicorn")

# Global connection manager instance (will be initialized in lifespan)
connection_manager: Optional[ConnectionManager] = None

# Dependency injection functions using ConnectionManager
def get_galaxy_client() -> Optional[GalaxyClient]:
    """Get Galaxy client instance from connection manager."""
    return connection_manager.get_galaxy_client()

def get_supabase_client() -> Optional[SupabaseClient]:
    """Get Supabase client instance from connection manager."""
    return connection_manager.get_supabase_client()

def get_storage_client() -> Optional[StorageClient]:
    """Get Storage client instance from connection manager."""
    return connection_manager.get_storage_client()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connections on startup and cleanup on shutdown."""
    global connection_manager
    
    logger.info("Starting up 3DTrees API...")
    
    # Create and validate centralized configuration
    try:
        config = AppConfig()
        config.validate()
        logger.info("Configuration validated successfully")
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise RuntimeError(f"Configuration error: {e}") from e
    
    # Initialize connection manager with config
    connection_manager = ConnectionManager(config)
    
    # Try to initialize all clients (but don't fail if they can't connect)
    connection_manager.connect_galaxy()
    connection_manager.connect_supabase()
    connection_manager.connect_storage()  # Processor storage (read raw, write products)
    connection_manager.connect_uploader_storage()  # Uploader storage (write raw)
    
    # Start background retry task
    retry_task = await connection_manager.start_retry_task(interval=60)
    
    if connection_manager.all_connected():
        logger.info("All clients initialized successfully")
    else:
        logger.warning("Some clients failed to initialize. API is running in degraded mode. Retrying in background...")
    
    yield  # FastAPI serves requests here
    
    # Cleanup on shutdown
    logger.info("Shutting down 3DTrees API...")
    await connection_manager.stop_retry_task()
    connection_manager.cleanup()
    logger.info("Shutdown complete")

 

app = FastAPI(title="3DTrees API", description="API for 3DTrees", lifespan=lifespan)

# CORS configuration for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# Register routers
app.include_router(upload_router)

# Pydantic models for request/response
class JobCreateRequest(BaseModel):
    dataset_id: str
    workflow_name: str
    overwrite: bool = False
    parameters: Dict[str, Any] = {}


def _invoke_workflow_with_file_source(
    galaxy: GalaxyClient,
    supabase: SupabaseClient,
    workflow_name: str,
    dataset_id: str,
    file_source_uri: str,
    history_name: str,
    workflow_parameters: Dict[int, Dict[str, str]],
    user_parameters: dict,
    dataset_item_id: Optional[int] = None
):
    """
    Invoke Galaxy workflow using file source URI directly (no pre-import).
    
    Galaxy will fetch the file from the file source as part of workflow execution.
    This is more efficient than pre-importing the file into history first.
    
    Args:
        galaxy: Connected Galaxy client
        supabase: Connected Supabase client
        workflow_name: Name of the workflow to invoke
        dataset_id: ID of the dataset being processed (for Supabase record)
        file_source_uri: File source URI for Galaxy to fetch
        history_name: Name of the Galaxy history
        workflow_parameters: Workflow step parameters (with integer keys)
        user_parameters: User-defined parameters to store
        dataset_item_id: Optional specific dataset_item_id (for multi-file datasets)
        
    Returns:
        WorkflowInvocation object from Supabase
        
    Raises:
        HTTPException: If workflow invocation or recording fails
    """
    try:
        logger.info(f"Invoking workflow '{workflow_name}' with file source: {file_source_uri} (item_id={dataset_item_id})")
        invocation_result = galaxy.invoke_workflow_with_file_source(
            workflow_name=workflow_name,
            file_source_uri=file_source_uri,
            history_name=history_name,
            parameters=workflow_parameters if workflow_parameters else None
        )
        logger.info(f"Workflow invoked successfully: {invocation_result['invocation_id']}")
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invoking workflow {workflow_name} with file source failed: {e}"
        ) from e
    
    # Record workflow invocation in Supabase
    try:
        workflow_invocation = supabase.create_workflow_invocation(
            workflow_uuid=invocation_result["invocation_id"],
            dataset_id=dataset_id,
            workflow_name=workflow_name,
            dataset_item_id=dataset_item_id
        )
        
        # Store user parameters if provided
        if user_parameters:
            supabase.update_workflow_invocation(
                workflow_invocation.invocation_id,
                parameters=user_parameters
            )
        
        return workflow_invocation
        
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Creating workflow invocation in Supabase failed: {e}"
        ) from e


@app.get("/")
def info():
    """Root endpoint showing API status and service health."""
    status = connection_manager.get_status()
    all_connected = connection_manager.all_connected()
    
    return {
        "message": "3DTrees API is running",
        "status": "healthy" if all_connected else "degraded",
        "services": {
            "galaxy": "connected" if status["galaxy"]["connected"] else "disconnected",
            "supabase": "connected" if status["supabase"]["connected"] else "disconnected",
            "storage": "connected" if status["storage"]["connected"] else "disconnected"
        }
    }

@app.get("/health")
def health_check():
    """
    Health check endpoint showing detailed status of all services.
    Returns 200 if all services are connected, 503 if any service is down.
    """
    status = connection_manager.get_status()
    all_connected = connection_manager.all_connected()
    
    return {
        "status": "healthy" if all_connected else "degraded",
        "services": status,
        "message": "All services operational" if all_connected else "Some services are unavailable"
    }


@app.post("/jobs")
def create_job(
    dataset_id: str, 
    workflow_name: str, 
    dataset_item_id: Optional[int] = None,  # For multi-file datasets, specify which file to process
    overwrite: bool = False, 
    parameters: dict = {},
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[StorageClient] = Depends(get_storage_client)
):
    """
    Create a new workflow job.
    
    Uses direct file source invocation - Galaxy fetches the file from S3 as part
    of workflow execution. No pre-import/download needed.
    """
    # Check if all required clients are available
    if not galaxy:
        raise HTTPException(status_code=503, detail="Galaxy service is unavailable. Please check /health for details.")
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service is unavailable. Please check /health for details.")
    # Storage client is optional - Galaxy accesses S3 directly via file sources
    if not storage:
        logger.warning("Storage service is unavailable - this is OK since Galaxy accesses S3 directly via file sources")
    
    # Get bucket path and item_id from Supabase
    try:
        if dataset_item_id:
            dataset_item = supabase.get_dataset_item(dataset_item_id)
            if not dataset_item:
                raise HTTPException(status_code=404, detail=f"Dataset item {dataset_item_id} not found")
            bucket_path = dataset_item.get('bucket_path') if isinstance(dataset_item, dict) else dataset_item.bucket_path
            actual_item_id = dataset_item_id
        else:
            # Fall back to first item in dataset (legacy behavior)
            database_dataset = supabase.get_dataset(dataset_id)
            bucket_path = database_dataset.bucket_path
            response = supabase.client.table("dataset_items").select("id").eq("dataset_id", dataset_id).limit(1).execute()
            actual_item_id = response.data[0]["id"] if response.data else None
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to get dataset info: {e}") from e
    
    # Build file source URI for Galaxy to fetch directly
    file_source_uri = galaxy.get_raw_file_source_uri(bucket_path)
    logger.info(f"Using direct file source: {file_source_uri} (dataset_id={dataset_id}, item_id={actual_item_id})")
    
    # Ensure workflow exists and create history
    history_name = f"{workflow_name} - {dataset_id}" + (f" (item {dataset_item_id})" if dataset_item_id else "")
    try:
        galaxy.ensure_workflow_available(workflow_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Loading Workflow {workflow_name} failed: {e}")
    
    # Prepare workflow parameters (export paths, etc.)
    workflow_parameters = build_workflow_parameters(
        galaxy_client=galaxy,
        supabase_client=supabase,
        workflow_name=workflow_name,
        dataset_id=int(dataset_id),
        dataset_item_id=actual_item_id
    )
    
    # Invoke workflow with file source URI directly - Galaxy fetches during execution
    return _invoke_workflow_with_file_source(
        galaxy=galaxy,
        supabase=supabase,
        workflow_name=workflow_name,
        dataset_id=dataset_id,
        file_source_uri=file_source_uri,
        history_name=history_name,
        workflow_parameters=workflow_parameters,
        user_parameters=parameters,
        dataset_item_id=actual_item_id
    )


    # OLD CODE REMOVED - 176 lines replaced with build_workflow_parameters() call above
    # See commit history for old if-elif workflow parameter building code
    # Key improvement: Dynamic step ID resolution fixes hardcoded step ID bug


@app.get("/jobs")
def list_jobs(
    dataset_id: Optional[int] = None,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client)
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service is unavailable. Please check /health for details.")
    if user_id is not None:
        # Get all dataset_ids that belong to the given user
        datasets = supabase.get_datasets(user_id=user_id)
        dataset_ids = [dataset.id for dataset in datasets if dataset.id is not None]
        
        if dataset_id is not None:
            # Filter to only the specific dataset_id if provided
            if dataset_id in dataset_ids:
                dataset_ids = [dataset_id]
            else:
                return []  # User doesn't have access to this dataset
        
        # Get workflow invocations for the user's datasets
        # Note: We need to get all invocations first, then apply limit/offset
        # This is because we're filtering across multiple dataset_ids
        all_invocations = []
        for d_id in dataset_ids:
            user_invocations = supabase.get_workflow_invocations_by_dataset_id(d_id, limit=1000)  # Get all for this dataset
            all_invocations.extend(user_invocations)
        
        # Sort by creation time descending and apply limit/offset
        all_invocations.sort(key=lambda x: x.created_at, reverse=True)
        return all_invocations[offset:offset + limit]
    
    elif dataset_id is not None:
        # Get workflow invocations for specific dataset_id
        return supabase.get_workflow_invocations_by_dataset_id(dataset_id, limit=limit, offset=offset)
    
    else:
        # Get all workflow invocations (respecting limit and offset)
        return supabase.get_workflow_invocations(limit=limit, offset=offset)


class APIServerSettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    reload: bool = False

    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_prefix="API_SERVER_",
    )

if __name__ == "__main__":
    import uvicorn
    settings = APIServerSettings()
    uvicorn.run("trees_api.server:app", host=settings.host, port=settings.port, reload=settings.reload)
