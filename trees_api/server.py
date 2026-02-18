import os
import json
import sys
from datetime import datetime, timezone
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
from trees_api.supabase_log_handler import setup_supabase_logging


logger = logging.getLogger("uvicorn")

APP_START_TIME = datetime.now(timezone.utc)


def _get_env_first(*names: str) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _get_package_version() -> Optional[str]:
    try:
        from importlib import metadata
    except Exception:
        return None
    try:
        return metadata.version("3dtrees-api")
    except Exception:
        return None

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
    
    # Set up Supabase logging if connected
    supabase_client = connection_manager.get_supabase_client()
    if supabase_client:
        setup_supabase_logging(supabase_client.client, source="api")
        logger.info("Supabase logging enabled")
    
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

# CORS configuration
# Include all production domains and local development
ALLOWED_ORIGINS = [
    "http://localhost:5173",           # Local dev (Vite)
    "http://localhost:5174",           # Local dev (Vite alternate port)
    "http://localhost:4173",           # Local preview (Vite)
    "http://127.0.0.1:5174",           # Local dev (Vite alternate port)
    "https://3dtrees.earth",           # Production (apex)
    "https://www.3dtrees.earth",       # Production (www)
    "https://threedtrees-dev.web.app", # Firebase hosting (live)
    "https://threedtrees-dev.firebaseapp.com",  # Firebase hosting (alt)
]

# Regex pattern for Firebase preview channels: threedtrees-dev--{channel}-{id}.web.app
ALLOWED_ORIGIN_REGEX = r"https://threedtrees-dev--.*\.web\.app"

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
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


def _invoke_workflow_with_collection(
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
    history_fk: Optional[int] = None
):
    """
    Invoke Galaxy workflow with collection input (all dataset_items).
    
    Galaxy will fetch files directly from S3 during job execution via
    deferred file-source URIs. No pre-import needed.
    
    Args:
        galaxy: Connected Galaxy client
        supabase: Connected Supabase client
        workflow_name: Name of the workflow to invoke
        dataset_id: ID of the dataset (parent of dataset_items)
        history_name: Name of the Galaxy history (for new history creation)
        workflow_parameters: Workflow step parameters (with integer keys)
        user_parameters: User-defined parameters to store
        preferred_object_store_id: Optional object store ID for all datasets
        preferred_intermediate_object_store_id: Optional object store for intermediate datasets
        preferred_outputs_object_store_id: Optional object store for marked outputs
        history_id: Optional existing Galaxy history ID to reuse
        history_fk: Optional ID of the galaxy_histories record to link
        
    Returns:
        WorkflowInvocation object from Supabase
        
    Raises:
        HTTPException: If workflow invocation or recording fails
    """
    try:
        logger.info(f"Invoking workflow '{workflow_name}' with collection for dataset_id={dataset_id} (history_id={history_id})")
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
        logger.info(f"Workflow invoked successfully: {invocation_result['invocation_id']}")
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invoking workflow {workflow_name} with collection failed: {e}"
        ) from e
    
    # Record workflow invocation in Supabase
    try:
        workflow_invocation = supabase.create_workflow_invocation(
            workflow_uuid=invocation_result["invocation_id"],
            dataset_id=dataset_id,
            workflow_name=workflow_name,
            history_fk=history_fk
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


@app.get("/version")
def version_info():
    """
    Version and runtime metadata for debugging deployments.
    """
    git_sha = _get_env_first("GIT_SHA", "GIT_COMMIT", "COMMIT_SHA", "SOURCE_VERSION", "BUILD_SHA")
    build_time = _get_env_first("BUILD_TIME", "BUILD_DATE", "SOURCE_DATE_EPOCH")
    image_tag = _get_env_first("IMAGE_TAG", "DOCKER_IMAGE_TAG", "RELEASE_TAG")
    deploy_env = _get_env_first("DEPLOY_ENV", "ENVIRONMENT", "APP_ENV")

    uptime_seconds = int((datetime.now(timezone.utc) - APP_START_TIME).total_seconds())

    workflows_path = None
    workflow_registry = None
    if connection_manager:
        galaxy_client = connection_manager.get_galaxy_client()
        if galaxy_client:
            workflows_path = str(galaxy_client.workflows_path)
            registry = galaxy_client.get_available_workflow_files()
            workflow_registry = {
                "count": len(registry),
                "workflows": sorted(registry.keys()),
            }

    return {
        "api_version": _get_env_first("API_VERSION", "APP_VERSION") or _get_package_version(),
        "git_sha": git_sha,
        "git_sha_short": git_sha[:7] if git_sha else None,
        "build_time": build_time,
        "image_tag": image_tag,
        "environment": deploy_env,
        "python_version": sys.version.split()[0],
        "started_at": APP_START_TIME.isoformat(),
        "uptime_seconds": uptime_seconds,
        "workflows_path": workflows_path,
        "workflow_registry": workflow_registry,
    }


@app.post("/jobs")
def create_job(
    dataset_id: str, 
    workflow_name: str, 
    overwrite: bool = False, 
    parameters: dict = {},
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[StorageClient] = Depends(get_storage_client)
):
    """
    Create a new workflow job for a dataset.
    
    Uses collection-based workflow invocation - Galaxy fetches all files from S3
    as part of workflow execution via deferred file-source URIs. Each dataset_item
    is processed as a collection element, with Galaxy's map-over handling per-item
    processing through the tools.
    """
    # Check if all required clients are available
    if not galaxy:
        raise HTTPException(status_code=503, detail="Galaxy service is unavailable. Please check /health for details.")
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service is unavailable. Please check /health for details.")
    # Storage client is optional - Galaxy accesses S3 directly via file sources
    if not storage:
        logger.warning("Storage service is unavailable - this is OK since Galaxy accesses S3 directly via file sources")
    
    dataset_id_int = int(dataset_id)
    
    # Verify dataset exists and get first item_id for s3_base_path
    try:
        response = supabase.client.table("dataset_items").select("id").eq("dataset_id", dataset_id_int).order("id").limit(1).execute()
        if not response.data:
            raise HTTPException(status_code=404, detail=f"No dataset_items found for dataset {dataset_id}")
        first_item_id = response.data[0]["id"]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to get dataset items: {e}") from e
    
    logger.info(f"Creating collection workflow job for dataset_id={dataset_id_int} (first_item_id={first_item_id})")
    
    # Handle overwrite: delete old invocations and history
    if overwrite:
        logger.info(f"Overwrite mode: cleaning up existing data for dataset {dataset_id}")
        
        # Delete old workflow invocations from Supabase
        deleted_count = supabase.delete_workflow_invocations_by_dataset(dataset_id_int)
        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} old workflow invocation(s)")
        
        # Delete old Galaxy history (both from Galaxy and Supabase)
        old_history_id = supabase.delete_galaxy_history_by_dataset(dataset_id_int)
        if old_history_id:
            # Also delete/purge from Galaxy to free up resources
            if galaxy.delete_history(old_history_id, purge=True):
                logger.info(f"Deleted old Galaxy history {old_history_id}")
            else:
                logger.warning(f"Failed to delete Galaxy history {old_history_id} - may be orphaned")
    
    # Get or create Galaxy history for this dataset
    history_name = f"{workflow_name} - Dataset {dataset_id}"
    
    # Check if galaxy_history already exists for this dataset
    existing_history = supabase.get_galaxy_history_by_dataset(dataset_id_int)
    
    if existing_history:
        # Reuse existing history
        galaxy_history_id = existing_history["history_id"]
        galaxy_history_fk = existing_history["id"]
        s3_base_path = existing_history.get("s3_base_path", f"{dataset_id}/")
        logger.info(f"Reusing existing Galaxy history {galaxy_history_id} for dataset {dataset_id}")
    else:
        # Create new Galaxy history
        try:
            new_history = galaxy.create_history(name=history_name)
            galaxy_history_id = new_history.id
            
            # S3 base path uses dataset_id/ for collection-based workflow
            s3_base_path = f"{dataset_id}/"
            history_record = supabase.get_or_create_galaxy_history(
                dataset_id=dataset_id_int,
                history_id=galaxy_history_id,
                history_name=history_name,
                s3_base_path=s3_base_path
            )
            galaxy_history_fk = history_record["id"]
            logger.info(f"Created new Galaxy history {galaxy_history_id} for dataset {dataset_id}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to create Galaxy history: {e}")
    
    # Prepare workflow parameters (export paths, etc.)
    workflow_parameters = build_workflow_parameters(
        galaxy_client=galaxy,
        supabase_client=supabase,
        workflow_name=workflow_name,
        dataset_id=dataset_id_int,
        s3_base_path=s3_base_path
    )

    # Default Galaxy EU pipeline to scratch object store to reduce pressure on
    # default storage. This can be overridden via GALAXY_DEFAULT_* env vars.
    preferred_object_store_id = None
    preferred_intermediate_object_store_id = None
    preferred_outputs_object_store_id = None
    if workflow_name == "EndToEndPipeline-GalaxyEU":
        preferred_object_store_id = (
            galaxy.config.default_object_store_id
            or "s3_scratch_netapp01"
        )
        preferred_intermediate_object_store_id = galaxy.config.default_intermediate_object_store_id
        preferred_outputs_object_store_id = galaxy.config.default_outputs_object_store_id
    
    # Invoke workflow with collection input - Galaxy fetches during execution
    return _invoke_workflow_with_collection(
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
        history_fk=galaxy_history_fk
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
