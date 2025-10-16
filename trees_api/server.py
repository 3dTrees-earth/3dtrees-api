import os
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from pathlib import Path
import logging
import tempfile

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient
from trees_api.connection_manager import connection_manager


logger = logging.getLogger("uvicorn")

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
    logger.info("Starting up 3DTrees API...")
    
    # Try to initialize all clients (but don't fail if they can't connect)
    connection_manager.connect_galaxy()
    connection_manager.connect_supabase()
    connection_manager.connect_storage()
    
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

# Pydantic models for request/response
class JobCreateRequest(BaseModel):
    dataset_id: str
    workflow_name: str
    overwrite: bool = False
    parameters: Dict[str, Any] = {}



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
    overwrite: bool = False, 
    parameters: dict = {},
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[StorageClient] = Depends(get_storage_client)
):
    # Check if all required clients are available
    if not galaxy:
        raise HTTPException(status_code=503, detail="Galaxy service is unavailable. Please check /health for details.")
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service is unavailable. Please check /health for details.")
    if not storage:
        raise HTTPException(status_code=503, detail="Storage service is unavailable. Please check /health for details.")
    workflow_name = workflow_name.capitalize()
    history_name = f"{workflow_name} - {dataset_id}"
    # make sure the requested workflow exists in galaxy
    try:
        workflow = galaxy.ensure_workflow_available(workflow_name)
        history = galaxy.create_history(history_name)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Loading Workflow {workflow_name} and history failed: {e} ")
    
    # development implementation: first download the dataset from s3
    try:
        database_dataset = supabase.get_dataset(dataset_id)
        with tempfile.NamedTemporaryFile(suffix=".laz") as temp_file:
            storage.download_file(database_dataset.bucket_path, temp_file.name)
            dataset = galaxy.upload_file(history, Path(temp_file.name))
            galaxy.wait_for_upload(dataset)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Downloading dataset {dataset_id} failed: {e} ")
    
    # now invoke the workflow
    try:
        invocation_result = galaxy.invoke_workflow_with_dataset(
            workflow_name=workflow_name,
            dataset_id=dataset.id,
            history_name=history_name
        )
        print(invocation_result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invoking workflow {workflow_name} failed: {e} ")
    
    # if there are no errors invoking the workflow, create the workflow invocation in Supabase
    try:
        workflow_invocation = supabase.create_workflow_invocation(
            workflow_uuid=invocation_result["invocation_id"],
            dataset_id=dataset_id,
            workflow_name=workflow_name
        )
        
        # Store the parameters in the parameters field
        if parameters:
            supabase.update_workflow_invocation(
                workflow_invocation.invocation_id,
                parameters=parameters
            )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Creating workflow invocation in Supabase failed: {e} ")

    return workflow_invocation

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
        env_file=".env",
        env_prefix="API_SERVER_",
    )

if __name__ == "__main__":
    import uvicorn
    settings = APIServerSettings()
    uvicorn.run("trees_api.server:app", host=settings.host, port=settings.port, reload=settings.reload)
