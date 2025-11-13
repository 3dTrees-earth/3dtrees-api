import os
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from trees_api.galaxy_client import GalaxyClient
from trees_api.supabase_client import SupabaseClient
from trees_api.storage_client import StorageClient
from trees_api.connection_manager import connection_manager
from trees_api.upload_router import router as upload_router


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
    
    # Import dataset directly from S3/MinIO using Galaxy file sources
    try:
        database_dataset = supabase.get_dataset(dataset_id)
        
        # Construct file source URI using helper method
        # bucket_path is like "LAS/Example_Platane.laz"
        file_source_uri = galaxy.build_file_source_uri("raw-storage", database_dataset.bucket_path)
        
        logger.info(f"Importing dataset from file source: {file_source_uri}")
        dataset = galaxy.import_from_file_source(history, file_source_uri)
        galaxy.wait_for_upload(dataset)  # Wait for import to complete
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Importing dataset {dataset_id} from file source failed: {e}")
    
    # Prepare workflow parameters (e.g., export path for Standard workflow)
    workflow_parameters = {}
    if workflow_name == "Standard":
        # Get dataset_item_id for constructing the export path
        try:
            dataset_item_resp = supabase.client.table("dataset_items").select("id").eq("dataset_id", dataset_id).limit(1).execute()
            if dataset_item_resp.data:
                dataset_item_id = dataset_item_resp.data[0]["id"]
                # Set export directory for step 2 (export_remote tool)
                # Path structure matches runner: standard/{dataset_id}/{dataset_item_id}/
                export_path = f"gxfiles://products-storage/standard/{dataset_id}/{dataset_item_id}/"
                workflow_parameters = {
                    "2": {  # Step ID of export_remote tool in Standard.ga
                        "d_uri": export_path
                    }
                }
                logger.info(f"Export path for Standard workflow: {export_path}")
        except Exception as e:
            logger.warning(f"Could not set export path: {e}")
    
    elif workflow_name == "Segmentation":
        # Get dataset_item_id for constructing the export paths
        try:
            dataset_item_resp = supabase.client.table("dataset_items").select("id").eq("dataset_id", dataset_id).limit(1).execute()
            if dataset_item_resp.data:
                dataset_item_id = dataset_item_resp.data[0]["id"]
                # Set export directories for all 4 export steps
                # Path structure matches runner
                workflow_parameters = {
                    "2": {  # Step 2: Export standardized LAZ
                        "d_uri": f"gxfiles://products-storage/standard/{dataset_id}/{dataset_item_id}/"
                    },
                    "4": {  # Step 4: Export overviews
                        "d_uri": f"gxfiles://products-storage/overviews/{dataset_id}/{dataset_item_id}/"
                    },
                    "6": {  # Step 6: Export segmentation
                        "d_uri": f"gxfiles://products-storage/segmentation/{dataset_id}/{dataset_item_id}/"
                    },
                    "8": {  # Step 8: Export 3dtiles
                        "d_uri": f"gxfiles://products-storage/3dtiles/{dataset_id}/{dataset_item_id}/"
                    }
                }
                logger.info(f"Export paths configured for Segmentation workflow")
        except Exception as e:
            logger.warning(f"Could not set export paths: {e}")
    
    # now invoke the workflow
    try:
        invocation_result = galaxy.invoke_workflow_with_dataset(
            workflow_name=workflow_name,
            dataset_id=dataset.id,
            history_name=history_name,
            parameters=workflow_parameters if workflow_parameters else None
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
        env_prefix="API_SERVER_",
    )

if __name__ == "__main__":
    import uvicorn
    settings = APIServerSettings()
    uvicorn.run("trees_api.server:app", host=settings.host, port=settings.port, reload=settings.reload)
