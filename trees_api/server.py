import os
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
from functools import lru_cache
import logging

from fastapi import FastAPI, Depends
from pydantic import BaseModel

from galaxy_client import GalaxyClient
from supabase_client import SupabaseClient



logger = logging.getLogger("uvicorn")

# Dependency injection functions
@lru_cache()
def get_galaxy_client() -> GalaxyClient:
    """Get Galaxy client instance."""
    return GalaxyClient()

@lru_cache()
def get_supabase_client() -> SupabaseClient:
    """Get Supabase client instance."""
    client = SupabaseClient()
    client.connect()

    return client

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize connections on startup."""
    # Pre-initialize clients to test connections
    client = get_supabase_client()

    user = os.getenv("SUPABASE_EMAIL")
    password = os.getenv("SUPABASE_PASSWORD")

    try:
        if user is not None and password is not None:
            auth = client.authenticate_user(user, password)
            logger.info(f"Authenticated user: {auth}")
    except RuntimeError as e:
        if "Authentication failed" in str(e):
            client.register_user(user, password)
            logger.info(f"New user created: {user}")
        else:
            raise e

    yield

    client.sign_out()

 

app = FastAPI(title="3DTrees API", description="API for 3DTrees", lifespan=lifespan)

# Pydantic models for request/response
class JobCreateRequest(BaseModel):
    dataset_id: str
    workflow_name: str
    overwrite: bool = False
    parameters: Dict[str, Any] = {}




@app.get("/")
def info():
    return {"message": "3DTrees API is running"}


@app.post("/jobs")
def create_job(
    dataset_id: str, 
    workflow_name: str, 
    overwrite: bool = False, 
    parameters: dict = {},
    galaxy: GalaxyClient = Depends(get_galaxy_client),
    supabase: SupabaseClient = Depends(get_supabase_client)
):
    # Clients are automatically injected
    pass

@app.get("/jobs")
def list_jobs(
    dataset_id: Optional[str] = None,
    supabase: SupabaseClient = Depends(get_supabase_client)
):
    # Supabase client is automatically injected
    pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
