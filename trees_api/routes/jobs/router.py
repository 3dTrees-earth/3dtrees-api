from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends

from trees_api.galaxy_client import GalaxyClient
from trees_api.routes.jobs import service
from trees_api.storage_client import StorageClient
from trees_api.supabase_client import SupabaseClient

router = APIRouter(tags=["jobs"])


def get_galaxy_client() -> Optional[GalaxyClient]:
    from trees_api.connection_manager import connection_manager

    return connection_manager.get_galaxy_client()


def get_supabase_client() -> Optional[SupabaseClient]:
    from trees_api.connection_manager import connection_manager

    return connection_manager.get_supabase_client()


def get_storage_client() -> Optional[StorageClient]:
    from trees_api.connection_manager import connection_manager

    return connection_manager.get_storage_client()


@router.post("/jobs")
def create_job(
    dataset_id: str,
    workflow_name: str,
    overwrite: bool = False,
    parameters: dict = {},
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[StorageClient] = Depends(get_storage_client),
):
    return service.create_job(
        dataset_id=dataset_id,
        workflow_name=workflow_name,
        overwrite=overwrite,
        parameters=parameters,
        galaxy=galaxy,
        supabase=supabase,
        storage=storage,
    )


@router.get("/jobs")
def list_jobs(
    dataset_id: Optional[int] = None,
    user_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    return service.list_jobs(
        dataset_id=dataset_id,
        user_id=user_id,
        limit=limit,
        offset=offset,
        supabase=supabase,
    )

