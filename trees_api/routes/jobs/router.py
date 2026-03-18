from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends

from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.routes.jobs import service
from trees_api.integrations.storage.client import StorageClient
from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import AuthenticatedUser, get_authenticated_user

router = APIRouter(tags=["jobs"])


def get_galaxy_client() -> Optional[GalaxyClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_galaxy_client()


def get_supabase_client() -> Optional[SupabaseClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_supabase_client()


def get_storage_client() -> Optional[StorageClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_storage_client()


@router.post("/jobs")
def create_job(
    dataset_id: str,
    workflow_name: str,
    overwrite: bool = False,
    parameters: dict = {},
    user: AuthenticatedUser = Depends(get_authenticated_user),
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[StorageClient] = Depends(get_storage_client),
):
    return service.create_job(
        dataset_id=dataset_id,
        workflow_name=workflow_name,
        overwrite=overwrite,
        parameters=parameters,
        requesting_user_id=user.id,
        galaxy=galaxy,
        supabase=supabase,
        storage=storage,
    )


@router.get("/jobs")
def list_jobs(
    dataset_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    return service.list_jobs(
        dataset_id=dataset_id,
        requesting_user_id=user.id,
        limit=limit,
        offset=offset,
        supabase=supabase,
    )

