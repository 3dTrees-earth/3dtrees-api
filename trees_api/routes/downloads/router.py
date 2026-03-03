import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from trees_api.integrations.supabase.client import (
    ActiveDownloadRequestExistsError,
    SupabaseClient,
)


logger = logging.getLogger("trees_api.routes.downloads.router")
router = APIRouter(prefix="/downloads", tags=["downloads"])


class AuthenticatedUser(BaseModel):
    id: str
    email: str


class CreateDownloadRequest(BaseModel):
    dataset_id: int
    include_raw: bool = True
    include_segmentation: bool = False


def get_supabase_client() -> Optional[SupabaseClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_supabase_client()


def _parse_bearer_token(authorization: Optional[str]) -> str:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization header format")
    token = parts[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    return token


def _resolve_user_from_token(supabase: SupabaseClient, token: str) -> AuthenticatedUser:
    # GoTrue /auth endpoints validate user tokens against the project API key
    # (anon/publishable), not the service role key.
    api_key = supabase.key
    if not api_key:
        raise HTTPException(status_code=503, detail="Supabase API key is not configured")
    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {token}",
    }
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.get(f"{supabase.url}/auth/v1/user", headers=headers)
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        payload: Dict[str, Any] = response.json()
        user_id = payload.get("id")
        email = payload.get("email")
        if not user_id or not email:
            raise HTTPException(status_code=401, detail="Could not resolve authenticated user")
        return AuthenticatedUser(id=user_id, email=email)
    except HTTPException:
        raise
    except Exception as error:
        logger.error("Token validation failed: %s", error)
        raise HTTPException(status_code=401, detail="Token validation failed")


def get_authenticated_user(
    authorization: Optional[str] = Header(default=None),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
) -> AuthenticatedUser:
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    token = _parse_bearer_token(authorization)
    return _resolve_user_from_token(supabase, token)


def _can_request_download(
    dataset: Dict[str, Any],
    user: AuthenticatedUser,
    is_core_team_member: bool,
) -> bool:
    visibility = str(dataset.get("visibility") or "").strip().lower()
    owner_id = dataset.get("user_id")
    is_owner = owner_id == user.id

    if visibility == "public":
        return True

    if visibility in {"private", "restricted", "view_only"}:
        return is_owner or is_core_team_member

    # Unknown visibility values remain locked down.
    return is_owner or is_core_team_member


@router.post("")
def create_download_request(
    request: CreateDownloadRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    if not request.include_raw and not request.include_segmentation:
        raise HTTPException(
            status_code=400, detail="At least one artifact type must be requested"
        )

    dataset = supabase.get_dataset_with_items(request.dataset_id)
    if not dataset:
        raise HTTPException(
            status_code=404, detail=f"Dataset {request.dataset_id} not found"
        )
    if dataset.get("archived"):
        raise HTTPException(
            status_code=400, detail=f"Dataset {request.dataset_id} is archived"
        )

    try:
        is_core_team_member = supabase.is_core_team_member(user.id)
    except Exception as error:
        logger.error("Failed to resolve core-team membership: %s", error)
        raise HTTPException(status_code=503, detail="Could not verify access permissions")

    if not _can_request_download(
        dataset=dataset,
        user=user,
        is_core_team_member=is_core_team_member,
    ):
        raise HTTPException(
            status_code=403,
            detail="Download access denied for this dataset",
        )

    try:
        return supabase.create_or_get_active_download_request(
            dataset_id=request.dataset_id,
            requested_by=user.id,
            requester_email=user.email,
            include_raw=request.include_raw,
            include_segmentation=request.include_segmentation,
        )
    except ActiveDownloadRequestExistsError as error:
        logger.warning("Active download intent conflict for user %s: %s", user.id, error)
        raise HTTPException(
            status_code=409,
            detail="An active request for this dataset and download mode already exists",
        )


@router.get("")
def list_download_requests(
    dataset_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    return supabase.list_download_requests_for_user(
        requested_by=user.id,
        dataset_id=dataset_id,
        limit=limit,
        offset=offset,
    )


@router.get("/{request_id}")
def get_download_request(
    request_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    row = supabase.get_download_request(request_id)
    if not row or row.get("requested_by") != user.id:
        raise HTTPException(status_code=404, detail="Download request not found")
    return row

