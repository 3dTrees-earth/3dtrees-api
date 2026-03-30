import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from trees_api.integrations.supabase.client import SupabaseClient
from trees_api.routes.downloads.router import AuthenticatedUser, get_authenticated_user

logger = logging.getLogger("trees_api.routes.collections.router")
router = APIRouter(prefix="/collections", tags=["collections"])


class CollectionResponse(BaseModel):
    id: int
    uuid: str
    owner_user_id: str
    name: str
    description: Optional[str] = None
    archived: bool
    created_at: str
    updated_at: str


class CreateCollectionRequest(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: Optional[str] = None
    owner_user_id: Optional[str] = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Collection name cannot be empty")
        return normalized


class UpdateCollectionRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    description: Optional[str] = None

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized:
            raise ValueError("Collection name cannot be empty")
        return normalized


class ArchiveCollectionResponse(BaseModel):
    id: int
    status: str
    unassigned_dataset_count: int


class DatasetCollectionAssignmentRequest(BaseModel):
    collection_id: Optional[int] = None


class DatasetCollectionAssignmentResponse(BaseModel):
    dataset_id: int
    collection_id: Optional[int] = None


class DatasetCollectionAssignmentListItem(BaseModel):
    id: int
    title: str
    collection_id: Optional[int] = None
    created_at: str


def get_supabase_client() -> Optional[SupabaseClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_supabase_client()


def _is_duplicate_collection_name_error(error: Exception) -> bool:
    message = str(error).lower()
    return "duplicate key value" in message and "idx_collections_owner_name_active" in message


def _serialize_collection(row: Dict[str, Any]) -> CollectionResponse:
    return CollectionResponse(
        id=row["id"],
        uuid=row["uuid"],
        owner_user_id=row["owner_user_id"],
        name=row["name"],
        description=row.get("description"),
        archived=bool(row.get("archived")),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _has_platform_dataset_admin(supabase: SupabaseClient, user: AuthenticatedUser) -> bool:
    try:
        return supabase.has_platform_dataset_admin(user.id)
    except Exception as error:
        logger.error(
            "Failed to resolve platform dataset admin access for user %s: %s",
            user.id,
            error,
        )
        raise HTTPException(
            status_code=503,
            detail="Could not verify platform dataset admin permissions",
        )


@router.get("", response_model=List[CollectionResponse])
def list_collections(
    include_archived: bool = False,
    owner_user_id: Optional[str] = Query(default=None),
    scope: Optional[str] = Query(default=None, pattern="^(all)$"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    has_platform_admin = _has_platform_dataset_admin(supabase, user)
    target_owner_user_id = owner_user_id or user.id

    if scope == "all":
        if not has_platform_admin:
            raise HTTPException(status_code=403, detail="Platform admin access required")
        rows = supabase.list_collections(
            include_archived=include_archived,
            limit=limit,
            offset=offset,
        )
        return [_serialize_collection(row) for row in rows]

    if target_owner_user_id != user.id and not has_platform_admin:
        raise HTTPException(status_code=403, detail="Platform admin access required")

    rows = supabase.list_collections(
        owner_user_id=target_owner_user_id,
        include_archived=include_archived,
        limit=limit,
        offset=offset,
    )
    return [_serialize_collection(row) for row in rows]


@router.post("", response_model=CollectionResponse)
def create_collection(
    request: CreateCollectionRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    target_owner_user_id = request.owner_user_id or user.id
    if target_owner_user_id != user.id and not _has_platform_dataset_admin(supabase, user):
        raise HTTPException(status_code=403, detail="Platform admin access required")

    try:
        row = supabase.create_collection(
            owner_user_id=target_owner_user_id,
            name=request.name,
            description=request.description,
        )
        return _serialize_collection(row)
    except Exception as error:
        if _is_duplicate_collection_name_error(error):
            raise HTTPException(status_code=409, detail="Collection name already exists")
        logger.error("Failed to create collection for user %s: %s", user.id, error)
        raise HTTPException(status_code=500, detail="Failed to create collection")


@router.patch("/{collection_id}", response_model=CollectionResponse)
def update_collection(
    collection_id: int,
    request: UpdateCollectionRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    existing = supabase.get_collection(collection_id=collection_id, include_archived=True)
    if not existing:
        raise HTTPException(status_code=404, detail="Collection not found")
    has_platform_admin = _has_platform_dataset_admin(supabase, user)
    if existing["owner_user_id"] != user.id and not has_platform_admin:
        raise HTTPException(status_code=404, detail="Collection not found")

    updates = request.model_dump(exclude_unset=True)

    if not updates:
        return _serialize_collection(existing)

    try:
        updated = supabase.update_collection_for_user(
            collection_id=collection_id,
            owner_user_id=existing["owner_user_id"],
            **updates,
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Collection not found")
        return _serialize_collection(updated)
    except HTTPException:
        raise
    except Exception as error:
        if _is_duplicate_collection_name_error(error):
            raise HTTPException(status_code=409, detail="Collection name already exists")
        logger.error("Failed to update collection %s for user %s: %s", collection_id, user.id, error)
        raise HTTPException(status_code=500, detail="Failed to update collection")


@router.delete("/{collection_id}", response_model=ArchiveCollectionResponse)
def archive_collection(
    collection_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    existing = supabase.get_collection(collection_id=collection_id, include_archived=True)
    if not existing:
        raise HTTPException(status_code=404, detail="Collection not found")
    has_platform_admin = _has_platform_dataset_admin(supabase, user)
    if existing["owner_user_id"] != user.id and not has_platform_admin:
        raise HTTPException(status_code=404, detail="Collection not found")
    if existing.get("archived"):
        return ArchiveCollectionResponse(
            id=collection_id,
            status="archived",
            unassigned_dataset_count=0,
        )

    unassigned_count = supabase.unassign_datasets_for_collection(
        collection_id=collection_id,
        owner_user_id=existing["owner_user_id"],
    )
    archived = supabase.archive_collection_for_user(
        collection_id=collection_id,
        owner_user_id=existing["owner_user_id"],
    )
    if not archived:
        raise HTTPException(status_code=500, detail="Failed to archive collection")

    return ArchiveCollectionResponse(
        id=collection_id,
        status="archived",
        unassigned_dataset_count=unassigned_count,
    )


@router.get("/datasets", response_model=List[DatasetCollectionAssignmentListItem])
def list_dataset_collection_assignments(
    owner_user_id: Optional[str] = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    target_owner_user_id = owner_user_id or user.id
    if target_owner_user_id != user.id and not _has_platform_dataset_admin(supabase, user):
        raise HTTPException(status_code=403, detail="Platform admin access required")

    rows = supabase.list_dataset_collection_assignments_for_user(
        owner_user_id=target_owner_user_id,
        limit=limit,
        offset=offset,
    )
    return [
        DatasetCollectionAssignmentListItem(
            id=row["id"],
            title=row.get("title") or f"Dataset {row['id']}",
            collection_id=row.get("collection_id"),
            created_at=row["created_at"],
        )
        for row in rows
    ]


@router.put("/datasets/{dataset_id}/collection", response_model=DatasetCollectionAssignmentResponse)
def assign_dataset_to_collection(
    dataset_id: int,
    request: DatasetCollectionAssignmentRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    dataset = supabase.get_dataset(dataset_id=dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    has_platform_admin = _has_platform_dataset_admin(supabase, user)
    if dataset["user_id"] != user.id and not has_platform_admin:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if request.collection_id is not None:
        collection = supabase.get_collection(collection_id=request.collection_id, include_archived=False)
        if not collection:
            raise HTTPException(status_code=404, detail="Collection not found")
        if collection["owner_user_id"] != dataset["user_id"]:
            raise HTTPException(
                status_code=400,
                detail="Dataset can only be assigned to a collection owned by the same user",
            )

    updated = supabase.assign_dataset_to_collection(
        dataset_id=dataset_id,
        owner_user_id=dataset["user_id"],
        collection_id=request.collection_id,
    )
    if not updated:
        raise HTTPException(status_code=500, detail="Failed to update dataset collection assignment")

    return DatasetCollectionAssignmentResponse(
        dataset_id=dataset_id,
        collection_id=updated.get("collection_id"),
    )

