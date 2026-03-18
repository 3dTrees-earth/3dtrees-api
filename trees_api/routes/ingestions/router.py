import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from trees_api.integrations.galaxy.client import GalaxyClient
from trees_api.integrations.storage.client import StorageClient, UploaderStorageClient
from trees_api.integrations.supabase.client import (
    ActiveIngestionSessionExistsError,
    SupabaseClient,
)
from trees_api.routes.downloads.router import AuthenticatedUser, get_authenticated_user
from trees_api.routes.jobs import service as jobs_service

logger = logging.getLogger("trees_api.routes.ingestions.router")
router = APIRouter(prefix="/ingestions", tags=["ingestions"])

MAX_UPLOAD_BYTES = 50 * 1024 * 1024 * 1024
VALID_EXTENSIONS = [".las", ".laz"]
PRESIGNED_URL_EXPIRATION = 15 * 60


class IngestionItemCreateInput(BaseModel):
    dataset_item_id: int
    file_name: str
    file_size_bytes: int
    content_type: str = "application/octet-stream"


class CreateIngestionRequest(BaseModel):
    dataset_id: int
    items: List[IngestionItemCreateInput] = Field(min_length=1)
    workflow_name: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class IngestionItemResponse(BaseModel):
    id: int
    dataset_item_id: int
    file_name: str
    file_size_bytes: int
    content_type: str
    s3_key: str
    upload_id: str
    status: str


class IngestionResponse(BaseModel):
    id: int
    dataset_id: int
    status: str
    workflow_name: Optional[str] = None
    metadata: Dict[str, Any]
    items: List[IngestionItemResponse]
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class PresignPartsRequest(BaseModel):
    ingestion_item_id: int
    part_numbers: List[int] = Field(min_length=1)


class PresignedPart(BaseModel):
    part_number: int
    url: str


class PresignPartsResponse(BaseModel):
    ingestion_item_id: int
    parts: List[PresignedPart]


class CompletePart(BaseModel):
    part_number: int
    e_tag: str


class CompleteIngestionItemRequest(BaseModel):
    ingestion_item_id: int
    parts: List[CompletePart] = Field(min_length=1)


class CompleteIngestionRequest(BaseModel):
    items: List[CompleteIngestionItemRequest] = Field(min_length=1)
    auto_process: bool = True
    workflow_name: Optional[str] = None
    overwrite: bool = False
    parameters: Dict[str, Any] = Field(default_factory=dict)


class CompleteIngestionResponse(BaseModel):
    ingestion_id: int
    dataset_id: int
    status: str
    workflow_triggered: bool
    workflow_invocation_id: Optional[str] = None


def get_supabase_client() -> Optional[SupabaseClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_supabase_client()


def get_uploader_storage() -> Optional[UploaderStorageClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_uploader_storage_client()


def get_galaxy_client() -> Optional[GalaxyClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_galaxy_client()


def get_storage_client() -> Optional[StorageClient]:
    from trees_api.app.connection_manager import connection_manager

    return connection_manager.get_storage_client()


def _validate_file_ext(file_name: str) -> str:
    lower = file_name.lower()
    if "." not in lower:
        raise HTTPException(status_code=400, detail=f"File '{file_name}' has no extension")
    ext = lower[lower.rfind(".") :]
    if ext not in VALID_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type for '{file_name}'. Only {', '.join(VALID_EXTENSIONS)} files are allowed.",
        )
    return ext


def _serialize_ingestion(session: Dict[str, Any], items: List[Dict[str, Any]]) -> IngestionResponse:
    return IngestionResponse(
        id=session["id"],
        dataset_id=session["dataset_id"],
        status=session["status"],
        workflow_name=session.get("workflow_name"),
        metadata=session.get("metadata") or {},
        created_at=session.get("created_at"),
        updated_at=session.get("updated_at"),
        items=[
            IngestionItemResponse(
                id=row["id"],
                dataset_item_id=row["dataset_item_id"],
                file_name=row["file_name"],
                file_size_bytes=row["file_size_bytes"],
                content_type=row.get("content_type") or "application/octet-stream",
                s3_key=row.get("s3_key") or "",
                upload_id=row.get("upload_id") or "",
                status=row["status"],
            )
            for row in items
        ],
    )


@router.post("", response_model=IngestionResponse)
def create_ingestion(
    request: CreateIngestionRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
    idempotency_key: Optional[str] = Header(default=None, alias="Idempotency-Key"),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    if not storage:
        raise HTTPException(status_code=503, detail="Storage service unavailable")

    dataset = supabase.get_dataset_with_items(request.dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {request.dataset_id} not found")
    if dataset.get("archived"):
        raise HTTPException(status_code=400, detail=f"Dataset {request.dataset_id} is archived")
    if dataset.get("user_id") != user.id:
        raise HTTPException(status_code=403, detail="Only the dataset owner can create ingestion")

    dataset_item_rows = dataset.get("dataset_items") or []
    item_ids = {int(row["id"]) for row in dataset_item_rows}
    for item in request.items:
        if item.dataset_item_id not in item_ids:
            raise HTTPException(
                status_code=400,
                detail=f"dataset_item_id {item.dataset_item_id} does not belong to dataset {request.dataset_id}",
            )
        if item.file_size_bytes <= 0:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file size for dataset_item_id {item.dataset_item_id}",
            )
        if item.file_size_bytes > MAX_UPLOAD_BYTES:
            raise HTTPException(
                status_code=400,
                detail=f"File too large for dataset_item_id {item.dataset_item_id}. Maximum size is {MAX_UPLOAD_BYTES} bytes.",
            )

    try:
        session = supabase.create_or_get_active_ingestion_session(
            dataset_id=request.dataset_id,
            created_by=user.id,
            idempotency_key=idempotency_key,
            workflow_name=request.workflow_name,
            metadata=request.metadata,
        )
    except ActiveIngestionSessionExistsError as error:
        logger.warning("Active ingestion conflict for user %s: %s", user.id, error)
        raise HTTPException(
            status_code=409,
            detail="An active ingestion session already exists for this dataset",
        )

    if session["status"] == "pending":
        supabase.update_ingestion_session(session["id"], status="uploading", started_at=datetime.now(timezone.utc))
        session["status"] = "uploading"

    response_items: List[Dict[str, Any]] = []
    existing_items_by_dataset_item_id: Dict[int, Dict[str, Any]] = {}
    for row in supabase.list_ingestion_session_items(session["id"]):
        existing_items_by_dataset_item_id[int(row["dataset_item_id"])] = row

    for item in request.items:
        existing_item = existing_items_by_dataset_item_id.get(item.dataset_item_id)
        if existing_item and existing_item.get("upload_id") and existing_item.get("s3_key"):
            response_items.append(existing_item)
            continue

        ext = _validate_file_ext(item.file_name)
        key = f"RAW/{request.dataset_id}/{item.dataset_item_id}/raw{ext}"
        try:
            result = storage.client.create_multipart_upload(
                Bucket=storage.bucket_name_raw,
                Key=key,
                ContentType=item.content_type or "application/octet-stream",
            )
        except Exception as error:
            logger.error("Failed to create multipart upload for item %s: %s", item.dataset_item_id, error)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to initialize multipart upload for dataset_item_id {item.dataset_item_id}",
            )

        upload_id = result.get("UploadId")
        if not upload_id:
            raise HTTPException(
                status_code=500,
                detail=f"Multipart initialization returned no upload ID for dataset_item_id {item.dataset_item_id}",
            )

        row = supabase.create_or_update_ingestion_session_item(
            session_id=session["id"],
            dataset_item_id=item.dataset_item_id,
            file_name=item.file_name,
            file_size_bytes=item.file_size_bytes,
            content_type=item.content_type,
            key=key,
            upload_id=upload_id,
            status="uploading",
        )
        response_items.append(row)

    session = supabase.get_ingestion_session_for_user(session["id"], user.id) or session
    return _serialize_ingestion(session, response_items)


@router.post("/{ingestion_id}/presign", response_model=PresignPartsResponse)
def presign_parts(
    ingestion_id: int,
    request: PresignPartsRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    if not storage:
        raise HTTPException(status_code=503, detail="Storage service unavailable")

    session = supabase.get_ingestion_session_for_user(ingestion_id, user.id)
    if not session:
        raise HTTPException(status_code=404, detail="Ingestion session not found")
    if session["status"] not in {"pending", "uploading"}:
        raise HTTPException(status_code=409, detail="Ingestion session is not uploadable")

    item = supabase.get_ingestion_session_item(ingestion_id, request.ingestion_item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Ingestion item not found")

    parts: List[PresignedPart] = []
    for part_number in request.part_numbers:
        try:
            url = storage.client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": storage.bucket_name_raw,
                    "Key": item["s3_key"],
                    "UploadId": item["upload_id"],
                    "PartNumber": part_number,
                },
                ExpiresIn=PRESIGNED_URL_EXPIRATION,
            )
        except Exception as error:
            logger.error("Failed to presign part %s for ingestion item %s: %s", part_number, item["id"], error)
            raise HTTPException(status_code=500, detail="Failed to generate presigned URL")
        parts.append(PresignedPart(part_number=part_number, url=url))

    return PresignPartsResponse(ingestion_item_id=item["id"], parts=parts)


@router.post("/{ingestion_id}/complete", response_model=CompleteIngestionResponse)
def complete_ingestion(
    ingestion_id: int,
    request: CompleteIngestionRequest,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
    galaxy: Optional[GalaxyClient] = Depends(get_galaxy_client),
    processing_storage: Optional[StorageClient] = Depends(get_storage_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    if not storage:
        raise HTTPException(status_code=503, detail="Storage service unavailable")

    session = supabase.get_ingestion_session_for_user(ingestion_id, user.id)
    if not session:
        raise HTTPException(status_code=404, detail="Ingestion session not found")
    if session["status"] in {"completed", "failed", "aborted", "expired"}:
        raise HTTPException(status_code=409, detail=f"Ingestion session is already {session['status']}")

    supabase.update_ingestion_session(ingestion_id, status="finalizing")

    try:
        for item_req in request.items:
            item = supabase.get_ingestion_session_item(ingestion_id, item_req.ingestion_item_id)
            if not item:
                raise HTTPException(
                    status_code=404,
                    detail=f"Ingestion item {item_req.ingestion_item_id} not found",
                )

            sorted_parts = sorted(item_req.parts, key=lambda p: p.part_number)
            multipart_upload = {
                "Parts": [
                    {"ETag": part.e_tag, "PartNumber": part.part_number}
                    for part in sorted_parts
                ]
            }

            storage.client.complete_multipart_upload(
                Bucket=storage.bucket_name_raw,
                Key=item["s3_key"],
                UploadId=item["upload_id"],
                MultipartUpload=multipart_upload,
            )
            storage.client.head_object(Bucket=storage.bucket_name_raw, Key=item["s3_key"])

            updated_item = supabase.update_dataset_item_bucket_path_by_id(
                dataset_item_id=item["dataset_item_id"],
                bucket_path=item["s3_key"],
            )
            if not updated_item:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to update dataset_item {item['dataset_item_id']} bucket path",
                )

            supabase.update_ingestion_session_item(
                session_id=ingestion_id,
                ingestion_item_id=item["id"],
                status="completed",
                completed_at=datetime.now(timezone.utc),
            )

        workflow_triggered = False
        workflow_invocation_id: Optional[str] = None
        if request.auto_process:
            workflow_name = request.workflow_name or session.get("workflow_name") or "EndToEndPipeline"
            invocation = jobs_service.create_job(
                dataset_id=str(session["dataset_id"]),
                workflow_name=workflow_name,
                overwrite=request.overwrite,
                parameters=request.parameters,
                requesting_user_id=user.id,
                galaxy=galaxy,
                supabase=supabase,
                storage=processing_storage,
            )
            workflow_triggered = True
            workflow_invocation_id = getattr(invocation, "invocation_id", None) or invocation.get("invocation_id")

        supabase.update_ingestion_session(
            ingestion_id,
            status="completed",
            finished_at=datetime.now(timezone.utc),
        )

        return CompleteIngestionResponse(
            ingestion_id=ingestion_id,
            dataset_id=session["dataset_id"],
            status="completed",
            workflow_triggered=workflow_triggered,
            workflow_invocation_id=workflow_invocation_id,
        )
    except HTTPException:
        supabase.update_ingestion_session(
            ingestion_id,
            status="failed",
            finished_at=datetime.now(timezone.utc),
            failure_code="api_error",
        )
        raise
    except Exception as error:
        logger.error("Failed to complete ingestion %s: %s", ingestion_id, error)
        supabase.update_ingestion_session(
            ingestion_id,
            status="failed",
            finished_at=datetime.now(timezone.utc),
            failure_code="exception",
            failure_reason=str(error),
        )
        raise HTTPException(status_code=500, detail=f"Failed to complete ingestion: {error}")


@router.get("/{ingestion_id}", response_model=IngestionResponse)
def get_ingestion(
    ingestion_id: int,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")

    session = supabase.get_ingestion_session_for_user(ingestion_id, user.id)
    if not session:
        raise HTTPException(status_code=404, detail="Ingestion session not found")
    items = supabase.list_ingestion_session_items(ingestion_id)
    return _serialize_ingestion(session, items)


@router.get("", response_model=List[IngestionResponse])
def list_ingestions(
    dataset_id: Optional[int] = None,
    limit: int = 100,
    offset: int = 0,
    user: AuthenticatedUser = Depends(get_authenticated_user),
    supabase: Optional[SupabaseClient] = Depends(get_supabase_client),
):
    if not supabase:
        raise HTTPException(status_code=503, detail="Supabase service unavailable")
    sessions = supabase.list_ingestion_sessions_for_user(
        created_by=user.id,
        dataset_id=dataset_id,
        limit=limit,
        offset=offset,
    )
    results: List[IngestionResponse] = []
    for session in sessions:
        items = supabase.list_ingestion_session_items(session["id"])
        results.append(_serialize_ingestion(session, items))
    return results

