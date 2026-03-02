import logging
from typing import Optional

from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from trees_api.storage_client import UploaderStorageClient

logger = logging.getLogger("trees_api.routes.uploads.router")

router = APIRouter(prefix="/upload/multipart", tags=["upload"])

MAX_UPLOAD_BYTES = 50 * 1024 * 1024 * 1024
VALID_EXTENSIONS = [".las", ".laz"]
PRESIGNED_URL_EXPIRATION = 15 * 60


class CreateMultipartRequest(BaseModel):
    datasetId: int
    datasetItemId: int
    fileName: str
    contentType: str
    fileSize: int


class CreateMultipartResponse(BaseModel):
    key: str
    uploadId: str


class PresignPartsRequest(BaseModel):
    key: str
    uploadId: str
    partNumbers: list[int]
    contentType: Optional[str] = None


class PresignPartsResponse(BaseModel):
    parts: list[dict]


class CompleteMultipartRequest(BaseModel):
    key: str
    uploadId: str
    parts: list[dict]


class CompleteMultipartResponse(BaseModel):
    location: Optional[str] = None
    etag: Optional[str] = None


class AbortMultipartRequest(BaseModel):
    key: str
    uploadId: str


class AbortMultipartResponse(BaseModel):
    ok: bool


class ListPartsResponse(BaseModel):
    parts: list[dict]


def get_uploader_storage() -> Optional[UploaderStorageClient]:
    from trees_api.connection_manager import connection_manager

    return connection_manager.get_uploader_storage_client()


@router.options("/create")
@router.options("/presign")
@router.options("/complete")
@router.options("/abort")
@router.options("/list")
async def handle_options(response: Response):
    return Response(status_code=204)


@router.post("/create", response_model=CreateMultipartResponse)
async def create_multipart_upload(
    request: CreateMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details.",
        )

    logger.info(
        "Creating multipart upload for dataset %s, item %s",
        request.datasetId,
        request.datasetItemId,
    )

    file_ext = request.fileName.lower()[request.fileName.rfind(".") :]
    if file_ext not in VALID_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Only {', '.join(VALID_EXTENSIONS)} files are allowed.",
        )

    if request.fileSize > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size is {MAX_UPLOAD_BYTES} bytes.",
        )

    key = f"RAW/{request.datasetId}/{request.datasetItemId}/raw{file_ext}"
    logger.info("Generated S3 key: %s", key)

    try:
        response = storage.client.create_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=key,
            ContentType=request.contentType or "application/octet-stream",
        )
        upload_id = response.get("UploadId")
        if not upload_id:
            raise HTTPException(
                status_code=500,
                detail="Failed to create multipart upload - no upload ID returned",
            )
        logger.info("Created multipart upload with ID: %s", upload_id)
        return CreateMultipartResponse(key=key, uploadId=upload_id)
    except ClientError as error:
        logger.error("S3 ClientError during create_multipart_upload: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create multipart upload: {error}",
        )
    except Exception as error:
        logger.error("Unexpected error during create_multipart_upload: %s", error)
        raise HTTPException(status_code=500, detail=f"Internal error: {error}")


@router.post("/presign", response_model=PresignPartsResponse)
async def presign_parts(
    request: PresignPartsRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details.",
        )
    if not request.partNumbers:
        raise HTTPException(status_code=400, detail="partNumbers array cannot be empty")

    logger.info("Generating presigned URLs for %s parts", len(request.partNumbers))

    try:
        parts = []
        for part_number in request.partNumbers:
            url = storage.client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": storage.bucket_name_raw,
                    "Key": request.key,
                    "UploadId": request.uploadId,
                    "PartNumber": part_number,
                },
                ExpiresIn=PRESIGNED_URL_EXPIRATION,
            )
            parts.append({"partNumber": part_number, "url": url})

        logger.info("Generated %s presigned URLs", len(parts))
        return PresignPartsResponse(parts=parts)
    except ClientError as error:
        logger.error("S3 ClientError during presign_parts: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate presigned URLs: {error}",
        )
    except Exception as error:
        logger.error("Unexpected error during presign_parts: %s", error)
        raise HTTPException(status_code=500, detail=f"Internal error: {error}")


@router.post("/complete", response_model=CompleteMultipartResponse)
async def complete_multipart_upload(
    request: CompleteMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details.",
        )
    if not request.parts:
        raise HTTPException(status_code=400, detail="parts array cannot be empty")

    logger.info("Completing multipart upload for key: %s", request.key)

    try:
        sorted_parts = sorted(request.parts, key=lambda part: part["partNumber"])
        multipart_upload = {
            "Parts": [
                {"ETag": part["eTag"], "PartNumber": part["partNumber"]}
                for part in sorted_parts
            ]
        }

        response = storage.client.complete_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=request.key,
            UploadId=request.uploadId,
            MultipartUpload=multipart_upload,
        )

        logger.info("Successfully completed multipart upload for key: %s", request.key)

        try:
            storage.client.head_object(Bucket=storage.bucket_name_raw, Key=request.key)
            logger.info("Verified object exists: %s", request.key)
        except ClientError as head_error:
            logger.warning("HEAD verification failed for %s: %s", request.key, head_error)

        return CompleteMultipartResponse(
            location=response.get("Location"),
            etag=response.get("ETag"),
        )
    except ClientError as error:
        logger.error("S3 ClientError during complete_multipart_upload: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete multipart upload: {error}",
        )
    except Exception as error:
        logger.error("Unexpected error during complete_multipart_upload: %s", error)
        raise HTTPException(status_code=500, detail=f"Internal error: {error}")


@router.post("/abort", response_model=AbortMultipartResponse)
async def abort_multipart_upload(
    request: AbortMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details.",
        )

    logger.info("Aborting multipart upload for key: %s", request.key)

    try:
        storage.client.abort_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=request.key,
            UploadId=request.uploadId,
        )
        logger.info("Successfully aborted multipart upload for key: %s", request.key)
        return AbortMultipartResponse(ok=True)
    except ClientError as error:
        logger.error("S3 ClientError during abort_multipart_upload: %s", error)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to abort multipart upload: {error}",
        )
    except Exception as error:
        logger.error("Unexpected error during abort_multipart_upload: %s", error)
        raise HTTPException(status_code=500, detail=f"Internal error: {error}")


@router.get("/list", response_model=ListPartsResponse)
async def list_parts(
    key: str,
    uploadId: str,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage),
):
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details.",
        )
    if not key or not uploadId:
        raise HTTPException(
            status_code=400, detail="Missing required query params: key, uploadId"
        )

    logger.info("Listing parts for key: %s, uploadId: %s", key, uploadId)

    try:
        parts = []
        part_number_marker = None
        while True:
            params = {
                "Bucket": storage.bucket_name_raw,
                "Key": key,
                "UploadId": uploadId,
            }
            if part_number_marker is not None:
                params["PartNumberMarker"] = part_number_marker

            response = storage.client.list_parts(**params)

            if "Parts" in response:
                for part in response["Parts"]:
                    if (
                        part.get("PartNumber")
                        and part.get("ETag")
                        and part.get("Size") is not None
                    ):
                        parts.append(
                            {
                                "partNumber": part["PartNumber"],
                                "eTag": part["ETag"],
                                "size": part["Size"],
                            }
                        )

            if response.get("IsTruncated") and response.get("NextPartNumberMarker"):
                part_number_marker = response["NextPartNumberMarker"]
            else:
                break

        logger.info("Found %s uploaded parts", len(parts))
        return ListPartsResponse(parts=parts)
    except ClientError as error:
        logger.error("S3 ClientError during list_parts: %s", error)
        raise HTTPException(status_code=500, detail=f"Failed to list parts: {error}")
    except Exception as error:
        logger.error("Unexpected error during list_parts: %s", error)
        raise HTTPException(status_code=500, detail=f"Internal error: {error}")


__all__ = [
    "router",
    "get_uploader_storage",
    "CreateMultipartRequest",
    "CreateMultipartResponse",
    "PresignPartsRequest",
    "PresignPartsResponse",
    "CompleteMultipartRequest",
    "CompleteMultipartResponse",
    "AbortMultipartRequest",
    "AbortMultipartResponse",
    "ListPartsResponse",
]

