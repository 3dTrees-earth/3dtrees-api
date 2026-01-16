import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel
from botocore.exceptions import ClientError

from trees_api.storage_client import UploaderStorageClient

logger = logging.getLogger("trees_api.upload_router")

router = APIRouter(prefix="/upload/multipart", tags=["upload"])

# Constants
MAX_UPLOAD_BYTES = 50 * 1024 * 1024 * 1024  # 50GB
VALID_EXTENSIONS = ['.las', '.laz']
PRESIGNED_URL_EXPIRATION = 15 * 60  # 15 minutes

# Request/Response Models
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
    parts: list[dict]  # [{"partNumber": int, "url": str}]


class CompleteMultipartRequest(BaseModel):
    key: str
    uploadId: str
    parts: list[dict]  # [{"partNumber": int, "eTag": str}]


class CompleteMultipartResponse(BaseModel):
    location: Optional[str] = None
    etag: Optional[str] = None


class AbortMultipartRequest(BaseModel):
    key: str
    uploadId: str


class AbortMultipartResponse(BaseModel):
    ok: bool


class ListPartsResponse(BaseModel):
    parts: list[dict]  # [{"partNumber": int, "eTag": str, "size": int}]


# Dependency injection
def get_uploader_storage() -> Optional[UploaderStorageClient]:
    """Get Uploader Storage client instance from connection manager.
    
    Uses uploader credentials (write access to raw bucket) for presigned URL generation.
    """
    from trees_api.connection_manager import connection_manager
    return connection_manager.get_uploader_storage_client()


# Endpoints
@router.options("/create")
@router.options("/presign")
@router.options("/complete")
@router.options("/abort")
@router.options("/list")
async def handle_options(response: Response):
    """Handle CORS preflight requests."""
    return Response(status_code=204)


@router.post("/create", response_model=CreateMultipartResponse)
async def create_multipart_upload(
    request: CreateMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage)
):
    """
    Initialize S3 multipart upload.
    
    This endpoint creates a new multipart upload session and returns
    the S3 key and upload ID needed for subsequent part uploads.
    """
    if not storage:
        raise HTTPException(
            status_code=503, 
            detail="Storage service is unavailable. Please check /health for details."
        )
    
    logger.info(f"Creating multipart upload for dataset {request.datasetId}, item {request.datasetItemId}")
    
    # Validate file extension
    file_ext = request.fileName.lower()[request.fileName.rfind('.'):]
    if file_ext not in VALID_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid file type. Only {', '.join(VALID_EXTENSIONS)} files are allowed."
        )
    
    # Validate file size
    if request.fileSize > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size is {MAX_UPLOAD_BYTES} bytes."
        )
    
    # Generate S3 key: RAW/{datasetId}/{datasetItemId}/raw{ext}
    key = f"RAW/{request.datasetId}/{request.datasetItemId}/raw{file_ext}"
    logger.info(f"Generated S3 key: {key}")
    
    try:
        # Create multipart upload
        response = storage.client.create_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=key,
            ContentType=request.contentType or 'application/octet-stream'
        )
        
        upload_id = response.get('UploadId')
        if not upload_id:
            raise HTTPException(
                status_code=500,
                detail="Failed to create multipart upload - no upload ID returned"
            )
        
        logger.info(f"Created multipart upload with ID: {upload_id}")
        return CreateMultipartResponse(key=key, uploadId=upload_id)
        
    except ClientError as e:
        logger.error(f"S3 ClientError during create_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create multipart upload: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during create_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(e)}"
        )


@router.post("/presign", response_model=PresignPartsResponse)
async def presign_parts(
    request: PresignPartsRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage)
):
    """
    Generate presigned URLs for uploading parts.
    
    This endpoint generates presigned URLs that allow the client to upload
    file parts directly to S3 without going through the API server.
    """
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details."
        )
    
    if not request.partNumbers:
        raise HTTPException(
            status_code=400,
            detail="partNumbers array cannot be empty"
        )
    
    logger.info(f"Generating presigned URLs for {len(request.partNumbers)} parts")
    
    try:
        parts = []
        for part_number in request.partNumbers:
            # Generate presigned URL for upload_part
            url = storage.client.generate_presigned_url(
                'upload_part',
                Params={
                    'Bucket': storage.bucket_name_raw,
                    'Key': request.key,
                    'UploadId': request.uploadId,
                    'PartNumber': part_number,
                },
                ExpiresIn=PRESIGNED_URL_EXPIRATION
            )
            parts.append({"partNumber": part_number, "url": url})
        
        logger.info(f"Generated {len(parts)} presigned URLs")
        return PresignPartsResponse(parts=parts)
        
    except ClientError as e:
        logger.error(f"S3 ClientError during presign_parts: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate presigned URLs: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during presign_parts: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(e)}"
        )


@router.post("/complete", response_model=CompleteMultipartResponse)
async def complete_multipart_upload(
    request: CompleteMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage)
):
    """
    Complete S3 multipart upload.
    
    This endpoint finalizes the multipart upload by combining all uploaded
    parts into a single S3 object.
    """
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details."
        )
    
    if not request.parts:
        raise HTTPException(
            status_code=400,
            detail="parts array cannot be empty"
        )
    
    logger.info(f"Completing multipart upload for key: {request.key}")
    
    try:
        # Sort parts by part number and format for S3 API
        sorted_parts = sorted(request.parts, key=lambda p: p['partNumber'])
        multipart_upload = {
            'Parts': [
                {
                    'ETag': part['eTag'],
                    'PartNumber': part['partNumber']
                }
                for part in sorted_parts
            ]
        }
        
        # Complete the multipart upload
        response = storage.client.complete_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=request.key,
            UploadId=request.uploadId,
            MultipartUpload=multipart_upload
        )
        
        logger.info(f"Successfully completed multipart upload for key: {request.key}")
        
        # Optional: verify object exists with head_object
        try:
            storage.client.head_object(
                Bucket=storage.bucket_name_raw,
                Key=request.key
            )
            logger.info(f"Verified object exists: {request.key}")
        except ClientError as head_err:
            logger.warning(f"HEAD verification failed for {request.key}: {head_err}")
        
        return CompleteMultipartResponse(
            location=response.get('Location'),
            etag=response.get('ETag')
        )
        
    except ClientError as e:
        logger.error(f"S3 ClientError during complete_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to complete multipart upload: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during complete_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(e)}"
        )


@router.post("/abort", response_model=AbortMultipartResponse)
async def abort_multipart_upload(
    request: AbortMultipartRequest,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage)
):
    """
    Abort S3 multipart upload.
    
    This endpoint cancels an in-progress multipart upload and cleans up
    any uploaded parts.
    """
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details."
        )
    
    logger.info(f"Aborting multipart upload for key: {request.key}")
    
    try:
        storage.client.abort_multipart_upload(
            Bucket=storage.bucket_name_raw,
            Key=request.key,
            UploadId=request.uploadId
        )
        
        logger.info(f"Successfully aborted multipart upload for key: {request.key}")
        return AbortMultipartResponse(ok=True)
        
    except ClientError as e:
        logger.error(f"S3 ClientError during abort_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to abort multipart upload: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during abort_multipart_upload: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(e)}"
        )


@router.get("/list", response_model=ListPartsResponse)
async def list_parts(
    key: str,
    uploadId: str,
    storage: Optional[UploaderStorageClient] = Depends(get_uploader_storage)
):
    """
    List uploaded parts for a multipart upload.
    
    This endpoint returns information about parts that have already been
    uploaded, which is useful for implementing upload resume functionality.
    """
    if not storage:
        raise HTTPException(
            status_code=503,
            detail="Storage service is unavailable. Please check /health for details."
        )
    
    if not key or not uploadId:
        raise HTTPException(
            status_code=400,
            detail="Missing required query params: key, uploadId"
        )
    
    logger.info(f"Listing parts for key: {key}, uploadId: {uploadId}")
    
    try:
        parts = []
        part_number_marker = None
        
        # Paginate through all parts if response is truncated
        while True:
            params = {
                'Bucket': storage.bucket_name_raw,
                'Key': key,
                'UploadId': uploadId,
            }
            if part_number_marker is not None:
                params['PartNumberMarker'] = part_number_marker
            
            response = storage.client.list_parts(**params)
            
            # Collect parts from this page
            if 'Parts' in response:
                for part in response['Parts']:
                    if part.get('PartNumber') and part.get('ETag') and part.get('Size') is not None:
                        parts.append({
                            'partNumber': part['PartNumber'],
                            'eTag': part['ETag'],
                            'size': part['Size']
                        })
            
            # Check if there are more parts to fetch
            if response.get('IsTruncated') and response.get('NextPartNumberMarker'):
                part_number_marker = response['NextPartNumberMarker']
            else:
                break
        
        logger.info(f"Found {len(parts)} uploaded parts")
        return ListPartsResponse(parts=parts)
        
    except ClientError as e:
        logger.error(f"S3 ClientError during list_parts: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list parts: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during list_parts: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error: {str(e)}"
        )

