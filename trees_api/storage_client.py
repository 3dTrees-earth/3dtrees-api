import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from trees_api.config import StorageConfig

logger = logging.getLogger("trees_api.storage_client")


class StorageClient:
    """S3/MinIO storage client for 3DTrees API.
    
    Uses PROCESSOR credentials (read raw, write products).
    """
    
    def __init__(self, config: StorageConfig):
        """
        Initialize Storage client with configuration.
        
        Uses processor credentials for reading from raw and writing to products.
        
        Args:
            config: StorageConfig instance with connection details
        """
        self.config = config
        # Use processor credentials (falls back to legacy if not set)
        self.access_key = config.processor_access_key
        self.secret_key = config.processor_secret_key
        self.bucket_name = config.bucket_name
        self.bucket_name_products = config.bucket_name_products
        self.bucket_name_raw = config.bucket_name_raw
        self.url = config.url
        self.region = config.region
        self.endpoint = config.url  # Alias for consistency with other code
        self.bucket_products = config.bucket_name_products  # Alias for consistency
        
        self.client: Optional[boto3.client] = None

    def connect(self) -> bool:
        try:
            logger.debug(f"Connecting to storage service at {self.url} (processor credentials)")
            
            self.client = boto3.client(
                's3',
                endpoint_url=self.url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            # Test connection by checking access to raw bucket (head_bucket)
            # This is more permissive than list_buckets which may not be allowed
            self.client.head_bucket(Bucket=self.bucket_name_raw)
            
            logger.info(f"Successfully connected to storage service at {self.url} (processor, bucket: {self.bucket_name_raw})")
            return True
            
        except NoCredentialsError as e:
            logger.error("Storage credentials not found or invalid")
            raise RuntimeError(f"Storage credentials error: {e}") from e
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '403':
                logger.error("Access denied to storage service")
                raise RuntimeError("Access denied to storage service") from e
            else:
                logger.error(f"Storage connection error: {e}")
                raise RuntimeError(f"Storage connection error: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error connecting to storage: {e}")
            raise RuntimeError(f"Unexpected error connecting to storage: {e}") from e

    def download_file(self, key: str, file_path: Path, bucket: Optional[str] = None):
        """Download file from storage. Uses bucket_name_raw (input data) by default if bucket not specified."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_raw
        try:
            self.client.download_file(bucket_name, key, str(file_path))
        except ClientError as e:
            logger.error(f"Failed to download file '{key}': {e}")
            raise RuntimeError(f"Failed to download file '{key}': {e}") from e

    def upload_file(self, file_path: Path, key: str, bucket: Optional[str] = None):
        """Upload file to storage. Uses bucket_name_products (output data) by default if bucket not specified."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.upload_file(str(file_path), bucket_name, key)
        except ClientError as e:
            logger.error(f"Failed to upload file '{key}': {e}")
            raise RuntimeError(f"Failed to upload file '{key}': {e}") from e

    def file_exists(self, key: str, bucket: Optional[str] = None) -> bool:
        """Check if a file exists in storage."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def download_json(self, key: str, bucket: Optional[str] = None) -> dict:
        """Download and parse a JSON file from storage."""
        import json
        import tempfile
        
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        
        with tempfile.NamedTemporaryFile(suffix='.json', delete=True) as tmp:
            self.download_file(key, Path(tmp.name), bucket=bucket_name)
            with open(tmp.name, 'r') as f:
                return json.load(f)

    def rename_object(self, old_key: str, new_key: str, bucket: Optional[str] = None) -> bool:
        """
        Rename an object in S3 (copy + delete).
        
        Args:
            old_key: Source key
            new_key: Destination key
            bucket: Bucket name (defaults to products bucket)
            
        Returns:
            True if rename was successful
        """
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        
        try:
            # Copy object to new key
            copy_source = {'Bucket': bucket_name, 'Key': old_key}
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=new_key
            )
            
            # Delete old object
            self.client.delete_object(Bucket=bucket_name, Key=old_key)
            
            logger.info(f"Renamed {old_key} -> {new_key} in {bucket_name}")
            return True
            
        except ClientError as e:
            logger.error(f"Failed to rename {old_key} -> {new_key}: {e}")
            return False


class UploaderStorageClient:
    """S3/MinIO storage client for uploads.
    
    Uses UPLOADER credentials (write to raw bucket for frontend uploads).
    """
    
    def __init__(self, config: StorageConfig):
        """
        Initialize Uploader storage client with configuration.
        
        Uses uploader credentials for writing to raw bucket.
        
        Args:
            config: StorageConfig instance with connection details
        """
        self.config = config
        self.access_key = config.uploader_access_key
        self.secret_key = config.uploader_secret_key
        self.bucket_name_raw = config.bucket_name_raw
        self.url = config.url
        self.region = config.region
        
        self.client: Optional[boto3.client] = None

    def connect(self) -> bool:
        try:
            logger.debug(f"Connecting to storage service at {self.url} (uploader credentials)")
            
            self.client = boto3.client(
                's3',
                endpoint_url=self.url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            # Don't test with HeadBucket - uploader may only have PutObject permission
            # The connection will be validated when actually uploading
            
            logger.info(f"Successfully connected to storage service at {self.url} (uploader, bucket: {self.bucket_name_raw})")
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to storage (uploader): {e}")
            raise RuntimeError(f"Storage connection error: {e}") from e

    def create_multipart_upload(self, key: str, content_type: str = "application/octet-stream") -> str:
        """Create a multipart upload and return the upload ID."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        response = self.client.create_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            ContentType=content_type
        )
        return response['UploadId']

    def generate_presigned_url_for_part(self, key: str, upload_id: str, part_number: int, expires_in: int = 900) -> str:
        """Generate a presigned URL for uploading a part."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        return self.client.generate_presigned_url(
            'upload_part',
            Params={
                'Bucket': self.bucket_name_raw,
                'Key': key,
                'UploadId': upload_id,
                'PartNumber': part_number
            },
            ExpiresIn=expires_in
        )

    def complete_multipart_upload(self, key: str, upload_id: str, parts: list) -> dict:
        """Complete a multipart upload."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        return self.client.complete_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )

    def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        """Abort a multipart upload."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        self.client.abort_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id
        )

    def list_parts(self, key: str, upload_id: str) -> list:
        """List uploaded parts for a multipart upload."""
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        response = self.client.list_parts(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id
        )
        return response.get('Parts', [])
