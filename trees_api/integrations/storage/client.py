import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from trees_api.core.config import StorageConfig

logger = logging.getLogger("trees_api.storage_client")


class StorageClient:
    """S3/MinIO storage client for 3DTrees API."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.access_key = config.processor_access_key
        self.secret_key = config.processor_secret_key
        self.bucket_name = config.bucket_name
        self.bucket_name_products = config.bucket_name_products
        self.bucket_name_raw = config.bucket_name_raw
        self.bucket_name_download = config.bucket_name_download
        self.url = config.url
        self.region = config.region
        self.endpoint = config.url
        self.bucket_products = config.bucket_name_products
        self.client: Optional[boto3.client] = None

    def connect(self) -> bool:
        try:
            logger.debug("Connecting to storage service at %s (processor credentials)", self.url)
            self.client = boto3.client(
                "s3",
                endpoint_url=self.url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
            self.client.head_bucket(Bucket=self.bucket_name_raw)
            logger.info(
                "Successfully connected to storage service at %s (processor, bucket: %s)",
                self.url,
                self.bucket_name_raw,
            )
            return True
        except NoCredentialsError as error:
            logger.error("Storage credentials not found or invalid")
            raise RuntimeError(f"Storage credentials error: {error}") from error
        except ClientError as error:
            if error.response["Error"]["Code"] == "403":
                logger.error("Access denied to storage service")
                raise RuntimeError("Access denied to storage service") from error
            logger.error("Storage connection error: %s", error)
            raise RuntimeError(f"Storage connection error: {error}") from error
        except Exception as error:
            logger.error("Unexpected error connecting to storage: %s", error)
            raise RuntimeError(f"Unexpected error connecting to storage: {error}") from error

    def download_file(self, key: str, file_path: Path, bucket: Optional[str] = None):
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_raw
        try:
            self.client.download_file(bucket_name, key, str(file_path))
        except ClientError as error:
            logger.error("Failed to download file '%s': %s", key, error)
            raise RuntimeError(f"Failed to download file '{key}': {error}") from error

    def upload_file(self, file_path: Path, key: str, bucket: Optional[str] = None):
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.upload_file(str(file_path), bucket_name, key)
        except ClientError as error:
            logger.error("Failed to upload file '%s': %s", key, error)
            raise RuntimeError(f"Failed to upload file '{key}': {error}") from error

    def delete_object(self, key: str, bucket: Optional[str] = None) -> None:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.delete_object(Bucket=bucket_name, Key=key)
        except ClientError as error:
            logger.error("Failed to delete object '%s': %s", key, error)
            raise RuntimeError(f"Failed to delete object '{key}': {error}") from error

    def file_exists(self, key: str, bucket: Optional[str] = None) -> bool:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.head_object(Bucket=bucket_name, Key=key)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                return False
            raise

    def generate_presigned_download_url(
        self,
        key: str,
        expires_in: int = 7 * 24 * 60 * 60,
        bucket: Optional[str] = None,
    ) -> str:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_download
        try:
            return self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": key},
                ExpiresIn=expires_in,
            )
        except ClientError as error:
            logger.error("Failed to generate presigned URL for '%s': %s", key, error)
            raise RuntimeError(f"Failed to generate presigned URL for '{key}': {error}") from error

    def download_json(self, key: str, bucket: Optional[str] = None) -> dict:
        import json
        import tempfile

        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        with tempfile.NamedTemporaryFile(suffix=".json", delete=True) as temp:
            self.download_file(key, Path(temp.name), bucket=bucket_name)
            with open(temp.name, "r") as file:
                return json.load(file)

    def download_jsonl_full(
        self, key: str, bucket: Optional[str] = None
    ) -> tuple[list, Optional[dict], Optional[dict]]:
        """
        Download and parse a JSONL file from storage.
        Returns (logs, raw_record, standard_record) for tool_standard metadata format.
        """
        import json

        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=True) as temp:
            self.download_file(key, Path(temp.name), bucket=bucket_name)
            logs = []
            raw_record = None
            standard_record = None
            with open(temp.name, "r") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    if "standardized" in entry:
                        if entry.get("standardized") is True:
                            standard_record = entry
                        else:
                            raw_record = entry
                    elif "bbox" in entry or "point_count" in entry:
                        if entry.get("standardized") is True:
                            standard_record = entry
                        else:
                            raw_record = entry
                    else:
                        logs.append(entry)
            return (logs, raw_record, standard_record)

    def rename_object(self, old_key: str, new_key: str, bucket: Optional[str] = None) -> bool:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        bucket_name = bucket or self.bucket_name_products
        try:
            self.client.copy_object(
                CopySource={"Bucket": bucket_name, "Key": old_key},
                Bucket=bucket_name,
                Key=new_key,
            )
            self.client.delete_object(Bucket=bucket_name, Key=old_key)
            logger.info("Renamed %s -> %s in %s", old_key, new_key, bucket_name)
            return True
        except ClientError as error:
            logger.error("Failed to rename %s -> %s: %s", old_key, new_key, error)
            return False


class UploaderStorageClient:
    """S3/MinIO storage client for uploads."""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.access_key = config.uploader_access_key
        self.secret_key = config.uploader_secret_key
        self.bucket_name_raw = config.bucket_name_raw
        self.url = config.url
        self.region = config.region
        self.client: Optional[boto3.client] = None

    def connect(self) -> bool:
        try:
            logger.debug("Connecting to storage service at %s (uploader credentials)", self.url)
            self.client = boto3.client(
                "s3",
                endpoint_url=self.url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
            )
            logger.info(
                "Successfully connected to storage service at %s (uploader, bucket: %s)",
                self.url,
                self.bucket_name_raw,
            )
            return True
        except Exception as error:
            logger.error("Error connecting to storage (uploader): %s", error)
            raise RuntimeError(f"Storage connection error: {error}") from error

    def create_multipart_upload(self, key: str, content_type: str = "application/octet-stream") -> str:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        response = self.client.create_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            ContentType=content_type,
        )
        return response["UploadId"]

    def generate_presigned_url_for_part(self, key: str, upload_id: str, part_number: int, expires_in: int = 900) -> str:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        return self.client.generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": self.bucket_name_raw,
                "Key": key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=expires_in,
        )

    def complete_multipart_upload(self, key: str, upload_id: str, parts: list) -> dict:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        return self.client.complete_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )

    def abort_multipart_upload(self, key: str, upload_id: str) -> None:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        self.client.abort_multipart_upload(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id,
        )

    def list_parts(self, key: str, upload_id: str) -> list:
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        response = self.client.list_parts(
            Bucket=self.bucket_name_raw,
            Key=key,
            UploadId=upload_id,
        )
        return response.get("Parts", [])


__all__ = ["StorageClient", "UploaderStorageClient"]

