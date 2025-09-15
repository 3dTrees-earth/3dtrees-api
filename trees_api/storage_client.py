import logging
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("trees_api.storage_client")


class StorageClient(BaseSettings):
    access_key: str
    secret_key: str
    bucket_name: str
    url: str = Field(default="https://storage.googleapis.com")
    region: str = Field(default="eu")

    client: Optional[boto3.client] = Field(default=None, init=False)

    model_config = SettingsConfigDict(
        case_sensitive=False,
        cli_parse_args=True,
        cli_ignore_unknown_args=True,
        env_file=".env",
        env_prefix="STORAGE_",
    )

    def connect(self) -> bool:
        try:
            logger.debug(f"Connecting to storage service at {self.url}")
            
            self.client = boto3.client(
                's3',
                endpoint_url=self.url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
            # Test connection by trying to list buckets
            # This will raise an exception if credentials are wrong
            self.client.list_buckets()
            
            logger.info(f"Successfully connected to storage service at {self.url}")
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

    def download_file(self, key: str, file_path: Path):
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        try:
            self.client.download_file(self.bucket_name, key, str(file_path))
        except ClientError as e:
            logger.error("Failed to download file '{key}': {e}")
            raise RuntimeError(f"Failed to download file '{key}': {e}") from e

    def upload_file(self, file_path: Path, key: str):
        if not self.client:
            raise RuntimeError("Not connected to storage service. Call connect() first.")
        try:
            self.client.upload_file(str(file_path), self.bucket_name, key)
        except ClientError as e:
            logger.error("Failed to upload file '{key}': {e}")
            raise RuntimeError(f"Failed to upload file '{key}': {e}") from e